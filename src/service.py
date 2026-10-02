import hashlib
from uuid import uuid4

from .audit import AuditTrail
from .domain import ConflictError, NotFoundError, OfflineMergeError, PermissionDenied, ValidationError
from .rules import RuleEngine, severity_for


class DomainService:
    def __init__(self, repository, rules=None):
        self.repository = repository
        self.rules = rules or RuleEngine()
        self.audit = AuditTrail(repository)

    def _lookup(self, kind, field, value):
        return self.repository.find_entities(self.rules.normalize_kind(kind), field, value)

    def health(self):
        return {"status": "ok" if self.repository.ping() else "error"}

    def create(self, actor, kind, data, idempotency_key=None):
        kind = self.rules.normalize_kind(kind)
        payload = dict(data or {})
        if idempotency_key:
            existing = self.repository.get_idempotency(actor.user_id, idempotency_key)
            if existing:
                entity = self.repository.get_entity(existing)
                if entity:
                    return entity
        self.rules.validate_create(actor, kind, payload, self._lookup)
        entity_id = str(payload.pop("id", "") or uuid4())
        if self.repository.get_entity(entity_id):
            raise ConflictError("entity already exists: " + entity_id)
        status = self.rules.initial_status(kind, payload)
        entity = self.repository.create_entity(entity_id, kind, status, payload, actor.user_id)
        self.audit.record(entity_id, actor, "create", None, status, {"kind": kind})
        if idempotency_key:
            self.repository.save_idempotency(actor.user_id, idempotency_key, entity_id)
        return entity

    def transition(self, actor, entity_id, action, data=None, expected_version=None):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        expected = int(expected_version) if expected_version is not None else entity["version"]
        next_status, patch = self.rules.validate_transition(
            actor, entity, action, dict(data or {}), self._lookup
        )
        merged = dict(entity["data"])
        merged.update(patch)
        updated = self.repository.update_entity(entity_id, expected, next_status, merged)
        self.audit.record(
            entity_id,
            actor,
            action,
            entity["status"],
            updated["status"],
            {"patch": patch},
        )
        return updated

    def merge_offline(self, actor, records):
        """Merge field records atomically.

        The whole batch is planned against the current center state first. Any
        record that conflicts with center state, or any batch that would push a
        refuge over capacity, rejects the entire batch so nothing is written.
        Already-merged records (same source_id + record_id) are idempotent.
        """
        if not isinstance(records, list):
            raise ValidationError("records must be a list")
        workers = {w["id"]: w for w in self.repository.list_entities(kind="worker")}
        sensors = {s["id"]: s for s in self.repository.list_entities(kind="sensor")}
        refuges = {r["id"]: r for r in self.repository.list_entities(kind="refuge")}

        plan = []
        conflicts = []
        seen_digests = set()
        for raw in records:
            if not isinstance(raw, dict):
                raise ValidationError("each offline record must be an object")
            source_id = str(raw.get("source_id", "")).strip()
            record_id = str(raw.get("record_id", "")).strip()
            if not source_id or not record_id:
                raise ValidationError("source_id and record_id are required")
            digest = hashlib.sha256((source_id + "\0" + record_id).encode("utf-8")).hexdigest()[:32]
            entity_id = "offline-" + digest
            if digest in seen_digests:
                raise ValidationError("duplicate record in batch: " + source_id + "/" + record_id)
            seen_digests.add(digest)
            existing = self.repository.get_entity(entity_id)
            if existing:
                plan.append({"raw": raw, "entity_id": entity_id, "existing": existing, "skip": True})
                continue
            payload = raw.get("payload")
            if not isinstance(payload, dict):
                raise ValidationError("offline payload must be an object")
            # Validate the envelope (role + recorded_at) before planning domain effects.
            self.rules.validate_create(actor, "offline_record", dict(raw), self._lookup)
            entry = {
                "raw": raw,
                "entity_id": entity_id,
                "source_id": source_id,
                "record_id": record_id,
                "payload": payload,
                "type": payload.get("type"),
                "skip": False,
                "ops": [],
                "actor_id": actor.user_id,
                "actor_role": actor.role,
            }
            rec_type = payload.get("type")
            if rec_type == "refuge_pass":
                self._plan_refuge_pass(entry, workers, refuges, conflicts)
            elif rec_type == "gas_reading":
                self._plan_gas_reading(entry, sensors, conflicts)
            # Unknown payload types are stored for traceability but have no domain effect.
            plan.append(entry)

        if conflicts:
            raise OfflineMergeError(
                "offline batch conflicts with current center state", conflicts=conflicts
            )
        capacity_issue = self._check_capacity(workers, refuges)
        if capacity_issue:
            raise OfflineMergeError(
                "offline batch exceeds refuge capacity", rejected_capacity=capacity_issue
            )

        ops = []
        affected_refuges = set()
        for entry in plan:
            if entry.get("skip"):
                continue
            raw = entry["raw"]
            entity_id = entry["entity_id"]
            payload = dict(raw)
            ops.append(
                {
                    "type": "create",
                    "id": entity_id,
                    "kind": "offline_record",
                    "status": self.rules.initial_status("offline_record", payload),
                    "data": payload,
                    "actor_id": actor.user_id,
                }
            )
            ops.append(
                {
                    "type": "audit",
                    "entity_id": entity_id,
                    "actor_id": actor.user_id,
                    "actor_role": actor.role,
                    "action": "merge_offline",
                    "from_status": None,
                    "to_status": "merged",
                    "detail": {
                        "source_id": entry["source_id"],
                        "record_id": entry["record_id"],
                    },
                }
            )
            for op in entry["ops"]:
                ops.append(op)
                if op["type"] == "update" and op.get("kind") == "refuge":
                    affected_refuges.add(op["id"])
            if entry["type"] == "refuge_pass":
                affected_refuges.add(entry["payload"].get("refuge_id"))
        for refuge_id in affected_refuges:
            if refuge_id and refuge_id in refuges:
                ops.extend(self._recalc_refuge_ops(refuge_id, workers, refuges, actor))

        self.repository.apply_batch(ops)

        result = []
        for entry in plan:
            if entry.get("skip"):
                result.append(entry["existing"])
            else:
                result.append(self.repository.get_entity(entry["entity_id"]))
        return result

    @staticmethod
    def _conflict(entry, reason):
        return {
            "source_id": entry["source_id"],
            "record_id": entry["record_id"],
            "reason": reason,
        }

    def _plan_refuge_pass(self, entry, workers, refuges, conflicts):
        payload = entry["payload"]
        worker_id = str(payload.get("worker_id", "")).strip()
        refuge_id = str(payload.get("refuge_id", "")).strip()
        direction = str(payload.get("direction", "")).strip()
        worker = workers.get(worker_id)
        refuge = refuges.get(refuge_id)
        if not worker:
            conflicts.append(self._conflict(entry, "worker not found: " + worker_id))
            return
        if worker["status"] in ("evacuated", "rescued", "inactive"):
            conflicts.append(
                self._conflict(
                    entry,
                    "worker %s already out of mine (status=%s)" % (worker_id, worker["status"]),
                )
            )
            return
        if not refuge:
            conflicts.append(self._conflict(entry, "refuge not found: " + refuge_id))
            return
        if refuge["status"] == "maintenance":
            conflicts.append(
                self._conflict(entry, "refuge %s is under maintenance" % refuge_id)
            )
            return
        in_refuge = worker["data"].get("in_refuge")
        if direction == "enter":
            if in_refuge and in_refuge != refuge_id:
                conflicts.append(
                    self._conflict(
                        entry, "worker %s already in refuge %s" % (worker_id, in_refuge)
                    )
                )
                return
            if in_refuge == refuge_id:
                # Already inside this refuge; nothing to change.
                return
            new_data = dict(worker["data"])
            new_data["in_refuge"] = refuge_id
            entry["ops"].append(
                {
                    "type": "update",
                    "id": worker_id,
                    "kind": "worker",
                    "expected_version": worker["version"],
                    "status": worker["status"],
                    "data": new_data,
                }
            )
            entry["ops"].append(
                {
                    "type": "audit",
                    "entity_id": worker_id,
                    "actor_id": entry["actor_id"],
                    "actor_role": entry["actor_role"],
                    "action": "refuge_enter",
                    "from_status": worker["status"],
                    "to_status": worker["status"],
                    "detail": {"refuge_id": refuge_id},
                }
            )
            worker["data"] = new_data
            worker["version"] += 1
        elif direction == "exit":
            if in_refuge != refuge_id:
                conflicts.append(
                    self._conflict(
                        entry, "worker %s is not in refuge %s" % (worker_id, refuge_id)
                    )
                )
                return
            new_data = dict(worker["data"])
            new_data.pop("in_refuge", None)
            entry["ops"].append(
                {
                    "type": "update",
                    "id": worker_id,
                    "kind": "worker",
                    "expected_version": worker["version"],
                    "status": worker["status"],
                    "data": new_data,
                }
            )
            entry["ops"].append(
                {
                    "type": "audit",
                    "entity_id": worker_id,
                    "actor_id": entry["actor_id"],
                    "actor_role": entry["actor_role"],
                    "action": "refuge_exit",
                    "from_status": worker["status"],
                    "to_status": worker["status"],
                    "detail": {"refuge_id": refuge_id},
                }
            )
            worker["data"] = new_data
            worker["version"] += 1
        else:
            conflicts.append(self._conflict(entry, "direction must be enter or exit"))

    def _plan_gas_reading(self, entry, sensors, conflicts):
        payload = entry["payload"]
        sensor_id = str(payload.get("sensor_id", "")).strip()
        sensor = sensors.get(sensor_id)
        if not sensor:
            conflicts.append(self._conflict(entry, "sensor not found: " + sensor_id))
            return
        if sensor["status"] == "faulty":
            conflicts.append(self._conflict(entry, "sensor %s is faulty" % sensor_id))
            return
        try:
            gas_val = float(payload.get("gas_ppm"))
        except (TypeError, ValueError):
            conflicts.append(self._conflict(entry, "gas_ppm must be numeric"))
            return
        threshold = float(sensor["data"].get("threshold_ppm", 0))
        new_severity = severity_for(gas_val, threshold)
        if sensor["status"] == "alarm" and new_severity != "alarm":
            conflicts.append(
                self._conflict(
                    entry,
                    "sensor %s is in alarm; reading (%s) disagrees" % (sensor_id, new_severity),
                )
            )
            return
        new_data = dict(sensor["data"])
        new_data["gas_ppm"] = gas_val
        new_data["severity"] = new_severity
        entry["ops"].append(
            {
                "type": "update",
                "id": sensor_id,
                "kind": "sensor",
                "expected_version": sensor["version"],
                "status": new_severity,
                "data": new_data,
            }
        )
        entry["ops"].append(
            {
                "type": "audit",
                "entity_id": sensor_id,
                "actor_id": entry["actor_id"],
                "actor_role": entry["actor_role"],
                "action": "gas_reading",
                "from_status": sensor["status"],
                "to_status": new_severity,
                "detail": {"gas_ppm": gas_val, "severity": new_severity},
            }
        )
        sensor["data"] = new_data
        sensor["status"] = new_severity
        sensor["version"] += 1

    def _check_capacity(self, workers, refuges):
        """Final occupancy (after the batch) must not exceed any refuge capacity."""
        for refuge_id, refuge in refuges.items():
            capacity = int(refuge["data"].get("capacity", 0))
            occupancy = sum(
                1 for w in workers.values() if w["data"].get("in_refuge") == refuge_id
            )
            if occupancy > capacity:
                return {
                    "refuge_id": refuge_id,
                    "capacity": capacity,
                    "would_be": occupancy,
                }
        return None

    def _recalc_refuge_ops(self, refuge_id, workers, refuges, actor):
        refuge = refuges[refuge_id]
        occupancy = sum(
            1 for w in workers.values() if w["data"].get("in_refuge") == refuge_id
        )
        new_data = dict(refuge["data"])
        new_data["occupancy"] = occupancy
        new_status = "occupied" if occupancy > 0 else "available"
        ops = [
            {
                "type": "update",
                "id": refuge_id,
                "kind": "refuge",
                "expected_version": refuge["version"],
                "status": new_status,
                "data": new_data,
            },
            {
                "type": "audit",
                "entity_id": refuge_id,
                "actor_id": actor.user_id,
                "actor_role": actor.role,
                "action": "recalc_occupancy",
                "from_status": refuge["status"],
                "to_status": new_status,
                "detail": {"occupancy": occupancy},
            },
        ]
        refuge["data"] = new_data
        refuge["status"] = new_status
        refuge["version"] += 1
        return ops

    def get(self, entity_id):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        return entity

    def list(self, kind=None, status=None):
        if kind:
            kind = self.rules.normalize_kind(kind)
        return self.repository.list_entities(kind=kind, status=status)

    def audit_log(self, entity_id=None):
        return self.repository.list_audit(entity_id=entity_id)
