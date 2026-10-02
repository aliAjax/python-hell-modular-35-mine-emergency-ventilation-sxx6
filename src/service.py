import copy
import hashlib
from datetime import datetime
from uuid import uuid4

from .audit import AuditTrail
from .domain import BatchRejectedError, ConflictError, NotFoundError, ValidationError
from .rules import RuleEngine, area_restore_blockers, compute_sensor_severity


MOVE_TYPES = ("refuge_enter", "refuge_leave")
SUPPORTED_PAYLOAD_TYPES = MOVE_TYPES + ("gas", "ventilation_restore", "incident_close")
REQUIRED_TARGET_FIELDS = {
    "gas": (),
    "refuge_enter": ("refuge_id", "worker_id"),
    "refuge_leave": ("refuge_id", "worker_id"),
    "ventilation_restore": ("ventilation_id",),
    "incident_close": ("incident_id", "summary"),
}


def _record_identity(raw):
    source_id = str(raw.get("source_id", "")).strip()
    record_id = str(raw.get("record_id", "")).strip()
    digest = hashlib.sha256((source_id + "\0" + record_id).encode("utf-8")).hexdigest()[:32]
    return source_id, record_id, "offline-" + digest


def _recorded_at_key(record):
    return datetime.fromisoformat(str(record["recorded_at"]).replace("Z", "+00:00"))


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
        """Atomically merge a field batch and apply its effects.

        Records are identified by (source_id, record_id): already merged records are
        idempotently skipped. New records are replayed in recorded_at order against the
        current central state. Any record that conflicts with central state or would
        overflow a refuge rejects the ENTIRE batch; the rejection lists every offending
        record so the field can fix and retry the same batch unchanged.
        """
        if not isinstance(records, list):
            raise ValidationError("records must be a list")
        if not records:
            return []

        prepared = self._prepare_batch(actor, records)
        with self.repository.transaction() as connection:
            rejected = self._collect_conflicts(actor, prepared, connection)
            if rejected:
                raise BatchRejectedError(rejected)
            result_by_identity = self._apply_batch(actor, prepared, connection)
        return [result_by_identity[item["identity"]] for item in prepared]

    def _prepare_batch(self, actor, records):
        prepared = []
        seen = set()
        for index, raw in enumerate(records):
            if not isinstance(raw, dict):
                raise ValidationError("each offline record must be an object")
            source_id, record_id, identity = _record_identity(raw)
            if not source_id or not record_id:
                raise ValidationError("source_id and record_id are required")
            if identity in seen:
                raise ValidationError(
                    "duplicate record in batch: %s/%s" % (source_id, record_id)
                )
            seen.add(identity)
            payload = dict(raw)
            # Envelope validation: role, payload object and recorded_at format.
            self.rules.validate_create(actor, "offline_record", payload, self._lookup)
            record_type = payload.get("payload", {}).get("type")
            if record_type not in SUPPORTED_PAYLOAD_TYPES:
                raise ValidationError(
                    "record %s/%s has unsupported payload type: %s"
                    % (source_id, record_id, record_type)
                )
            body = payload["payload"]
            for field in REQUIRED_TARGET_FIELDS[record_type]:
                value = body.get(field)
                if value is None or (isinstance(value, str) and not value.strip()):
                    raise ValidationError(
                        "record %s/%s missing required field: payload.%s"
                        % (source_id, record_id, field)
                    )
            if record_type == "gas":
                if body.get("gas_ppm") is not None:
                    gas = self._numeric(body.get("gas_ppm"), "gas_ppm")
                    if gas < 0:
                        raise ValidationError("gas_ppm must not be negative")
                if body.get("threshold_ppm") is not None:
                    if self._numeric(body.get("threshold_ppm"), "threshold_ppm") <= 0:
                        raise ValidationError("threshold_ppm must be positive")
            prepared.append({
                "index": index,
                "raw": payload,
                "source_id": source_id,
                "record_id": record_id,
                "identity": identity,
                "type": record_type,
                "body": body,
                "recorded_at": payload["recorded_at"],
                "sort_key": _recorded_at_key(payload),
            })
        return prepared

    @staticmethod
    def _numeric(value, field):
        try:
            return float(value)
        except (TypeError, ValueError):
            raise ValidationError(field + " must be numeric")

    def _load_snapshot(self, connection):
        state = {}
        for kind in ("worker", "refuge", "sensor", "ventilation", "incident", "offline_record"):
            for entity in self.repository.list_entities(kind=kind, connection=connection):
                state[entity["id"]] = copy.deepcopy(entity)

        def lookup(kind, field, value):
            kind = self.rules.normalize_kind(kind)
            rows = [e for e in state.values() if e["kind"] == kind]
            if field == "*":
                return rows
            if field == "id":
                return [e for e in rows if e["id"] == value]
            return [e for e in rows if e["data"].get(field) == value]

        return state, lookup

    def _historical_moves(self, state):
        """All previously merged refuge moves, in chronological order."""
        moves = []
        for entity in state.values():
            if entity["kind"] != "offline_record":
                continue
            body = entity["data"].get("payload", {})
            if body.get("type") in MOVE_TYPES:
                moves.append({
                    "type": body["type"],
                    "refuge_id": body.get("refuge_id"),
                    "worker_id": body.get("worker_id"),
                    "recorded_at": entity["data"].get("recorded_at"),
                    "identity": entity["id"],
                })
        moves.sort(key=lambda m: (_recorded_at_key(m), m["identity"]))
        return moves

    @staticmethod
    def _replay_moves(refuge_moves):
        occupants = []
        for move in refuge_moves:
            worker_id = move["worker_id"]
            if move["type"] == "refuge_enter":
                if worker_id not in occupants:
                    occupants.append(worker_id)
            elif worker_id in occupants:
                occupants.remove(worker_id)
        return occupants

    def _collect_conflicts(self, actor, prepared, connection):
        state, lookup = self._load_snapshot(connection)
        new_items = [item for item in prepared if item["identity"] not in state]
        new_items.sort(key=lambda item: (item["sort_key"], item["index"]))

        history = self._historical_moves(state)
        history_by_refuge = {}
        for move in history:
            history_by_refuge.setdefault(move["refuge_id"], []).append(move)

        rejected = []

        def reject(item, reason, message, **extra):
            entry = {
                "index": item["index"],
                "source_id": item["source_id"],
                "record_id": item["record_id"],
                "type": item["type"],
                "reason": reason,
                "message": message,
            }
            entry.update(extra)
            rejected.append(entry)

        running_moves = {}
        for item in new_items:
            body = item["body"]
            record_type = item["type"]

            if record_type == "gas":
                sensor_id = body.get("sensor_id")
                if not sensor_id:
                    # A reading without a target sensor is archived only.
                    continue
                sensor = state.get(sensor_id)
                if not sensor or sensor["kind"] != "sensor":
                    reject(item, "sensor_not_found", "sensor does not exist: " + sensor_id)
                elif sensor["status"] == "faulty":
                    reject(
                        item,
                        "sensor_marked_faulty",
                        "sensor %s was marked faulty at the centre" % sensor["id"],
                        current_status=sensor["status"],
                    )
                else:
                    self._apply_gas(sensor, body)

            elif record_type in MOVE_TYPES:
                refuge = state.get(body["refuge_id"])
                worker = state.get(body["worker_id"])
                if not refuge or refuge["kind"] != "refuge":
                    reject(item, "refuge_not_found", "refuge does not exist: " + body["refuge_id"])
                    continue
                if not worker or worker["kind"] != "worker":
                    reject(item, "worker_not_found", "worker does not exist: " + body["worker_id"])
                    continue
                if refuge["status"] == "maintenance":
                    reject(
                        item,
                        "refuge_unavailable",
                        "refuge %s is under maintenance at the centre" % refuge["id"],
                        current_status=refuge["status"],
                    )
                    continue
                occupants = running_moves.get(refuge["id"])
                if occupants is None:
                    occupants = self._replay_moves(history_by_refuge.get(refuge["id"], []))
                    running_moves[refuge["id"]] = occupants
                    refuge["data"]["occupants"] = list(occupants)
                if record_type == "refuge_enter":
                    if worker["status"] in ("evacuated", "rescued", "inactive"):
                        reject(
                            item,
                            "worker_already_withdrawn",
                            "worker %s is already %s at the centre" % (worker["id"], worker["status"]),
                            current_status=worker["status"],
                        )
                        continue
                    if worker["id"] in occupants:
                        reject(
                            item,
                            "worker_already_inside",
                            "worker %s is already inside refuge %s" % (worker["id"], refuge["id"]),
                        )
                        continue
                    capacity = int(refuge["data"].get("capacity", 0))
                    if len(occupants) + 1 > capacity:
                        reject(
                            item,
                            "refuge_capacity_exceeded",
                            "refuge %s capacity is %d, already %d inside"
                            % (refuge["id"], capacity, len(occupants)),
                            refuge_id=refuge["id"],
                            capacity=capacity,
                            occupancy=len(occupants),
                        )
                        continue
                    occupants.append(worker["id"])
                    refuge["data"]["occupants"] = list(occupants)
                else:
                    if worker["id"] not in occupants:
                        reject(
                            item,
                            "worker_not_inside",
                            "worker %s is not inside refuge %s" % (worker["id"], refuge["id"]),
                            refuge_id=refuge["id"],
                        )
                        continue
                    occupants.remove(worker["id"])
                    refuge["data"]["occupants"] = list(occupants)

            elif record_type == "ventilation_restore":
                vent = state.get(body["ventilation_id"])
                if not vent or vent["kind"] != "ventilation":
                    reject(
                        item,
                        "ventilation_not_found",
                        "ventilation does not exist: " + body["ventilation_id"],
                    )
                elif vent["status"] != "running":
                    blockers = area_restore_blockers(lookup, vent["data"].get("area_code"))
                    if blockers:
                        reject(
                            item,
                            "area_not_clear",
                            "ventilation %s area %s is not clear: %s"
                            % (vent["id"], vent["data"].get("area_code"), blockers),
                            ventilation_id=vent["id"],
                            area_code=vent["data"].get("area_code"),
                            blockers=blockers,
                        )
                    else:
                        vent["status"] = "running"

            elif record_type == "incident_close":
                incident = state.get(body["incident_id"])
                if not incident or incident["kind"] != "incident":
                    reject(
                        item,
                        "incident_not_found",
                        "incident does not exist: " + body["incident_id"],
                    )
                elif incident["status"] != "closed":
                    try:
                        self.rules.CUSTOM_TRANSITIONS[("incident", "close")](
                            actor, incident, {"summary": body["summary"]}, lookup
                        )
                    except ConflictError as exc:
                        reject(
                            item,
                            "incident_not_ready",
                            str(exc),
                            incident_id=incident["id"],
                            current_status=incident["status"],
                        )
                    else:
                        incident["status"] = "closed"

        rejected.sort(key=lambda entry: entry["index"])
        return rejected

    @staticmethod
    def _apply_gas(sensor, body):
        sensor["data"]["gas_ppm"] = float(body["gas_ppm"])
        if body.get("threshold_ppm") is not None:
            sensor["data"]["threshold_ppm"] = float(body["threshold_ppm"])
        severity = compute_sensor_severity(
            sensor["data"]["gas_ppm"], sensor["data"].get("threshold_ppm", 1)
        )
        sensor["data"]["severity"] = severity
        sensor["status"] = severity

    def _apply_batch(self, actor, prepared, connection):
        # Reload a clean snapshot: nothing was written while collecting conflicts.
        state, _ = self._load_snapshot(connection)
        new_items = [item for item in prepared if item["identity"] not in state]
        new_items.sort(key=lambda item: (item["sort_key"], item["index"]))

        affected_refuges = set()
        for item in new_items:
            body = item["body"]
            record_type = item["type"]

            offline_entity = self.repository.create_entity(
                item["identity"],
                "offline_record",
                self.rules.initial_status("offline_record", item["raw"]),
                item["raw"],
                actor.user_id,
                connection=connection,
            )
            state[item["identity"]] = offline_entity
            self.audit.record(
                item["identity"],
                actor,
                "merge_offline",
                None,
                offline_entity["status"],
                {"source_id": item["source_id"], "record_id": item["record_id"], "type": record_type},
                connection=connection,
            )

            if record_type == "gas":
                sensor_id = body.get("sensor_id")
                if not sensor_id:
                    continue
                sensor = state[sensor_id]
                from_status = sensor["status"]
                self._apply_gas(sensor, body)
                self._persist(actor, sensor, from_status, "offline_gas", item, connection)

            elif record_type in MOVE_TYPES:
                affected_refuges.add(body["refuge_id"])

            elif record_type == "ventilation_restore":
                vent = state[body["ventilation_id"]]
                if vent["status"] != "running":
                    from_status = vent["status"]
                    vent["status"] = "running"
                    vent["data"]["restored_by"] = actor.user_id
                    if body.get("tested_at"):
                        vent["data"]["tested_at"] = body["tested_at"]
                    self._persist(actor, vent, from_status, "offline_restore", item, connection)

            elif record_type == "incident_close":
                incident = state[body["incident_id"]]
                if incident["status"] != "closed":
                    from_status = incident["status"]
                    incident["status"] = "closed"
                    incident["data"]["closed_by"] = actor.user_id
                    incident["data"]["summary"] = body["summary"]
                    self._persist(actor, incident, from_status, "offline_close", item, connection)

        # Recalculate refuge occupancy from the full move timeline (old + new).
        history = self._historical_moves(state)
        history_by_refuge = {}
        for move in history:
            history_by_refuge.setdefault(move["refuge_id"], []).append(move)
        for refuge_id in affected_refuges:
            refuge = state.get(refuge_id)
            if not refuge or refuge["kind"] != "refuge":
                continue
            occupants = self._replay_moves(history_by_refuge.get(refuge_id, []))
            from_status = refuge["status"]
            refuge["data"]["occupants"] = occupants
            if refuge["status"] != "maintenance":
                refuge["status"] = "occupied" if occupants else "available"
            updated = self.repository.update_entity(
                refuge_id, refuge["version"], refuge["status"], refuge["data"], connection=connection
            )
            refuge["version"] = updated["version"]
            if from_status != refuge["status"]:
                self.audit.record(
                    refuge_id, actor, "offline_recompute", from_status, refuge["status"],
                    {"occupants": occupants, "sources": [item["source_id"] for item in new_items
                                                         if item["body"].get("refuge_id") == refuge_id]},
                    connection=connection,
                )

        return {
            item["identity"]: self.repository.get_entity(item["identity"], connection=connection)
            for item in prepared
        }

    def _persist(self, actor, entity, from_status, action, item, connection):
        updated = self.repository.update_entity(
            entity["id"], entity["version"], entity["status"], entity["data"], connection=connection
        )
        entity["version"] = updated["version"]
        self.audit.record(
            entity["id"], actor, action, from_status, entity["status"],
            {"source_id": item["source_id"], "record_id": item["record_id"]},
            connection=connection,
        )

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
