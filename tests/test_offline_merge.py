import tempfile
import unittest
from pathlib import Path

from src.domain import Actor, BatchRejectedError, ConflictError, ValidationError
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


def rec(record_id, payload, source="field-a", recorded_at="2026-09-27T10:00:00Z"):
    return {"source_id": source, "record_id": record_id, "recorded_at": recorded_at,
            "payload": payload}


class OfflineMergeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = DomainService(SQLiteRepository(Path(self.tmp.name) / "test.db"), RuleEngine())
        self.admin = Actor("admin", "admin")
        self.field = Actor("field-1", "field")

    def tearDown(self):
        self.tmp.cleanup()

    def _mine(self, area="M-01"):
        incident = self.service.create(
            self.admin, "incident", {"area_code": area, "severity": "high", "summary": "gas"}
        )
        refuge = self.service.create(
            self.admin, "refuge", {"location_code": area, "capacity": 2}
        )
        sensor = self.service.create(
            self.admin, "sensor", {"location_code": area, "gas_ppm": 5, "threshold_ppm": 10}
        )
        vent = self.service.create(
            self.admin, "ventilation", {"name": "fan", "area_code": area, "capacity": 100}
        )
        workers = [
            self.service.create(self.admin, "worker",
                                {"name": "Worker %d" % i, "location_code": area, "team": "A"})
            for i in (1, 2, 3)
        ]
        return incident, refuge, sensor, vent, workers

    # --- gas readings -------------------------------------------------------

    def test_gas_record_updates_sensor_and_status(self):
        _, _, sensor, _, _ = self._mine()
        self.service.merge_offline(self.field, [
            rec("g1", {"type": "gas", "sensor_id": sensor["id"], "gas_ppm": 20},
                recorded_at="2026-09-27T10:00:00Z")
        ])
        updated = self.service.get(sensor["id"])
        self.assertEqual(updated["status"], "alarm")
        self.assertEqual(updated["data"]["gas_ppm"], 20.0)
        self.assertEqual(updated["data"]["severity"], "alarm")

    def test_gas_records_replay_in_recorded_order_latest_wins(self):
        _, _, sensor, _, _ = self._mine()
        self.service.merge_offline(self.field, [
            rec("g-old", {"type": "gas", "sensor_id": sensor["id"], "gas_ppm": 20},
                recorded_at="2026-09-27T12:00:00Z"),
            rec("g-new", {"type": "gas", "sensor_id": sensor["id"], "gas_ppm": 2},
                recorded_at="2026-09-27T09:00:00Z"),
        ])
        self.assertEqual(self.service.get(sensor["id"])["status"], "alarm")

    def test_gas_record_on_faulty_sensor_rejects_batch(self):
        _, _, sensor, _, _ = self._mine()
        self.service.transition(self.admin, sensor["id"], "mark_faulty", {"reason": "broken"})
        with self.assertRaises(BatchRejectedError) as caught:
            self.service.merge_offline(self.field, [
                rec("g1", {"type": "gas", "sensor_id": sensor["id"], "gas_ppm": 20})
            ])
        entry = caught.exception.rejected[0]
        self.assertEqual(entry["reason"], "sensor_marked_faulty")
        self.assertEqual(entry["record_id"], "g1")
        # Nothing was stored: the record can be retried later.
        self.assertEqual(self.service.list("offline_record"), [])

    def test_gas_record_on_unknown_sensor_lists_conflict(self):
        with self.assertRaises(BatchRejectedError) as caught:
            self.service.merge_offline(self.field, [
                rec("g1", {"type": "gas", "sensor_id": "nope", "gas_ppm": 20})
            ])
        self.assertEqual(caught.exception.rejected[0]["reason"], "sensor_not_found")

    # --- refuge occupancy ---------------------------------------------------

    def test_refuge_enter_records_recompute_occupancy(self):
        _, refuge, _, _, workers = self._mine()
        self.service.merge_offline(self.field, [
            rec("e1", {"type": "refuge_enter", "refuge_id": refuge["id"],
                       "worker_id": workers[0]["id"]}, recorded_at="2026-09-27T09:00:00Z"),
            rec("e2", {"type": "refuge_enter", "refuge_id": refuge["id"],
                       "worker_id": workers[1]["id"]}, recorded_at="2026-09-27T09:05:00Z"),
        ])
        updated = self.service.get(refuge["id"])
        self.assertEqual(updated["status"], "occupied")
        self.assertEqual(set(updated["data"]["occupants"]),
                         {workers[0]["id"], workers[1]["id"]})

    def test_refuge_leave_records_recompute_and_release(self):
        _, refuge, _, _, workers = self._mine()
        self.service.merge_offline(self.field, [
            rec("e1", {"type": "refuge_enter", "refuge_id": refuge["id"],
                       "worker_id": workers[0]["id"]}, recorded_at="2026-09-27T09:00:00Z"),
        ])
        self.service.merge_offline(self.field, [
            rec("l1", {"type": "refuge_leave", "refuge_id": refuge["id"],
                       "worker_id": workers[0]["id"]}, recorded_at="2026-09-27T10:00:00Z"),
        ])
        updated = self.service.get(refuge["id"])
        self.assertEqual(updated["status"], "available")
        self.assertEqual(updated["data"]["occupants"], [])

    def test_capacity_overflow_rejects_entire_batch_and_points_out_records(self):
        _, refuge, _, _, workers = self._mine()
        batch = [
            rec("e1", {"type": "refuge_enter", "refuge_id": refuge["id"],
                       "worker_id": workers[0]["id"]}, recorded_at="2026-09-27T09:00:00Z"),
            rec("e2", {"type": "refuge_enter", "refuge_id": refuge["id"],
                       "worker_id": workers[1]["id"]}, recorded_at="2026-09-27T09:05:00Z"),
            rec("e3", {"type": "refuge_enter", "refuge_id": refuge["id"],
                       "worker_id": workers[2]["id"]}, recorded_at="2026-09-27T09:10:00Z"),
        ]
        with self.assertRaises(BatchRejectedError) as caught:
            self.service.merge_offline(self.field, batch)
        rejected = caught.exception.rejected
        self.assertEqual(len(rejected), 1)
        self.assertEqual(rejected[0]["record_id"], "e3")
        self.assertEqual(rejected[0]["reason"], "refuge_capacity_exceeded")
        self.assertEqual(rejected[0]["capacity"], 2)
        self.assertEqual(rejected[0]["occupancy"], 2)
        # Whole batch rejected: refuge untouched, no records stored.
        refuge_row = self.service.get(refuge["id"])
        self.assertEqual(refuge_row["status"], "available")
        self.assertEqual(refuge_row["data"]["occupants"], [])
        self.assertEqual(self.service.list("offline_record"), [])

    def test_conflict_with_central_worker_status_rejects_with_reason(self):
        _, refuge, _, _, workers = self._mine()
        # Centre evacuated the worker during the outage.
        self.service.transition(self.admin, workers[0]["id"], "mark_missing")
        self.service.transition(self.admin, workers[0]["id"], "locate",
                                {"located_at": "2026-09-27T08:00:00Z"})
        self.service.transition(self.admin, workers[0]["id"], "evacuate")
        with self.assertRaises(BatchRejectedError) as caught:
            self.service.merge_offline(self.field, [
                rec("e1", {"type": "refuge_enter", "refuge_id": refuge["id"],
                           "worker_id": workers[0]["id"]})
            ])
        entry = caught.exception.rejected[0]
        self.assertEqual(entry["reason"], "worker_already_withdrawn")
        self.assertEqual(entry["current_status"], "evacuated")

    def test_maintenance_refuge_rejects_enter(self):
        _, refuge, _, _, workers = self._mine()
        self.service.transition(self.admin, refuge["id"], "maintain")
        with self.assertRaises(BatchRejectedError) as caught:
            self.service.merge_offline(self.field, [
                rec("e1", {"type": "refuge_enter", "refuge_id": refuge["id"],
                           "worker_id": workers[0]["id"]})
            ])
        self.assertEqual(caught.exception.rejected[0]["reason"], "refuge_unavailable")

    def test_duplicate_enter_and_orphan_leave_are_conflicts(self):
        _, refuge, _, _, workers = self._mine()
        with self.assertRaises(BatchRejectedError) as caught:
            self.service.merge_offline(self.field, [
                rec("e1", {"type": "refuge_enter", "refuge_id": refuge["id"],
                           "worker_id": workers[0]["id"]}, recorded_at="2026-09-27T09:00:00Z"),
                rec("e1b", {"type": "refuge_enter", "refuge_id": refuge["id"],
                            "worker_id": workers[0]["id"]}, recorded_at="2026-09-27T09:05:00Z"),
            ])
        reasons = {r["record_id"]: r["reason"] for r in caught.exception.rejected}
        self.assertEqual(reasons["e1b"], "worker_already_inside")

        with self.assertRaises(BatchRejectedError) as caught:
            self.service.merge_offline(self.field, [
                rec("l1", {"type": "refuge_leave", "refuge_id": refuge["id"],
                           "worker_id": workers[1]["id"]})
            ])
        self.assertEqual(caught.exception.rejected[0]["reason"], "worker_not_inside")

    def test_field_fixes_and_retries_same_batch(self):
        _, refuge, sensor, _, workers = self._mine()
        self.service.transition(self.admin, sensor["id"], "mark_faulty", {"reason": "broken"})
        batch = [
            rec("g1", {"type": "gas", "sensor_id": sensor["id"], "gas_ppm": 20}),
            rec("e1", {"type": "refuge_enter", "refuge_id": refuge["id"],
                       "worker_id": workers[0]["id"]}),
        ]
        with self.assertRaises(BatchRejectedError):
            self.service.merge_offline(self.field, batch)
        # Central clears the faulty sensor; field retries the exact same payload.
        self.service.transition(self.admin, sensor["id"], "verify_misread")
        items = self.service.merge_offline(self.field, batch)
        self.assertEqual(len(items), 2)
        self.assertEqual(self.service.get(refuge["id"])["status"], "occupied")

    def test_retry_skips_already_merged_records(self):
        _, refuge, _, _, workers = self._mine()
        first = rec("e1", {"type": "refuge_enter", "refuge_id": refuge["id"],
                           "worker_id": workers[0]["id"]})
        self.service.merge_offline(self.field, [first])
        again = self.service.merge_offline(self.field, [first])
        self.assertEqual(len(again), 1)
        self.assertEqual(len(self.service.list("offline_record")), 1)

    def test_mixed_batch_is_all_or_nothing(self):
        _, refuge, sensor, _, workers = self._mine()
        good = [
            rec("e1", {"type": "refuge_enter", "refuge_id": refuge["id"],
                       "worker_id": workers[0]["id"]}, recorded_at="2026-09-27T09:00:00Z"),
        ]
        bad = rec("g1", {"type": "gas", "sensor_id": sensor["id"], "gas_ppm": 50},
                  recorded_at="2026-09-27T09:30:00Z")
        self.service.transition(self.admin, sensor["id"], "mark_faulty", {"reason": "broken"})
        with self.assertRaises(BatchRejectedError):
            self.service.merge_offline(self.field, good + [bad])
        # The valid record must not have landed either.
        self.assertEqual(self.service.get(refuge["id"])["data"]["occupants"], [])
        self.assertEqual(self.service.list("offline_record"), [])
        # Retrying only the good part works after the conflict is resolved.
        self.service.transition(self.admin, sensor["id"], "verify_misread")
        self.service.merge_offline(self.field, good + [bad])
        self.assertEqual(self.service.get(refuge["id"])["status"], "occupied")

    def test_malformed_batch_is_400(self):
        with self.assertRaises(ValidationError):
            self.service.merge_offline(self.field, [
                rec("x", {"type": "gas", "sensor_id": "s", "gas_ppm": "oops"})
            ])
        with self.assertRaises(ValidationError):
            self.service.merge_offline(self.field, [
                rec("x", {"type": "mystery"})
            ])
        # Same record identity twice inside one batch is rejected outright.
        duplicate = rec("e1", {"type": "gas"})
        with self.assertRaises(ValidationError):
            self.service.merge_offline(self.field, [dict(duplicate), dict(duplicate)])

    # --- ventilation restore ------------------------------------------------

    def test_offline_restore_blocked_by_alarm_and_remaining_workers(self):
        _, refuge, sensor, vent, workers = self._mine()
        self.service.transition(self.admin, vent["id"], "stop")
        # Someone is still in the mine area.
        with self.assertRaises(BatchRejectedError) as caught:
            self.service.merge_offline(self.field, [
                rec("v1", {"type": "ventilation_restore", "ventilation_id": vent["id"]})
            ])
        entry = caught.exception.rejected[0]
        self.assertEqual(entry["reason"], "area_not_clear")
        self.assertIn(workers[0]["id"], entry["blockers"]["worker_ids"])

        # Move everyone into the refuge still counts as "not withdrawn", and an alarm
        # raised by a merged gas reading must also block the same batch.
        batch = [
            rec("e1", {"type": "refuge_enter", "refuge_id": refuge["id"],
                       "worker_id": workers[0]["id"]}, recorded_at="2026-09-27T09:00:00Z"),
            rec("g1", {"type": "gas", "sensor_id": sensor["id"], "gas_ppm": 50},
                recorded_at="2026-09-27T09:05:00Z"),
            rec("v1", {"type": "ventilation_restore", "ventilation_id": vent["id"]},
                recorded_at="2026-09-27T09:10:00Z"),
        ]
        with self.assertRaises(BatchRejectedError) as caught:
            self.service.merge_offline(self.field, batch)
        entry = caught.exception.rejected[0]
        self.assertEqual(entry["reason"], "area_not_clear")
        self.assertIn(sensor["id"], entry["blockers"]["alarming_sensor_ids"])
        self.assertIn(workers[0]["id"], entry["blockers"]["worker_ids"])
        # Whole batch rejected: gas and enter effects never landed.
        self.assertEqual(self.service.get(sensor["id"])["status"], "normal")
        self.assertEqual(self.service.get(refuge["id"])["data"]["occupants"], [])

    def test_offline_restore_succeeds_when_area_clear(self):
        _, refuge, sensor, vent, workers = self._mine()
        self.service.transition(self.admin, vent["id"], "degrade", {"reason": "power"})
        for worker in workers:
            self.service.transition(self.admin, worker["id"], "deactivate")
        self.service.merge_offline(self.field, [
            rec("v1", {"type": "ventilation_restore", "ventilation_id": vent["id"],
                       "tested_at": "2026-09-27T11:00:00Z"})
        ])
        self.assertEqual(self.service.get(vent["id"])["status"], "running")
        self.assertEqual(self.service.get(vent["id"])["data"]["tested_at"],
                         "2026-09-27T11:00:00Z")

    def test_online_restore_gate_blocks_with_alarm(self):
        _, _, _, vent, _ = self._mine()
        alarming = self.service.create(
            self.admin, "sensor", {"location_code": "M-01", "gas_ppm": 20, "threshold_ppm": 10}
        )
        self.service.transition(self.admin, vent["id"], "stop")
        self.service.transition(self.admin, alarming["id"], "raise_alarm", {})
        with self.assertRaises(ConflictError):
            self.service.transition(self.admin, vent["id"], "restore",
                                    {"tested_at": "2026-09-27T11:00:00Z"})

    # --- incident close -----------------------------------------------------

    def test_incident_close_blocked_while_refuge_occupied(self):
        _, refuge, _, _, workers = self._mine()
        self.service.merge_offline(self.field, [
            rec("e1", {"type": "refuge_enter", "refuge_id": refuge["id"],
                       "worker_id": workers[0]["id"]})
        ])
        # Drive the incident through its lifecycle.
        incident = self.service.list("incident")[0]
        for action in ("begin_evacuation", "search", "stabilize", "recover"):
            incident = self.service.transition(self.admin, incident["id"], action)
        with self.assertRaises(ConflictError):
            self.service.transition(self.admin, incident["id"], "close", {"summary": "done"})

    def test_offline_incident_close_after_refuge_cleared_and_fans_running(self):
        _, refuge, _, vent, workers = self._mine()
        self.service.transition(self.admin, vent["id"], "stop")
        incident = self.service.list("incident")[0]
        for action in ("begin_evacuation", "search", "stabilize", "recover"):
            incident = self.service.transition(self.admin, incident["id"], action)
        # Worker shelters, leaves and is withdrawn; fan then restored; close record lands.
        self.service.merge_offline(self.field, [
            rec("e1", {"type": "refuge_enter", "refuge_id": refuge["id"],
                       "worker_id": workers[0]["id"]}, recorded_at="2026-09-27T09:00:00Z"),
        ])
        self.service.merge_offline(self.field, [
            rec("l1", {"type": "refuge_leave", "refuge_id": refuge["id"],
                       "worker_id": workers[0]["id"]}, recorded_at="2026-09-27T10:00:00Z"),
        ])
        self.service.transition(self.admin, workers[0]["id"], "deactivate")
        for w in workers[1:]:
            self.service.transition(self.admin, w["id"], "deactivate")
        self.service.transition(self.admin, vent["id"], "restore",
                                {"tested_at": "2026-09-27T11:00:00Z"})
        self.service.merge_offline(self.field, [
            rec("c1", {"type": "incident_close", "incident_id": incident["id"],
                       "summary": "all clear"}, recorded_at="2026-09-27T12:00:00Z")
        ])
        self.assertEqual(self.service.get(incident["id"])["status"], "closed")


if __name__ == "__main__":
    unittest.main()
