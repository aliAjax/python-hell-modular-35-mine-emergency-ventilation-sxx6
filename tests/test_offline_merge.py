import tempfile
import unittest
from pathlib import Path

from src.domain import Actor, ConflictError, OfflineMergeError
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


class OfflineMergeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = DomainService(SQLiteRepository(Path(self.tmp.name) / "test.db"), RuleEngine())
        self.actor = Actor("admin", "admin")

    def tearDown(self):
        self.tmp.cleanup()

    def create(self, kind, data):
        return self.service.create(self.actor, kind, data)

    def act(self, entity, action, data=None, version=None):
        return self.service.transition(self.actor, entity["id"], action, data or {}, version)

    def record(self, source, record_id, payload):
        return {
            "source_id": source,
            "record_id": record_id,
            "recorded_at": "2026-09-27T10:00:00Z",
            "payload": payload,
        }

    def test_conflicting_record_rejects_whole_batch(self):
        refuge = self.create("refuge", {"location_code": "M-01", "capacity": 5})
        worker = self.create("worker", {"name": "Li Wei", "location_code": "M-01", "team": "A"})
        worker = self.act(worker, "mark_missing")
        worker = self.act(worker, "locate")
        worker = self.act(worker, "rescue", {"incident_id": "inc-1"})
        self.assertEqual(worker["status"], "rescued")

        good_worker = self.create("worker", {"name": "Wang Fang", "location_code": "M-01", "team": "B"})
        records = [
            self.record("field-a", "1", {"type": "refuge_pass", "refuge_id": refuge["id"], "worker_id": good_worker["id"], "direction": "enter"}),
            self.record("field-a", "2", {"type": "refuge_pass", "refuge_id": refuge["id"], "worker_id": worker["id"], "direction": "enter"}),
        ]
        with self.assertRaises(OfflineMergeError) as ctx:
            self.service.merge_offline(self.actor, records)
        self.assertEqual(len(ctx.exception.conflicts), 1)
        conflict = ctx.exception.conflicts[0]
        self.assertEqual(conflict["source_id"], "field-a")
        self.assertEqual(conflict["record_id"], "2")
        self.assertIn("already out of mine", conflict["reason"])
        # Whole batch rejected: nothing written.
        self.assertEqual(self.service.list("offline_record"), [])
        self.assertIsNone(self.service.get(good_worker["id"])["data"].get("in_refuge"))

    def test_capacity_exceeded_rejects_whole_batch(self):
        refuge = self.create("refuge", {"location_code": "M-01", "capacity": 2})
        workers = [
            self.create("worker", {"name": "W%d" % i, "location_code": "M-01", "team": "A"})
            for i in range(3)
        ]
        records = [
            self.record("field-a", str(i), {"type": "refuge_pass", "refuge_id": refuge["id"], "worker_id": w["id"], "direction": "enter"})
            for i, w in enumerate(workers)
        ]
        with self.assertRaises(OfflineMergeError) as ctx:
            self.service.merge_offline(self.actor, records)
        self.assertIsNotNone(ctx.exception.rejected_capacity)
        self.assertEqual(ctx.exception.rejected_capacity["would_be"], 3)
        self.assertEqual(ctx.exception.rejected_capacity["capacity"], 2)
        # Whole batch rejected: nothing written.
        self.assertEqual(self.service.list("offline_record"), [])
        self.assertEqual(self.service.get(refuge["id"])["data"].get("occupancy", 0), 0)

    def test_retry_as_is_after_fix_succeeds(self):
        refuge = self.create("refuge", {"location_code": "M-01", "capacity": 5})
        worker = self.create("worker", {"name": "Li Wei", "location_code": "M-01", "team": "A"})
        worker = self.act(worker, "mark_missing")
        worker = self.act(worker, "locate")
        worker = self.act(worker, "rescue", {"incident_id": "inc-1"})
        records = [
            self.record("field-a", "1", {"type": "refuge_pass", "refuge_id": refuge["id"], "worker_id": worker["id"], "direction": "enter"}),
        ]
        with self.assertRaises(OfflineMergeError):
            self.service.merge_offline(self.actor, records)
        # Field fixes the record (new worker) and retries the same batch as-is.
        replacement = self.create("worker", {"name": "Wang Fang", "location_code": "M-01", "team": "B"})
        records[0]["payload"]["worker_id"] = replacement["id"]
        merged = self.service.merge_offline(self.actor, records)
        self.assertEqual(len(merged), 1)
        self.assertEqual(self.service.get(refuge["id"])["data"]["occupancy"], 1)
        self.assertEqual(self.service.get(replacement["id"])["data"]["in_refuge"], refuge["id"])

    def test_occupancy_recalculated_after_merge(self):
        refuge = self.create("refuge", {"location_code": "M-01", "capacity": 5})
        workers = [
            self.create("worker", {"name": "W%d" % i, "location_code": "M-01", "team": "A"})
            for i in range(3)
        ]
        enter_records = [
            self.record("field-a", "e%d" % i, {"type": "refuge_pass", "refuge_id": refuge["id"], "worker_id": w["id"], "direction": "enter"})
            for i, w in enumerate(workers)
        ]
        self.service.merge_offline(self.actor, enter_records)
        refuge = self.service.get(refuge["id"])
        self.assertEqual(refuge["data"]["occupancy"], 3)
        self.assertEqual(refuge["status"], "occupied")

        exit_records = [
            self.record("field-a", "x0", {"type": "refuge_pass", "refuge_id": refuge["id"], "worker_id": workers[0]["id"], "direction": "exit"}),
        ]
        self.service.merge_offline(self.actor, exit_records)
        refuge = self.service.get(refuge["id"])
        self.assertEqual(refuge["data"]["occupancy"], 2)
        self.assertEqual(refuge["status"], "occupied")

        more_exits = [
            self.record("field-a", "x1", {"type": "refuge_pass", "refuge_id": refuge["id"], "worker_id": workers[1]["id"], "direction": "exit"}),
            self.record("field-a", "x2", {"type": "refuge_pass", "refuge_id": refuge["id"], "worker_id": workers[2]["id"], "direction": "exit"}),
        ]
        self.service.merge_offline(self.actor, more_exits)
        refuge = self.service.get(refuge["id"])
        self.assertEqual(refuge["data"]["occupancy"], 0)
        self.assertEqual(refuge["status"], "available")

    def test_gas_reading_updates_sensor_and_blocks_restore(self):
        sensor = self.create("sensor", {"location_code": "M-01", "gas_ppm": 10, "threshold_ppm": 80})
        vent = self.create("ventilation", {"name": "fan-1", "area_code": "M-01", "capacity": 100})
        vent = self.act(vent, "stop")
        records = [
            self.record("field-a", "g1", {"type": "gas_reading", "sensor_id": sensor["id"], "gas_ppm": 120}),
        ]
        self.service.merge_offline(self.actor, records)
        sensor = self.service.get(sensor["id"])
        self.assertEqual(sensor["data"]["gas_ppm"], 120)
        self.assertEqual(sensor["data"]["severity"], "alarm")
        self.assertEqual(sensor["status"], "alarm")
        with self.assertRaises(ConflictError):
            self.act(vent, "restore", {"tested_at": "2026-09-27T11:00:00Z"})

    def test_restore_blocked_by_unevacuated_worker(self):
        vent = self.create("ventilation", {"name": "fan-1", "area_code": "M-01", "capacity": 100})
        vent = self.act(vent, "stop")
        self.create("worker", {"name": "Li Wei", "location_code": "M-01", "team": "A"})
        with self.assertRaises(ConflictError):
            self.act(vent, "restore", {"tested_at": "2026-09-27T11:00:00Z"})

    def test_restore_succeeds_when_clear_and_evacuated(self):
        sensor = self.create("sensor", {"location_code": "M-01", "gas_ppm": 10, "threshold_ppm": 80})
        vent = self.create("ventilation", {"name": "fan-1", "area_code": "M-01", "capacity": 100})
        vent = self.act(vent, "stop")
        worker = self.create("worker", {"name": "Li Wei", "location_code": "M-01", "team": "A"})
        worker = self.act(worker, "mark_missing")
        worker = self.act(worker, "locate")
        worker = self.act(worker, "rescue", {"incident_id": "inc-1"})
        self.assertEqual(sensor["status"], "normal")
        vent = self.act(vent, "restore", {"tested_at": "2026-09-27T11:00:00Z"})
        self.assertEqual(vent["status"], "running")

    def test_center_alarm_not_downgraded_by_reading(self):
        sensor = self.create("sensor", {"location_code": "M-01", "gas_ppm": 120, "threshold_ppm": 80})
        sensor = self.act(sensor, "raise_alarm")
        records = [
            self.record("field-a", "g1", {"type": "gas_reading", "sensor_id": sensor["id"], "gas_ppm": 10}),
        ]
        with self.assertRaises(OfflineMergeError) as ctx:
            self.service.merge_offline(self.actor, records)
        self.assertEqual(len(ctx.exception.conflicts), 1)
        self.assertIn("disagrees", ctx.exception.conflicts[0]["reason"])
        sensor = self.service.get(sensor["id"])
        self.assertEqual(sensor["status"], "alarm")

    def test_faulty_sensor_reading_is_conflict(self):
        sensor = self.create("sensor", {"location_code": "M-01", "gas_ppm": 10, "threshold_ppm": 80})
        sensor = self.act(sensor, "mark_faulty", {"reason": "broken"})
        records = [
            self.record("field-a", "g1", {"type": "gas_reading", "sensor_id": sensor["id"], "gas_ppm": 120}),
        ]
        with self.assertRaises(OfflineMergeError) as ctx:
            self.service.merge_offline(self.actor, records)
        self.assertIn("faulty", ctx.exception.conflicts[0]["reason"])


if __name__ == "__main__":
    unittest.main()
