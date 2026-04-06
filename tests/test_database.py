import tempfile
import unittest
from pathlib import Path

from database import Database


class DatabaseTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tmp.name) / "test.db")
        self.db = Database(db_path=self.db_path)
        self.db.add_user(1001, "alice")

    def tearDown(self):
        self.tmp.cleanup()

    def test_segment_campaign_due_and_limit(self):
        self.db.increment_scenario_visit(1, user_id=1001)
        segment_id = self.db.upsert_segment(name="Step 1", scenario_ref="1")
        rule_id = self.db.upsert_segment_campaign_rule(
            segment_id=segment_id,
            delay_days=0,
            weekly_limit=1,
            send_time="00:00",
            message_text="hello",
        )

        due = self.db.get_users_due_for_segment_campaign_rule(
            rule_id=rule_id,
            scenario_id=1,
            delay_days=0,
            weekly_limit=1,
            send_time="00:00",
        )
        self.assertIn(1001, due)

        self.db.log_segment_campaign_delivery(rule_id, 1001)
        due_after = self.db.get_users_due_for_segment_campaign_rule(
            rule_id=rule_id,
            scenario_id=1,
            delay_days=0,
            weekly_limit=1,
            send_time="00:00",
        )
        self.assertNotIn(1001, due_after)

    def test_broadcast_segment_fields_persist(self):
        self.db.log_broadcast(
            message_text="msg",
            buttons_json=None,
            photo_path=None,
            timezone="UTC",
            scheduled_at="2000-01-01 00:00:00",
            segment_type="step",
            segment_value=None,
            segment_step_ref="2",
            status="pending",
        )
        pending = self.db.get_pending_broadcasts()
        self.assertEqual(pending[0]["segment_type"], "step")
        self.assertEqual(pending[0]["segment_step_ref"], "2")

    def test_delete_step_broadcast_rule_hard_delete(self):
        rule_id = self.db.upsert_step_broadcast_rule(
            segment_name=None,
            scenario_ref="1",
            delay_days=1,
            weekly_limit=1,
            send_time="10:00",
            required_tag=None,
            message_text="legacy",
        )
        self.db.log_step_rule_delivery(rule_id, 1001)
        self.db.delete_step_broadcast_rule(rule_id)
        rules = self.db.get_step_broadcast_rules(active_only=False)
        self.assertFalse(any(int(r["id"]) == rule_id for r in rules))

    def test_error_logs_store_datetime_and_message(self):
        log_id = self.db.log_error(source="test", message="boom", details="trace")
        logs = self.db.get_error_logs(limit=10)
        self.assertTrue(any(int(item["id"]) == log_id for item in logs))
        first = logs[0]
        self.assertIn("created_at", first)
        self.assertEqual(first["source"], "test")


if __name__ == "__main__":
    unittest.main()
