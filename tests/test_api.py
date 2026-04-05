import asyncio
import tempfile
import unittest
from pathlib import Path

import admin
from database import Database


class DummyRequest:
    def __init__(self):
        self.session = {"auth": True}
        self.query_params = {}


class ApiLikeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tmp.name) / "api.db")
        admin.db = Database(db_path=self.db_path)

    def tearDown(self):
        self.tmp.cleanup()

    def test_save_segment_endpoint_function(self):
        req = DummyRequest()
        response = asyncio.run(
            admin.save_user_segment(
                request=req,
                segment_id=None,
                segment_rule_id=None,
                segment_name="Segment A",
                step_ref="1",
                delay_days=0,
                weekly_limit=2,
                send_time="09:00",
                text="hello",
            )
        )
        self.assertEqual(response.status_code, 302)
        segments = admin.db.get_segments(active_only=False)
        rules = admin.db.get_segment_campaign_rules(active_only=False)
        self.assertEqual(len(segments), 1)
        self.assertEqual(len(rules), 1)

    def test_broadcast_function_saves_segment_fields(self):
        req = DummyRequest()
        response = asyncio.run(
            admin.broadcast(
                request=req,
                text="test",
                buttons_json="",
                photo=None,
                scheduled_at="2030-01-01T10:00",
                timezone="Europe/Moscow",
                segment_type="tag",
                segment_value="vip",
                segment_step_ref="",
            )
        )
        self.assertEqual(response.status_code, 302)
        row = admin.db.get_broadcast_history(limit=1)[0]
        self.assertEqual(row["segment_type"], "tag")
        self.assertEqual(row["segment_value"], "vip")


if __name__ == "__main__":
    unittest.main()
