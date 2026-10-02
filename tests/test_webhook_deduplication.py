"""Offline regression tests: no real leads, Telegram messages or CRM calls."""
import importlib.util
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('webhook_under_test', ROOT / 'tilda_webhook.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

class DeduplicationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        module.IDEMPOTENCY_DB = str(Path(self.tmp.name) / 'requests.sqlite3')
        self.count = 0
        self.entered = threading.Event()
        self.release = threading.Event()
        self.slow = False
        self.app = module.Flask(__name__)
        def create():
            self.count += 1
            if self.slow:
                self.entered.set()
                self.release.wait(5)
            return module.jsonify(status='ok', lead_id=self.count), 200
        self.app.add_url_rule('/lead', view_func=module.deduplicate_submission(create), methods=['POST'])
        self.body = {'Phone': '+79001234567', 'tranid': 'offline-test-1'}

    def tearDown(self):
        self.tmp.cleanup()

    def post(self, body=None):
        with self.app.test_client() as client:
            return client.post('/lead', json=body or self.body)

    def test_same_transaction_returns_same_lead(self):
        a, b = self.post(), self.post()
        self.assertEqual(a.json, b.json)
        self.assertEqual(self.count, 1)

    def test_new_transaction_same_phone_is_new_lead(self):
        self.post()
        self.post(dict(self.body, tranid='offline-test-2'))
        self.assertEqual(self.count, 2)

    def test_legacy_payload_without_id(self):
        data = {'Phone': '+79001234567', 'name': 'Offline test'}
        self.post(data); self.post(data)
        self.assertEqual(self.count, 1)

    def test_simultaneous_requests(self):
        self.slow = True
        thread = threading.Thread(target=self.post)
        thread.start()
        self.assertTrue(self.entered.wait(3))
        response = self.post()
        self.assertEqual(response.status_code, 202)
        self.release.set(); thread.join(5)
        self.assertEqual(self.count, 1)
        self.assertEqual(self.post().json['lead_id'], 1)

    def test_reservation_survives_new_client(self):
        self.post()
        self.assertEqual(self.post().json['lead_id'], 1)
        self.assertEqual(self.count, 1)

    def test_telegram_does_not_block(self):
        started, release = threading.Event(), threading.Event()
        def slow_post(*args, **kwargs):
            started.set(); release.wait(3)
            result = type('Response', (), {'ok': True, 'json': lambda self: {'ok': True}})()
            return result
        with patch.object(module, 'TELEGRAM_BOT_TOKEN', 'offline'), patch.object(module, 'TELEGRAM_CHAT_ID', 'offline'), patch.object(module.requests, 'post', side_effect=slow_post):
            before = time.monotonic()
            module.send_telegram_message('offline')
            self.assertLess(time.monotonic() - before, 0.5)
            self.assertTrue(started.wait(1)); release.set()

    def test_uncertain_crm_result_not_retried(self):
        with patch.object(module.requests, 'post', side_effect=module.requests.exceptions.ReadTimeout), patch.object(module, 'send_telegram_message') as notify:
            result = module.create_lead('test', '', '79001234567', '', '', 58)
            self.assertIsNone(result)
            self.assertEqual(module.requests.post.call_count, 1)

if __name__ == '__main__':
    unittest.main()
