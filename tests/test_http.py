"""HTTP 接口集成测试：真实线程并发派工、道路管制、离线回执、重启恢复。"""
from __future__ import annotations

import json
import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from app import SERVICE_NAME, create_server  # noqa: E402
from drainage import timeutil  # noqa: E402

DAYTIME = timeutil.parse("2026-09-22T10:00:00+08:00")


def fixed_clock():
    return DAYTIME


class HttpCase(unittest.TestCase):
    def setUp(self):
        self.data_dir = tempfile.mkdtemp()
        self.server = create_server(self.data_dir, clock=fixed_clock)
        self.port = self.server.server_port
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)

    def call(self, method: str, path: str, payload=None):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8") if payload is not None else None
        req = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=body,
                                     headers={"Content-Type": "application/json"}, method=method)
        try:
            with urllib.request.urlopen(req) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode("utf-8"))

    def test_health(self):
        status, body = self.call("GET", "/health")
        self.assertEqual(status, 200)
        self.assertEqual(body, {"status": "ok", "service": SERVICE_NAME})

    def test_end_to_end_high_risk_flow(self):
        status, r = self.call("POST", "/reports",
                              {"compound": "春风里小区", "depthCm": 60, "reportId": "h-1"})
        self.assertEqual(status, 201)
        inc_id = r["incidentId"]
        board = self.call("GET", "/board")[1]
        self.assertEqual(len(board["incidents"]), 1)
        row = board["incidents"][0]
        self.assertEqual(row["dangerLevel"], "high")
        self.assertTrue(row["isolation"]["required"])
        wo_id = self.call("GET", f"/incidents/{inc_id}")[1]["workOrders"][0]["id"]

        # 未隔离派工 -> 阻塞（200，blocked=true）
        status, blocked = self.call("POST", f"/work-orders/{wo_id}/dispatch",
                                    {"dispatcher": "甲"})
        self.assertEqual(status, 200)
        self.assertTrue(blocked["blocked"])

        # 先隔离再确认
        self.assertEqual(self.call("POST", f"/incidents/{inc_id}/isolation", {})[0], 201)
        self.assertEqual(self.call("POST", "/receipts",
                                   {"incidentId": inc_id, "teamId": "T-C",
                                    "type": "isolation_confirmed", "receiptId": "h-iso"})[0], 201)
        status, assigned = self.call("POST", f"/work-orders/{wo_id}/dispatch",
                                     {"dispatcher": "甲", "expectedVersion": 0})
        self.assertEqual(status, 200)
        self.assertTrue(assigned["assigned"])
        self.assertEqual(assigned["assignment"]["teamId"], "T-A")
        self.assertEqual(assigned["assignment"]["routeEdges"], ["E1", "E2"])

    def test_concurrent_dispatch_only_one_wins(self):
        self.call("POST", "/reports", {"compound": "柳岸庄小区", "depthCm": 30, "reportId": "c-1"})
        inc_id = self.call("GET", "/board")[1]["incidents"][0]["incidentId"]
        wo_id = self.call("GET", f"/incidents/{inc_id}")[1]["workOrders"][0]["id"]

        results = []
        lock = threading.Lock()

        def dispatcher(name):
            status, body = self.call("POST", f"/work-orders/{wo_id}/dispatch",
                                     {"dispatcher": name, "expectedVersion": 0})
            with lock:
                results.append((status, body))

        threads = [threading.Thread(target=dispatcher, args=(f"调度员{i}",)) for i in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assigned = [r for r in results if r[0] == 200 and r[1].get("assigned")]
        conflicts = [r for r in results if r[0] == 409]
        self.assertEqual(len(assigned), 1)
        self.assertEqual(len(conflicts), 5)

    def test_offline_receipt_idempotent_and_timeline(self):
        self.call("POST", "/reports", {"wellIds": ["W-201"], "depthCm": 30, "reportId": "o-1"})
        inc_id = self.call("GET", "/board")[1]["incidents"][0]["incidentId"]
        wo_id = self.call("GET", f"/incidents/{inc_id}")[1]["workOrders"][0]["id"]
        self.call("POST", f"/work-orders/{wo_id}/dispatch", {"dispatcher": "甲"})
        receipt = {"workOrderId": wo_id, "teamId": "T-B", "type": "arrived",
                   "occurredAt": "2026-09-22T09:10:00+08:00", "offline": True,
                   "receiptId": "o-rc"}
        s1, _ = self.call("POST", "/receipts", receipt)
        s2, body = self.call("POST", "/receipts", receipt)
        self.assertEqual((s1, s2), (201, 201))
        self.assertTrue(body["idempotent"])
        timeline = self.call("GET", f"/work-orders/{wo_id}/timeline")[1]["entries"]
        receipts = [e for e in timeline if e["type"] == "receipt"]
        self.assertEqual(len(receipts), 1)
        self.assertEqual(receipts[0]["receiptType"], "arrived")
        self.assertTrue(receipts[0]["offline"])

    def test_restart_persists_state(self):
        self.call("POST", "/reports", {"compound": "柳岸庄小区", "depthCm": 10, "reportId": "p-1"})
        incidents_before = self.call("GET", "/board")[1]["incidents"]
        self.assertEqual(len(incidents_before), 1)
        inc_id = incidents_before[0]["incidentId"]

        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)

        self.server = create_server(self.data_dir, clock=fixed_clock)
        self.port = self.server.server_port
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

        status, inc = self.call("GET", f"/incidents/{inc_id}")
        self.assertEqual(status, 200)
        self.assertEqual(inc["impactArea"]["compounds"], ["柳岸庄小区"])
        self.assertEqual(len(inc["workOrders"]), 1)

    def test_bad_request_and_conflict(self):
        status, body = self.call("POST", "/reports", {"wellIds": ["NOPE"], "depthCm": 1})
        self.assertEqual(status, 400)
        self.assertEqual(body["error"], "bad_request")
        self.assertEqual(self.call("GET", "/nope")[0], 404)

    def test_road_close_shows_blocker_on_board(self):
        self.call("POST", "/roads/E3/close", {"reason": "下穿积水"})
        self.call("POST", "/reports", {"compound": "柳岸庄小区", "depthCm": 30, "reportId": "r-1"})
        inc_id = self.call("GET", "/board")[1]["incidents"][0]["incidentId"]
        wo_id = self.call("GET", f"/incidents/{inc_id}")[1]["workOrders"][0]["id"]
        status, res = self.call("POST", f"/work-orders/{wo_id}/dispatch", {"dispatcher": "甲"})
        self.assertEqual(status, 200)
        self.assertTrue(res["blocked"])
        self.assertTrue(any("E3" in b for b in res["blockedReasons"]))


if __name__ == "__main__":
    unittest.main()
