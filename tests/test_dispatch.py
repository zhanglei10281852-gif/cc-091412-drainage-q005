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
from dispatch.engine import AppError  # noqa: E402
from dispatch.service import build_service  # noqa: E402
from dispatch.timeutil import parse  # noqa: E402

T = "2026-09-22T09:{:02d}:00+08:00"
DAY_TIME = parse("2026-09-22T09:00:00+08:00")


class DispatchTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp()
        self.svc = build_service(data_dir=self.tmp)
        self.e = self.svc.engine
        # 固定在白班窗口，避免真实时钟影响班次判定
        self.e.clock = lambda: DAY_TIME

    def deep_event(self, node="W-001", minute=0, report="CALL-1", depth=0.55):
        res = self.e.ingest_report({
            "reportId": report, "nodeId": node, "depthM": depth,
            "occurredAt": T.format(minute),
        })
        return res

    def isolate_and_dispatch(self, event_id, wo, minute=2, crew=None):
        self.e.isolate(event_id, {"by": "警戒组", "occurredAt": T.format(minute)})
        payload = {"by": "调度员甲", "occurredAt": T.format(minute + 1)}
        if crew:
            payload["crewId"] = crew
        return self.e.dispatch(wo, payload)


class MergeTests(DispatchTestBase):
    def test_repeated_calls_same_impact_area_merge(self):
        a = self.deep_event(minute=0, report="CALL-1")
        b = self.deep_event(node="W-002", minute=3, report="CALL-2")
        c = self.deep_event(node="W-003", minute=4, report="CALL-3")
        self.assertEqual(a["eventId"], b["eventId"])
        self.assertEqual(b["eventId"], c["eventId"])
        view = self.e.event_view(a["eventId"])
        self.assertEqual(view["callCount"], 3)
        self.assertEqual(set(view["impactArea"]["nodeIds"]), {"W-001", "W-002", "W-003"})
        self.assertEqual(view["impactArea"]["groupId"], "MG-BINHE")
        self.assertEqual(view["workOrderStatus"], "open")

    def test_duplicate_report_is_idempotent(self):
        a = self.deep_event(minute=0)
        again = self.deep_event(minute=0)
        self.assertFalse(again["created"])
        self.assertTrue(again["deduplicated"])
        self.assertEqual(self.e.event_view(a["eventId"])["callCount"], 1)

    def test_sensor_reading_joins_same_event(self):
        a = self.deep_event(minute=0)
        s = self.e.ingest_sensor({
            "sensorId": "LV-W002", "nodeId": "W-002", "depthM": 0.61,
            "occurredAt": T.format(2),
        })
        self.assertEqual(s["eventId"], a["eventId"])
        view = self.e.event_view(a["eventId"])
        self.assertEqual(view["impactArea"]["depthM"], 0.61)
        self.assertEqual(view["severity"], "critical")

    def test_sensor_below_warning_opens_nothing(self):
        s = self.e.ingest_sensor({
            "sensorId": "LV-W001", "nodeId": "W-001", "depthM": 0.05,
            "occurredAt": T.format(0),
        })
        self.assertIsNone(s["eventId"])


class HazardGateTests(DispatchTestBase):
    def test_high_hazard_must_isolate_before_dispatch(self):
        a = self.deep_event(minute=0)
        with self.assertRaises(AppError) as ctx:
            self.e.dispatch(a["workOrderId"], {"by": "甲", "occurredAt": T.format(1)})
        self.assertEqual(ctx.exception.code, "no_eligible_crew")
        codes = {r["code"] for c in ctx.exception.details["allCrews"] for r in c["reasons"]}
        self.assertIn("not_isolated", codes)
        view = self.e.order_view(a["workOrderId"])
        self.assertTrue(view["hazard"]["isolationRequired"])
        self.assertFalse(view["isolated"])

        self.e.isolate(a["eventId"], {"by": "警戒组", "occurredAt": T.format(2)})
        d = self.e.dispatch(a["workOrderId"], {"by": "甲", "occurredAt": T.format(3)})
        self.assertTrue(d["created"])
        self.assertEqual(d["dispatch"]["crewId"], "C-A")

    def test_unqualified_crew_rejected(self):
        a = self.deep_event(minute=0)
        self.e.isolate(a["eventId"], {"occurredAt": T.format(1)})
        with self.assertRaises(AppError) as ctx:
            self.e.dispatch(a["workOrderId"], {"crewId": "C-B", "occurredAt": T.format(2)})
        self.assertEqual(ctx.exception.code, "crew_unavailable")
        self.assertIn("unqualified",
                      [r["code"] for r in ctx.exception.details["blockers"]])

    def test_parts_contention_blocks_second_order(self):
        a = self.deep_event(node="W-001", minute=0, report="CALL-1")
        self.isolate_and_dispatch(a["eventId"], a["workOrderId"], minute=1)
        b = self.deep_event(node="W-020", minute=5, report="CALL-20", depth=0.62)
        self.e.isolate(b["eventId"], {"occurredAt": T.format(6)})
        # C-A 的隔离围栏/检测仪已被前一单占用
        with self.assertRaises(AppError) as ctx:
            self.e.dispatch(b["workOrderId"], {"crewId": "C-A", "occurredAt": T.format(7)})
        self.assertIn("missing_parts",
                      [r["code"] for r in ctx.exception.details["blockers"]])

    def test_off_shift_night_crew(self):
        a = self.deep_event(node="W-001", minute=0)
        self.e.isolate(a["eventId"], {"occurredAt": T.format(1)})
        with self.assertRaises(AppError) as ctx:
            self.e.dispatch(a["workOrderId"], {"crewId": "C-C", "occurredAt": T.format(2)})
        self.assertIn("off_shift",
                      [r["code"] for r in ctx.exception.details["blockers"]])


class ConcurrencyTests(DispatchTestBase):
    def test_two_dispatchers_only_one_wins(self):
        a = self.deep_event(minute=0)
        self.e.isolate(a["eventId"], {"occurredAt": T.format(1)})
        d = self.e.dispatch(a["workOrderId"],
                            {"by": "甲", "expectedVersion": self.e.version,
                             "occurredAt": T.format(2)})
        self.assertTrue(d["created"])
        # 乙持有旧版本再派 → 冲突
        with self.assertRaises(AppError) as ctx:
            self.e.dispatch(a["workOrderId"],
                            {"by": "乙", "expectedVersion": d["version"] - 1,
                             "occurredAt": T.format(3)})
        self.assertEqual(ctx.exception.code, "version_conflict")
        # 乙用新版本改派仍可成功（尚未到场）
        d2 = self.e.dispatch(a["workOrderId"],
                             {"by": "乙", "expectedVersion": self.e.version,
                              "crewId": "C-C", "occurredAt": "2026-09-22T20:30:00+08:00"})
        self.assertTrue(d2["created"])
        self.assertEqual(d2["dispatch"]["crewId"], "C-C")
        view = self.e.order_view(a["workOrderId"])
        self.assertEqual(len(view["currentCrew"]["dispatchedAt"]) and 1, 1)


class OnSceneProtectionTests(DispatchTestBase):
    def test_onsite_order_survives_normal_merge(self):
        a = self.deep_event(node="W-001", minute=0, report="CALL-1")
        self.isolate_and_dispatch(a["eventId"], a["workOrderId"], minute=1)
        self.e.acknowledge(a["workOrderId"], {"occurredAt": T.format(4)})
        self.e.ingest_receipt({"receiptId": "RC-1", "workOrderId": a["workOrderId"],
                               "status": "onsite", "occurredAt": T.format(8)})
        other = self.deep_event(node="W-030", minute=9, report="CALL-9", depth=0.1)
        # 已到场的事件作为合并源必须被拒绝
        with self.assertRaises(AppError) as ctx:
            self.e.merge_events(a["eventId"],
                                {"targetEventId": other["eventId"], "occurredAt": T.format(10)})
        self.assertEqual(ctx.exception.code, "order_onscene")
        self.assertEqual(self.e.order_view(a["workOrderId"])["status"], "onsite")

    def test_onsite_order_cannot_be_redispatched(self):
        a = self.deep_event(minute=0)
        self.isolate_and_dispatch(a["eventId"], a["workOrderId"], minute=1)
        self.e.ingest_receipt({"receiptId": "RC-1", "workOrderId": a["workOrderId"],
                               "status": "onsite", "occurredAt": T.format(8)})
        with self.assertRaises(AppError) as ctx:
            self.e.dispatch(a["workOrderId"], {"by": "乙", "occurredAt": T.format(9)})
        self.assertEqual(ctx.exception.code, "order_onscene")

    def test_open_order_merge_cancels_duplicate_order(self):
        a = self.deep_event(node="W-010", minute=0, report="CALL-1", depth=0.2)
        # 非合并组的独立事件，手工并入；其 open 工单应取消
        b = self.e.ingest_report({"reportId": "CALL-2", "community": "某马路",
                                  "locationText": "路边井盖", "depthM": 0.2,
                                  "occurredAt": T.format(2)})
        self.assertNotEqual(a["eventId"], b["eventId"])
        result = self.e.merge_events(b["eventId"],
                                     {"targetEventId": a["eventId"], "occurredAt": T.format(3)})
        self.assertTrue(result["created"])
        cancelled = self.e.order_view(b["workOrderId"])
        self.assertEqual(cancelled["status"], "cancelled")
        self.assertIn("合并", cancelled["cancelReason"])


class ReceiptTests(DispatchTestBase):
    def test_offline_receipts_inserted_by_occurrence_time(self):
        a = self.deep_event(minute=0)
        self.isolate_and_dispatch(a["eventId"], a["workOrderId"], minute=1)
        # 后到的两个回执，发生时间是乱序补传
        self.e.ingest_receipt({"receiptId": "RC-2", "workOrderId": a["workOrderId"],
                               "status": "working", "occurredAt": T.format(20),
                               "note": "抽排中"})
        self.e.ingest_receipt({"receiptId": "RC-1", "workOrderId": a["workOrderId"],
                               "status": "onsite", "occurredAt": T.format(12)})
        view = self.e.order_view(a["workOrderId"])
        stamps = [r["at"] for r in view["receipts"]]
        self.assertEqual(stamps, sorted(stamps))
        self.assertEqual([r["receiptId"] for r in view["receipts"]], ["RC-1", "RC-2"])
        kinds = [t["kind"] for t in view["timeline"]]
        self.assertLess(kinds.index("receipt"), kinds.index("receipt") + 1)

    def test_receipt_deduplication_and_conflict(self):
        a = self.deep_event(minute=0)
        self.isolate_and_dispatch(a["eventId"], a["workOrderId"], minute=1)
        payload = {"receiptId": "RC-X", "workOrderId": a["workOrderId"],
                   "status": "onsite", "occurredAt": T.format(10)}
        first = self.e.ingest_receipt(payload)
        second = self.e.ingest_receipt(payload)
        self.assertTrue(first["created"])
        self.assertTrue(second["deduplicated"])
        with self.assertRaises(AppError) as ctx:
            self.e.ingest_receipt({"receiptId": "RC-X", "workOrderId": a["workOrderId"],
                                   "status": "working", "occurredAt": T.format(11)})
        self.assertEqual(ctx.exception.code, "conflict")

    def test_recovery_closes_event_and_reports_remain(self):
        a = self.deep_event(minute=0)
        self.isolate_and_dispatch(a["eventId"], a["workOrderId"], minute=1)
        self.e.ingest_receipt({"receiptId": "RC-1", "workOrderId": a["workOrderId"],
                               "status": "restored", "occurredAt": T.format(40),
                               "depthM": 0.05})
        view = self.e.event_view(a["eventId"])
        self.assertEqual(view["status"], "restored")
        self.assertEqual(view["callCount"], 1)
        # 恢复后同组新来电开新事件，旧记录保留
        new = self.deep_event(node="W-002", minute=50, report="CALL-7", depth=0.4)
        self.assertNotEqual(new["eventId"], a["eventId"])


class RoutingTests(DispatchTestBase):
    def _dispatch_pump_order(self):
        a = self.deep_event(node="W-001", minute=0, report="CALL-1")
        self.isolate_and_dispatch(a["eventId"], a["workOrderId"], minute=1)
        self.e.ingest_receipt({"receiptId": "RC-1", "workOrderId": a["workOrderId"],
                               "status": "restored", "occurredAt": T.format(10),
                               "depthM": 0.05})
        b = self.deep_event(node="W-020", minute=14, report="CALL-20", depth=0.62)
        self.e.isolate(b["eventId"], {"occurredAt": T.format(15)})
        d = self.e.dispatch(b["workOrderId"],
                            {"crewId": "C-A", "by": "乙", "occurredAt": T.format(16)})
        return b, d

    def test_closure_forces_reroute_with_evidence(self):
        b, d = self._dispatch_pump_order()
        self.assertEqual(d["dispatch"]["routeSegmentIds"][-1], "R-BINHE-PUMP")
        old_eta = d["dispatch"]["expectedArrivalEarly"]
        self.e.set_closure({"segmentId": "R-BINHE-PUMP", "action": "close",
                            "reason": "道路临时封闭", "occurredAt": T.format(18)})
        view = self.e.order_view(b["workOrderId"])
        self.assertEqual(len(view["reroutes"]), 1)
        r = view["reroutes"][0]
        self.assertIn("R-BINHE-PUMP", r["oldSegmentIds"])
        self.assertNotIn("R-BINHE-PUMP", r["newSegmentIds"])
        self.assertEqual(r["closedSegmentIds"], ["R-BINHE-PUMP"])
        self.assertIn("道路临时封闭", r["reason"])
        self.assertNotEqual(old_eta, r["newExpectedAt"])
        self.assertEqual(view["currentCrew"]["routeSegmentIds"], r["newSegmentIds"])

    def test_full_blockage_records_blocker_once(self):
        b, _ = self._dispatch_pump_order()
        self.e.set_closure({"segmentId": "R-BINHE-PUMP", "action": "close",
                            "occurredAt": T.format(18)})
        self.e.set_closure({"segmentId": "R-WT-PUMP", "action": "close",
                            "occurredAt": T.format(19)})
        view = self.e.order_view(b["workOrderId"])
        self.assertTrue(view["blockers"])
        self.assertEqual(view["blockers"][0]["reasons"][0]["code"], "route_blocked")
        blocked = [t for t in view["timeline"] if t["kind"] == "route_blocked"]
        self.assertEqual(len(blocked), 1)
        # 再次巡检不重复记录
        self.e.tick()
        view = self.e.order_view(b["workOrderId"])
        self.assertEqual(len([t for t in view["timeline"] if t["kind"] == "route_blocked"]), 1)

    def test_height_restriction_reroutes_high_vehicle_only(self):
        b, d = self._dispatch_pump_order()
        # C-A 车高 2.35m 可走 2.4m 限高路段，路线不变
        self.e.set_closure({"segmentId": "R-BINHE-PUMP", "action": "close",
                            "restriction": {"maxHeightM": 2.4}, "occurredAt": T.format(18)})
        view = self.e.order_view(b["workOrderId"])
        self.assertFalse(view["reroutes"])
        closures = self.e.closures
        route_a = self.e.router.shortest("W-001", "W-020", closures,
                                         self.e.ref.crews["C-A"]["vehicle"])
        route_c = self.e.router.shortest("W-001", "W-020", closures,
                                         self.e.ref.crews["C-C"]["vehicle"])
        self.assertIn("R-BINHE-PUMP", route_a["segmentIds"])
        self.assertNotIn("R-BINHE-PUMP", route_c["segmentIds"])
        self.assertGreater(route_c["lengthM"], route_a["lengthM"])


class ReminderAndRestartTests(DispatchTestBase):
    def test_ack_timeout_reminder_persists_and_is_idempotent(self):
        a = self.deep_event(minute=0)
        self.isolate_and_dispatch(a["eventId"], a["workOrderId"], minute=1)
        # 时钟推进到派工 6 分钟后（阈值 300s）
        from dispatch import timeutil
        frozen = [timeutil.parse(T.format(9))]
        self.e.clock = lambda: frozen[0]
        result = self.e.tick()
        self.assertEqual(len(result["reminders"]), 1)
        self.e.tick()
        view = self.e.order_view(a["workOrderId"])
        ack_reminders = [x for x in view["escalations"] if x["kind"] == "ack_timeout"]
        self.assertEqual(len(ack_reminders), 1)

        # 重启：未签收工单与提醒都在
        from dispatch.engine import Engine
        from dispatch.journal import Journal
        e2 = Engine(self.svc.ref, Journal(self.tmp), self.svc.config, clock=lambda: frozen[0])
        view2 = e2.order_view(a["workOrderId"])
        self.assertEqual(view2["status"], "dispatched")
        self.assertEqual(len([x for x in view2["escalations"] if x["kind"] == "ack_timeout"]), 1)
        # 重启后再 tick 不重复提醒
        self.assertEqual(e2.tick()["reminders"], [])

    def test_restore_timeout_after_onsite(self):
        a = self.deep_event(minute=0)
        self.isolate_and_dispatch(a["eventId"], a["workOrderId"], minute=1)
        self.e.ingest_receipt({"receiptId": "RC-1", "workOrderId": a["workOrderId"],
                               "status": "onsite", "occurredAt": T.format(8)})
        from dispatch import timeutil
        self.e.clock = lambda: timeutil.parse("2026-09-22T11:00:00+08:00")
        result = self.e.tick()
        self.assertTrue(any("restore" in r for r in result["reminders"]))


class AutoDispatchTests(DispatchTestBase):
    def test_tick_auto_dispatches_severity_first(self):
        self.e.clock = lambda: parse(T.format(4))
        # 一般积水（梧桐，C-B 可处理）
        self.e.ingest_report({"reportId": "CALL-1", "nodeId": "W-010", "depthM": 0.32,
                              "occurredAt": T.format(0)})
        # 高危（滨河）：未隔离 → 不能自动派
        hi = self.e.ingest_report({"reportId": "CALL-2", "nodeId": "W-001", "depthM": 0.6,
                                   "occurredAt": T.format(1)})
        result = self.e.tick()
        self.assertEqual(len(result["autoDispatched"]), 1)
        order = next(o for o in self.e.list_orders() if o["eventId"] != hi["eventId"])
        self.assertEqual(order["crewId"], "C-B")
        hi_view = self.e.order_view(hi["workOrderId"])
        self.assertIsNone(hi_view["currentCrew"])
        # 隔离后 tick 自动派出 C-A
        self.e.isolate(hi["eventId"], {"occurredAt": T.format(3)})
        self.e.tick()
        self.assertEqual(self.e.order_view(hi["workOrderId"])["currentCrew"]["crewId"], "C-A")


class DeterminismTests(DispatchTestBase):
    def _feed_scenario(self, svc, order):
        e = svc.engine

        def ids():
            event_id = e.group_active["MG-BINHE"]
            wo = e.events[event_id]["workOrderId"]
            return event_id, wo

        for step in order:
            if step == "report1":
                e.ingest_report({"reportId": "CALL-1", "nodeId": "W-001", "depthM": 0.55,
                                 "occurredAt": T.format(0)})
            elif step == "report2":
                e.ingest_report({"reportId": "CALL-2", "nodeId": "W-002", "depthM": 0.4,
                                 "occurredAt": T.format(5)})
            elif step == "isolate":
                e.isolate(ids()[0], {"occurredAt": T.format(2)})
            elif step == "dispatch":
                e.dispatch(ids()[1], {"by": "甲", "occurredAt": T.format(3)})
            elif step == "ack":
                e.acknowledge(ids()[1], {"occurredAt": T.format(4)})
            elif step == "receipt_late":
                # 离线补传：发生时间最早，最后才送达
                e.ingest_receipt({"receiptId": "RC-0", "workOrderId": ids()[1],
                                  "status": "onsite", "occurredAt": T.format(6)})
            elif step == "receipt_restore":
                e.ingest_receipt({"receiptId": "RC-1", "workOrderId": ids()[1],
                                  "status": "restored", "occurredAt": T.format(40),
                                  "depthM": 0.05})

    def _signature(self, svc):
        e = svc.engine
        out = {}
        for wid, order in e.orders.items():
            # 以影响范围分组为键，避免首个到达事件时间不同导致的单号差异
            key = order["groupId"] or wid
            timeline = e.order_view(wid)["timeline"]
            out[key] = {
                "status": order["status"], "crewId": order["crewId"],
                "receipts": sorted((rid, r["status"], r["at"]) for rid, r in order["receipts"].items()),
                "timelineKinds": [t["kind"] for t in timeline],
                "timelineSorted": [t["at"] for t in timeline] == sorted(t["at"] for t in timeline),
                "callCount": len(e.events[order["eventId"]]["reportIds"]),
                "depth": e.events[order["eventId"]]["latestDepthM"],
            }
        return out

    def test_arrival_order_does_not_change_result(self):
        import tempfile as tf
        dirs = [tf.mkdtemp(), tf.mkdtemp()]
        # 因果链（报修→隔离→派工→签收→回执）保持物理顺序；
        # 独立事件（重复来电 CALL-2、离线补传回执）可以任意穿插到达
        orders = [
            ["report1", "isolate", "dispatch", "ack", "report2", "receipt_late", "receipt_restore"],
            ["report2", "report1", "isolate", "dispatch", "receipt_late", "ack", "receipt_restore"],
        ]
        signatures = []
        for d, order in zip(dirs, orders):
            svc = build_service(data_dir=d)
            svc.engine.clock = lambda: DAY_TIME
            self._feed_scenario(svc, order)
            signatures.append(self._signature(svc))
        self.assertEqual(signatures[0], signatures[1])
        # 同一日志重放两次结果一致（重启幂等）
        from dispatch.engine import Engine
        from dispatch.journal import Journal
        svc = build_service(data_dir=dirs[0])
        rebuilt = Engine(svc.ref, Journal(dirs[0]), svc.config, clock=lambda: DAY_TIME)
        again = Engine(svc.ref, Journal(dirs[0]), svc.config, clock=lambda: DAY_TIME)
        holder = lambda engine: type("S", (), {"engine": engine})()
        self.assertEqual(self._signature(holder(rebuilt)), self._signature(holder(again)))


class ApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp()

        def factory():
            return build_service(data_dir=self.tmp)

        self.server = create_server(factory())
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.server.shutdown)
        self.addCleanup(self.server.server_close)
        self.base = f"http://127.0.0.1:{self.server.server_port}"

    def _req(self, method, path, body=None):
        data = json.dumps(body).encode("utf-8") if body is not None else None
        req = urllib.request.Request(self.base + path, data=data, method=method,
                                     headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req) as resp:
                return resp.status, json.load(resp)
        except urllib.error.HTTPError as exc:
            return exc.code, json.load(exc)

    def test_health_and_full_flow(self):
        status, body = self._req("GET", "/health")
        self.assertEqual(status, 200)
        self.assertEqual(body, {"status": "ok", "service": SERVICE_NAME})

        status, a = self._req("POST", "/v1/reports", {
            "reportId": "CALL-1", "nodeId": "W-001", "depthM": 0.55,
            "occurredAt": "2026-09-22T09:00:00+08:00"})
        self.assertEqual(status, 201)
        wo = a["workOrderId"]; ev = a["eventId"]

        # 影响范围视图
        status, eview = self._req("GET", f"/v1/events/{ev}")
        self.assertEqual(status, 200)
        self.assertEqual(eview["impactArea"]["groupId"], "MG-BINHE")
        self.assertEqual(eview["severity"], "critical")

        # 未隔离先派 → 422
        status, err = self._req("POST", f"/v1/work-orders/{wo}/dispatch",
                                {"by": "甲", "occurredAt": "2026-09-22T09:01:00+08:00"})
        self.assertEqual(status, 422)
        self.assertEqual(err["error"], "no_eligible_crew")

        self._req("POST", f"/v1/events/{ev}/isolation",
                  {"by": "警戒组", "occurredAt": "2026-09-22T09:02:00+08:00"})
        status, d = self._req("POST", f"/v1/work-orders/{wo}/dispatch",
                              {"by": "甲", "occurredAt": "2026-09-22T09:03:00+08:00"})
        self.assertEqual(status, 201)
        self.assertTrue(d["dispatch"]["expectedArrivalEarly"])
        self.assertTrue(d["dispatch"]["expectedArrivalLate"])

        # 并发派工：旧版本 409
        status, err = self._req("POST", f"/v1/work-orders/{wo}/dispatch",
                                {"by": "乙", "expectedVersion": 1,
                                 "occurredAt": "2026-09-22T09:04:00+08:00"})
        self.assertEqual(status, 409)
        self.assertEqual(err["error"], "version_conflict")

        # 队伍视图含当前队列
        status, crews = self._req("GET", "/v1/crews")
        ca = next(c for c in crews["crews"] if c["crewId"] == "C-A")
        self.assertIn(wo, ca["queue"])

        # 工单视图：影响范围/当前队伍/ETA窗口/阻塞/事件记录
        status, ov = self._req("GET", f"/v1/work-orders/{wo}")
        self.assertEqual(status, 200)
        self.assertEqual(ov["currentCrew"]["crewId"], "C-A")
        self.assertEqual(ov["impactArea"]["groupId"], "MG-BINHE")
        self.assertIn("timeline", ov)
        self.assertTrue(any(t["kind"] == "opened" for t in ov["timeline"]))

        # 离线补传
        status, r = self._req("POST", "/v1/receipts", {
            "receiptId": "RC-1", "workOrderId": wo, "status": "onsite",
            "occurredAt": "2026-09-22T09:10:00+08:00"})
        self.assertEqual(status, 201)
        status, r = self._req("POST", "/v1/receipts", {
            "receiptId": "RC-1", "workOrderId": wo, "status": "onsite",
            "occurredAt": "2026-09-22T09:10:00+08:00"})
        self.assertEqual(status, 200)
        self.assertTrue(r["deduplicated"])

        # 态势总览
        status, sit = self._req("GET", "/v1/situation")
        self.assertEqual(status, 200)
        self.assertIn("closures", sit)
        self.assertTrue(sit["workOrders"])


if __name__ == "__main__":
    unittest.main()
