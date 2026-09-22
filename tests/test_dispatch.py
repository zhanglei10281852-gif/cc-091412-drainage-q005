"""抢修调度核心测试：合并、隔离、派工互斥、路线、回执、提醒、重启恢复。"""
from __future__ import annotations

import sys
import tempfile
import threading
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from drainage.reference import load_reference
from drainage.service import ConflictError, DispatchService, ValidationError
from drainage.store import EventStore

TZ = ZoneInfo("Asia/Shanghai")


class FakeClock:
    def __init__(self, moment: datetime):
        self.moment = moment

    def __call__(self) -> datetime:
        return self.moment

    def advance(self, minutes: float):
        self.moment += timedelta(minutes=minutes)


class ServiceCase(unittest.TestCase):
    def setUp(self):
        self.data_dir = tempfile.mkdtemp()
        self.clock = FakeClock(datetime(2026, 9, 22, 10, 0, tzinfo=TZ))
        self.svc = DispatchService(load_reference(), EventStore(self.data_dir), clock=self.clock)

    def restart(self) -> DispatchService:
        return DispatchService(load_reference(), EventStore(self.data_dir), clock=self.clock)

    def report(self, wells=None, compound=None, depth=10, report_id=None):
        payload = {"depthCm": depth, "reportId": report_id}
        if wells:
            payload["wellIds"] = wells
        if compound:
            payload["compound"] = compound
        return self.svc.ingest_report(payload)

    def isolate_and_confirm(self, incident_id, team="T-C"):
        self.svc.dispatch_isolation(incident_id)
        self.svc.receive_receipt({"incidentId": incident_id, "teamId": team,
                                  "type": "isolation_confirmed",
                                  "receiptId": f"iso-{incident_id}"})

    def test_reports_on_hydraulically_connected_wells_merge(self):
        r1 = self.report(wells=["W-101"], depth=10, report_id="call-1")
        r2 = self.report(wells=["W-102"], depth=12, report_id="call-2")  # W-101/W-102 同管段
        self.assertEqual(r1["incidentId"], r2["incidentId"])
        view = self.svc.incident_view(r1["incidentId"])
        self.assertEqual(set(view["impactArea"]["wells"][i]["id"] for i in range(2)), {"W-101", "W-102"})
        self.assertEqual(view["reports"], ["call-1", "call-2"])

    def test_duplicate_report_id_is_idempotent(self):
        r1 = self.report(wells=["W-101"], report_id="call-x")
        r2 = self.report(wells=["W-101"], report_id="call-x")
        self.assertTrue(r2.get("idempotent"))
        self.assertEqual(r1["incidentId"], r2["incidentId"])
        self.assertEqual(len(self.svc.incidents[r1["incidentId"]]["reportIds"]), 1)

    def test_high_danger_requires_isolation_before_dispatch(self):
        r = self.report(compound="春风里小区", depth=60, report_id="call-h")
        inc_id = r["incidentId"]
        wo_id = self.svc.incidents[inc_id]["workOrderIds"][0]
        blocked = self.svc.dispatch(wo_id, "调度员甲")
        self.assertNotIn(True, (blocked.get("assigned"),))
        self.assertTrue(any("隔离" in b for b in blocked["blockedReasons"]))
        # 先隔离
        self.svc.dispatch_isolation(inc_id)
        # 隔离未确认仍不能派工
        blocked2 = self.svc.dispatch(wo_id, "调度员甲")
        self.assertTrue(any("隔离" in b for b in blocked2["blockedReasons"]))
        self.isolate_and_confirm(inc_id)
        res = self.svc.dispatch(wo_id, "调度员甲")
        self.assertTrue(res["assigned"])
        self.assertEqual(res["assignment"]["teamId"], "T-A")  # 高危资质只有猛虎班

    def test_two_dispatchers_only_one_result(self):
        r = self.report(compound="春风里小区", depth=60, report_id="call-c")
        inc_id = r["incidentId"]
        wo_id = self.svc.incidents[inc_id]["workOrderIds"][0]
        self.isolate_and_confirm(inc_id)
        results: list = []
        errors: list = []

        def dispatcher(name):
            try:
                results.append(self.svc.dispatch(wo_id, name, expected_version=0))
            except ConflictError as exc:
                errors.append(exc)

        threads = [threading.Thread(target=dispatcher, args=(f"调度员{i}",)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(len(results), 1)
        self.assertEqual(len(errors), 7)
        wo = self.svc.work_order_view(wo_id)
        self.assertEqual(len([h for h in self.svc.work_orders[wo_id]["history"]
                              if h["kind"] == "assigned"]), 1)
        self.assertEqual(wo["version"], 1)

    def test_dispatch_with_stale_version_conflicts(self):
        r = self.report(compound="柳岸庄小区", depth=30, report_id="call-m")
        wo_id = self.svc.incidents[r["incidentId"]]["workOrderIds"][0]
        self.assertTrue(self.svc.dispatch(wo_id, "甲", expected_version=0)["assigned"])
        with self.assertRaises(ConflictError):
            self.svc.dispatch(wo_id, "乙", expected_version=0)

    def test_arrived_work_order_survives_merge(self):
        # 春风里与五金厂先按不同事件派工，春风里到场后确认同源并单
        r1 = self.report(compound="春风里小区", depth=60, report_id="a")
        inc1 = r1["incidentId"]
        wo1 = self.svc.incidents[inc1]["workOrderIds"][0]
        self.isolate_and_confirm(inc1)
        self.svc.dispatch(wo1, "甲")
        self.svc.receive_receipt({"workOrderId": wo1, "teamId": "T-A", "type": "arrived",
                                  "receiptId": "rc-arr"})
        r2 = self.report(compound="五金厂区", depth=30, report_id="b")
        inc2 = r2["incidentId"]
        wo2 = self.svc.incidents[inc2]["workOrderIds"][0]
        self.assertNotEqual(inc1, inc2)
        merged = self.svc.merge_incidents(inc1, [inc2], reason="泵站顶水确认同源")
        self.assertEqual(merged["incidentId"], inc1)
        self.assertEqual(self.svc.work_orders[wo1]["status"], "arrived")  # 到场工单不取消
        self.assertEqual(self.svc.work_orders[wo2]["status"], "cancelled")  # 未到场重复工单取消
        self.assertEqual(self.svc.work_orders[wo1]["incidentId"], inc1)

    def test_road_closure_reroutes_and_records_basis(self):
        r = self.report(compound="柳岸庄小区", depth=30, report_id="road-1")
        wo_id = self.svc.incidents[r["incidentId"]]["workOrderIds"][0]
        self.svc.dispatch(wo_id, "甲")
        assignment = self.svc.work_orders[wo_id]["assignment"]
        self.assertEqual(assignment["routeEdges"], ["E1", "E3", "E4"])
        # 封闭 E3：T-B 的中型车没有替代路线
        closed = self.svc.close_road("E3", "下穿积水")
        self.assertEqual(closed["affectedWorkOrders"], [wo_id])
        wo = self.svc.work_orders[wo_id]
        self.assertTrue(any("E3" in b for b in wo["blockedReasons"]))
        reroutes = [h for h in wo["history"] if h["kind"] == "route_blocked"]
        self.assertEqual(len(reroutes), 1)
        self.assertIn("下穿积水", reroutes[0]["reason"])
        # 恢复通行后重算
        self.svc.reopen_road("E3")
        self.svc.close_road("E1", "路口塌陷")  # 仅剩路线也中断
        wo = self.svc.work_orders[wo_id]
        self.assertTrue(any("E1" in b for b in wo["blockedReasons"]))

    def test_vehicle_class_restriction_and_escalation_block(self):
        # E5 限中型及以下：中危五金厂事件由中型车 T-B 走 E5 到达
        r = self.report(compound="五金厂区", depth=30, report_id="veh-1")
        inc_id = r["incidentId"]
        wo_id = self.svc.incidents[inc_id]["workOrderIds"][0]
        res = self.svc.dispatch(wo_id, "甲")
        self.assertTrue(res["assigned"])
        self.assertEqual(res["assignment"]["vehicleClass"], "中型")
        self.assertEqual(res["assignment"]["routeEdges"][-1], "E5")
        # IoT 液位暴涨升级为高危：T-B 缺高危资质，唯一合格的大型车 T-A 又过不了 E5
        self.svc.ingest_measurement({"wellId": "W-301", "depthCm": 70, "measurementId": "veh-m"})
        self.assertEqual(self.svc.incidents[inc_id]["dangerLevel"], "high")
        wo = self.svc.work_order_view(wo_id)
        self.assertTrue(any("E5" in b for b in wo["blockedReasons"]))
        self.assertIn("teamId", self.svc.incidents[inc_id]["isolation"])  # 隔离已自动派出

    def test_no_available_team_blocks_dispatch(self):
        # 第一单中危派给唯一合适的中型班 T-B；第二单中危时：
        # T-B 忙、T-A 大型车受 E5 限行、T-C 缺资质 -> 全员不可用，阻塞
        r1 = self.report(compound="柳岸庄小区", depth=30, report_id="p1")
        wo1 = self.svc.incidents[r1["incidentId"]]["workOrderIds"][0]
        self.svc.dispatch(wo1, "甲")
        r2 = self.report(compound="五金厂区", depth=30, report_id="p2")
        wo2 = self.svc.incidents[r2["incidentId"]]["workOrderIds"][0]
        blocked = self.svc.dispatch(wo2, "甲")
        self.assertTrue(blocked.get("blocked"))
        self.assertTrue(blocked["blockedReasons"])

    def test_offline_receipts_inserted_by_occurrence_time_and_idempotent(self):
        r = self.report(compound="柳岸庄小区", depth=30, report_id="off-1")
        wo_id = self.svc.incidents[r["incidentId"]]["workOrderIds"][0]
        self.svc.dispatch(wo_id, "甲")
        base = "2026-09-22T09:{:02d}:00+08:00"
        # 先收 arrived（离线补传，发生时间 09:10），再补 enroute（09:05）和 signed（09:00）
        self.svc.receive_receipt({"workOrderId": wo_id, "teamId": "T-B", "type": "arrived",
                                  "occurredAt": base.format(10), "offline": True, "receiptId": "rc-a"})
        again = self.svc.receive_receipt({"workOrderId": wo_id, "teamId": "T-B", "type": "arrived",
                                          "occurredAt": base.format(10), "offline": True,
                                          "receiptId": "rc-a"})
        self.assertTrue(again["idempotent"])
        self.svc.receive_receipt({"workOrderId": wo_id, "teamId": "T-B", "type": "enroute",
                                  "occurredAt": base.format(5), "offline": True, "receiptId": "rc-e"})
        self.svc.receive_receipt({"workOrderId": wo_id, "teamId": "T-B", "type": "signed",
                                  "occurredAt": base.format(0), "receiptId": "rc-s"})
        receipts = [e for e in self.svc.timeline_view(wo_id)["entries"] if e["type"] == "receipt"]
        self.assertEqual([r_["receiptType"] for r_ in receipts],
                         ["signed", "enroute", "arrived"])  # 按发生时间，不是接收顺序
        self.assertEqual(self.svc.work_order_view(wo_id)["status"], "arrived")  # 状态只升不降

    def test_reminders_fire_and_survive_restart(self):
        r = self.report(compound="柳岸庄小区", depth=30, report_id="tm-1")
        wo_id = self.svc.incidents[r["incidentId"]]["workOrderIds"][0]
        self.clock.advance(16)  # 超过 15 分钟未签收
        fired = self.svc.scan_reminders()
        self.assertEqual(len(fired), 1)
        self.assertEqual(fired[0]["kind"], "sign_timeout")
        # 再次扫描不重复提醒
        self.assertEqual(self.svc.scan_reminders(), [])
        # 重启后提醒记录仍在，且不会重复
        svc2 = self.restart()
        self.assertEqual([x["kind"] for x in svc2.work_orders[wo_id]["reminders"]], ["sign_timeout"])
        self.assertEqual(svc2.scan_reminders(), [])

        # 派工后 30 分钟未到场 -> 到场超时
        self.assertTrue(svc2.dispatch(wo_id, "甲")["assigned"])
        svc2.receive_receipt({"workOrderId": wo_id, "teamId": "T-B", "type": "signed",
                              "receiptId": "tm-s"})
        self.clock.advance(31)
        arrive_reminders = svc2.scan_reminders()
        self.assertEqual(len(arrive_reminders), 1)
        self.assertEqual(arrive_reminders[0]["kind"], "arrive_timeout")
        svc3 = self.restart()
        kinds = [x["kind"] for x in svc3.work_orders[wo_id]["reminders"]]
        self.assertEqual(kinds, ["sign_timeout", "arrive_timeout"])

    def test_restart_restores_assignment_stock_and_timeline(self):
        r = self.report(compound="春风里小区", depth=60, report_id="persist-1")
        inc_id = r["incidentId"]
        wo_id = self.svc.incidents[inc_id]["workOrderIds"][0]
        self.isolate_and_confirm(inc_id)
        self.svc.dispatch(wo_id, "甲")
        self.svc.receive_receipt({"workOrderId": wo_id, "teamId": "T-A", "type": "enroute",
                                  "receiptId": "p-en"})
        svc2 = self.restart()
        wo = svc2.work_order_view(wo_id)
        self.assertEqual(wo["status"], "enroute")
        self.assertEqual(wo["assignment"]["teamId"], "T-A")
        self.assertEqual(svc2.team_stock["T-A"]["P-PUMP"], 0)  # 备件占用已恢复
        types = {e["type"] for e in svc2.timeline_view(wo_id)["entries"]}
        self.assertIn("report", types)
        self.assertIn("isolation", types)
        self.assertIn("decision", types)
        # 新井位号不与历史冲突
        r2 = svc2.ingest_report({"wellIds": ["W-201"], "depthCm": 5, "reportId": "persist-2"})
        self.assertTrue(r2["incidentId"].startswith("event-"))
        self.assertNotEqual(r2["incidentId"], inc_id)

    def test_iot_measurement_escalates_danger_level(self):
        r = self.report(wells=["W-101"], depth=10, report_id="iot-1")
        inc_id = r["incidentId"]
        self.assertEqual(self.svc.incidents[inc_id]["dangerLevel"], "low")
        self.svc.ingest_measurement({"wellId": "W-101", "depthCm": 55, "measurementId": "m1"})
        self.assertEqual(self.svc.incidents[inc_id]["dangerLevel"], "high")
        self.assertTrue(self.svc.incidents[inc_id]["isolation"]["required"])
        # 升级自动派出隔离班
        self.assertIn("teamId", self.svc.incidents[inc_id]["isolation"])

    def test_board_shows_impact_teams_eta_and_blockers(self):
        self.report(compound="柳岸庄小区", depth=30, report_id="b1")
        board = self.svc.board_view()
        self.assertEqual(len(board["incidents"]), 1)
        row = board["incidents"][0]
        self.assertEqual(row["impactArea"]["compounds"], ["柳岸庄小区"])
        self.assertTrue(any("迅龙班" in b for b in row["blockedReasons"]) or row["currentTeams"] == [])
        self.assertIsNotNone(row["queueRank"])

    def test_unknown_well_rejected(self):
        with self.assertRaises(ValidationError):
            self.svc.ingest_report({"wellIds": ["W-999"], "depthCm": 3})

    def test_receipt_wrong_team_rejected(self):
        r = self.report(compound="柳岸庄小区", depth=30, report_id="wt-1")
        wo_id = self.svc.incidents[r["incidentId"]]["workOrderIds"][0]
        self.svc.dispatch(wo_id, "甲")
        with self.assertRaises(ValidationError):
            self.svc.receive_receipt({"workOrderId": wo_id, "teamId": "T-C", "type": "arrived",
                                      "receiptId": "wt-x"})

    def test_full_lifecycle_from_call_to_recovery(self):
        r = self.report(compound="春风里小区", depth=70, report_id="life-1")
        inc_id = r["incidentId"]
        wo_id = self.svc.incidents[inc_id]["workOrderIds"][0]
        self.isolate_and_confirm(inc_id)
        self.svc.dispatch(wo_id, "甲")
        for i, stage in enumerate(["signed", "enroute", "arrived", "resolved"]):
            self.svc.receive_receipt({"workOrderId": wo_id, "teamId": "T-A", "type": stage,
                                      "receiptId": f"life-{i}"})
        self.svc.receive_receipt({"workOrderId": wo_id, "incidentId": inc_id, "teamId": "T-A",
                                  "type": "recovered", "receiptId": "life-rec"})
        self.assertEqual(self.svc.incidents[inc_id]["status"], "recovered")
        timeline = self.svc.timeline_view(wo_id)["entries"]
        self.assertEqual(timeline[0]["type"], "report")  # 始于报修
        self.assertEqual(timeline[-1]["receiptType"], "recovered")  # 终于恢复
        # 恢复后同分量新报修开新事件
        r2 = self.report(wells=["W-101"], depth=8, report_id="life-2")
        self.assertNotEqual(r2["incidentId"], inc_id)


if __name__ == "__main__":
    unittest.main()
