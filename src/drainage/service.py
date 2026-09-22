"""抢修调度核心：事件合并、危险分级、隔离、派工、改派与提醒。

所有变更以事件形式落盘（见 store），命令在同一把锁内执行，
派工采用工单版本号 CAS：两个调度员并发派工只有一个成功。
"""
from __future__ import annotations

import threading
from collections import defaultdict
from datetime import timedelta

from . import timeutil
from .reference import VEHICLE_CLASS_RANK
from .routing import RoutePlanner

DANGER_RANK = {"high": 3, "medium": 2, "low": 1}
PHASE_RANK = {"signed": 1, "enroute": 2, "arrived": 3, "resolved": 4, "recovered": 5}
PHASE_STATUS = {v: k for k, v in PHASE_RANK.items()}
TERMINAL = {"resolved", "recovered", "cancelled"}
ARRIVED_PHASES = {"arrived", "resolved", "recovered"}


class ConflictError(Exception):
    """并发冲突（版本号过期）。"""


class ValidationError(Exception):
    """请求数据不合法。"""


class DispatchService:
    def __init__(self, reference, store, reminder_sink=None, clock=None):
        self.ref = reference
        self.store = store
        self.routes = RoutePlanner(reference)
        self._lock = threading.RLock()
        self._reminder_sink = reminder_sink or (lambda payload: None)
        self._clock = clock or timeutil.now

        self.incidents: dict[str, dict] = {}
        self.work_orders: dict[str, dict] = {}
        self.reports: dict[str, dict] = {}
        self.measurements: dict[str, dict] = {}
        self.receipts: dict[str, dict] = {}
        self.closed_edges: set[str] = set()
        self.closures: list[dict] = []
        self.team_stock: dict[str, dict[str, int]] = {}
        self.isolation_busy: dict[str, str] = {}  # incidentId -> teamId（隔离未确认期间占用）
        self.component_owner: dict[str, str] = {}  # 水力分量 -> 在处事件（并单后指向存留事件）
        self._seq: dict[str, int] = defaultdict(int)
        self._event_seq = 0  # 全局事件序，重启由日志重放恢复
        # 库存基线：车辆装载；重放派工/回执事件时在此基础上扣减
        for team in reference.teams.values():
            self.team_stock[team["id"]] = dict(reference.vehicles[team["vehicleId"]]["parts"])
        self._replay()

    def _bump_from_id(self, prefix: str, identifier: str):
        try:
            num = int(str(identifier).split("-")[1])
        except (IndexError, ValueError):
            return
        self._seq[prefix] = max(self._seq[prefix], num)

    # ------------------------------------------------------------------ 重放

    def _replay(self):
        for event in self.store.replay():
            self._apply(event)
            self._event_seq = max(self._event_seq, event.get("_seq", 0))

    def _next_id(self, prefix: str) -> str:
        self._seq[prefix] += 1
        return f"{prefix}-{self._seq[prefix]:04d}"

    def _emit(self, event: dict) -> dict:
        self._event_seq += 1
        event["_seq"] = self._event_seq
        self.store.append(event)
        self._apply(event)
        return event

    def _apply(self, e: dict):  # noqa: C901 - 事件类型分发
        t = e["type"]
        if t == "report_received":
            self.reports[e["reportId"]] = e
            self._bump_from_id("R", e["reportId"])
        elif t == "measurement_received":
            self.measurements[e["measurementId"]] = e
            self._bump_from_id("M", e["measurementId"])
        elif t == "incident_opened":
            self._incident_from_event(e)
            self._bump_from_id("event", e["incidentId"])
        elif t == "incident_merged":
            survivor = self.incidents[e["incidentId"]]
            for absorbed_id in e["absorbed"]:
                absorbed = self.incidents[absorbed_id]
                survivor["wellIds"] = sorted(set(survivor["wellIds"]) | set(absorbed["wellIds"]))
                survivor["components"] = sorted(set(survivor["components"]) | set(absorbed["components"]))
                survivor["reportIds"] = list(dict.fromkeys(survivor["reportIds"] + absorbed["reportIds"]))
                survivor["measurementIds"] = list(dict.fromkeys(survivor["measurementIds"] + absorbed["measurementIds"]))
                survivor["workOrderIds"] = list(dict.fromkeys(survivor["workOrderIds"] + absorbed["workOrderIds"]))
                survivor["depthCm"] = max(survivor["depthCm"], absorbed["depthCm"])
                absorbed["mergedInto"] = e["incidentId"]
                absorbed["status"] = "merged"
                # 被并事件的工单归属到存留事件
                for wo_id in absorbed["workOrderIds"]:
                    if wo_id in self.work_orders:
                        self.work_orders[wo_id]["incidentId"] = e["incidentId"]
            for comp in survivor["components"]:
                self.component_owner[comp] = survivor["id"]
            new_level = self.ref.danger_rule(survivor["depthCm"])["level"]
            if DANGER_RANK[new_level] > DANGER_RANK[survivor["dangerLevel"]]:
                survivor["dangerLevel"] = new_level
                if new_level == "high":
                    survivor["isolation"]["required"] = True
            for wo_id in survivor["workOrderIds"]:
                wo = self.work_orders.get(wo_id)
                if wo and wo["status"] != "cancelled":
                    wo["history"].append({"kind": "incident_merged", "at": e["at"],
                                          "absorbed": e["absorbed"], "reason": e.get("reason", ""),
                                          "_seq": e["_seq"]})
        elif t == "incident_updated":
            inc = self.incidents[e["incidentId"]]
            inc["depthCm"] = e["depthCm"]
            inc["dangerLevel"] = e["dangerLevel"]
            for key in ("wellIds", "reportIds", "measurementIds"):
                if key in e:
                    inc[key] = e[key]
        elif t == "danger_escalated":
            inc = self.incidents[e["incidentId"]]
            inc["dangerLevel"] = e["to"]
            if e["to"] == "high":
                inc["isolation"]["required"] = True
            for wo_id in inc["workOrderIds"]:
                wo = self.work_orders[wo_id]
                wo["dangerLevel"] = e["to"]
                if "depthCm" in e:
                    wo["depthCm"] = e["depthCm"]
        elif t == "incident_recovered":
            inc = self.incidents[e["incidentId"]]
            inc["status"] = "recovered"
            inc["recoveredAt"] = e["at"]
            for comp, owner in list(self.component_owner.items()):
                if owner == e["incidentId"]:
                    self.component_owner.pop(comp)
        elif t == "road_closed":
            self.closed_edges.add(e["edgeId"])
            self.closures.append(e)
        elif t == "road_reopened":
            self.closed_edges.discard(e["edgeId"])
            self.closures.append(e)
        elif t == "isolation_dispatched":
            self.incidents[e["incidentId"]]["isolation"] = {
                "required": True, "teamId": e["teamId"], "dispatchedAt": e["at"], "confirmed": False,
                "dispatchedSeq": e["_seq"],
            }
            self.isolation_busy[e["incidentId"]] = e["teamId"]
        elif t == "isolation_confirmed":
            self.incidents[e["incidentId"]]["isolation"]["confirmed"] = True
            self.incidents[e["incidentId"]]["isolation"]["confirmedAt"] = e["at"]
            self.incidents[e["incidentId"]]["isolation"]["confirmedSeq"] = e["_seq"]
            self.isolation_busy.pop(e["incidentId"], None)
        elif t == "work_order_created":
            self.work_orders[e["workOrderId"]] = {
                "id": e["workOrderId"], "incidentId": e["incidentId"], "dangerLevel": e["dangerLevel"],
                "depthCm": e["depthCm"], "status": "created", "version": 0,
                "assignment": None, "history": [], "timeline": [], "blockedReasons": e.get("blockedReasons", []),
                "signDeadline": e.get("signDeadline"), "arriveDeadline": None, "reminders": [],
                "createdAt": e["at"],
            }
            self.incidents[e["incidentId"]]["workOrderIds"] = list(dict.fromkeys(
                self.incidents[e["incidentId"]].get("workOrderIds", []) + [e["workOrderId"]]))
            self._bump_from_id("WO", e["workOrderId"])
        elif t == "work_order_assigned":
            wo = self.work_orders[e["workOrderId"]]
            wo["assignment"] = e["assignment"]
            wo["version"] = e["version"]
            wo["status"] = "assigned"
            wo["blockedReasons"] = []
            wo["signDeadline"] = None
            wo["arriveDeadline"] = e.get("arriveDeadline")
            for part_id, qty in e["assignment"].get("reservedParts", {}).items():
                self.team_stock[e["assignment"]["teamId"]][part_id] -= qty
            wo["history"].append({"kind": "assigned", "at": e["at"], "dispatcher": e.get("dispatcher"),
                                  "assignment": e["assignment"], "version": e["version"],
                                  "_seq": e["_seq"]})
        elif t == "dispatch_blocked":
            wo = self.work_orders[e["workOrderId"]]
            wo["blockedReasons"] = e["blockedReasons"]
            wo["history"].append({"kind": "blocked", "at": e["at"], "dispatcher": e.get("dispatcher"),
                                  "blockedReasons": e["blockedReasons"], "_seq": e["_seq"]})
        elif t == "dispatch_rejected":
            self.work_orders[e["workOrderId"]]["history"].append(
                {"kind": "rejected", "at": e["at"], "dispatcher": e.get("dispatcher"), "reason": e["reason"],
                 "_seq": e["_seq"]})
        elif t == "work_order_route_blocked":
            wo = self.work_orders[e["workOrderId"]]
            wo["blockedReasons"] = e.get("blockers", wo["blockedReasons"])
            wo["history"].append({"kind": "route_blocked", "at": e["at"], "reason": e["reason"],
                                  "_seq": e["_seq"]})
        elif t == "work_order_escalation_gap":
            self.work_orders[e["workOrderId"]]["history"].append(
                {"kind": "escalation_gap", "at": e["at"],
                 "missingQualifications": e["missingQualifications"],
                 "missingParts": e["missingParts"], "_seq": e["_seq"]})
        elif t == "queue_reordered":
            for wo_id in e["affectedWorkOrderIds"]:
                wo = self.work_orders.get(wo_id)
                if wo:
                    wo["history"].append(
                        {"kind": "queue_reordered", "at": e["at"], "edgeId": e["edgeId"],
                         "reason": e["reason"], "rankBefore": e["before"].index(wo_id)
                         if wo_id in e["before"] else None,
                         "rankAfter": e["after"].index(wo_id) if wo_id in e["after"] else None,
                         "queueBefore": e["before"], "queueAfter": e["after"], "_seq": e["_seq"]})
        elif t == "work_order_blockers_cleared":
            wo = self.work_orders[e["workOrderId"]]
            wo["blockedReasons"] = []
            wo["history"].append({"kind": "blockers_cleared", "at": e["at"],
                                  "reason": e["reason"], "_seq": e["_seq"]})
        elif t == "work_order_reassigned":
            wo = self.work_orders[e["workOrderId"]]
            old = e["oldAssignment"]
            for part_id, qty in old.get("reservedParts", {}).items():
                self.team_stock[old["teamId"]][part_id] += qty
            wo["assignment"] = e["assignment"]
            wo["version"] = e["version"]
            wo["status"] = "assigned"
            for part_id, qty in e["assignment"].get("reservedParts", {}).items():
                self.team_stock[e["assignment"]["teamId"]][part_id] -= qty
            wo["history"].append({"kind": "reassigned", "at": e["at"], "reason": e.get("reason"),
                                  "oldAssignment": old, "assignment": e["assignment"],
                                  "version": e["version"], "_seq": e["_seq"]})
        elif t == "work_order_cancelled":
            wo = self.work_orders[e["workOrderId"]]
            if wo["assignment"]:
                for part_id, qty in wo["assignment"].get("reservedParts", {}).items():
                    self.team_stock[wo["assignment"]["teamId"]][part_id] += qty
            wo["status"] = "cancelled"
            wo["cancelReason"] = e.get("reason", "")
            wo["history"].append({"kind": "cancelled", "at": e["at"], "reason": e.get("reason"),
                                  "_seq": e["_seq"]})
        elif t == "reroute_reordered":
            wo = self.work_orders[e["workOrderId"]]
            wo["assignment"] = e["assignment"]
            wo["history"].append({"kind": "reroute", "at": e["at"], "reason": e["reason"],
                                  "oldAssignment": e["oldAssignment"], "assignment": e["assignment"],
                                  "reorderBasis": e["reorderBasis"], "_seq": e["_seq"]})
        elif t == "receipt_recorded":
            self.receipts[e["receiptId"]] = e
            wo = self.work_orders.get(e["workOrderId"])
            if wo:
                wo["timeline"].append(self._receipt_timeline_entry(e))
                rank = PHASE_RANK.get(e["receiptType"])
                if rank and rank > PHASE_RANK.get(wo["status"], 0):
                    wo["status"] = PHASE_STATUS[rank]
                    if e["receiptType"] == "signed":
                        wo["arriveDeadline"] = e.get("arriveDeadline")
                if e["receiptType"] == "parts_used":
                    for part_id, qty in (e.get("parts") or {}).items():
                        self.team_stock[e["teamId"]][part_id] = max(
                            0, self.team_stock[e["teamId"]].get(part_id, 0) - qty)
            if e["receiptType"] == "isolation_confirmed":
                isolation = self.incidents[e["incidentId"]]["isolation"]
                isolation["confirmed"] = True
                isolation["confirmedAt"] = e["occurredAt"]
                isolation["confirmedSeq"] = e["_seq"]
                self.isolation_busy.pop(e["incidentId"], None)
            if e["receiptType"] == "recovered" and e.get("incidentId"):
                inc = self.incidents[e["incidentId"]]
                inc["status"] = "recovered"
                inc["recoveredAt"] = e["occurredAt"]
                for comp, owner in list(self.component_owner.items()):
                    if owner == e["incidentId"]:
                        self.component_owner.pop(comp)
        elif t == "reminder_fired":
            wo = self.work_orders[e["workOrderId"]]
            wo["reminders"].append({"kind": e["kind"], "at": e["at"], "message": e["message"],
                                    "_seq": e["_seq"]})

    def _incident_from_event(self, e: dict):
        self.incidents[e["incidentId"]] = {
            "id": e["incidentId"], "wellIds": list(e["wellIds"]),
            "components": list(e["components"]), "depthCm": e["depthCm"],
            "dangerLevel": e["dangerLevel"], "reportIds": list(e.get("reportIds", [])),
            "measurementIds": list(e.get("measurementIds", [])), "workOrderIds": [],
            "status": "active", "isolation": {"required": e["dangerLevel"] == "high",
                                              "confirmed": False},
            "createdAt": e["at"], "openedSeq": e.get("_seq", 0),
        }
        for comp in e["components"]:
            self.component_owner[comp] = e["incidentId"]

    @staticmethod
    def _receipt_timeline_entry(e: dict) -> dict:
        return {"at": e["occurredAt"], "receivedAt": e["receivedAt"], "type": "receipt",
                "receiptType": e["receiptType"], "teamId": e["teamId"], "note": e.get("note", ""),
                "offline": e.get("offline", False), "receiptId": e["receiptId"],
                "_seq": e["_seq"]}

    # ------------------------------------------------------------- 公众报修/IoT

    def ingest_report(self, payload: dict) -> dict:
        with self._lock:
            occurred = timeutil.parse(payload.get("occurredAt") or timeutil.iso(self._clock()))
            received = self._clock()
            wells = self._resolve_wells(payload)
            depth = int(payload.get("depthCm", 0))
            if depth < 0:
                raise ValidationError("depthCm 不能为负")
            report_id = payload.get("reportId") or self._next_id("R")
            if report_id in self.reports:
                incident_id = self._incident_id_of_report(report_id)
                result = {"idempotent": True, "report": self._report_view(self.reports[report_id])}
                if incident_id:
                    result["incidentId"] = incident_id
                return result
            event = {"type": "report_received", "reportId": report_id,
                     "source": payload.get("source", "公众来电"), "wellIds": wells,
                     "depthCm": depth, "occurredAt": timeutil.iso(occurred),
                     "receivedAt": timeutil.iso(received), "note": payload.get("note", "")}
            self._emit(event)
            incident = self._attach_to_incident(wells=wells, depth=depth, report_id=report_id,
                                                measurement_id=None, at=timeutil.iso(received))
            return {"report": self._report_view(event), "incidentId": incident["id"]}

    def ingest_measurement(self, payload: dict) -> dict:
        with self._lock:
            well_id = payload.get("wellId")
            if not well_id:
                raise ValidationError("需要提供 wellId")
            if well_id not in self.ref.wells:
                raise ValidationError(f"未知井位: {well_id}")
            occurred = timeutil.parse(payload.get("occurredAt") or timeutil.iso(self._clock()))
            received = self._clock()
            depth = int(payload["depthCm"])
            if depth < 0:
                raise ValidationError("depthCm 不能为负")
            mid = payload.get("measurementId") or self._next_id("M")
            if mid in self.measurements:
                return {"idempotent": True, "measurement": self.measurements[mid]}
            event = {"type": "measurement_received", "measurementId": mid, "wellId": well_id,
                     "depthCm": depth, "occurredAt": timeutil.iso(occurred),
                     "receivedAt": timeutil.iso(received)}
            self._emit(event)
            incident = self._attach_to_incident(wells=[well_id], depth=depth, report_id=None,
                                                measurement_id=mid, at=timeutil.iso(received))
            return {"measurement": event, "incidentId": incident["id"]}

    def _incident_id_of_report(self, report_id: str) -> str | None:
        for inc in self.incidents.values():
            if report_id in inc["reportIds"]:
                return inc["id"]
        return None

    def _resolve_wells(self, payload: dict) -> list[str]:
        wells = payload.get("wellIds")
        if wells:
            try:
                return self.ref.well_ids(wells)
            except ValueError as exc:
                raise ValidationError(str(exc)) from exc
        compound = payload.get("compound")
        if compound:
            resolved = [w["id"] for w in self.ref.raw["wells"] if w.get("compound") == compound]
            if not resolved:
                raise ValidationError(f"未知小区: {compound}")
            return sorted(resolved)
        raise ValidationError("需要提供 wellIds 或 compound")

    def _components_of(self, wells: list[str]) -> list[str]:
        return sorted({self.ref.well_component[w] for w in wells})

    def _attach_to_incident(self, wells: list[str], depth: int, report_id: str | None,
                            measurement_id: str | None, at: str) -> dict:
        components = set(self._components_of(wells))
        owner_ids = {self.component_owner[c] for c in components if c in self.component_owner}
        open_incidents = [self.incidents[i] for i in owner_ids
                          if self.incidents.get(i, {}).get("status") == "active"]
        if not open_incidents:
            return self._open_incident(wells, depth, report_id, measurement_id, at)

        # 一次报修可能连通多个在处事件 -> 合并到同一影响范围
        survivor = open_incidents[0]
        absorbed = open_incidents[1:]
        if absorbed:
            self._merge_incidents(survivor, absorbed, at)
        return self._update_incident(survivor, wells, depth, report_id, measurement_id, at)

    def merge_incidents(self, target_id: str, source_ids: list[str],
                        reason: str = "现场确认同一影响范围") -> dict:
        """显式并单：调度员确认多个事件属同一影响范围（如泵站顶水）。"""
        with self._lock:
            survivor = self.incidents.get(target_id)
            if not survivor or survivor["status"] != "active":
                raise ValidationError("存留事件不存在或已结束")
            if not source_ids:
                raise ValidationError("需要提供并入的事件ID")
            absorbed = []
            for sid in dict.fromkeys(source_ids):
                if sid == target_id:
                    continue
                inc = self.incidents.get(sid)
                if not inc or inc["status"] != "active":
                    raise ValidationError(f"事件 {sid} 不存在或已结束，不能并入")
                absorbed.append(inc)
            if not absorbed:
                return {"idempotent": True, "incidentId": target_id}
            old_level = survivor["dangerLevel"]
            at = timeutil.iso(self._clock())
            self._merge_incidents(survivor, absorbed, at, reason=reason)
            new_level = survivor["dangerLevel"]
            if DANGER_RANK[new_level] > DANGER_RANK[old_level]:
                self._emit({"type": "danger_escalated", "incidentId": survivor["id"],
                            "from": old_level, "to": new_level,
                            "depthCm": survivor["depthCm"], "at": at})
                self._handle_escalation(survivor, at)
            return {"incidentId": survivor["id"], "merged": [i["id"] for i in absorbed],
                    "reason": reason}

    def _open_incident(self, wells, depth, report_id, measurement_id, at) -> dict:
        incident_id = self._next_id("event")
        rule = self.ref.danger_rule(depth)
        event = {"type": "incident_opened", "incidentId": incident_id, "wellIds": sorted(wells),
                 "components": self._components_of(wells), "depthCm": depth,
                 "dangerLevel": rule["level"], "at": at,
                 "reportIds": [report_id] if report_id else [],
                 "measurementIds": [measurement_id] if measurement_id else []}
        self._emit(event)
        self._create_work_order(self.incidents[incident_id], at)
        return self.incidents[incident_id]

    def _merge_incidents(self, survivor: dict, absorbed: list[dict], at: str, reason: str = ""):
        all_ids = list(survivor["workOrderIds"]) + [w for i in absorbed for w in i["workOrderIds"]]
        active = [self.work_orders[w] for w in dict.fromkeys(all_ids)
                  if self.work_orders[w]["status"] not in TERMINAL]
        arrived = [wo for wo in active if wo["status"] in ARRIVED_PHASES]
        pre_arrival = [wo for wo in active if wo["status"] not in ARRIVED_PHASES]
        # 已到场工单一律保留；若已有到场力量处置，到场前的重复工单全部取消；
        # 否则在到场前工单中保留进度最靠前/预计最早到的一条
        keeper = None
        if pre_arrival and not arrived:
            keeper = max(pre_arrival, key=lambda wo: (
                PHASE_RANK.get(wo["status"], 0),
                wo["assignment"]["etaEarliest"] if wo["assignment"] else "9999"))
        for wo in pre_arrival:
            if wo is not keeper:
                self._cancel_wo(
                    wo["id"],
                    f"影响范围合并，并入事件 {survivor['id']}"
                    f"{('，保留 ' + keeper['id']) if keeper else '，到场力量已处置'}"
                    f"{('：' + reason) if reason else ''}",
                    at)
        self._emit({"type": "incident_merged", "incidentId": survivor["id"],
                    "absorbed": [i["id"] for i in absorbed], "at": at,
                    "reason": reason, "keptWorkOrderId": keeper["id"] if keeper else None})

    def _update_incident(self, inc: dict, wells, depth, report_id, measurement_id, at) -> dict:
        old_level = inc["dangerLevel"]
        new_wells = sorted(set(inc["wellIds"]) | set(wells))
        new_reports = list(dict.fromkeys(inc["reportIds"] + ([report_id] if report_id else [])))
        new_meas = list(dict.fromkeys(inc["measurementIds"] + ([measurement_id] if measurement_id else [])))
        new_depth = max(inc["depthCm"], depth)
        new_level = self.ref.danger_rule(new_depth)["level"]
        self._emit({"type": "incident_updated", "incidentId": inc["id"], "wellIds": new_wells,
                    "reportIds": new_reports, "measurementIds": new_meas,
                    "depthCm": new_depth, "dangerLevel": new_level, "at": at})
        if DANGER_RANK[new_level] > DANGER_RANK[old_level]:
            self._emit({"type": "danger_escalated", "incidentId": inc["id"],
                        "from": old_level, "to": new_level, "depthCm": new_depth, "at": at})
            self._handle_escalation(inc, at)
        return inc

    def _handle_escalation(self, inc: dict, at: str):
        """危险升级：未到场工单重新评估资质/备件；已到场工单保留班组，记录能力缺口。"""
        rule = self.ref.danger_rule(inc["depthCm"])
        if inc["dangerLevel"] == "high" and not inc["isolation"].get("confirmed") \
                and "teamId" not in inc["isolation"]:
            # 升级为高危且尚未派出隔离：自动派隔离班（派不出则阻塞原因体现在待派工单）
            try:
                self.dispatch_isolation(inc["id"], dispatcher="system:escalation")
            except ValidationError:
                pass
        for wo_id in list(inc["workOrderIds"]):
            wo = self.work_orders[wo_id]
            if wo["status"] in TERMINAL:
                continue
            assignment = wo["assignment"]
            if not assignment:
                wo["dangerLevel"] = inc["dangerLevel"]
                wo["depthCm"] = inc["depthCm"]
                if rule["isolationRequired"] and not inc["isolation"].get("confirmed"):
                    wo["blockedReasons"] = ["高危事件须先完成隔离警戒，待隔离确认后派工"]
                else:
                    self._find_candidate(wo)  # 刷新阻塞原因，等待调度员派工
                continue
            team = self.ref.teams[assignment["teamId"]]
            missing_quals = [q for q in rule["requiredQualifications"] if q not in team["qualifications"]]
            missing_parts = [p for p in rule["requiredParts"]
                             if self.team_stock[team["id"]].get(p, 0) < 1 and p not in assignment["reservedParts"]]
            if wo["status"] in ARRIVED_PHASES:
                if missing_quals or missing_parts:
                    self._emit({"type": "work_order_escalation_gap", "workOrderId": wo["id"],
                                "at": at, "missingQualifications": missing_quals,
                                "missingParts": missing_parts})
                continue
            if missing_quals or missing_parts:
                candidate = self._find_candidate(wo, exclude={team["id"]})
                if candidate:
                    self._reassign(wo, candidate, f"危险升级至{inc['dangerLevel']}，原班组资质/备件不足", at)
                else:
                    blockers = [f"危险升级，{team['name']}缺口: "
                                f"资质{missing_quals or '无'} 备件{missing_parts or '无'}"] + wo["blockedReasons"]
                    self._emit({"type": "dispatch_blocked", "workOrderId": wo["id"], "at": at,
                                "dispatcher": "system:escalation", "blockedReasons": blockers})

    # --------------------------------------------------------------- 工单与派工

    def _create_work_order(self, inc: dict, at: str):
        wo_id = self._next_id("WO")
        rule = self.ref.danger_rule(inc["depthCm"])
        blocked = []
        if rule["isolationRequired"] and not inc["isolation"]["confirmed"]:
            blocked.append("高危事件须先完成隔离警戒，待隔离确认后派工")
        event = {"type": "work_order_created", "workOrderId": wo_id, "incidentId": inc["id"],
                 "dangerLevel": inc["dangerLevel"], "depthCm": inc["depthCm"], "at": at,
                 "blockedReasons": blocked,
                 "signDeadline": timeutil.iso(timeutil.parse(at) + timedelta(
                     minutes=self.ref.rules["reminders"]["signTimeoutMinutes"]))}
        self._emit(event)
        # 工单等待调度员派工；高危工单的阻塞原因为隔离未确认
        if blocked:
            return
        # 预计算一次候选队伍与阻塞原因供看板展示，不产生派工结果
        self._find_candidate(self.work_orders[wo_id])

    def dispatch(self, wo_id: str, dispatcher: str, expected_version: int | None = None) -> dict:
        """调度员派工；expected_version 为读到的工单版本，过期则 409。

        已有派工结果的工单再次派工一律 409——两个调度员同时处理只能有一个结果。
        """
        with self._lock:
            wo = self._require_wo(wo_id)
            if wo["status"] in TERMINAL:
                raise ValidationError(f"工单已终结：{wo['status']}")
            if wo["assignment"] is not None:
                self._emit({"type": "dispatch_rejected", "workOrderId": wo_id, "at": timeutil.iso(self._clock()),
                            "dispatcher": dispatcher, "reason": "工单已有派工结果"})
                raise ConflictError(f"工单{wo_id}已派给{wo['assignment']['teamName']}，不能重复派工")
            if expected_version is not None and expected_version != wo["version"]:
                self._emit({"type": "dispatch_rejected", "workOrderId": wo_id, "at": timeutil.iso(self._clock()),
                            "dispatcher": dispatcher,
                            "reason": f"版本冲突：期望{expected_version}，当前{wo['version']}"})
                raise ConflictError(f"工单已被其他调度员处理（版本 {wo['version']}）")
            return self._try_assign(wo, dispatcher=dispatcher, at=timeutil.iso(self._clock()), explicit=True)

    def _try_assign(self, wo: dict, dispatcher: str, at: str, explicit: bool = False) -> dict:
        inc = self.incidents[wo["incidentId"]]
        rule = self.ref.danger_rule(inc["depthCm"])
        if rule["isolationRequired"] and not inc["isolation"]["confirmed"]:
            blocked = ["高危事件须先完成隔离警戒，待隔离确认后派工"]
            self._emit({"type": "dispatch_blocked", "workOrderId": wo["id"], "at": at,
                        "dispatcher": dispatcher, "blockedReasons": blocked})
            return {"blocked": True, "blockedReasons": blocked}
        candidate = self._find_candidate(wo)
        if not candidate:
            self._emit({"type": "dispatch_blocked", "workOrderId": wo["id"], "at": at,
                        "dispatcher": dispatcher, "blockedReasons": wo["blockedReasons"] or ["暂无可用队伍"]})
            result = {"blocked": True, "blockedReasons": wo["blockedReasons"]}
            if explicit:
                return result
            return result
        assignment = self._build_assignment(candidate, inc, at)
        version = wo["version"] + 1
        arrive_deadline = timeutil.iso(timeutil.parse(at) + timedelta(
            minutes=self.ref.rules["reminders"]["arriveTimeoutMinutes"]))
        self._emit({"type": "work_order_assigned", "workOrderId": wo["id"], "at": at,
                    "dispatcher": dispatcher, "version": version, "assignment": assignment,
                    "arriveDeadline": arrive_deadline})
        return {"assigned": True, "assignment": assignment, "version": version}

    def _active_team_ids(self) -> set[str]:
        busy = set(self.isolation_busy.values())
        for wo in self.work_orders.values():
            if wo["status"] not in TERMINAL and wo["assignment"]:
                busy.add(wo["assignment"]["teamId"])
        return busy

    def _find_candidate(self, wo: dict, exclude: set[str] | None = None):
        inc = self.incidents[wo["incidentId"]]
        rule = self.ref.danger_rule(inc["depthCm"])
        target_node = self._incident_node(inc)
        moment = self._clock()
        blockers: list[str] = []
        candidates = []
        busy = self._active_team_ids()
        for team in self.ref.teams.values():
            if exclude and team["id"] in exclude:
                continue
            if team["id"] in busy:
                blockers.append(f"{team['name']}({team['id']}):正在执行其他工单")
                continue
            if not timeutil.on_shift(team["shift"], moment):
                blockers.append(f"{team['name']}({team['id']}):不在班次")
                continue
            missing_q = [q for q in rule["requiredQualifications"] if q not in team["qualifications"]]
            if missing_q:
                blockers.append(f"{team['name']}({team['id']}):缺资质{','.join(missing_q)}")
                continue
            stock = self.team_stock.setdefault(team["id"], dict(self.ref.vehicles[team["vehicleId"]]["parts"]))
            missing_p = [p for p in rule["requiredParts"] if stock.get(p, 0) < 1]
            if missing_p:
                labels = [self.ref.parts[p]["label"] for p in missing_p]
                blockers.append(f"{team['name']}({team['id']}):缺备件{','.join(labels)}")
                continue
            vehicle = self.ref.vehicles[team["vehicleId"]]
            route = self.routes.shortest("D", target_node, vehicle["class"], self.closed_edges)
            if route is None:
                blockers.append(f"{team['name']}({team['id']}):无可达路线({';'.join(self._edge_blockers(target_node, vehicle['class']))})")
                continue
            extra_quals = len(set(team["qualifications"]) - set(rule["requiredQualifications"]))
            candidates.append((extra_quals, route[0], team["id"], team, vehicle, route))
        if not candidates:
            wo["blockedReasons"] = blockers
            return None
        wo["blockedReasons"] = []
        # 优先资质刚好匹配的队伍（把高危班等稀缺资质留给更高危事件），再比路程
        candidates.sort(key=lambda c: (c[0], c[1], c[2]))
        extra, distance, _tid, team, vehicle, route = candidates[0]
        return {"team": team, "vehicle": vehicle, "route": route, "distance": distance}

    def _edge_blockers(self, target_node: str, vehicle_class: str) -> list[str]:
        # 可达性失败时给出与目标相关的阻塞原因
        reasons = []
        for edge in self.ref.edges.values():
            if edge["id"] in self.closed_edges:
                reasons.append(f"{edge['id']}封闭")
            elif edge.get("maxClass") and VEHICLE_CLASS_RANK[vehicle_class] > VEHICLE_CLASS_RANK[edge["maxClass"]]:
                reasons.append(f"{edge['id']}限{edge['maxClass']}")
        return reasons

    def _incident_node(self, inc: dict) -> str:
        for well_id in inc["wellIds"]:
            node = self.ref.wells[well_id].get("node")
            if node:
                return node
        raise ValidationError("影响范围无道路节点")

    def _build_assignment(self, candidate: dict, inc: dict, at: str) -> dict:
        team, vehicle = candidate["team"], candidate["vehicle"]
        distance, edges, edge_blockers = candidate["route"]
        rule = self.ref.danger_rule(inc["depthCm"])
        reserved = {p: 1 for p in rule["requiredParts"]}
        earliest, latest = timeutil.eta_window(
            timeutil.parse(at), distance, self.ref.rules["travelSpeedMPerMin"],
            tuple(self.ref.rules["etaSlackMinutes"]))
        return {"teamId": team["id"], "teamName": team["name"], "vehicleId": vehicle["id"],
                "vehicleClass": vehicle["class"], "routeEdges": edges, "distanceM": distance,
                "etaEarliest": earliest, "etaLatest": latest, "decidedAt": at,
                "reservedParts": reserved, "routeBlockers": edge_blockers}

    def _reassign(self, wo: dict, candidate: dict, reason: str, at: str):
        old = wo["assignment"]
        assignment = self._build_assignment(candidate, self.incidents[wo["incidentId"]], at)
        self._emit({"type": "work_order_reassigned", "workOrderId": wo["id"], "at": at,
                    "reason": reason, "oldAssignment": old, "assignment": assignment,
                    "version": wo["version"] + 1})

    def _cancel_wo(self, wo_id: str, reason: str, at: str):
        self._emit({"type": "work_order_cancelled", "workOrderId": wo_id, "at": at, "reason": reason})

    # ----------------------------------------------------------------- 隔离

    def dispatch_isolation(self, incident_id: str, dispatcher: str = "调度员") -> dict:
        with self._lock:
            inc = self.incidents.get(incident_id)
            if not inc:
                raise ValidationError("事件不存在")
            if inc["isolation"].get("confirmed"):
                return {"idempotent": True, "isolation": inc["isolation"]}
            if "teamId" in inc["isolation"]:
                return {"idempotent": True, "pending": True, "isolation": inc["isolation"]}
            at = timeutil.iso(self._clock())
            moment = self._clock()
            target_node = self._incident_node(inc)
            busy = self._active_team_ids()
            options = []
            blockers = []
            for team in self.ref.teams.values():
                if "隔离警戒" not in team["qualifications"]:
                    continue
                if team["id"] in busy:
                    blockers.append(f"{team['name']}:正在执行其他工单")
                    continue
                if not timeutil.on_shift(team["shift"], moment):
                    blockers.append(f"{team['name']}:不在班次")
                    continue
                vehicle = self.ref.vehicles[team["vehicleId"]]
                route = self.routes.shortest("D", target_node, vehicle["class"], self.closed_edges)
                if route is None:
                    blockers.append(f"{team['name']}:无可达路线")
                    continue
                options.append((route[0], team, vehicle, route))
            if not options:
                raise ValidationError("无可派遣隔离队伍: " + ";".join(blockers))
            options.sort(key=lambda o: (o[0], o[1]["id"]))
            _, team, _vehicle, _route = options[0]
            self._emit({"type": "isolation_dispatched", "incidentId": incident_id,
                        "teamId": team["id"], "at": at, "dispatcher": dispatcher})
            return {"isolation": inc["isolation"]}

    # --------------------------------------------------------------- 道路变化

    def close_road(self, edge_id: str, reason: str = "临时封闭") -> dict:
        with self._lock:
            if edge_id not in self.ref.edges:
                raise ValidationError(f"未知道路: {edge_id}")
            at = timeutil.iso(self._clock())
            if edge_id in self.closed_edges:
                return {"idempotent": True, "edgeId": edge_id, "closed": True}
            before_order = self._queue_order()
            affected = self._affected_wos_by_edge(edge_id)
            self._emit({"type": "road_closed", "edgeId": edge_id, "at": at, "reason": reason})
            for wo_id in affected:
                self._reroute_wo(self.work_orders[wo_id], f"道路{edge_id}封闭：{reason}", at)
            after_order = self._queue_order()
            if affected and before_order != after_order:
                self._record_reorder_basis(edge_id, reason, before_order, after_order, at)
            return {"edgeId": edge_id, "closed": True, "affectedWorkOrders": affected,
                    "queueBefore": before_order, "queueAfter": after_order}

    def reopen_road(self, edge_id: str) -> dict:
        with self._lock:
            if edge_id not in self.ref.edges:
                raise ValidationError(f"未知道路: {edge_id}")
            if edge_id not in self.closed_edges:
                return {"idempotent": True, "edgeId": edge_id, "closed": False}
            at = timeutil.iso(self._clock())
            before_order = self._queue_order()
            affected = self._affected_wos_by_edge(edge_id)
            self._emit({"type": "road_reopened", "edgeId": edge_id, "at": at})
            for wo_id in affected:
                self._reroute_wo(self.work_orders[wo_id], f"道路{edge_id}恢复通行", at)
            # 道路恢复后刷新待派工单的阻塞原因（仍需调度员显式派工）
            for wo in self.work_orders.values():
                if wo["status"] == "created" and not wo["assignment"]:
                    inc = self.incidents[wo["incidentId"]]
                    rule = self.ref.danger_rule(inc["depthCm"])
                    if not (rule["isolationRequired"] and not inc["isolation"].get("confirmed")):
                        self._find_candidate(wo)
            after_order = self._queue_order()
            if affected and before_order != after_order:
                self._record_reorder_basis(edge_id, "道路恢复", before_order, after_order, at)
            return {"edgeId": edge_id, "closed": False, "affectedWorkOrders": affected}

    def _affected_wos_by_edge(self, edge_id: str) -> list[str]:
        ids = []
        for wo in self.work_orders.values():
            if wo["status"] in TERMINAL or not wo["assignment"]:
                continue
            if wo["status"] in ARRIVED_PHASES:
                continue  # 已到场不再受路线变化影响
            if edge_id in wo["assignment"].get("routeEdges", []):
                ids.append(wo["id"])
        return ids

    def _reroute_wo(self, wo: dict, reason: str, at: str):
        inc = self.incidents[wo["incidentId"]]
        team = self.ref.teams[wo["assignment"]["teamId"]]
        vehicle = self.ref.vehicles[team["vehicleId"]]
        target_node = self._incident_node(inc)
        old = wo["assignment"]
        route = self.routes.shortest("D", target_node, vehicle["class"], self.closed_edges)
        if route is None:
            # 原队伍被断路：尝试改派其他可到达队伍，否则挂阻塞
            candidate = self._find_candidate(wo, exclude={team["id"]})
            blockers = [f"{reason}，原路线中断且暂无替代队伍"] + self._edge_blockers(
                target_node, vehicle["class"])
            if candidate:
                self._reassign(wo, candidate, f"{reason}，原路线中断，改派{candidate['team']['name']}", at)
            else:
                self._emit({"type": "work_order_route_blocked", "workOrderId": wo["id"], "at": at,
                            "reason": reason, "blockers": blockers})
            return
        distance, edges, edge_blockers = route
        if edges == old["routeEdges"]:
            if wo["blockedReasons"]:
                self._emit({"type": "work_order_blockers_cleared", "workOrderId": wo["id"],
                            "at": at, "reason": reason})
            return
        earliest, latest = timeutil.eta_window(
            timeutil.parse(at), distance, self.ref.rules["travelSpeedMPerMin"],
            tuple(self.ref.rules["etaSlackMinutes"]))
        new_assignment = dict(old)
        new_assignment.update({"routeEdges": edges, "distanceM": distance,
                               "etaEarliest": earliest, "etaLatest": latest,
                               "decidedAt": at, "routeBlockers": edge_blockers})
        basis = {"edgeChange": reason, "oldDistanceM": old["distanceM"], "newDistanceM": distance,
                 "oldEta": [old["etaEarliest"], old["etaLatest"]],
                 "newEta": [earliest, latest]}
        self._emit({"type": "reroute_reordered", "workOrderId": wo["id"], "at": at,
                    "reason": reason, "oldAssignment": old, "assignment": new_assignment,
                    "reorderBasis": basis})

    def _queue_order(self) -> list[str]:
        active = [wo for wo in self.work_orders.values()
                  if wo["status"] not in TERMINAL and wo["status"] != "cancelled"]
        active.sort(key=lambda wo: (-DANGER_RANK[self.incidents[wo["incidentId"]]["dangerLevel"]],
                                    -self.incidents[wo["incidentId"]]["depthCm"],
                                    wo["assignment"]["etaEarliest"] if wo["assignment"] else "9999",
                                    wo["id"]))
        return [wo["id"] for wo in active]

    def _record_reorder_basis(self, edge_id, reason, before, after, at):
        # 留排序依据（落盘），便于事后回答“为什么这个工单的顺序变了”
        self._emit({"type": "queue_reordered", "edgeId": edge_id, "reason": reason,
                    "before": before, "after": after, "at": at,
                    "affectedWorkOrderIds": sorted(set(before) | set(after))})

    # ------------------------------------------------------------------ 回执

    def receive_receipt(self, payload: dict) -> dict:
        with self._lock:
            receipt_id = payload.get("receiptId") or self._next_id("RC")
            if receipt_id in self.receipts:  # 幂等：同一回执不重复入账
                existing = self.receipts[receipt_id]
                return {"idempotent": True, "receipt": self._receipt_view(existing)}
            rtype = payload.get("type")
            if rtype not in self.ref.rules["receiptTypes"]:
                raise ValidationError(f"未知回执类型: {rtype}")
            wo = None
            incident_id = payload.get("incidentId")
            if payload.get("workOrderId"):
                wo = self._require_wo(payload["workOrderId"])
                incident_id = wo["incidentId"]
            if not incident_id:
                raise ValidationError("回执需要 workOrderId 或 incidentId")
            if incident_id not in self.incidents:
                raise ValidationError("事件不存在")
            team_id = payload.get("teamId")
            if team_id and team_id not in self.ref.teams:
                raise ValidationError(f"未知队伍: {team_id}")
            if wo and wo["assignment"] and team_id and team_id != wo["assignment"]["teamId"]:
                raise ValidationError(
                    f"回执队伍{team_id}与派工队伍{wo['assignment']['teamId']}不符")
            if rtype in ("signed", "enroute", "arrived", "resolved") and (
                    not wo or not wo["assignment"]):
                raise ValidationError(f"{rtype} 回执要求工单已派工")
            # 离线补传可能带回更早阶段：允许入账并按发生时间插入时间线，
            # 当前阶段只升不降（见 receipt_recorded 的应用逻辑）
            occurred = timeutil.parse(payload.get("occurredAt") or timeutil.iso(self._clock()))
            received = self._clock()
            parts = payload.get("parts")
            if parts:
                if not team_id:
                    raise ValidationError("备件消耗回执必须提供 teamId")
                for pid in parts:
                    if pid not in self.ref.parts:
                        raise ValidationError(f"未知备件: {pid}")
            arrive_deadline = None
            if rtype == "signed" and wo:
                arrive_deadline = timeutil.iso(occurred + timedelta(
                    minutes=self.ref.rules["reminders"]["arriveTimeoutMinutes"]))
            event = {"type": "receipt_recorded", "receiptId": receipt_id,
                     "workOrderId": wo["id"] if wo else None, "incidentId": incident_id,
                     "teamId": team_id, "receiptType": rtype,
                     "occurredAt": timeutil.iso(occurred), "receivedAt": timeutil.iso(received),
                     "note": payload.get("note", ""), "offline": bool(payload.get("offline", False)),
                     "parts": parts, "arriveDeadline": arrive_deadline}
            self._emit(event)
            # 隔离确认后刷新等待派工工单的阻塞原因（派工仍由调度员显式发起）
            if rtype == "isolation_confirmed":
                for woid in self.incidents[incident_id]["workOrderIds"]:
                    pending = self.work_orders[woid]
                    if pending["status"] == "created" and not pending["assignment"]:
                        pending["blockedReasons"] = []
                        self._find_candidate(pending)
            return {"receipt": self._receipt_view(event)}

    # ------------------------------------------------------------------ 提醒

    def scan_reminders(self) -> list[dict]:
        """检查未签收/未到场超时；已提醒过的不重复。重启后由落盘记录去重。"""
        fired = []
        with self._lock:
            now = self._clock()
            for wo in self.work_orders.values():
                if wo["status"] in TERMINAL:
                    continue
                kinds = {r["kind"] for r in wo["reminders"]}
                if "sign_timeout" not in kinds and wo.get("signDeadline") and not wo["assignment"]:
                    if timeutil.parse(wo["signDeadline"]) <= now:
                        fired.append(self._fire_reminder(wo, "sign_timeout",
                                                         f"工单{wo['id']}创建后超时未签收派工"))
                if "arrive_timeout" not in kinds and wo.get("arriveDeadline") and wo["assignment"]:
                    if wo["status"] in ("assigned", "signed", "enroute") and timeutil.parse(wo["arriveDeadline"]) <= now:
                        fired.append(self._fire_reminder(wo, "arrive_timeout",
                                                         f"工单{wo['id']}已派工但超时未到场"))
        for payload in fired:
            self._reminder_sink(payload)
        return fired

    def _fire_reminder(self, wo: dict, kind: str, message: str) -> dict:
        event = {"type": "reminder_fired", "workOrderId": wo["id"], "kind": kind,
                 "at": timeutil.iso(self._clock()), "message": message}
        self._emit(event)
        return event

    # ------------------------------------------------------------------ 视图

    def _require_wo(self, wo_id: str) -> dict:
        wo = self.work_orders.get(wo_id)
        if not wo:
            raise ValidationError(f"工单不存在: {wo_id}")
        return wo

    def _report_view(self, e: dict) -> dict:
        return {"reportId": e["reportId"], "source": e["source"], "wellIds": e["wellIds"],
                "depthCm": e["depthCm"], "occurredAt": e["occurredAt"],
                "receivedAt": e["receivedAt"], "note": e.get("note", "")}

    def _receipt_view(self, e: dict) -> dict:
        return {"receiptId": e["receiptId"], "workOrderId": e["workOrderId"],
                "incidentId": e["incidentId"], "teamId": e["teamId"], "type": e["receiptType"],
                "occurredAt": e["occurredAt"], "receivedAt": e["receivedAt"],
                "offline": e["offline"], "note": e.get("note", ""), "parts": e.get("parts")}

    def incident_view(self, inc_id: str) -> dict:
        inc = self.incidents.get(inc_id)
        if not inc:
            raise ValidationError("事件不存在")
        wells = [{"id": w, "label": self.ref.wells[w]["label"],
                  "compound": self.ref.wells[w].get("compound", "")} for w in inc["wellIds"]]
        compounds = sorted({w["compound"] for w in wells if w["compound"]})
        wos = [self.work_order_view(w) for w in inc["workOrderIds"]
               if self.work_orders[w]["status"] != "cancelled"]
        return {"id": inc["id"], "status": inc["status"], "dangerLevel": inc["dangerLevel"],
                "depthCm": inc["depthCm"], "impactArea": {"wells": wells, "compounds": compounds,
                "hydraulicComponents": inc["components"], "roadNode": self._incident_node(inc)},
                "isolation": inc.get("isolation"), "reports": inc["reportIds"],
                "measurements": inc["measurementIds"], "workOrders": wos,
                "createdAt": inc["createdAt"], "recoveredAt": inc.get("recoveredAt")}

    def work_order_view(self, wo_id: str) -> dict:
        wo = self._require_wo(wo_id)
        inc = self.incidents[wo["incidentId"]]
        return {"id": wo["id"], "incidentId": wo["incidentId"], "status": wo["status"],
                "version": wo["version"], "dangerLevel": wo["dangerLevel"], "depthCm": wo["depthCm"],
                "impactWells": inc["wellIds"], "assignment": wo["assignment"],
                "blockedReasons": wo["blockedReasons"], "reminders": wo["reminders"],
                "signDeadline": wo.get("signDeadline"), "arriveDeadline": wo.get("arriveDeadline"),
                "createdAt": wo["createdAt"]}

    def timeline_view(self, wo_id: str) -> dict:
        """从报修到恢复的完整事件记录；离线回执按发生时间归位。"""
        wo = self._require_wo(wo_id)
        inc = self.incidents[wo["incidentId"]]
        entries: list[dict] = []
        isolation = inc.get("isolation") or {}
        if isolation.get("dispatchedAt"):
            entries.append({"at": isolation["dispatchedAt"], "type": "isolation",
                            "stage": "dispatched", "teamId": isolation.get("teamId"),
                            "_seq": isolation.get("dispatchedSeq", 0)})
        if isolation.get("confirmedAt"):
            entries.append({"at": isolation["confirmedAt"], "type": "isolation",
                            "stage": "confirmed", "teamId": isolation.get("teamId"),
                            "_seq": isolation.get("confirmedSeq", 0)})
        for rid in inc["reportIds"]:
            r = self.reports.get(rid)
            if r:
                entries.append({"at": r["occurredAt"], "receivedAt": r["receivedAt"], "type": "report",
                                "source": r["source"], "depthCm": r["depthCm"], "wellIds": r["wellIds"],
                                "note": r.get("note", ""), "_seq": r.get("_seq", 0)})
        for mid in inc["measurementIds"]:
            m = self.measurements.get(mid)
            if m:
                entries.append({"at": m["occurredAt"], "receivedAt": m["receivedAt"],
                                "type": "measurement", "wellId": m["wellId"], "depthCm": m["depthCm"],
                                "_seq": m.get("_seq", 0)})
        for item in wo["timeline"]:
            entries.append(item)
        for h in wo["history"]:
            entries.append({"at": h["at"], "type": "decision", "kind": h["kind"], "_seq": h.get("_seq", 0),
                            "detail": {k: v for k, v in h.items()
                                       if k not in ("at", "kind", "_seq")}})
        for reminder in wo["reminders"]:
            entries.append({"at": reminder["at"], "type": "reminder", "kind": reminder["kind"],
                            "message": reminder["message"], "_seq": reminder.get("_seq", 0)})
        # 同秒内按事件因果序排列（离线回执仍可凭更早的 occurredAt 插到前面）
        entries.sort(key=lambda x: (x["at"], x["_seq"]))
        for entry in entries:
            entry.pop("_seq", None)
        return {"workOrderId": wo_id, "incidentId": wo["incidentId"], "entries": entries}

    def board_view(self) -> dict:
        order = self._queue_order()
        rank = {wo_id: i for i, wo_id in enumerate(order)}
        rows = []
        for inc in self.incidents.values():
            if inc["status"] not in ("active",):
                continue
            teams, eta, blockers = [], [], []
            for wo_id in inc["workOrderIds"]:
                wo = self.work_orders[wo_id]
                if wo["status"] in TERMINAL:
                    continue
                if wo["assignment"]:
                    teams.append({"teamId": wo["assignment"]["teamId"],
                                  "teamName": wo["assignment"]["teamName"],
                                  "vehicleClass": wo["assignment"]["vehicleClass"],
                                  "workOrderId": wo_id, "status": wo["status"]})
                    eta.append({"workOrderId": wo_id,
                                "earliest": wo["assignment"]["etaEarliest"],
                                "latest": wo["assignment"]["etaLatest"]})
                blockers.extend([f"{wo_id}:{b}" for b in wo["blockedReasons"]])
            rows.append({"incidentId": inc["id"], "dangerLevel": inc["dangerLevel"],
                         "depthCm": inc["depthCm"], "impactArea": self.incident_view(inc["id"])["impactArea"],
                         "isolation": inc["isolation"], "currentTeams": teams, "etaWindows": eta,
                         "blockedReasons": blockers, "queueRank": rank.get(next(
                             (w for w in inc["workOrderIds"] if w in rank), ""))})
        rows.sort(key=lambda r: (r["queueRank"] if r["queueRank"] is not None else 10**6))
        return {"queue": order, "incidents": rows}

    def teams_view(self) -> list[dict]:
        moment = self._clock()
        busy = self._active_team_ids()
        result = []
        for team in self.ref.teams.values():
            stock = self.team_stock.setdefault(team["id"], dict(self.ref.vehicles[team["vehicleId"]]["parts"]))
            result.append({"id": team["id"], "name": team["name"],
                           "qualifications": team["qualifications"],
                           "vehicle": {"id": team["vehicleId"],
                                       "class": self.ref.vehicles[team["vehicleId"]]["class"]},
                           "onShift": timeutil.on_shift(team["shift"], moment),
                           "busy": team["id"] in busy, "partsStock": stock})
        return result
