"""抢修调度引擎。

所有处置事实只追加到 journal；每次写入后按发生时间（occurredAt）全量重放，
因此离线补传的回执会按发生时刻插入，重放结果与在线到达顺序无关。
去重依赖条目确定性 id，重放/重启天然幂等。
"""
from __future__ import annotations

import threading
from datetime import timedelta
from typing import Any, Callable

from .journal import Journal
from .models import (
    ONSCENE_STATUSES,
    RECEIPT_STATUSES,
    SEVERITY_ORDER,
    evaluate,
)
from .reference import ReferenceData
from .router import Router
from .timeutil import compact, format, now, parse

RECEIPT_RANK = {"dispatched": 1, "isolated": 2, "onsite": 3, "working": 4, "restored": 5}


class AppError(Exception):
    def __init__(self, code: str, message: str, status: int = 422,
                 details: dict | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status
        self.details = details or {}


def _tl(at, kind: str, message: str, ref: str | None = None) -> dict[str, Any]:
    return {"at": format(at), "kind": kind, "message": message, "ref": ref}


class Engine:
    def __init__(self, ref: ReferenceData, journal: Journal, config,
                 clock: Callable = now) -> None:
        self.ref = ref
        self.journal = journal
        self.config = config
        self.clock = clock
        self.router = Router(
            list(ref.road_segments.values()),
            ref.speed_kmh,
            ref.fixed_seconds,
            ref.slack_seconds,
        )
        self.lock = threading.RLock()
        self._rebuild()

    # ------------------------------------------------------------------ 基础

    @property
    def version(self) -> int:
        return len(self.journal.entries)

    def _new_entry(self, type_: str, occurred_at, **payload) -> dict[str, Any]:
        entry = {"type": type_, "occurredAt": format(occurred_at)}
        entry.update(payload)
        return entry

    def _submit(self, entries: list[dict[str, Any]], *, id_key: str = "id") -> list[dict]:
        """校验由调用方完成；这里按键去重落盘并按发生时间重建状态。"""
        for e in entries:
            if "receivedAt" not in e:
                e["receivedAt"] = format(self.clock())
        written = self.journal.append_many(entries)
        if written:
            self._rebuild()
        return written

    # ------------------------------------------------------------- 状态重放

    def _rebuild(self) -> None:
        entries = sorted(
            enumerate(self.journal.entries),
            key=lambda pair: (pair[1]["occurredAt"], pair[0]),
        )
        self.events: dict[str, dict] = {}
        self.orders: dict[str, dict] = {}
        self.closures: dict[str, dict] = {}
        self.crew_used: dict[str, dict[str, int]] = {
            cid: {pid: 0 for pid in crew["vehicle"]["capacity"]}
            for cid, crew in self.ref.crews.items()
        }
        self.crew_location: dict[str, str] = {
            cid: crew["homeNodeId"] for cid, crew in self.ref.crews.items()
        }
        self.group_active: dict[str, str] = {}
        self._event_seq = 0
        self._order_seq = 0

        for _, entry in entries:
            self._apply(entry)

    def _new_event(self, *, group_id, community, node_id, at, source, source_ref) -> dict:
        if group_id:
            key = group_id
        else:
            key = "X"
        self._event_seq += 1
        event_id = f"EV-{key}-{compact(at)}-{self._event_seq:02d}"
        node = self.ref.nodes.get(node_id) if node_id else None
        event = {
            "eventId": event_id,
            "groupId": group_id,
            "community": community or (node.get("community") if node else None),
            "primaryNodeId": node_id,
            "nodeIds": [node_id] if node_id else [],
            "locationText": None,
            "openedAt": at,
            "restoredAt": None,
            "isolatedAt": None,
            "mergedInto": None,
            "reportIds": [],
            "sensorIds": [],
            "latestDepthM": None,
            "depthAt": None,
            "sources": [],
            "timeline": [_tl(at, "opened", f"影响事件建立（来源：{source}）", source_ref)],
            "workOrderId": None,
            "radiusM": self.ref.merge_groups[group_id]["radiusM"] if group_id else None,
        }
        self.events[event_id] = event
        if group_id:
            self.group_active[group_id] = event_id
        self._recompute_hazard(event)
        return event

    def _new_order(self, event: dict, at) -> dict:
        self._order_seq += 1
        gid = event["groupId"] or "STANDALONE"
        base = f"WO-{gid}-{compact(at)}"
        work_order_id = base
        n = 1
        while work_order_id in self.orders:
            n += 1
            work_order_id = f"{base}-{n}"
        order = {
            "workOrderId": work_order_id,
            "eventId": event["eventId"],
            "groupId": event["groupId"],
            "community": event["community"],
            "primaryNodeId": event["primaryNodeId"],
            "nodeIds": list(event["nodeIds"]),
            "openedAt": at,
            "status": "open",
            "crewId": None,
            "dispatchSeq": 0,
            "dispatches": [],
            "dispatch": None,
            "ackAt": None,
            "onsiteAt": None,
            "restoredAt": None,
            "cancelledAt": None,
            "cancelReason": None,
            "receipts": {},
            "escalations": [],
            "reroutes": [],
            "timeline": [_tl(at, "order_opened", "工单建立，等待派工", work_order_id)],
        }
        self.orders[work_order_id] = order
        event["workOrderId"] = work_order_id
        event["timeline"].append(_tl(at, "order_opened", f"建立工单 {work_order_id}", work_order_id))
        return order

    def _ensure_order(self, event: dict, at) -> dict | None:
        wid = event.get("workOrderId")
        if wid:
            current = self.orders[wid]
            if current["status"] not in ("restored", "cancelled"):
                return current
        return self._new_order(event, at)

    def _attach_depth(self, event: dict, depth_m, at) -> None:
        if depth_m is None:
            return
        if event["depthAt"] is None or at >= event["depthAt"]:
            event["latestDepthM"] = depth_m
            event["depthAt"] = at
        self._recompute_hazard(event)

    def _recompute_hazard(self, event: dict) -> None:
        node = self.ref.nodes.get(event["primaryNodeId"])
        hazard = evaluate(
            event["latestDepthM"], node, event["community"], self.ref.hazards
        )
        event["hazard"] = hazard

    def _resolve_group_event(self, *, node_id, community, at, create, source, ref):
        group_id = self.ref.group_for(node_id=node_id, community=community)
        if group_id:
            event_id = self.group_active.get(group_id)
            if event_id and not self.events[event_id]["restoredAt"]:
                return self.events[event_id], False
            if create:
                return self._new_event(
                    group_id=group_id, community=community, node_id=node_id,
                    at=at, source=source, source_ref=ref,
                ), True
            return None, False
        if create:
            return self._new_event(
                group_id=None, community=community, node_id=node_id,
                at=at, source=source, source_ref=ref,
            ), True
        return None, False

    def _apply(self, entry: dict) -> None:
        at = parse(entry["occurredAt"])
        kind = entry["type"]
        if kind == "report":
            self._apply_report(entry, at)
        elif kind == "sensor":
            self._apply_sensor(entry, at)
        elif kind == "isolation":
            event = self.events.get(entry["eventId"])
            if event and not event["isolatedAt"]:
                event["isolatedAt"] = at
                event["timeline"].append(_tl(at, "isolated", f"危险区域已隔离（{entry.get('by', '?')}）", entry["id"]))
                wid = event.get("workOrderId")
                if wid:
                    self.orders[wid]["timeline"].append(_tl(at, "isolated", "现场完成隔离，可进入作业", entry["id"]))
        elif kind == "route_closure":
            self._apply_closure(entry, at)
        elif kind == "dispatch":
            self._apply_dispatch(entry, at)
        elif kind == "ack":
            order = self.orders.get(entry["workOrderId"])
            # 只接受当前派工序号的签收；改派前的旧签收不得重新生效
            if order and entry.get("seq") == order["dispatchSeq"] and not order["ackAt"]:
                order["ackAt"] = at
                if order["status"] == "dispatched":
                    order["status"] = "acked"
                order["timeline"].append(_tl(at, "acked", f"{entry.get('crewId', '')} 已签收出动".strip(), entry["id"]))
        elif kind == "receipt":
            self._apply_receipt(entry, at)
        elif kind == "merge":
            self._apply_merge(entry, at)
        elif kind == "reroute":
            self._apply_reroute(entry, at)
        elif kind == "route_blocked":
            order = self.orders.get(entry["workOrderId"])
            if order:
                order["timeline"].append(_tl(
                    at, "route_blocked", entry["message"], entry["id"]
                ))
        elif kind == "escalation":
            order = self.orders.get(entry["workOrderId"])
            if order:
                order["escalations"].append({"at": format(at), "kind": entry["escalationKind"], "detail": entry.get("detail", "")})
                order["timeline"].append(_tl(at, "escalation", f"超时提醒：{entry.get('detail', entry['escalationKind'])}", entry["id"]))

    def _apply_report(self, entry: dict, at) -> None:
        node_id = entry.get("nodeId")
        community = entry.get("community")
        event, _ = self._resolve_group_event(
            node_id=node_id, community=community, at=at, create=True,
            source="公众报修", ref=entry["id"],
        )
        if node_id and node_id not in event["nodeIds"]:
            event["nodeIds"].append(node_id)
        if not event["primaryNodeId"] and node_id:
            event["primaryNodeId"] = node_id
            self._recompute_hazard(event)
        if entry.get("locationText"):
            event["locationText"] = entry["locationText"]
        event["reportIds"].append(entry["reportId"])
        event["sources"].append({"kind": "report", "ref": entry["reportId"], "at": format(at)})
        event["timeline"].append(_tl(
            at, "report",
            f"公众报修 {entry['reportId']}"
            + (f"，报水深 {entry['depthM']}m" if entry.get("depthM") is not None else ""),
            entry["id"],
        ))
        self._attach_depth(event, entry.get("depthM"), at)
        self._ensure_order(event, at)

    def _apply_sensor(self, entry: dict, at) -> None:
        node = self.ref.nodes.get(entry["nodeId"])
        depth = entry["depthM"]
        event, _ = self._resolve_group_event(
            node_id=entry["nodeId"], community=None, at=at,
            create=bool(node and depth >= node["warningDepth"]),
            source="物联网液位", ref=entry["id"],
        )
        if event is None:
            return
        if entry["nodeId"] not in event["nodeIds"]:
            event["nodeIds"].append(entry["nodeId"])
        event["sensorIds"].append(entry["sensorId"])
        event["sources"].append({"kind": "sensor", "ref": entry["sensorId"], "at": format(at)})
        event["timeline"].append(_tl(
            at, "sensor", f"液位上报 {entry['sensorId']}：{depth}m", entry["id"]
        ))
        self._attach_depth(event, depth, at)
        self._ensure_order(event, at)

    def _apply_closure(self, entry: dict, at) -> None:
        seg_id = entry["segmentId"]
        if entry["action"] == "close":
            self.closures[seg_id] = {
                "segmentId": seg_id,
                "since": format(at),
                "reason": entry.get("reason", ""),
                "restriction": entry.get("restriction"),
            }
        else:
            self.closures.pop(seg_id, None)
        for order in self.orders.values():
            if order["status"] in ("dispatched", "acked"):
                order["timeline"].append(_tl(
                    at, "route",
                    f"道路 {seg_id} {'封闭/限行' if entry['action'] == 'close' else '恢复通行'}"
                    + (f"：{entry.get('reason', '')}" if entry.get("reason") else ""),
                    entry["id"],
                ))

    def _consume_parts(self, crew_id: str, parts: dict[str, int]) -> None:
        for pid, qty in parts.items():
            self.crew_used[crew_id][pid] = self.crew_used[crew_id].get(pid, 0) + qty

    def _return_parts(self, crew_id: str, parts: dict[str, int]) -> None:
        for pid, qty in parts.items():
            self.crew_used[crew_id][pid] = max(0, self.crew_used[crew_id].get(pid, 0) - qty)

    def _apply_dispatch(self, entry: dict, at) -> None:
        order = self.orders[entry["workOrderId"]]
        if order["dispatch"]:
            # 改派：释放前车占用
            self._return_parts(order["dispatch"]["crewId"], order["dispatch"].get("partsUsed", {}))
        order["dispatchSeq"] += 1
        record = {
            "seq": order["dispatchSeq"],
            "crewId": entry["crewId"],
            "by": entry.get("by", "?"),
            "at": format(at),
            "expectedAt": entry["expectedAt"],
            "routeSegmentIds": entry["routeSegmentIds"],
            "routeLengthM": entry["routeLengthM"],
            "routeSource": entry["routeSource"],
            "partsUsed": entry.get("partsUsed", {}),
        }
        order["dispatches"].append(record)
        order["dispatch"] = record
        order["crewId"] = entry["crewId"]
        order["status"] = "dispatched"
        order["ackAt"] = None
        self._consume_parts(entry["crewId"], entry.get("partsUsed", {}))
        self.crew_location[entry["crewId"]] = entry["routeSource"]
        order["timeline"].append(_tl(
            at, "dispatched",
            f"派工 {entry['crewId']}（{entry.get('by', '?')}），预计 {entry['expectedAt']} 到达",
            entry["id"],
        ))

    def _apply_receipt(self, entry: dict, at) -> None:
        order = self.orders[entry["workOrderId"]]
        existing = order["receipts"].get(entry["receiptId"])
        receipt = {
            "receiptId": entry["receiptId"],
            "status": entry["status"],
            "at": format(at),
            "depthM": entry.get("depthM"),
            "note": entry.get("note"),
            "receivedAt": entry["receivedAt"],
            "offline": entry["receivedAt"] != entry["occurredAt"]
            and parse(entry["receivedAt"]) - at > timedelta(minutes=1),
        }
        order["receipts"][entry["receiptId"]] = receipt
        if existing and existing["status"] != entry["status"]:
            order["timeline"].append(_tl(at, "receipt", f"回执 {entry['receiptId']} 状态更新为 {entry['status']}", entry["id"]))
        elif not existing:
            order["timeline"].append(_tl(
                at, "receipt",
                f"现场回执 {entry['receiptId']}：{entry['status']}"
                + ("（离线补传）" if receipt["offline"] else "")
                + (f"，{entry.get('note')}" if entry.get("note") else ""),
                entry["id"],
            ))
        event = self.events[order["eventId"]]
        self._attach_depth(event, entry.get("depthM"), at)
        if entry["status"] == "isolated" and not event["isolatedAt"]:
            event["isolatedAt"] = at
        # 按发生时间推进状态机（重放保证顺序）
        cur_rank = RECEIPT_RANK.get(order["status"], 0)
        new_rank = RECEIPT_RANK[entry["status"]]
        if new_rank >= cur_rank:
            order["status"] = entry["status"]
        if entry["status"] == "onsite" and not order["onsiteAt"]:
            order["onsiteAt"] = at
        if entry["status"] == "restored":
            order["restoredAt"] = at
            event["restoredAt"] = at
            if event["groupId"] and self.group_active.get(event["groupId"]) == event["eventId"]:
                self.group_active.pop(event["groupId"], None)
            if order["dispatch"]:
                self._return_parts(order["crewId"], order["dispatch"].get("partsUsed", {}))
                if order["primaryNodeId"]:
                    self.crew_location[order["crewId"]] = order["primaryNodeId"]
            order["timeline"].append(_tl(at, "restored", "水位回落，抢修恢复", entry["id"]))
            event["timeline"].append(_tl(at, "restored", "影响事件关闭", entry["id"]))

    def _apply_merge(self, entry: dict, at) -> None:
        source = self.events[entry["sourceEventId"]]
        target = self.events[entry["targetEventId"]]
        source["mergedInto"] = target["eventId"]
        for rid in source["reportIds"]:
            if rid not in target["reportIds"]:
                target["reportIds"].append(rid)
        for sid in source["sensorIds"]:
            if sid not in target["sensorIds"]:
                target["sensorIds"].append(sid)
        for nid in source["nodeIds"]:
            if nid not in target["nodeIds"]:
                target["nodeIds"].append(nid)
        target["sources"].extend(source["sources"])
        target["timeline"].append(_tl(
            at, "merged", f"合并同源事件 {source['eventId']}（{len(source['reportIds'])} 个报修来电）", entry["id"]
        ))
        wid = source.get("workOrderId")
        if wid:
            order = self.orders[wid]
            if order["status"] not in ("restored", "cancelled"):
                order["status"] = "cancelled"
                order["cancelledAt"] = at
                order["cancelReason"] = f"合并入 {target['eventId']}"
                if order["dispatch"] and order["crewId"]:
                    self._return_parts(order["crewId"], order["dispatch"].get("partsUsed", {}))
                order["timeline"].append(_tl(at, "cancelled", f"普通合并取消（并入 {target['eventId']} 的工单）", entry["id"]))
        # 合并后重算危险等级（取较深水位）
        if source.get("depthAt") and (not target.get("depthAt") or source["depthAt"] >= target["depthAt"]):
            target["latestDepthM"] = source["latestDepthM"]
            target["depthAt"] = source["depthAt"]
        self._recompute_hazard(target)
        # 影响范围组的活跃事件指针迁移到目标事件，后续同组来电继续并入
        gid = source.get("groupId")
        if gid and self.group_active.get(gid) == source["eventId"]:
            if not target.get("restoredAt"):
                self.group_active[gid] = target["eventId"]
            else:
                self.group_active.pop(gid, None)

    def _apply_reroute(self, entry: dict, at) -> None:
        order = self.orders[entry["workOrderId"]]
        record = {
            "at": format(at),
            "reason": entry["reason"],
            "closedSegmentIds": entry.get("closedSegmentIds", []),
            "oldSegmentIds": entry["oldSegmentIds"],
            "newSegmentIds": entry["newSegmentIds"],
            "oldRank": entry["oldRank"],
            "newRank": entry["newRank"],
            "oldExpectedAt": entry["oldExpectedAt"],
            "newExpectedAt": entry["newExpectedAt"],
        }
        order["reroutes"].append(record)
        if order["dispatch"]:
            order["dispatch"]["routeSegmentIds"] = entry["newSegmentIds"]
            order["dispatch"]["expectedAt"] = entry["newExpectedAt"]
        order["timeline"].append(_tl(
            at, "reroute",
            f"路线变更：{entry['reason']}；队列位次 {entry['oldRank']}→{entry['newRank']}，"
            f"预计到达 {entry['oldExpectedAt']}→{entry['newExpectedAt']}",
            entry["id"],
        ))

    # ----------------------------------------------------------- 输入接口

    def ingest_report(self, payload: dict) -> dict:
        report_id = payload.get("reportId")
        if not report_id:
            raise AppError("bad_request", "reportId 必填", 400)
        node_id = payload.get("nodeId")
        if node_id and node_id not in self.ref.nodes:
            raise AppError("unknown_node", f"未知井位 {node_id}")
        at = parse(payload.get("occurredAt"), default=self.clock())
        entry = self._new_entry(
            "report", at,
            id=f"RPT-{report_id}",
            reportId=report_id,
            nodeId=node_id,
            community=payload.get("community"),
            locationText=payload.get("locationText"),
            depthM=_depth(payload),
            callerPhone=payload.get("callerPhone"),
        )
        with self.lock:
            existed = entry["id"] in self.journal.seen
            self._submit([entry])
            event = self._find_event_by_report(report_id)
            return {"created": not existed, "eventId": event["eventId"],
                    "workOrderId": event.get("workOrderId"), "deduplicated": existed}

    def ingest_sensor(self, payload: dict) -> dict:
        sensor_id = payload.get("sensorId")
        node_id = payload.get("nodeId")
        if not sensor_id or not node_id:
            raise AppError("bad_request", "sensorId 与 nodeId 必填", 400)
        if node_id not in self.ref.nodes:
            raise AppError("unknown_node", f"未知井位 {node_id}")
        depth = _depth(payload)
        if depth is None:
            raise AppError("bad_request", "depthM 必填", 400)
        at = parse(payload.get("occurredAt"), default=self.clock())
        stamp = at.strftime("%Y%m%d%H%M%S")
        entry = self._new_entry(
            "sensor", at,
            id=f"SEN-{sensor_id}-{stamp}",
            sensorId=sensor_id, nodeId=node_id, depthM=depth,
        )
        with self.lock:
            existed = entry["id"] in self.journal.seen
            self._submit([entry])
            return {"created": not existed, "deduplicated": existed,
                    "eventId": self._active_event_for_node(node_id)}

    def _active_event_for_node(self, node_id):
        group_id = self.ref.node_group.get(node_id)
        if group_id and group_id in self.group_active:
            return self.group_active[group_id]
        return None

    def _find_event_by_report(self, report_id):
        for event in self.events.values():
            if report_id in event["reportIds"]:
                return event
        raise AppError("internal", "事件定位失败", 500)

    def isolate(self, event_id: str, payload: dict) -> dict:
        with self.lock:
            event = self.events.get(event_id)
            if not event:
                raise AppError("not_found", "事件不存在", 404)
            if event["isolatedAt"]:
                return {"created": False, "eventId": event_id, "isolatedAt": event["isolatedAt"]}
            at = parse(payload.get("occurredAt"), default=self.clock())
            by = payload.get("by", "调度员")
            node_ids = payload.get("nodeIds") or list(event["nodeIds"])
            entry = self._new_entry(
                "isolation", at,
                id=f"ISO-{event_id}-{compact(at)}-{by}",
                eventId=event_id, nodeIds=node_ids, by=by,
                note=payload.get("note"),
            )
            if entry["id"] in self.journal.seen:
                self._submit([entry])
                return {"created": False, "eventId": event_id, "isolatedAt": event["isolatedAt"]}
            self._submit([entry])
            return {"created": True, "eventId": event_id, "isolatedAt": format(at), "nodeIds": node_ids}

    def set_closure(self, payload: dict) -> dict:
        seg_id = payload.get("segmentId")
        action = payload.get("action")
        if seg_id not in self.ref.road_segments:
            raise AppError("unknown_segment", f"未知路段 {seg_id}", 404)
        if action not in ("close", "open"):
            raise AppError("bad_request", "action 必须为 close/open", 400)
        at = parse(payload.get("occurredAt"), default=self.clock())
        stamp = at.strftime("%Y%m%d%H%M%S")
        entry = self._new_entry(
            "route_closure", at,
            id=f"CLS-{seg_id}-{action}-{stamp}",
            segmentId=seg_id, action=action,
            restriction=payload.get("restriction"),
            reason=payload.get("reason"),
        )
        with self.lock:
            existed = entry["id"] in self.journal.seen
            self._submit([entry])
            # 道路变化立即重算在途路线，留下重新排序依据
            self._reroute_enroute(at)
            return {"created": not existed, "segmentId": seg_id, "action": action,
                    "closures": list(self.closures)}

    def ingest_receipt(self, payload: dict) -> dict:
        receipt_id = payload.get("receiptId")
        wo_id = payload.get("workOrderId")
        status = payload.get("status")
        if not receipt_id or not wo_id or not status:
            raise AppError("bad_request", "receiptId/workOrderId/status 必填", 400)
        if status not in RECEIPT_STATUSES:
            raise AppError("bad_status", f"未知回执状态 {status}", 400)
        with self.lock:
            order = self.orders.get(wo_id)
            if not order:
                raise AppError("not_found", "工单不存在", 404)
            duplicate = order["receipts"].get(receipt_id)
            if duplicate:
                if duplicate["status"] != status:
                    raise AppError("conflict", f"回执 {receipt_id} 已存在且状态不同", 409)
                return {"created": False, "workOrderId": wo_id, "status": order["status"],
                        "deduplicated": True, "receiptId": receipt_id}
            at = parse(payload.get("occurredAt"), default=self.clock())
            entry = self._new_entry(
                "receipt", at,
                id=f"RCP-{receipt_id}",
                receiptId=receipt_id, workOrderId=wo_id, status=status,
                depthM=_depth(payload), note=payload.get("note"),
            )
            self._submit([entry])
            order = self.orders[wo_id]
            return {"created": True, "workOrderId": wo_id, "status": order["status"],
                    "insertedAt": format(at), "offline": format(at) != format(self.clock())}

    def acknowledge(self, wo_id: str, payload: dict | None = None) -> dict:
        payload = payload or {}
        with self.lock:
            order = self.orders.get(wo_id)
            if not order:
                raise AppError("not_found", "工单不存在", 404)
            if not order["dispatch"]:
                raise AppError("not_dispatched", "工单尚未派工，无法签收", 409)
            if order["ackAt"]:
                return {"created": False, "ackAt": order["ackAt"]}
            at = parse(payload.get("occurredAt"), default=self.clock())
            entry = self._new_entry(
                "ack", at,
                id=f"ACK-{wo_id}-{order['dispatchSeq']}",
                seq=order["dispatchSeq"],
                workOrderId=wo_id, crewId=order["crewId"],
            )
            self._submit([entry])
            return {"created": True, "ackAt": format(at), "crewId": order["crewId"]}

    def merge_events(self, source_id: str, payload: dict) -> dict:
        target_id = payload.get("targetEventId")
        with self.lock:
            source = self.events.get(source_id)
            target = self.events.get(target_id)
            if not source or not target:
                raise AppError("not_found", "事件不存在", 404)
            if source_id == target_id:
                raise AppError("bad_request", "不能合并自身", 400)
            if source.get("mergedInto"):
                raise AppError("already_merged", f"事件已并入 {source['mergedInto']}", 409)
            wid = source.get("workOrderId")
            if wid and self.orders[wid]["status"] in ONSCENE_STATUSES:
                raise AppError(
                    "order_onscene",
                    f"工单 {wid} 已到场，普通合并不得取消",
                    409,
                    {"workOrderId": wid, "status": self.orders[wid]["status"]},
                )
            at = parse(payload.get("occurredAt"), default=self.clock())
            entry = self._new_entry(
                "merge", at,
                id=f"MRG-{source_id}-{target_id}-{compact(at)}",
                sourceEventId=source_id, targetEventId=target_id,
            )
            self._submit([entry])
            return {"created": True, "sourceEventId": source_id,
                    "targetEventId": target_id, "workOrderId": target.get("workOrderId")}

    # ------------------------------------------------------------- 派工核心

    def _crew_on_shift(self, crew: dict, at) -> bool:
        return crew["_shiftStart"] <= at <= crew["_shiftEnd"]

    def _parts_available(self, crew_id: str, required: dict[str, int]) -> dict[str, int]:
        cap = self.ref.crews[crew_id]["vehicle"]["capacity"]
        missing = {}
        for pid, qty in required.items():
            have = cap.get(pid, 0) - self.crew_used[crew_id].get(pid, 0)
            if have < qty:
                missing[pid] = qty - have
        return missing

    def evaluate_crew(self, order: dict, event: dict, crew_id: str, at) -> dict:
        crew = self.ref.crews[crew_id]
        vehicle = crew["vehicle"]
        hazard = event.get("hazard") or evaluate(None, None, None, [])
        reasons = []
        if not self._crew_on_shift(crew, at):
            reasons.append({
                "code": "off_shift",
                "message": f"不在班次内（{format(crew['_shiftStart'])}~{format(crew['_shiftEnd'])}）",
            })
        missing_quals = [q for q in hazard["requiredQualifications"] if q not in crew["qualifications"]]
        if missing_quals:
            reasons.append({"code": "unqualified", "message": f"缺少资质：{','.join(missing_quals)}",
                            "missingQualifications": missing_quals})
        missing_parts = self._parts_available(crew_id, hazard["requiredParts"])
        if missing_parts:
            reasons.append({"code": "missing_parts", "message": "车载备件不足",
                            "missingParts": missing_parts})
        if not order["primaryNodeId"]:
            reasons.append({"code": "location_unresolved", "message": "影响井位未确定，无法规划路线"})
        route = None
        if order["primaryNodeId"]:
            source = self.crew_location[crew_id]
            route = self.router.shortest(
                source, order["primaryNodeId"], self.closures, vehicle
            )
            if route is None:
                blocked = self._blocked_summary(crew_id, source, order["primaryNodeId"])
                reasons.append({"code": "no_route", "message": "封闭/限行导致无可达路线",
                                "blockedSegments": blocked})
        eligible = not reasons
        return {
            "crewId": crew_id,
            "eligible": eligible,
            "reasons": reasons,
            "route": route,
            "sourceNodeId": self.crew_location[crew_id],
            "travelSeconds": route["travelSeconds"] if route else None,
        }

    def _blocked_summary(self, crew_id, source, target) -> list[dict]:
        vehicle = self.ref.crews[crew_id]["vehicle"]
        result = []
        for seg_id, closure in self.closures.items():
            result.append({
                "segmentId": seg_id,
                "name": self.ref.road_segments[seg_id]["name"],
                "reason": closure.get("reason"),
                "restriction": closure.get("restriction"),
            })
        return result

    def _dispatch_candidates(self, order: dict, at) -> list[dict]:
        event = self.events[order["eventId"]]
        candidates = []
        isolation_blocked = event.get("hazard", {}).get("isolationRequired") and not event["isolatedAt"]
        for crew_id in sorted(self.ref.crews):
            ev = self.evaluate_crew(order, event, crew_id, at)
            if isolation_blocked:
                ev["reasons"].insert(0, {
                    "code": "not_isolated",
                    "message": "高危事件必须先隔离再派工",
                })
                ev["eligible"] = False
            candidates.append(ev)
        candidates.sort(key=lambda c: (
            0 if c["eligible"] else 1,
            c["travelSeconds"] if c["travelSeconds"] is not None else 10**18,
            c["crewId"],
        ))
        return candidates

    def dispatch(self, wo_id: str, payload: dict) -> dict:
        expected_version = payload.get("expectedVersion")
        with self.lock:
            order = self.orders.get(wo_id)
            if not order:
                raise AppError("not_found", "工单不存在", 404)
            if expected_version is not None and int(expected_version) != self.version:
                raise AppError(
                    "version_conflict",
                    f"工单已被其他调度员修改（版本 {self.version}）",
                    409, {"currentVersion": self.version},
                )
            if order["status"] in ONSCENE_STATUSES:
                raise AppError("order_onscene", "已到场工单不可改派", 409)
            if order["status"] == "restored":
                raise AppError("order_restored", "工单已恢复", 409)
            at = parse(payload.get("occurredAt"), default=self.clock())
            event = self.events[order["eventId"]]

            # 幂等：同一班组重复派工且路线未变 → 直接回显
            if order["dispatch"] and order["crewId"] == payload.get("crewId") and order["status"] in (
                "dispatched", "acked"
            ):
                return {"created": False, "workOrderId": wo_id,
                        "crewId": order["crewId"], "dispatch": self._dispatch_view(order)}

            candidates = self._dispatch_candidates(order, at)
            crew_id = payload.get("crewId")
            if crew_id:
                if crew_id not in self.ref.crews:
                    raise AppError("unknown_crew", f"未知队伍 {crew_id}", 404)
                chosen = next(c for c in candidates if c["crewId"] == crew_id)
                if not chosen["eligible"]:
                    raise AppError("crew_unavailable", f"队伍 {crew_id} 当前不能承接",
                                   422, {"blockers": chosen["reasons"]})
            else:
                chosen = candidates[0]
                if not chosen["eligible"]:
                    raise AppError("no_eligible_crew", "当前没有可派队伍", 422,
                                   {"blockers": chosen["reasons"],
                                    "allCrews": [{"crewId": c["crewId"], "reasons": c["reasons"]}
                                                 for c in candidates]})
            expected = at + timedelta(seconds=chosen["route"]["travelSeconds"])
            entry = self._new_entry(
                "dispatch", at,
                id=f"DSP-{wo_id}-{order['dispatchSeq'] + 1}",
                workOrderId=wo_id,
                crewId=chosen["crewId"],
                by=payload.get("by", "调度员"),
                expectedAt=format(expected),
                routeSegmentIds=chosen["route"]["segmentIds"],
                routeLengthM=chosen["route"]["lengthM"],
                routeSource=chosen["sourceNodeId"],
                partsUsed=event["hazard"]["requiredParts"],
            )
            self._submit([entry])
            order = self.orders[wo_id]
            return {"created": True, "workOrderId": wo_id, "version": self.version,
                    "dispatch": self._dispatch_view(order)}

    # ------------------------------------------------------- 在途改线/提醒

    def _crew_queue(self, crew_id: str) -> list[dict]:
        active = [o for o in self.orders.values()
                  if o["crewId"] == crew_id and o["status"] in ("dispatched", "acked")]
        return self._rank_orders(active)

    def _rank_orders(self, orders: list[dict], eta_override: dict[str, str] | None = None) -> list[dict]:
        """队伍作业队列策略：危险等级优先，同级按 ETA，再按开工时间。"""
        def key(o):
            event = self.events[o["eventId"]]
            eta = (eta_override or {}).get(o["workOrderId"]) or (
                o["dispatch"]["expectedAt"] if o["dispatch"] else format(o["openedAt"])
            )
            return (-event.get("hazard", {}).get("priority", 0), eta, format(o["openedAt"]), o["workOrderId"])
        return sorted(orders, key=key)

    def _reroute_enroute(self, at) -> list[dict]:
        """道路变化后对在途工单重算路线；变化与重新排序依据全部落盘。"""
        new_entries: list[dict] = []
        changed = []
        for crew_id, crew in self.ref.crews.items():
            queue = self._crew_queue(crew_id)
            # 变更前快照：路线、ETA、队列位次
            before = {
                o["workOrderId"]: {
                    "segments": list(o["dispatch"]["routeSegmentIds"]),
                    "expected": o["dispatch"]["expectedAt"],
                    "rank": i + 1,
                }
                for i, o in enumerate(queue)
            }
            for order in queue:
                record = order["dispatch"]
                route = self.router.shortest(
                    record["routeSource"], order["primaryNodeId"],
                    self.closures, crew["vehicle"],
                )
                old = before[order["workOrderId"]]
                if route is None:
                    # 完全不可达：阻塞事实落盘，接口展示阻塞原因。
                    # 同一段阻断只记一次（直到新派工/成功改线后再次阻断）。
                    if not self._already_blocked(order):
                        entry = self._new_entry(
                            "route_blocked", at,
                            id=(f"BLK-{order['workOrderId']}-{compact(at)}"
                                f"-{len(order['reroutes'])}"),
                            workOrderId=order["workOrderId"],
                            message=(f"道路封闭/限行后无可达路线，{crew_id} 无法到达 "
                                     f"{order['primaryNodeId']}，需改派或等待放行"),
                        )
                        new_entries.append(entry)
                        changed.append({"workOrderId": order["workOrderId"], "blocked": True})
                    continue
                if route["segmentIds"] == old["segments"]:
                    continue
                expected = at + timedelta(seconds=route["travelSeconds"])
                # 只把“在旧路线上且当前确实封闭/限行”的路段记为封闭原因；
                # 其余差异路段属于绕行路径变化
                closed = sorted((set(old["segments"]) - set(route["segmentIds"]))
                                & set(self.closures))
                if not closed:
                    closed = sorted(s for s in route["segmentIds"] if s in self.closures)
                # 新队列位次：以新 ETA 与其他在途工单比较
                rank_after = self._new_rank(crew_id, order["workOrderId"], format(expected))
                reason_bits = []
                for seg_id in closed:
                    note = self.closures.get(seg_id, {}).get("reason")
                    reason_bits.append(f"{seg_id}（{note}）" if note else seg_id)
                reason = "道路封闭/限行：" + ",".join(reason_bits) if reason_bits else "道路通行条件变化"
                reroute_no = 1 + len([
                    e for e in self.journal.entries + new_entries
                    if e.get("type") == "reroute" and e.get("workOrderId") == order["workOrderId"]
                ])
                entry = self._new_entry(
                    "reroute", at,
                    id=f"RRT-{order['workOrderId']}-{compact(at)}-{reroute_no}",
                    workOrderId=order["workOrderId"],
                    reason=reason,
                    closedSegmentIds=closed,
                    oldSegmentIds=old["segments"],
                    newSegmentIds=route["segmentIds"],
                    oldRank=old["rank"],
                    newRank=rank_after,
                    oldExpectedAt=old["expected"],
                    newExpectedAt=format(expected),
                )
                new_entries.append(entry)
                changed.append({"workOrderId": order["workOrderId"], "blocked": False})
        if new_entries:
            self._submit(new_entries)
        return changed

    def _already_blocked(self, order: dict) -> bool:
        for item in reversed(order["timeline"]):
            if item["kind"] in ("dispatched", "reroute", "receipt"):
                return False
            if item["kind"] == "route_blocked":
                return True
        return False

    def _new_rank(self, crew_id: str, wo_id: str, new_expected: str) -> int:
        others = [o for o in self.orders.values()
                  if o["crewId"] == crew_id and o["status"] in ("dispatched", "acked")]
        ranked = self._rank_orders(others, eta_override={wo_id: new_expected})
        for i, o in enumerate(ranked, start=1):
            if o["workOrderId"] == wo_id:
                return i
        return len(ranked) + 1

    def tick(self) -> dict:
        """周期任务：自动派工、超时提醒、路线巡检。提醒落盘，重启不丢。"""
        with self.lock:
            at = self.clock()
            auto: list[str] = []
            pending: list[dict] = []
            # 1. 自动派工（危险高者优先）
            open_orders = [o for o in self.orders.values() if o["status"] == "open"]
            open_orders.sort(key=lambda o: (
                -self.events[o["eventId"]].get("hazard", {}).get("priority", 0),
                o["openedAt"], o["workOrderId"],
            ))
            for order in open_orders:
                candidates = self._dispatch_candidates(order, at)
                chosen = candidates[0]
                if not chosen["eligible"]:
                    continue
                event = self.events[order["eventId"]]
                expected = at + timedelta(seconds=chosen["route"]["travelSeconds"])
                entry = self._new_entry(
                    "dispatch", at,
                    id=f"DSP-{order['workOrderId']}-{order['dispatchSeq'] + 1}",
                    workOrderId=order["workOrderId"],
                    crewId=chosen["crewId"], by="auto-scheduler",
                    expectedAt=format(expected),
                    routeSegmentIds=chosen["route"]["segmentIds"],
                    routeLengthM=chosen["route"]["lengthM"],
                    routeSource=chosen["sourceNodeId"],
                    partsUsed=event["hazard"]["requiredParts"],
                )
                if entry["id"] not in self.journal.seen:
                    pending.append(entry)
                    auto.append(order["workOrderId"])
            if pending:
                self._submit(pending)

            # 2. 超时提醒（幂等 id，重启后仍只提醒一次）
            reminders: list[str] = []
            pending = []
            for order in self.orders.values():
                if order["status"] == "dispatched" and order["dispatch"]:
                    dispatched_at = parse(order["dispatch"]["at"])
                    seq = order["dispatch"]["seq"]
                    esc_id = f"ESC-{order['workOrderId']}-ack{seq}"
                    if at - dispatched_at > timedelta(seconds=self.config.ack_timeout_seconds) \
                            and esc_id not in self.journal.seen:
                        entry = self._new_entry(
                            "escalation", at, id=esc_id,
                            workOrderId=order["workOrderId"], escalationKind="ack_timeout",
                            detail=f"派工后 {self.config.ack_timeout_seconds // 60} 分钟未签收",
                        )
                        pending.append(entry)
                        reminders.append(esc_id)
                if order["status"] in ("onsite", "working") and order["onsiteAt"]:
                    onsite_at = parse(order["onsiteAt"])
                    esc_id = f"ESC-{order['workOrderId']}-restore"
                    if at - onsite_at > timedelta(seconds=self.config.restore_timeout_seconds) \
                            and esc_id not in self.journal.seen:
                        entry = self._new_entry(
                            "escalation", at, id=esc_id,
                            workOrderId=order["workOrderId"], escalationKind="restore_timeout",
                            detail=f"到场后 {self.config.restore_timeout_seconds // 60} 分钟未恢复",
                        )
                        pending.append(entry)
                        reminders.append(esc_id)
            if pending:
                self._submit(pending)

            # 3. 路线巡检（道路变化事件已即时处理，这里兜底）
            rerouted = self._reroute_enroute(at)
            return {"at": format(at), "autoDispatched": auto,
                    "reminders": reminders, "rerouted": rerouted}

    # --------------------------------------------------------------- 视图

    def _dispatch_view(self, order: dict) -> dict | None:
        rec = order["dispatch"]
        if not rec:
            return None
        expected = parse(rec["expectedAt"])
        crew = self.ref.crews[order["crewId"]]
        return {
            "crewId": order["crewId"],
            "crewName": crew["name"],
            "vehicleId": crew["vehicle"]["vehicleId"],
            "plate": crew["vehicle"]["plate"],
            "dispatchedAt": rec["at"],
            "by": rec["by"],
            "sourceNodeId": rec["routeSource"],
            "routeSegmentIds": rec["routeSegmentIds"],
            "routeLengthM": rec["routeLengthM"],
            "expectedArrivalEarly": rec["expectedAt"],
            "expectedArrivalLate": format(expected + timedelta(seconds=self.router.slack_seconds)),
            "partsUsed": rec.get("partsUsed", {}),
            "seq": rec["seq"],
        }

    def _order_blockers(self, order: dict, at) -> list[dict]:
        if order["status"] in ("open",):
            candidates = self._dispatch_candidates(order, at)
            top = candidates[0]
            blockers = []
            if not top["eligible"]:
                for c in candidates:
                    if c["reasons"]:
                        blockers.append({"crewId": c["crewId"], "reasons": c["reasons"]})
            return blockers
        if order["status"] in ("dispatched", "acked") and order["dispatch"]:
            crew = self.ref.crews[order["crewId"]]
            route = self.router.shortest(
                order["dispatch"]["routeSource"], order["primaryNodeId"],
                self.closures, crew["vehicle"],
            )
            if route is None:
                return [{"crewId": order["crewId"], "reasons": [{
                    "code": "route_blocked",
                    "message": "在途路线因封闭/限行完全阻断，需改派或等待放行",
                    "closedSegments": [s for s in self.closures],
                }]}]
        return []

    def order_view(self, wo_id: str) -> dict:
        with self.lock:
            order = self.orders.get(wo_id)
            if not order:
                raise AppError("not_found", "工单不存在", 404)
            at = self.clock()
            event = self.events[order["eventId"]]
            queue_rank = None
            queued_behind = 0
            if order["crewId"] and order["status"] in ("dispatched", "acked"):
                queue = self._crew_queue(order["crewId"])
                for i, o in enumerate(queue):
                    if o["workOrderId"] == wo_id:
                        queue_rank = i + 1
                        queued_behind = i
                        break
            receipts = sorted(order["receipts"].values(), key=lambda r: r["at"])
            return {
                "workOrderId": wo_id,
                "eventId": order["eventId"],
                "status": order["status"],
                "community": order["community"],
                "primaryNodeId": order["primaryNodeId"],
                "affectedNodeIds": order["nodeIds"],
                "impactArea": self._impact_view(event),
                "severity": event.get("hazard", {}).get("severity"),
                "hazard": event.get("hazard"),
                "isolated": bool(event.get("isolatedAt")),
                "openedAt": format(order["openedAt"]),
                "ackAt": format(order["ackAt"]) if order["ackAt"] else None,
                "onsiteAt": format(order["onsiteAt"]) if order["onsiteAt"] else None,
                "restoredAt": format(order["restoredAt"]) if order["restoredAt"] else None,
                "cancelledAt": format(order["cancelledAt"]) if order["cancelledAt"] else None,
                "cancelReason": order["cancelReason"],
                "currentCrew": self._dispatch_view(order),
                "queueRank": queue_rank,
                "queuedBehind": queued_behind,
                "blockers": self._order_blockers(order, at),
                "receipts": receipts,
                "escalations": order["escalations"],
                "reroutes": order["reroutes"],
                "timeline": self._event_record(event, order)["timeline"],
            }

    def _impact_view(self, event: dict) -> dict:
        return {
            "groupId": event["groupId"],
            "community": event["community"],
            "radiusM": event.get("radiusM"),
            "nodeIds": list(event["nodeIds"]),
            "locationText": event.get("locationText"),
            "depthM": event.get("latestDepthM"),
            "depthAt": format(event["depthAt"]) if event.get("depthAt") else None,
        }

    def _event_record(self, event: dict, order: dict | None = None) -> dict:
        timeline = list(event["timeline"])
        if order:
            timeline = timeline + [t for t in order["timeline"]
                                   if t not in event["timeline"]]
        timeline.sort(key=lambda t: (t["at"], t.get("ref") or ""))
        return {"timeline": timeline}

    def event_view(self, event_id: str) -> dict:
        with self.lock:
            event = self.events.get(event_id)
            if not event:
                raise AppError("not_found", "事件不存在", 404)
            order = self.orders.get(event["workOrderId"]) if event.get("workOrderId") else None
            hazard = event.get("hazard") or evaluate(None, None, None, [])
            return {
                "eventId": event_id,
                "status": self._event_status(event),
                "mergedInto": event.get("mergedInto"),
                "impactArea": self._impact_view(event),
                "severity": hazard["severity"],
                "hazard": hazard,
                "isolated": bool(event.get("isolatedAt")),
                "isolatedAt": format(event["isolatedAt"]) if event.get("isolatedAt") else None,
                "openedAt": format(event["openedAt"]),
                "restoredAt": format(event["restoredAt"]) if event.get("restoredAt") else None,
                "reportIds": event["reportIds"],
                "sensorIds": event["sensorIds"],
                "callCount": len(event["reportIds"]),
                "workOrderId": event.get("workOrderId"),
                "workOrderStatus": order["status"] if order else None,
                "timeline": self._event_record(event, order)["timeline"],
            }

    def _event_status(self, event: dict) -> str:
        if event.get("mergedInto"):
            return "merged"
        if event.get("restoredAt"):
            return "restored"
        order = self.orders.get(event["workOrderId"]) if event.get("workOrderId") else None
        return order["status"] if order else "active"

    def list_events(self, *, status: str | None = None) -> list[dict]:
        with self.lock:
            views = []
            for eid, event in self.events.items():
                view = self.event_view(eid)
                if status and view["status"] != status:
                    continue
                views.append({k: view[k] for k in (
                    "eventId", "status", "severity", "impactArea", "workOrderId",
                    "workOrderStatus", "isolated", "callCount", "openedAt")})
            views.sort(key=lambda v: v["openedAt"], reverse=True)
            return views

    def list_orders(self) -> list[dict]:
        with self.lock:
            result = []
            at = self.clock()
            for wid, order in self.orders.items():
                event = self.events[order["eventId"]]
                result.append({
                    "workOrderId": wid,
                    "eventId": order["eventId"],
                    "status": order["status"],
                    "severity": event.get("hazard", {}).get("severity"),
                    "community": order["community"],
                    "primaryNodeId": order["primaryNodeId"],
                    "crewId": order["crewId"],
                    "expectedArrivalEarly": order["dispatch"]["expectedAt"] if order["dispatch"] else None,
                    "openedAt": format(order["openedAt"]),
                    "blockerCount": sum(len(b["reasons"]) for b in self._order_blockers(order, at)),
                })
            result.sort(key=lambda o: (o["openedAt"], o["workOrderId"]), reverse=True)
            return result

    def crews_view(self) -> list[dict]:
        with self.lock:
            at = self.clock()
            out = []
            for cid, crew in sorted(self.ref.crews.items()):
                queue = self._crew_queue(cid)
                cap = crew["vehicle"]["capacity"]
                used = self.crew_used[cid]
                out.append({
                    "crewId": cid,
                    "name": crew["name"],
                    "qualifications": crew["qualifications"],
                    "onShift": self._crew_on_shift(crew, at),
                    "shift": {"start": format(crew["_shiftStart"]), "end": format(crew["_shiftEnd"])},
                    "locationNodeId": self.crew_location[cid],
                    "vehicle": {"vehicleId": crew["vehicle"]["vehicleId"], "plate": crew["vehicle"]["plate"],
                                "wadingLimitM": crew["vehicle"]["wadingLimitM"],
                                "heightM": crew["vehicle"]["heightM"], "gvwT": crew["vehicle"]["gvwT"]},
                    "queue": [o["workOrderId"] for o in queue],
                    "partsAvailable": {pid: cap.get(pid, 0) - used.get(pid, 0)
                                       for pid in cap},
                })
            return out

    def situation(self) -> dict:
        with self.lock:
            at = self.clock()
            return {
                "at": format(at),
                "version": self.version,
                "closures": list(self.closures.values()),
                "events": self.list_events(),
                "workOrders": self.list_orders(),
                "crews": self.crews_view(),
            }


def _depth(payload: dict) -> float | None:
    value = payload.get("depthM")
    if value is None:
        return None
    try:
        depth = float(value)
    except (TypeError, ValueError):
        raise AppError("bad_request", "depthM 必须是数字", 400)
    if depth < 0:
        raise AppError("bad_request", "depthM 不能为负", 400)
    return depth
