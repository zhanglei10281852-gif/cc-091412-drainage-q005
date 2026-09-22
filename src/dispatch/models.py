"""危险等级判定与事件/工单状态常量。"""
from __future__ import annotations

from typing import Any

# 现场回执状态机（按发生时间排序，restored 为终态）
RECEIPT_STATUSES = {"dispatched", "isolated", "onsite", "working", "restored"}
ORDER_TERMINAL = "restored"
# 已到场之后的状态：普通合并/改派不得再取消
ONSCENE_STATUSES = {"onsite", "working", "restored"}

SEVERITY_ORDER = {"none": 0, "moderate": 1, "high": 2, "critical": 3}


def _cmp(value: float, op: str, target: float) -> bool:
    if op == ">=":
        return value >= target
    if op == ">":
        return value > target
    if op == "<=":
        return value <= target
    if op == "<":
        return value < target
    raise ValueError(f"unsupported op: {op}")


def match_hazards(depth_m: float | None, node: dict | None, community: str | None,
                  rules: list[dict]) -> list[dict]:
    matched = []
    for rule in rules:
        cond = rule["condition"]
        ok = True
        if "field" in cond:
            if depth_m is None:
                ok = False
            else:
                ok = _cmp(depth_m, cond["op"], cond["value"])
        if ok and "nodeKindIn" in cond:
            ok = bool(node) and node.get("kind") in cond["nodeKindIn"]
        if ok and "communityIn" in cond:
            ok = community in cond["communityIn"]
        if ok:
            matched.append(rule)
    return sorted(matched, key=lambda r: (-r["priority"], r["hazardId"]))


def evaluate(depth_m: float | None, node: dict | None, community: str | None,
             rules: list[dict]) -> dict[str, Any]:
    matched = match_hazards(depth_m, node, community, rules)
    quals: set[str] = set()
    parts: dict[str, int] = {}
    isolation_required = False
    severity = "none"
    for rule in matched:
        quals.update(rule.get("requiredQualifications", []))
        for part_id, qty in rule.get("requiredParts", {}).items():
            parts[part_id] = max(parts.get(part_id, 0), qty)
        isolation_required = isolation_required or rule.get("isolateBeforeDispatch", False)
        if rule["priority"] >= 100:
            severity = "critical"
        elif rule["priority"] >= 80 and SEVERITY_ORDER[severity] < SEVERITY_ORDER["high"]:
            severity = "high"
        elif SEVERITY_ORDER[severity] < SEVERITY_ORDER["moderate"]:
            severity = "moderate"
    return {
        "hazardIds": [r["hazardId"] for r in matched],
        "hazardLabels": [r["label"] for r in matched],
        "severity": severity,
        "priority": matched[0]["priority"] if matched else 0,
        "requiredQualifications": sorted(quals),
        "requiredParts": dict(sorted(parts.items())),
        "isolationRequired": isolation_required,
    }
