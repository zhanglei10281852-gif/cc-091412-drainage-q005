"""基础参考数据加载：井位拓扑、队伍班次/车辆装载、危险作业规则、备件、路网。"""
from __future__ import annotations

import json
from pathlib import Path

from .timeutil import parse


def _load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _load_jsonl(path: Path) -> list[dict]:
    rows = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


class ReferenceData:
    def __init__(self, directory: Path) -> None:
        self.directory = directory
        topology = _load_json(directory / "topology.json")
        crews_doc = _load_json(directory / "crews.json")
        hazards_doc = _load_json(directory / "hazard_rules.json")
        parts_doc = _load_json(directory / "parts.json")
        roads_doc = _load_json(directory / "roads.json")

        self.nodes: dict[str, dict] = {n["nodeId"]: n for n in topology["nodes"]}
        self.edges = topology["edges"]
        self.merge_groups: dict[str, dict] = {
            g["groupId"]: g for g in topology["mergeGroups"]
        }
        self.node_group: dict[str, str] = {}
        self.community_group: dict[str, str] = {}
        for group in topology["mergeGroups"]:
            for node_id in group["nodeIds"]:
                self.node_group[node_id] = group["groupId"]
            self.community_group.setdefault(group["community"], group["groupId"])

        self.crews: dict[str, dict] = {c["crewId"]: c for c in crews_doc["crews"]}
        self.hazards = hazards_doc["hazards"]
        self.parts = {p["partId"]: p for p in parts_doc["parts"]}
        self.warehouses = parts_doc["warehouses"]

        self.road_segments: dict[str, dict] = {
            s["segmentId"]: s for s in roads_doc["segments"]
        }
        self.speed_kmh = roads_doc["speedKmh"]
        self.fixed_seconds = roads_doc["fixedSeconds"]
        self.slack_seconds = roads_doc["slackSeconds"]

        for crew in self.crews.values():
            crew["_shiftStart"] = parse(crew["shift"]["start"])
            crew["_shiftEnd"] = parse(crew["shift"]["end"])

    def group_for(self, *, node_id: str | None, community: str | None) -> str | None:
        if node_id and node_id in self.node_group:
            return self.node_group[node_id]
        if community and community in self.community_group:
            return self.community_group[community]
        return None

    def group_node(self, group_id: str, node_id: str | None) -> str:
        members = self.merge_groups[group_id]["nodeIds"]
        if node_id in members:
            return node_id  # type: ignore[return-value]
        return members[0]
