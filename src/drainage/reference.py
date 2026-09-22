"""参考数据加载与预处理：井位拓扑、道路图、队伍、车辆装载、危险规则。"""
from __future__ import annotations

import json
from pathlib import Path

VEHICLE_CLASS_RANK = {"小型": 1, "中型": 2, "大型": 3}


class Reference:
    def __init__(self, data: dict):
        self.raw = data
        self.wells = {w["id"]: w for w in data["wells"]}
        self.parts = {p["id"]: p for p in data["parts"]}
        self.vehicles = {v["id"]: v for v in data["vehicles"]}
        self.teams = {t["id"]: t for t in data["teams"]}
        self.road_nodes = {n["id"]: n for n in data["roadNodes"]}
        self.edges = {e["id"]: e for e in data["roadEdges"]}
        self.rules = data["rules"]

        # 井位水力连通分量（并查集）
        parent = {w: w for w in self.wells}

        def find(x: str) -> str:
            parent.setdefault(x, x)
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        for pipe in data["pipes"]:
            parent[find(pipe["from"])] = find(pipe["to"])
        self.well_component = {w: find(w) for w in self.wells}

        # 道路邻接表（双向通行）
        self.adj: dict[str, list[tuple[str, str, int, str | None]]] = {}
        for edge in data["roadEdges"]:
            row = (edge["id"], edge["to"], edge["lengthM"], edge.get("maxClass"))
            self.adj.setdefault(edge["from"], []).append(row)
            rev = (edge["id"], edge["from"], edge["lengthM"], edge.get("maxClass"))
            self.adj.setdefault(edge["to"], []).append(rev)

    def danger_rule(self, depth_cm: int) -> dict:
        """按积水深度取最高适用的危险等级规则。"""
        chosen = self.rules["dangerLevels"][-1]
        for rule in sorted(self.rules["dangerLevels"], key=lambda r: -r["minDepthCm"]):
            if depth_cm >= rule["minDepthCm"]:
                chosen = rule
                break
        return chosen

    def well_ids(self, values: list[str] | None) -> list[str]:
        if not values:
            return []
        unknown = [w for w in values if w not in self.wells]
        if unknown:
            raise ValueError(f"未知井位: {', '.join(unknown)}")
        return values


def load_reference(path: str | Path | None = None) -> Reference:
    path = Path(path) if path else Path(__file__).resolve().parents[2] / "reference" / "drainage.json"
    with path.open(encoding="utf-8") as fh:
        return Reference(json.load(fh))
