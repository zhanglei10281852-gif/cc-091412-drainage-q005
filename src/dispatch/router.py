"""可通行路线：道路图 + 临时封闭/限行，最短路与预计到达窗口。"""
from __future__ import annotations

import heapq
from typing import Any


class Router:
    def __init__(self, segments: list[dict], speed_kmh: float, fixed_seconds: int,
                 slack_seconds: int) -> None:
        self.adj: dict[str, list[tuple[str, str, int]]] = {}
        self.segment_by_nodes: dict[tuple[str, str], str] = {}
        self.segments = {s["segmentId"]: s for s in segments}
        for seg in segments:
            a, b = seg["from"], seg["to"]
            self.adj.setdefault(a, []).append((b, seg["segmentId"], seg["lengthM"]))
            self.adj.setdefault(b, []).append((a, seg["segmentId"], seg["lengthM"]))
            self.segment_by_nodes[(a, b)] = seg["segmentId"]
            self.segment_by_nodes[(b, a)] = seg["segmentId"]
        self.speed_ms = speed_kmh * 1000.0 / 3600.0
        self.fixed_seconds = fixed_seconds
        self.slack_seconds = slack_seconds

    def _blocked_segments(self, closures: dict[str, dict], vehicle: dict | None) -> set[str]:
        """返回对该车辆不可通行的路段集合。"""
        blocked = set()
        for seg_id, closure in closures.items():
            restriction = closure.get("restriction")
            if not restriction:
                blocked.add(seg_id)  # 全封闭
                continue
            if vehicle is not None:
                max_h = restriction.get("maxHeightM")
                max_gvw = restriction.get("maxGvwT")
                min_wading = restriction.get("minWadingM")
                if max_h is not None and vehicle.get("heightM", 0) > max_h:
                    blocked.add(seg_id)
                elif max_gvw is not None and vehicle.get("gvwT", 0) > max_gvw:
                    blocked.add(seg_id)
                elif min_wading is not None and vehicle.get("wadingLimitM", 0) < min_wading:
                    blocked.add(seg_id)
        return blocked

    def shortest(self, source: str, target: str, closures: dict[str, dict],
                 vehicle: dict | None = None,
                 via: str | None = None) -> dict[str, Any] | None:
        blocked = self._blocked_segments(closures, vehicle)
        waypoints = [source] + ([via] if via else []) + [target]
        total_len = 0
        node_path: list[str] = []
        seg_path: list[str] = []
        for a, b in zip(waypoints, waypoints[1:]):
            part = self._dijkstra(a, b, blocked)
            if part is None:
                return None
            if node_path:
                part_nodes = part["nodeIds"][1:]
                part_segs = part["segmentIds"]
            else:
                part_nodes = part["nodeIds"]
                part_segs = part["segmentIds"]
            node_path.extend(part_nodes)
            seg_path.extend(part_segs)
            total_len += part["lengthM"]
        seconds = self.fixed_seconds + int(total_len / self.speed_ms)
        return {
            "nodeIds": node_path,
            "segmentIds": seg_path,
            "lengthM": total_len,
            "travelSeconds": seconds,
        }

    def _dijkstra(self, source: str, target: str, blocked: set[str]) -> dict | None:
        if source == target:
            return {"nodeIds": [source], "segmentIds": [], "lengthM": 0}
        dist = {source: 0}
        prev: dict[str, tuple[str, str]] = {}
        heap = [(0, source)]
        while heap:
            d, node = heapq.heappop(heap)
            if d != dist.get(node):
                continue
            if node == target:
                break
            for nxt, seg_id, length in self.adj.get(node, []):
                if seg_id in blocked:
                    continue
                nd = d + length
                if nd < dist.get(nxt, 10**18):
                    dist[nxt] = nd
                    prev[nxt] = (node, seg_id)
                    heapq.heappush(heap, (nd, nxt))
        if target not in dist:
            return None
        node_ids = [target]
        seg_ids: list[str] = []
        cur = target
        while cur != source:
            prev_node, seg_id = prev[cur]
            seg_ids.append(seg_id)
            node_ids.append(prev_node)
            cur = prev_node
        node_ids.reverse()
        seg_ids.reverse()
        return {"nodeIds": node_ids, "segmentIds": seg_ids, "lengthM": dist[target]}

    def blocked_on_route(self, segment_ids: list[str], closures: dict[str, dict],
                         vehicle: dict | None) -> list[str]:
        return sorted(set(segment_ids) & self._blocked_segments(closures, vehicle))

    def eta_window(self, route_seconds: int) -> int:
        return self.slack_seconds
