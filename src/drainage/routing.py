"""可通行路线：在当前未封闭道路上按车型限制求最短路（Dijkstra）。"""
from __future__ import annotations

import heapq

from .reference import VEHICLE_CLASS_RANK


class RoutePlanner:
    def __init__(self, reference):
        self.ref = reference

    def shortest(self, origin: str, dest: str, vehicle_class: str,
                 closed_edges: set[str]) -> tuple[int, list[str], list[str]] | None:
        """返回 (总长度米, 途经边ID列表, 阻塞原因列表——不可通行边)。不可达返回 None。"""
        blocked_reasons: list[str] = []
        dist = {origin: 0}
        prev: dict[str, tuple[str, str]] = {}
        queue = [(0, origin)]
        visited: set[str] = set()
        while queue:
            acc, node = heapq.heappop(queue)
            if node in visited:
                continue
            visited.add(node)
            if node == dest:
                edges: list[str] = []
                cur = dest
                while cur != origin:
                    edge_id, parent = prev[cur]
                    edges.append(edge_id)
                    cur = parent
                edges.reverse()
                return acc, edges, blocked_reasons
            for edge_id, nxt, length, max_class in self.ref.adj.get(node, []):
                if edge_id in closed_edges:
                    reason = f"{edge_id}:道路封闭"
                    if reason not in blocked_reasons:
                        blocked_reasons.append(reason)
                    continue
                if max_class and VEHICLE_CLASS_RANK[vehicle_class] > VEHICLE_CLASS_RANK[max_class]:
                    reason = f"{edge_id}:禁止{vehicle_class}车通行(限{max_class}及以下)"
                    if reason not in blocked_reasons:
                        blocked_reasons.append(reason)
                    continue
                nxt_dist = acc + length
                if nxt_dist < dist.get(nxt, 10**18):
                    dist[nxt] = nxt_dist
                    prev[nxt] = (edge_id, node)
                    heapq.heappush(queue, (nxt_dist, nxt))
        return None
