"""HTTP 接口层（标准库 http.server，线程模型由引擎锁保证一致）。"""
from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse

from .engine import AppError

ROUTES = []


def route(method: str, pattern: str):
    def register(func):
        ROUTES.append((method, pattern, func))
        return func
    return register


class Api:
    """所有业务路由的纯函数式处理器集合，便于测试直接调用。"""

    def __init__(self, service) -> None:
        self.service = service
        self.engine = service.engine

    @route("POST", "/v1/reports")
    def create_report(self, req, body):
        return 201, self.engine.ingest_report(body)

    @route("POST", "/v1/sensor-readings")
    def create_sensor(self, req, body):
        return 201, self.engine.ingest_sensor(body)

    @route("POST", "/v1/events/<eventId>/isolation")
    def isolate(self, req, body, eventId):
        return 201, self.engine.isolate(eventId, body or {})

    @route("POST", "/v1/events/<eventId>/merge")
    def merge(self, req, body, eventId):
        return 201, self.engine.merge_events(eventId, body or {})

    @route("GET", "/v1/events/<eventId>")
    def event(self, req, _body, eventId):
        return 200, self.engine.event_view(eventId)

    @route("GET", "/v1/events")
    def events(self, req, _body):
        status = req["query"].get("status", [None])[0]
        return 200, {"events": self.engine.list_events(status=status)}

    @route("POST", "/v1/route-closures")
    def closures(self, req, body):
        return 201, self.engine.set_closure(body)

    @route("GET", "/v1/route-closures")
    def list_closures(self, req, _body):
        return 200, {"closures": list(self.engine.closures.values())}

    @route("POST", "/v1/work-orders/<woId>/dispatch")
    def dispatch(self, req, body, woId):
        result = self.engine.dispatch(woId, body or {})
        return 201 if result.get("created") else 200, result

    @route("POST", "/v1/work-orders/<woId>/ack")
    def ack(self, req, body, woId):
        result = self.engine.acknowledge(woId, body or {})
        return 201 if result.get("created") else 200, result

    @route("GET", "/v1/work-orders/<woId>")
    def work_order(self, req, _body, woId):
        return 200, self.engine.order_view(woId)

    @route("GET", "/v1/work-orders")
    def work_orders(self, req, _body):
        return 200, {"workOrders": self.engine.list_orders()}

    @route("POST", "/v1/receipts")
    def receipts(self, req, body):
        result = self.engine.ingest_receipt(body)
        return 201 if result.get("created") else 200, result

    @route("GET", "/v1/crews")
    def crews(self, req, _body):
        return 200, {"crews": self.engine.crews_view()}

    @route("GET", "/v1/situation")
    def situation(self, req, _body):
        return 200, self.engine.situation()

    @route("POST", "/v1/scheduler/tick")
    def tick(self, req, _body):
        return 200, self.engine.tick()


def _match(pattern: str, path: str):
    p_parts = [p for p in pattern.split("/") if p]
    a_parts = [p for p in path.split("/") if p]
    if len(p_parts) != len(a_parts):
        return None
    kwargs = {}
    for pp, ap in zip(p_parts, a_parts):
        if pp.startswith("<") and pp.endswith(">"):
            kwargs[pp[1:-1]] = ap
        elif pp != ap:
            return None
    return kwargs


def make_handler(service, name: str = "drainage-dispatch-service") -> type[BaseHTTPRequestHandler]:
    api = Api(service)

    class Handler(BaseHTTPRequestHandler):
        def _send(self, status: int, payload: dict) -> None:
            data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _handle(self, method: str) -> None:
            parsed = urlparse(self.path)
            if parsed.path == "/health":
                self._send(200, {"status": "ok", "service": name})
                return
            body = None
            if method == "POST":
                length = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(length) if length else b""
                if raw:
                    try:
                        body = json.loads(raw.decode("utf-8"))
                    except json.JSONDecodeError:
                        self._send(400, {"error": "bad_json"})
                        return
                    if not isinstance(body, dict):
                        self._send(400, {"error": "body_must_be_object"})
                        return
            for m, pattern, func in ROUTES:
                if m != method:
                    continue
                kwargs = _match(pattern, parsed.path)
                if kwargs is None:
                    continue
                try:
                    status, payload = func(
                        api,
                        {"query": parse_qs(parsed.query)},
                        body,
                        **kwargs,
                    )
                    self._send(status, payload)
                except AppError as exc:
                    self._send(exc.status, {
                        "error": exc.code, "message": exc.message, "details": exc.details,
                    })
                return
            self._send(404, {"error": "not_found"})

        def do_GET(self):  # noqa: N802
            self._handle("GET")

        def do_POST(self):  # noqa: N802
            self._handle("POST")

        def log_message(self, *_args):
            return

    return Handler
