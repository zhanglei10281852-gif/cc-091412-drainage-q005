"""HTTP 接口：报修/IoT 接入、隔离派工、回执、道路管制、调度看板与事件记录。

健康接口只表示进程存活；业务数据落盘到 DATA_DIR（默认 .data）。
"""
from __future__ import annotations

import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

from drainage.reference import load_reference
from drainage.service import ConflictError, DispatchService, ValidationError
from drainage.store import EventStore

SERVICE_NAME = "drainage-emergency-dispatch"


class _ReminderWorker(threading.Thread):
    """后台扫描未签收/超时未到场提醒；事件已落盘，重启后不丢、不重复。"""

    def __init__(self, service: DispatchService, interval: float):
        super().__init__(daemon=True, name="reminder-worker")
        self._service = service
        self._interval = interval
        self._stop = threading.Event()

    def run(self):
        self._service.scan_reminders()
        while not self._stop.wait(self._interval):
            self._service.scan_reminders()

    def stop(self):
        self._stop.set()


def _json_response(handler: BaseHTTPRequestHandler, status: int, payload: dict | list):
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def create_handler(service: DispatchService):
    class Handler(BaseHTTPRequestHandler):
        def _read_json(self) -> dict:
            length = int(self.headers.get("Content-Length", 0))
            if not length:
                return {}
            try:
                payload = json.loads(self.rfile.read(length).decode("utf-8"))
            except json.JSONDecodeError as exc:
                raise ValidationError(f"请求体不是合法 JSON: {exc}")
            if not isinstance(payload, dict):
                raise ValidationError("请求体必须是 JSON 对象")
            return payload

        def _handle_error(self, exc: Exception):
            if isinstance(exc, ValidationError):
                _json_response(self, 400, {"error": "bad_request", "message": str(exc)})
            elif isinstance(exc, ConflictError):
                _json_response(self, 409, {"error": "conflict", "message": str(exc)})
            else:
                _json_response(self, 500, {"error": "internal", "message": str(exc)})

        def do_GET(self):  # noqa: N802
            try:
                parsed = urlparse(self.path)
                path, query = parsed.path, parse_qs(parsed.query)
                if path == "/health":
                    _json_response(self, 200, {"status": "ok", "service": SERVICE_NAME})
                elif path == "/board":
                    _json_response(self, 200, service.board_view())
                elif path == "/teams":
                    _json_response(self, 200, {"teams": service.teams_view()})
                elif path.startswith("/incidents/"):
                    _json_response(self, 200, service.incident_view(path.rsplit("/", 1)[1]))
                elif path.startswith("/work-orders/") and path.endswith("/timeline"):
                    wo_id = path.split("/")[2]
                    _json_response(self, 200, service.timeline_view(wo_id))
                elif path.startswith("/work-orders/"):
                    wo_id = path.rsplit("/", 1)[1]
                    _json_response(self, 200, service.work_order_view(wo_id))
                else:
                    _json_response(self, 404, {"error": "not_found"})
            except Exception as exc:  # noqa: BLE001
                self._handle_error(exc)

        def do_POST(self):  # noqa: N802
            try:
                path = urlparse(self.path).path.rstrip("/") or "/"
                payload = self._read_json()
                if path == "/reports":
                    _json_response(self, 201, service.ingest_report(payload))
                elif path == "/measurements":
                    _json_response(self, 201, service.ingest_measurement(payload))
                elif path == "/receipts":
                    _json_response(self, 201, service.receive_receipt(payload))
                elif path == "/incidents/merge":
                    _json_response(self, 200, service.merge_incidents(
                        payload.get("incidentId"), payload.get("sourceIds", []),
                        payload.get("reason", "现场确认同一影响范围")))
                elif path.startswith("/incidents/") and path.endswith("/isolation"):
                    incident_id = path.split("/")[2]
                    _json_response(self, 201, service.dispatch_isolation(
                        incident_id, dispatcher=payload.get("dispatcher", "调度员")))
                elif path.startswith("/work-orders/") and path.endswith("/dispatch"):
                    wo_id = path.split("/")[2]
                    result = service.dispatch(
                        wo_id, dispatcher=payload.get("dispatcher", "调度员"),
                        expected_version=payload.get("expectedVersion"))
                    _json_response(self, 200, result)
                elif path.startswith("/roads/") and path.endswith("/close"):
                    edge_id = path.split("/")[2]
                    _json_response(self, 200, service.close_road(edge_id, payload.get("reason", "临时封闭")))
                elif path.startswith("/roads/") and path.endswith("/reopen"):
                    edge_id = path.split("/")[2]
                    _json_response(self, 200, service.reopen_road(edge_id))
                elif path == "/admin/scan-reminders":
                    _json_response(self, 200, {"fired": service.scan_reminders()})
                else:
                    _json_response(self, 404, {"error": "not_found"})
            except Exception as exc:  # noqa: BLE001
                self._handle_error(exc)

        def log_message(self, *_args):
            return

    return Handler


def build_service(data_dir: str | None = None, clock=None) -> DispatchService:
    return DispatchService(load_reference(), EventStore(data_dir), clock=clock)


def create_server(data_dir: str | None = None, clock=None):
    port = int(os.environ.get("PORT", "8000"))
    host = os.environ.get("HOST", "0.0.0.0")
    service = build_service(data_dir or os.environ.get("DATA_DIR") or None, clock=clock)
    handler = create_handler(service)
    server = ThreadingHTTPServer((host, port), handler)
    interval = float(os.environ.get("REMINDER_INTERVAL_SECONDS", "30"))
    server.reminder_worker = _ReminderWorker(service, interval)
    server.reminder_worker.start()
    original_shutdown = server.shutdown

    def shutdown():
        server.reminder_worker.stop()
        original_shutdown()

    server.shutdown = shutdown
    return server
