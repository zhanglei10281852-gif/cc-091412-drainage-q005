import os
from http.server import ThreadingHTTPServer

from dispatch.api import make_handler
from dispatch.service import Service

SERVICE_NAME = "drainage-service-starter"


def build_service() -> Service:
    return Service(
        start_scheduler=os.environ.get("DISPATCH_SCHEDULER", "1") == "1"
    )


def create_server(service: Service | None = None):
    service = service or build_service()
    port = int(os.environ.get("PORT", "8000"))
    host = os.environ.get("HOST", "0.0.0.0")
    handler = make_handler(service, name=SERVICE_NAME)
    server = ThreadingHTTPServer((host, port), handler)
    server.dispatch_service = service  # type: ignore[attr-defined]
    return server
