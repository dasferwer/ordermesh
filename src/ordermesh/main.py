import logging
import time
from uuid import uuid4

from fastapi import FastAPI, Request, Response

from ordermesh.api import router
from ordermesh.config import get_settings

settings = get_settings()
logging.basicConfig(
    level=settings.log_level.upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("ordermesh.http")
logging.getLogger("pika").setLevel(logging.WARNING)

app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    summary="Event-driven order processing with idempotency, retry queues and DLQ",
)
app.include_router(router)


@app.middleware("http")
async def correlation_context(request: Request, call_next) -> Response:  # type: ignore[no-untyped-def]
    correlation_id = request.headers.get("X-Correlation-ID", str(uuid4()))[:128]
    request.state.correlation_id = correlation_id
    started_at = time.perf_counter()
    response = await call_next(request)
    response.headers["X-Correlation-ID"] = correlation_id
    logger.info(
        "correlation_id=%s method=%s path=%s status=%s duration_ms=%s",
        correlation_id,
        request.method,
        request.url.path,
        response.status_code,
        round((time.perf_counter() - started_at) * 1000, 2),
    )
    return response


@app.get("/", include_in_schema=False)
def root() -> dict[str, str]:
    return {"service": settings.app_name, "docs": "/docs", "health": "/health"}
