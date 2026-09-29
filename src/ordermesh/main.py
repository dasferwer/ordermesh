import logging
import re
import time
from collections.abc import Awaitable, Callable
from uuid import uuid4

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from sqlalchemy.exc import OperationalError
from sqlalchemy.exc import TimeoutError as PoolTimeout

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
async def correlation_context(
    request: Request, call_next: Callable[[Request], Awaitable[Response]]
) -> Response:
    correlation_id = request.headers.get("X-Correlation-ID", str(uuid4()))[:128]
    if not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", correlation_id):
        correlation_id = str(uuid4())
    request.state.correlation_id = correlation_id
    started_at = time.perf_counter()
    response = await call_next(request)
    response.headers["X-Correlation-ID"] = correlation_id
    logger.info(
        "correlation_id=%s method=%s path=%s status=%s duration_ms=%s",
        correlation_id,
        request.method,
        getattr(request.scope.get("route"), "path", "unmatched"),
        response.status_code,
        round((time.perf_counter() - started_at) * 1000, 2),
    )
    return response


@app.get("/", include_in_schema=False)
def root() -> dict[str, str]:
    return {"service": settings.app_name, "docs": "/docs", "health": "/health"}


@app.exception_handler(OperationalError)
@app.exception_handler(PoolTimeout)
async def database_unavailable(request: Request, exc: Exception) -> JSONResponse:
    logger.warning("database_unavailable kind=%s", type(exc).__name__)
    return JSONResponse(
        status_code=503,
        content={"detail": "База данных временно недоступна"},
        headers={"Retry-After": "1"},
    )
