# app/main.py
from __future__ import annotations

import importlib
import logging
from fastapi import FastAPI

from starlette.middleware.gzip import GZipMiddleware
app.add_middleware(GZipMiddleware, minimum_size=500)

logger = logging.getLogger("country-radar")
logging.basicConfig(level=logging.INFO)

app = FastAPI(
    title="Country Radar API",
    description="Macroeconomic data API",
    version="2025.10.19",
)


def _safe_include(prefix: str, module_path: str) -> bool:
    """Import a router module and include its `router` if present."""
    try:
        mod = importlib.import_module(module_path)
    except Exception as e:
        logger.error("failed to import %s: %s", module_path, e)
        return False

    router = getattr(mod, "router", None)
    if router is None:
        logger.error("module %s has no `router`", module_path)
        return False

    app.include_router(router)
    logger.info("[init] %s router mounted from: %s", prefix, module_path)
    return True


# ---------------------------------------------------------------------
# STARTUP: mount ONLY the light/cheap router(s)
# ---------------------------------------------------------------------
# ✅ probe is now light enough, so we can mount it at startup
_safe_include("probe", "app.routes.probe")

# ❌ DO NOT mount country / debt / country-lite here.
# That’s what is making Render time out.


@app.get("/")
def root():
    return {
        "ok": True,
        "routers_now": ["probe"],
        "routers_later": [
            "country",
            "debt_bundle",
            "debt",
            "country-lite",
        ],
        "hint": "call /__load_heavy after deploy to mount the rest",
    }


@app.get("/healthz")
def healthz():
    # keep this super fast
    return {"status": "ok"}


@app.get("/__load_heavy")
def load_heavy():
    """
    Call this endpoint manually AFTER the service is up.
    This mounts the slow/big routers.
    """
    mounted = {
        "country": _safe_include("country", "app.routes.country"),
        "debt_bundle": _safe_include("debt_bundle", "app.routes.debt_bundle"),
        "debt": _safe_include("debt", "app.routes.debt"),
        "country-lite": _safe_include("country-lite", "app.routes.country_lite"),
    }
    return {"ok": True, "mounted": mounted}

import threading, time
def _delayed_load():
    time.sleep(5)
    try:
        _safe_include("country-lite", "app.routes.country_lite")
    except Exception as e:
        logger.error("Delayed include failed: %s", e)

threading.Thread(target=_delayed_load, daemon=True).start()

