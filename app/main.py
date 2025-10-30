# app/main.py
from __future__ import annotations

import importlib
import logging
from fastapi import FastAPI

logger = logging.getLogger("country-radar")
logging.basicConfig(level=logging.INFO)

app = FastAPI(
    title="Country Radar API",
    description="Macroeconomic data API",
    version="2025.10.19",
)


def _safe_include(prefix: str, module_path: str) -> None:
    """
    Import a router module and include its `router` if present.
    Logs instead of crashing so one bad router doesn't kill the app.
    """
    try:
        mod = importlib.import_module(module_path)
    except Exception as e:
        logger.error("failed to import %s: %s", module_path, e)
        return

    router = getattr(mod, "router", None)
    if router is None:
        logger.error("module %s has no `router`", module_path)
        return

    app.include_router(router)
    logger.info("[init] %s router mounted from: %s", prefix, module_path)


# ------------------------------------------------------------
# 1) include all the routers we already saw in openapi.json
#    (these must exist in your codebase right now)
# ------------------------------------------------------------
# You may already have something like this in your old main.py;
# if so, keep that AND add the probe include below.
_safe_include("country", "app.routes.country")          # for /country-data
_safe_include("debt_bundle", "app.routes.debt_bundle")  # for /v1/debt-bundle
_safe_include("debt", "app.routes.debt")                # for /v1/debt

# ------------------------------------------------------------
# 2) include the probe router (THIS is what adds /v1/country-lite)
# ------------------------------------------------------------
_safe_include("probe", "app.routes.probe")

# ------------------------------------------------------------
# 3) root
# ------------------------------------------------------------
@app.get("/")
def root():
    return {
        "ok": True,
        "routers": [
            "country",
            "debt_bundle",
            "debt",
            "probe",
        ],
        "hint": "see /docs or /openapi.json",
    }
