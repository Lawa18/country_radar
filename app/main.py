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


def _safe_include(prefix: str, module_path: str) -> bool:
    """
    Import a router module and include its `router` if present.
    Return True/False so we can see what actually mounted.
    """
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
# STARTUP: mount ONLY the lightest router(s)
# ---------------------------------------------------------------------
# probe is cheap now (sync, lazy imports), so we can mount it
_safe_include("probe", "app.routes.probe")

# ---------------------------------------------------------------------
# ROOT + HEALTH
# ---------------------------------------------------------------------
@app.get("/")
def root():
    return {
        "ok": True,
        "routers_maybe_mounted": [
            "probe",     # guaranteed
            # the rest are mounted on-demand via /__load_heavy
        ],
        "hint": "call /__load_heavy once after deploy to mount country/debt routes",
    }


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


# ---------------------------------------------------------------------
# ON-DEMAND MOUNTER
# ---------------------------------------------------------------------
@app.get("/__load_heavy")
def load_heavy():
    """
    Call this ONCE (from browser or from your GPT action) after the service starts.
    This will mount the heavy routers that sometimes make Render grumpy
    when we mount them at startup.
    """
    mounted = {}
    mounted["country"] = _safe_include("country", "app.routes.country")
    mounted["debt_bundle"] = _safe_include("debt_bundle", "app.routes.debt_bundle")
    mounted["debt"] = _safe_include("debt", "app.routes.debt")
    # if you keep a separate country-lite router, mount it here too:
    mounted["country-lite"] = _safe_include("country-lite", "app.routes.country_lite")

    return {
        "ok": True,
        "mounted": mounted,
        "note": "Heavy routers loaded. If one is False, check logs on Render.",
    }
