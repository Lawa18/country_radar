# app/tools/audit.py
from __future__ import annotations

import os, sys, json, inspect, importlib, pathlib, time, traceback
from typing import Any, Dict

def _safe(fn, *a, **kw):
    try:
        return {"ok": True, "value": fn(*a, **kw)}
    except Exception as e:
        return {"ok": False, "error": f"{e.__class__.__name__}: {e}", "trace": traceback.format_exc()}

def _stat(p: pathlib.Path) -> Dict[str, Any]:
    if not p.exists():
        return {"exists": False}
    return {
        "exists": True,
        "size": p.stat().st_size,
        "mtime": time.ctime(p.stat().st_mtime),
    }

def audit() -> Dict[str, Any]:
    out: Dict[str, Any] = {"env": {}, "files": {}, "routes": {}, "indicator": {}, "probe": {}}

    # --- env
    out["env"] = {
        "exe": sys.executable,
        "cwd": os.getcwd(),
        "sys.path[0]": sys.path[0] if sys.path else None,
    }

    # --- file stats
    files = [
        "app/main.py",
        "app/routes/country.py",
        "app/routes/probe.py",
        "app/routes/country_lite.py",
        "app/routes/action_probe.py",
        "app/services/indicator_service.py",
    ]
    out["files"] = {rel: _stat(pathlib.Path(rel)) for rel in files}

    # --- import app and list mounted routes
    try:
        app_mod = importlib.import_module("app.main")
        app = app_mod.app
        out["routes"]["mounted"] = [
            {"path": getattr(r, "path", ""), "methods": sorted(getattr(r, "methods", []) or [])}
            for r in app.routes
        ]
    except Exception as e:
        out["routes"]["error"] = f"{e.__class__.__name__}: {e}"

    # --- Inspect /country-data handler (where does it live; what does it call?)
    try:
        from app.routes import country as country_mod
        r = next(rt for rt in country_mod.router.routes if getattr(rt, "path", "") == "/country-data")
        fn = r.endpoint
        src_lines, src_file = inspect.getsourcelines(fn), inspect.getsourcefile(fn)
        head = "".join(src_lines[0][:30])
        out["routes"]["/country-data"] = {
            "module_file": country_mod.__file__,
            "handler_file": src_file,
            "handler_head": head,
        }
    except Exception as e:
        out["routes"]["/country-data"] = {"error": f"{e.__class__.__name__}: {e}"}

    # --- Indicator builders available
    try:
        from app.services import indicator_service as svc
        cand = [
            "build_country_payload",           # legacy in your tree (country only)
            "build_country_payload_v2",        # (optional) modern
            "assemble_country_payload",        # (optional)
            "build_country_data", "assemble_country_data",
            "get_country_data", "make_country_data",
        ]
        out["indicator"]["file"] = getattr(svc, "__file__", None)
        out["indicator"]["builders"] = {}
        for name in cand:
            f = getattr(svc, name, None)
            if callable(f):
                try:
                    out["indicator"]["builders"][name] = {
                        "callable": True,
                        "file": inspect.getsourcefile(f),
                        "sig": str(inspect.signature(f)),
                    }
                except Exception:
                    out["indicator"]["builders"][name] = {"callable": True}
            else:
                out["indicator"]["builders"][name] = {"callable": False}
    except Exception as e:
        out["indicator"]["error"] = f"{e.__class__.__name__}: {e}"

    # --- Probe router present?
    try:
        probe_mod = importlib.import_module("app.routes.probe")
        routes = getattr(probe_mod, "router", None)
        out["probe"]["module_file"] = getattr(probe_mod, "__file__", None)
        if routes is not None:
            out["probe"]["routes"] = [
                {"path": getattr(r, "path", ""), "methods": sorted(getattr(r, "methods", []) or [])}
                for r in routes.routes
            ]
    except Exception as e:
        out["probe"]["error"] = f"{e.__class__.__name__}: {e}"

    return out

if __name__ == "__main__":
    print(json.dumps(audit(), indent=2))
