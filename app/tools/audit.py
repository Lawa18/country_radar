# tools/audit.py
from __future__ import annotations
import os, sys, inspect, json, importlib, pathlib, textwrap, time, traceback
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
    out: Dict[str, Any] = {"env": {}, "files": {}, "routes": {}, "indicator": {}, "providers": {}, "debt": {}, "caches": {}}

    out["env"]["python"] = sys.executable
    out["env"]["cwd"]    = os.getcwd()
    out["env"]["sys.path[0]"] = sys.path[0]
    out["env"]["COUNTRY_RADAR_BASE_URL"] = os.getenv("COUNTRY_RADAR_BASE_URL")

    # --- File stats
    for rel in [
        "app/main.py",
        "app/routes/country.py",
        "app/routes/probe.py",
        "app/routes/debt.py",
        "app/services/indicator_service.py",
        "app/services/debt_service.py",
        "app/providers/imf_provider.py",
        "app/providers/wb_provider.py",
        "app/providers/eurostat_provider.py",
        "app/providers/ecb_provider.py",
        "app/utils/country_codes.py",
    ]:
        out["files"][rel] = _stat(pathlib.Path(rel))

    # --- Import app & routes
    app_mod = _safe(importlib.import_module, "app.main")
    out["routes"]["import_app_main"] = app_mod
    app = None
    if app_mod.get("ok"):
        app = app_mod["value"].app
        # enumerate routes
        routes = []
        for r in app.routes:
            routes.append({
                "path": getattr(r, "path", ""),
                "methods": sorted(getattr(r, "methods", []) or []),
                "name": getattr(r, "name", None),
                "endpoint_file": inspect.getsourcefile(r.endpoint) if hasattr(r, "endpoint") else None,
            })
        out["routes"]["mounted"] = routes

    # --- Inspect /country-data handler
    try:
        from app.routes import country as country_mod
        out["routes"]["country_module"] = {"file": getattr(country_mod, "__file__", None)}
        # find the handler object on the router
        handler_info = {}
        for r in getattr(country_mod, "router", []).routes:
            if getattr(r, "path", None) == "/country-data":
                fn = r.endpoint
                handler_info["endpoint_file"] = inspect.getsourcefile(fn)
                lines, start = inspect.getsourcelines(fn)
                handler_info["starts_at"] = start
                handler_info["head"] = "".join(lines[:24])
                break
        out["routes"]["country_data_handler"] = handler_info or {"error":"not found on router"}
    except Exception as e:
        out["routes"]["country_data_handler"] = {"error": f"{e.__class__.__name__}: {e}"}

    # --- Inspect indicator_service
    try:
        from app.services import indicator_service as svc
        ind: Dict[str, Any] = {"file": getattr(svc, "__file__", None), "builders": {}}

        names = [
            "build_country_payload",
            "build_country_payload_modern",
            "build_country_payload_v2",
            "assemble_country_payload",
            "country_data",
            "build_country_data",
            "assemble_country_data",
            "get_country_data",
            "make_country_data",
        ]
        for n in names:
            fn = getattr(svc, n, None)
            if callable(fn):
                sig = str(inspect.signature(fn))
                srcfile = inspect.getsourcefile(fn)
                head = "".join(inspect.getsourcelines(fn)[0][:12])
                ind["builders"][n] = {"sig": sig, "file": srcfile, "head": head}
            else:
                ind["builders"][n] = "missing"
        out["indicator"] = ind
    except Exception as e:
        out["indicator"] = {"error": f"{e.__class__.__name__}: {e}", "trace": traceback.format_exc()}

    # --- Providers smoke (Mexico / Germany small probes)
    probes = [
        ("imf",  "app.providers.imf_provider",  [
            "imf_cpi_yoy_monthly", "imf_unemployment_rate_monthly",
            "imf_fx_usd_monthly", "imf_reserves_usd_monthly",
            "imf_policy_rate_monthly", "imf_gdp_growth_quarterly",
        ], ["MX","DE"]),
        ("wb",   "app.providers.wb_provider",   [
            "wb_cpi_yoy_annual","wb_unemployment_rate_annual","wb_fx_rate_usd_annual",
            "wb_reserves_usd_annual","wb_gdp_growth_annual_pct",
            "wb_current_account_balance_pct_gdp_annual","wb_government_effectiveness_annual",
        ], ["MEX","DEU"]),
        ("ecb",  "app.providers.ecb_provider",  ["ecb_policy_rate_for_country"], ["DE","IT","FR"]),
        ("eu",   "app.providers.eurostat_provider", ["eurostat_hicp_yoy_monthly","eurostat_unemployment_rate_monthly"], ["DE","IT","FR"]),
    ]
    prov_out: Dict[str, Any]= {}
    for key, modname, funcs, codes in probes:
        entry: Dict[str, Any] = {"module": modname, "file": None, "functions": {}}
        modr = _safe(importlib.import_module, modname)
        if not modr["ok"]:
            entry["error"] = modr["error"]
        else:
            m = modr["value"]
            entry["file"] = getattr(m, "__file__", None)
            for f in funcs:
                fn = getattr(m, f, None)
                if not callable(fn):
                    entry["functions"][f] = "missing"
                    continue
                sample = {}
                for c in codes:
                    r = _safe(fn, c)
                    if r["ok"]:
                        v = r["value"]
                        sample[c] = {"type": type(v).__name__, "len": (len(v) if hasattr(v,"__len__") else None)}
                    else:
                        sample[c] = {"error": r["error"]}
                entry["functions"][f] = {"ok": True, "sample": sample}
        prov_out[key] = entry
    out["providers"] = prov_out

    # --- Debt service quick check
    try:
        from app.services import debt_service as debt
        fn = getattr(debt, "compute_debt_payload", None)
        info = {}
        if callable(fn):
            info["sig"] = str(inspect.signature(fn))
            info["file"] = inspect.getsourcefile(fn)
        else:
            info["error"] = "compute_debt_payload missing"
        out["debt"] = info
    except Exception as e:
        out["debt"] = {"error": f"{e.__class__.__name__}: {e}"}

    # --- Cache class check
    try:
        from app.services import indicator_service as svc
        cls = getattr(svc, "_TTLCache", None)
        res = {"present": bool(cls)}
        if cls:
            res["has_get"] = hasattr(cls, "get")
            res["has_set"] = hasattr(cls, "set")
        out["caches"]["_TTLCache"] = res
    except Exception as e:
        out["caches"]["_TTLCache"] = {"error": str(e)}

    return out

if __name__ == "__main__":
    print(json.dumps(audit(), indent=2))
