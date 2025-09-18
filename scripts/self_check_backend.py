#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
后端自检脚本（v1.2 联调版）

覆盖以下检查：
- 应用健康：GET /health
- 文档可达：GET /docs
- v1.2 路由：GET /api/v12/batch/{batch}/regional（可选学校级）
- 数据库连通：调用部署包内 connection.check_database_health()

使用：
  python scripts/self_check_backend.py \
    --base http://127.0.0.1:8000 \
    --batch G4-2025 \
    [--school 5044] [--timeout 15] [--json] \
    [--env-file deployment_package_production/config/.env.production] \
    [--db-url mysql+pymysql://user:pwd@host:port/db?charset=utf8mb4]

返回码：
  0 = 所有关键检查通过（允许 v1.2 返回非 200 但路由存在视为“存在”）
  1 = 存在失败项
"""

from __future__ import annotations

import argparse
import json as _json
import os
import sys
import time
from typing import Any, Dict, Optional


def _load_env_file(path: str) -> Dict[str, str]:
    """最小 .env 解析：KEY=VALUE（忽略注释与空行），返回设置的项。"""
    loaded: Dict[str, str] = {}
    if not os.path.exists(path):
        return loaded
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if not s or s.startswith("#"):
                    continue
                # 仅处理简单 KEY=VALUE
                if "=" in s:
                    k, v = s.split("=", 1)
                    k = k.strip()
                    v = v.strip().strip('"').strip("'")
                    if k and v and k not in os.environ:
                        os.environ[k] = v
                        loaded[k] = v
    except Exception:
        pass
    return loaded


def _http_get(url: str, timeout: int = 10) -> Dict[str, Any]:
    """轻量 GET，优先 requests，不在则回退 urllib。返回统一结果字典。"""
    try:
        import requests  # type: ignore

        try:
            resp = requests.get(url, timeout=timeout)
            data = None
            try:
                data = resp.json()
            except Exception:
                data = resp.text
            return {
                "ok": True,
                "status": resp.status_code,
                "data": data,
                "headers": dict(resp.headers),
            }
        except Exception as e:  # 请求失败
            return {"ok": False, "error": str(e)}

    except Exception:
        # 回退 urllib
        try:
            from urllib import request, error

            req = request.Request(url, method="GET")
            with request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                text = raw.decode("utf-8", errors="replace")
                status = getattr(resp, "status", 200)
                # 简单探测 JSON
                data: Any
                try:
                    data = _json.loads(text)
                except Exception:
                    data = text
                return {
                    "ok": True,
                    "status": status,
                    "data": data,
                    "headers": dict(resp.headers),
                }
        except Exception as e:
            return {"ok": False, "error": str(e)}


def check_health(base: str, timeout: int) -> Dict[str, Any]:
    url = base.rstrip("/") + "/health"
    res = _http_get(url, timeout)
    passed = bool(res.get("ok") and res.get("status") == 200)
    return {
        "item": "health",
        "url": url,
        "passed": passed,
        "detail": res,
    }


def check_docs(base: str, timeout: int) -> Dict[str, Any]:
    url = base.rstrip("/") + "/docs"
    res = _http_get(url, timeout)
    # 有些环境 docs 可能 behind auth，200 视为通过
    passed = bool(res.get("ok") and res.get("status") in (200,))
    return {
        "item": "docs",
        "url": url,
        "passed": passed,
        "detail": res,
    }


def check_v12_regional(base: str, batch: str, timeout: int) -> Dict[str, Any]:
    url = base.rstrip("/") + f"/api/v12/batch/{batch}/regional"
    res = _http_get(url, timeout)
    # 只要路由存在（非 404/405/Not Found），即判定“存在”；
    # 返回 200 则说明可用；500 多为数据/DB 问题，但路由存在。
    exists = bool(res.get("ok") and res.get("status") not in (404, 405))
    usable = bool(res.get("ok") and res.get("status") == 200)
    return {
        "item": "v12_regional",
        "url": url,
        "passed": exists,  # 关键：验证路由存在性
        "usable": usable,
        "detail": res,
    }


def check_v12_school(base: str, batch: str, school: str, timeout: int) -> Dict[str, Any]:
    url = base.rstrip("/") + f"/api/v12/batch/{batch}/school/{school}"
    res = _http_get(url, timeout)
    exists = bool(res.get("ok") and res.get("status") not in (404, 405))
    usable = bool(res.get("ok") and res.get("status") == 200)
    return {
        "item": "v12_school",
        "url": url,
        "passed": exists,
        "usable": usable,
        "detail": res,
    }


def check_database_connectivity(env_file: Optional[str] = None, db_url_override: Optional[str] = None) -> Dict[str, Any]:
    """从部署包路径精确加载 connection.py，避免与根目录 app 包冲突。

    增强项：
    - 若提供 env_file，优先加载其中的环境变量（不覆盖已有 os.environ）
    - 若提供 db_url_override，则设置 DATABASE_URL 覆盖后再导入模块
    - 若模块检查失败，且可获取到 DATABASE_URL，则额外尝试 direct 直连校验
    """
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
    conn_path = os.path.join(
        root, "deployment_package_production", "app", "database", "connection.py"
    )
    result: Dict[str, Any] = {"item": "database", "passed": False, "detail": {}}

    # 预加载 .env 文件变量
    loaded_env: Dict[str, str] = {}
    if env_file:
        loaded_env = _load_env_file(env_file)
        if loaded_env:
            result["detail"]["env_file_loaded"] = {k: ("***" if k.lower().endswith("password") else v) for k, v in loaded_env.items()}

    # 覆盖 DATABASE_URL（若提供）
    if db_url_override:
        os.environ["DATABASE_URL"] = db_url_override
        result["detail"]["db_url_override"] = True
    if not os.path.exists(conn_path):
        result["detail"] = {"error": f"connection.py not found: {conn_path}"}
        return result

    module_ok = False
    try:
        import importlib.util

        spec = importlib.util.spec_from_file_location(
            "prod_app.database.connection", conn_path
        )
        assert spec and spec.loader
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)  # type: ignore

        # 优先使用 check_database_health（包含池/缓存信息）
        if hasattr(module, "check_database_health"):
            info = module.check_database_health()  # type: ignore
            passed = bool(info and info.get("status") == "healthy" and info.get("connection_test"))
            result.update({"passed": passed, "detail": info})
            module_ok = passed
        elif hasattr(module, "test_connection"):
            ok = bool(module.test_connection())  # type: ignore
            result.update({"passed": ok, "detail": {"connection_test": ok}})
            module_ok = ok
        else:
            result["detail"] = {"error": "No check_database_health/test_connection in module"}
    except Exception as e:
        result["detail"] = {"error": str(e)}
        module_ok = False

    # 若模块检查失败且可获取 DATABASE_URL，则尝试 direct 校验
    try:
        db_url = os.environ.get("DATABASE_URL")
        if not result.get("passed") and db_url:
            direct_info: Dict[str, Any] = {"attempt": "direct", "url": db_url}
            try:
                from sqlalchemy import create_engine, text  # type: ignore
                engine = create_engine(db_url)
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                direct_info["ok"] = True
            except Exception as e:
                direct_info["ok"] = False
                direct_info["error"] = str(e)
            # 合并到 detail
            detail = result.get("detail") or {}
            detail["direct_check"] = direct_info
            result["detail"] = detail
            # 只要 direct 成功也视为通过（避免宿主机“假阴性”）
            if direct_info.get("ok"):
                result["passed"] = True
    except Exception:
        pass

    return result


def summarize(results: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    failed = [k for k, v in results.items() if not v.get("passed")]
    summary = {
        "passed": len(failed) == 0,
        "failed_items": failed,
    }
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="ConvergedComputing 后端自检（v1.2）")
    parser.add_argument("--base", default="http://127.0.0.1:8000", help="服务基址，如 http://<host>:8000")
    parser.add_argument("--batch", default="G4-2025", help="批次代码，用于 v1.2 路由验证")
    parser.add_argument("--school", default=None, help="可选：学校ID，验证学校级 v1.2")
    parser.add_argument("--timeout", type=int, default=12, help="HTTP 请求超时秒数")
    parser.add_argument("--json", action="store_true", help="以 JSON 输出结果")
    parser.add_argument("--env-file", default=None, help="可选：从该 .env 文件加载数据库与环境配置")
    parser.add_argument("--db-url", default=None, help="可选：覆盖 DATABASE_URL 进行数据库直连校验")
    args = parser.parse_args()

    print("== 后端自检开始 ==")
    print(f"Base: {args.base}  Batch: {args.batch}  Timeout: {args.timeout}s")

    results: Dict[str, Dict[str, Any]] = {}

    # 1) /health
    results["health"] = check_health(args.base, args.timeout)
    print(f"[health] {'PASS' if results['health']['passed'] else 'FAIL'} -> {results['health']['url']}")

    # 2) /docs
    results["docs"] = check_docs(args.base, args.timeout)
    print(f"[docs]   {'PASS' if results['docs']['passed'] else 'FAIL'} -> {results['docs']['url']}")

    # 3) v1.2 regional
    results["v12_regional"] = check_v12_regional(args.base, args.batch, args.timeout)
    state = "PASS" if results["v12_regional"]["passed"] else "FAIL"
    usable = "OK" if results["v12_regional"].get("usable") else "NOT-OK"
    print(f"[v12/regional] {state} (usable={usable}) -> {results['v12_regional']['url']}")

    # 4) v1.2 school (optional)
    if args.school:
        results["v12_school"] = check_v12_school(args.base, args.batch, args.school, args.timeout)
        state = "PASS" if results["v12_school"]["passed"] else "FAIL"
        usable = "OK" if results["v12_school"].get("usable") else "NOT-OK"
        print(f"[v12/school]   {state} (usable={usable}) -> {results['v12_school']['url']}")

    # 5) Database connectivity
    results["database"] = check_database_connectivity(env_file=args.env_file, db_url_override=args.db_url)
    print(f"[database] {'PASS' if results['database']['passed'] else 'FAIL'}")

    summary = summarize(results)

    if args.json:
        print(_json.dumps({"summary": summary, "results": results}, ensure_ascii=False, indent=2))
    else:
        print("== 检查总结 ==")
        print(f"Overall: {'PASS' if summary['passed'] else 'FAIL'}")
        if not summary["passed"]:
            print("Failed items:", ", ".join(summary["failed_items"]) or "-")

        # 关键提示：
        if not results["v12_regional"]["passed"]:
            print("提示：/api/v12 路由不存在（404/405），请检查后端是否挂载 subjects_v12_api 及 Nginx /api/ 代理")
        elif not results["v12_regional"].get("usable"):
            print("提示：v1.2 路由存在但不可用（非 200），多为数据库或数据准备问题，请检查数据库连通与批次数据")

    return 0 if summary["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
