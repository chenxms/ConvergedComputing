"""
API Key认证中间件
"""
import os
from typing import Optional
from fastapi import HTTPException, Request, status
from fastapi.security import APIKeyHeader
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse


# 从环境变量获取API Key
API_KEY = os.getenv("API_KEY", "JDCIWWDAODAJJFAAFAJFJjdsmdjf23232")
API_KEY_HEADER_NAME = "X-API-Key"


class APIKeyAuthMiddleware(BaseHTTPMiddleware):
    """API Key认证中间件"""

    def __init__(self, app, enabled: bool = True, exclude_paths: list = None):
        super().__init__(app)
        self.enabled = enabled
        self.exclude_paths = exclude_paths or [
            "/",
            "/health",
            "/docs",
            "/redoc",
            "/openapi.json"
        ]

    async def dispatch(self, request: Request, call_next):
        # 如果未启用认证，直接通过
        if not self.enabled:
            return await call_next(request)

        # 检查是否在排除路径中
        path = request.url.path
        if any(path.startswith(excluded) for excluded in self.exclude_paths):
            return await call_next(request)

        # 获取API Key
        api_key = request.headers.get(API_KEY_HEADER_NAME)

        # 也支持从查询参数获取（备选方案）
        if not api_key:
            api_key = request.query_params.get("api_key")

        # 验证API Key
        if not api_key:
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={
                    "detail": "API Key required",
                    "message": "Missing API Key in header X-API-Key"
                },
                headers={"WWW-Authenticate": f"ApiKey realm=\"{API_KEY_HEADER_NAME}\""}
            )

        if api_key != API_KEY:
            return JSONResponse(
                status_code=status.HTTP_403_FORBIDDEN,
                content={
                    "detail": "Invalid API Key",
                    "message": "The provided API Key is invalid"
                }
            )

        # API Key验证通过，继续处理请求
        response = await call_next(request)
        return response


def setup_api_key_auth(app, enabled: bool = None):
    """配置API Key认证"""

    # 从环境变量决定是否启用
    if enabled is None:
        enabled = os.getenv("ENABLE_API_KEY_AUTH", "false").lower() == "true"

    if enabled:
        print(f"API Key authentication enabled")
        print(f"API Key header name: {API_KEY_HEADER_NAME}")

    app.add_middleware(
        APIKeyAuthMiddleware,
        enabled=enabled,
        exclude_paths=[
            "/",
            "/health",
            "/docs",
            "/redoc",
            "/openapi.json"
        ]
    )