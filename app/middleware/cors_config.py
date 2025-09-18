"""
增强的CORS配置中间件
"""
from typing import List, Optional
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response


class EnhancedCORSMiddleware(BaseHTTPMiddleware):
    """增强的CORS中间件，确保OPTIONS请求得到正确处理"""

    async def dispatch(self, request: Request, call_next):
        # 处理预检请求
        if request.method == "OPTIONS":
            response = Response()
            response.headers["Access-Control-Allow-Origin"] = "*"
            response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS, PATCH"
            response.headers["Access-Control-Allow-Headers"] = "*"
            response.headers["Access-Control-Allow-Credentials"] = "true"
            response.headers["Access-Control-Max-Age"] = "3600"
            return response

        # 处理常规请求
        response = await call_next(request)

        # 添加CORS头
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS, PATCH"
        response.headers["Access-Control-Allow-Headers"] = "*"
        response.headers["Access-Control-Allow-Credentials"] = "true"

        return response


def setup_cors(
    app: FastAPI,
    allow_origins: Optional[List[str]] = None,
    allow_credentials: bool = True,
    allow_methods: Optional[List[str]] = None,
    allow_headers: Optional[List[str]] = None,
):
    """配置CORS中间件"""

    # 默认配置
    if allow_origins is None:
        # 生产环境应该配置具体的域名
        allow_origins = [
            "http://localhost:8080",
            "http://localhost:3000",
            "http://localhost:5173",
            "http://117.72.14.166:8080",
            "*"  # 开发环境允许所有，生产环境应该删除
        ]

    if allow_methods is None:
        allow_methods = ["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"]

    if allow_headers is None:
        allow_headers = ["*"]

    # 使用FastAPI内置的CORS中间件
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allow_origins,
        allow_credentials=allow_credentials,
        allow_methods=allow_methods,
        allow_headers=allow_headers,
        expose_headers=["*"],
        max_age=3600,  # 预检请求缓存时间
    )

    # 添加增强的CORS中间件作为备用
    app.add_middleware(EnhancedCORSMiddleware)