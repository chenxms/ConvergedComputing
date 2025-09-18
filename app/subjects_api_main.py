from fastapi import FastAPI

from app.api.subjects_v12_api import router as subjects_v12_router
from app.middleware.cors_config import setup_cors

app = FastAPI(
    title="Subjects v1.2 API",
    description="统一 subjects（v1.2）接口，仅包含 v1.2 路由",
    version="1.2.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# 使用增强的CORS配置
setup_cors(app)

app.include_router(subjects_v12_router, prefix="/api/v12", tags=["Subjects v1.2"])


@app.get("/")
async def root():
    return {"service": "subjects-v1.2", "status": "running"}


@app.get("/health")
async def health():
    return {"status": "healthy"}

