from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.management_api import router as management_router
from app.api.reporting_api import router as reporting_router
from app.api.calculation_api import router as calculation_router

app = FastAPI(
    title="学业发展质量监测统计分析服务",
    description="统计分析服务API文档",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# 配置CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 生产环境请设置具体的域名
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 注册路由
app.include_router(management_router, prefix="/api/v1/management", tags=["管理API"])
app.include_router(reporting_router, prefix="/api/v1/reporting", tags=["报告API"])
app.include_router(calculation_router, prefix="/api/v1/statistics", tags=["统计计算API"])

@app.get("/")
async def root():
    return {
        "message": "学业发展质量监测统计分析服务",
        "version": "1.0.0",
        "status": "running"
    }

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=False)