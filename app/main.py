from fastapi import FastAPI

from app.api.management_api import router as management_router
from app.api.reporting_api import router as reporting_router
from app.api.calculation_api import router as calculation_router
from app.api.subjects_v12_api import router as subjects_v12_router
from app.api.questionnaire_distribution_api import router as questionnaire_distribution_router
from app.middleware.cors_config import setup_cors
from app.middleware.api_key_auth import setup_api_key_auth

app = FastAPI(
    title="学业发展质量监测统计分析服务",
    description="统计分析服务API文档",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# 配置增强的CORS（必须在API Key认证之前）
setup_cors(app)

# 配置API Key认证（可选，通过环境变量控制）
# 设置 ENABLE_API_KEY_AUTH=true 启用
# 设置 API_KEY=your_key 配置密钥
setup_api_key_auth(app)

# 注册路由
app.include_router(management_router, prefix="/api/v1/management", tags=["管理API"])
app.include_router(reporting_router, prefix="/api/v1/reporting", tags=["报告API"])
app.include_router(calculation_router, prefix="/api/v1/statistics", tags=["统计计算API"])
app.include_router(subjects_v12_router, prefix="/api/v12", tags=["Subjects v1.2"])
app.include_router(questionnaire_distribution_router, tags=["问卷题目分布API"])

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
