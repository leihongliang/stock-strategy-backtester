from fastapi import FastAPI
from app.config.settings import settings
from app.routes import stock_router

app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description="A股股票涨跌模式查询API"
)

# 注册路由
app.include_router(stock_router.router)

@app.get("/")
def read_root():
    """根路径"""
    return {
        "name": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "description": "A股股票涨跌模式查询API",
        "endpoints": {
            "find_stocks": "/api/stocks/pattern",
            "refresh_data": "/api/stocks/refresh",
            "update_stock": "/api/stocks/update",
            "refresh_companies": "/api/stocks/companies",
            "get_hsgt_stocks": "/api/stocks/hsgt"
        }
    }
