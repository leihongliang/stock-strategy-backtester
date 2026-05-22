from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from pathlib import Path
from app.config.settings import settings
from app.routes import update_router, strategy_router, kline_router

app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description="A股股票涨跌模式查询API"
)

# 注册路由
app.include_router(update_router.router)
app.include_router(strategy_router.router)
app.include_router(kline_router.router)

# 挂载静态文件（前端页面）
static_dir = Path(__file__).parent.parent / "static"
if static_dir.exists():
    app.mount("/", StaticFiles(directory=str(static_dir), html=True), name="static")

