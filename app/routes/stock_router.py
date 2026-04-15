from fastapi import APIRouter, HTTPException
from app.models.stock import StockPatternRequest, StockPatternResponse
from app.services.stock_service import StockService
from pydantic import BaseModel

class UpdateStockRequest(BaseModel):
    """更新单只股票数据的请求模型"""
    stock_code: str      # 股票代码
    start_date: str      # 开始日期
    end_date: str        # 结束日期
    data_source: str = "akshare"  # 数据源，可选值为"akshare"或"tushare"

router = APIRouter(prefix="/api/stocks", tags=["stocks"])
stock_service = StockService()

@router.post("/companies")
def refresh_stock_companies():
    """获取所有A股公司信息并存入数据库"""
    try:
        success = stock_service.save_stock_companies()
        if success:
            return {"message": "A股公司信息获取并保存成功"}
        else:
            return {"message": "A股公司信息获取或保存失败"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"服务器内部错误: {str(e)}")

class SyncStockDataRequest(BaseModel):
    """同步股票数据的请求模型"""
    start_date: str          # 开始日期，格式为"YYYYMMDD"
    end_date: str            # 结束日期，格式为"YYYYMMDD"
    stock_codes: list[str] = None  # 股票代码列表，为None时同步所有股票
    data_source: str = "akshare"  # 数据源，可选值为"akshare"或"tushare"

@router.post("/sync-range")
def sync_stock_data_in_range(request: SyncStockDataRequest):
    """同步固定时间范围内的股票数据到数据库
    
    如果股票代码列表不传，则默认同步所有股票（从数据库获取股票列表）。
    """
    try:
        result = stock_service.sync_stock_data_in_range(
            request.start_date,
            request.end_date,
            request.stock_codes,
            request.data_source
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"服务器内部错误: {str(e)}")

@router.post("/sync-calendar")
def sync_trade_calendar(start_date: str = None, end_date: str = None):
    """同步交易日历数据到数据库
    
    从AkShare获取A股交易日历数据，并保存到数据库。
    
    Args:
        start_date: 开始日期，格式为"YYYYMMDD"，akshare有数据限制 默认从2023-01-01开始
        end_date: 结束日期，格式为"YYYYMMDD"，默认到当天
    """
    try:
        # 转换日期格式
        start_date_obj = None
        end_date_obj = None
        
        if start_date:
            from datetime import datetime
            start_date_obj = datetime.strptime(start_date, "%Y%m%d").date()
        
        if end_date:
            from datetime import datetime
            end_date_obj = datetime.strptime(end_date, "%Y%m%d").date()
        
        result = stock_service.sync_trade_calendar(start_date=start_date_obj, end_date=end_date_obj)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"服务器内部错误: {str(e)}")

class StrategyValidationRequest(BaseModel):
    """策略验证请求模型"""
    strategy_name: str      # 策略名称，目前支持 "strategy1"
    start_date: str = None  # 开始日期，格式为"YYYY-MM-DD"
    end_date: str = None    # 结束日期，格式为"YYYY-MM-DD"

@router.post("/strategy/validate")
def validate_strategy(request: StrategyValidationRequest):
    """根据策略从历史数据中找到符合的股票及其时间段区间，并验证之后几天的股票涨幅，计算策略的正确率
    
    目前支持的策略：
    - strategy1: 至少连续上涨≥4天（允许夹一根小阴线），出现放量大阳线，后续3天不跌破异动阳线的开盘价
    """
    try:
        # 验证策略
        result = stock_service.validate_strategy(
            request.strategy_name,
            request.start_date,
            request.end_date
        )
        
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"服务器内部错误: {str(e)}")

@router.post("/daily-update")
def daily_update():
    """每日更新股票数据
    
    执行以下操作：
    1. 更新交易日历到最新的一天
    2. 更新新增的A股公司，去掉没有的
    3. 更新日K线到最新的一天
    """
    try:
        result = stock_service.daily_update()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"服务器内部错误: {str(e)}")
