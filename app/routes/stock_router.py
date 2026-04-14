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

@router.post("/pattern", response_model=list[StockPatternResponse])
def find_stocks_by_pattern(request: StockPatternRequest):
    """根据涨跌模式查找匹配的股票"""
    try:
        # 检查数据是否存在
        if not stock_service.check_data_exists():
            # 数据不存在，获取数据
            stock_service.save_stock_data()
        
        # 查找匹配的股票
        matching_stocks = stock_service.find_stocks_by_pattern(
            request.pattern,
            request.start_date,
            request.end_date
        )
        
        return matching_stocks
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"服务器内部错误: {str(e)}")

@router.post("/refresh")
def refresh_stock_data():
    """刷新所有股票数据"""
    try:
        stock_service.save_stock_data()
        return {"message": "股票数据刷新成功"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"服务器内部错误: {str(e)}")

@router.post("/update")
def update_single_stock_data(request: UpdateStockRequest):
    """更新单只股票的日K线数据"""
    try:
        success = stock_service.update_single_stock_data(
            request.stock_code, 
            request.start_date, 
            request.end_date,
            request.data_source
        )
        if success:
            return {"message": f"股票 {request.stock_code} 数据更新成功，数据源: {request.data_source}"}
        else:
            return {"message": f"股票 {request.stock_code} 数据更新失败，数据源: {request.data_source}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"服务器内部错误: {str(e)}")

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

@router.get("/hsgt")
def get_hsgt_stocks(trade_date: str = None):
    """获取沪港通/深港通股票列表"""
    try:
        # 如果未传trade_date，使用当天日期
        if trade_date is None:
            from datetime import datetime
            trade_date = datetime.now().strftime("%Y%m%d")
        stocks = stock_service.get_hsgt_stocks(trade_date)
        return stocks
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
def sync_trade_calendar():
    """同步交易日历数据到数据库
    
    从AkShare获取A股交易日历数据，并保存到数据库。
    """
    try:
        result = stock_service.sync_trade_calendar()
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
        # 检查数据是否存在
        if not stock_service.check_data_exists():
            # 数据不存在，获取数据
            stock_service.save_stock_data()
        
        # 验证策略
        result = stock_service.validate_strategy(
            request.strategy_name,
            request.start_date,
            request.end_date
        )
        
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"服务器内部错误: {str(e)}")
