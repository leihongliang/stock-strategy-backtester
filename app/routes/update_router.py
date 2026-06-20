from fastapi import APIRouter, HTTPException

from app.models.stock import SyncStockDataRequest
from app.services.stock_service import StockService
from app.services.stock_company_service import StockCompanyService
from app.services.trade_calendar_service import TradeCalendarService

router = APIRouter(prefix="/api/stocks", tags=["stocks-update"])
stock_service = StockService()
stock_company_service = StockCompanyService()
trade_calendar_service = TradeCalendarService()

@router.post("/companies")
def refresh_stock_companies():
    """获取所有A股公司信息并存入数据库"""
    try:
        success = stock_company_service.save_all_stock_companies()
        if success:
            return {"message": "A股公司信息获取并保存成功"}
        else:
            return {"message": "A股公司信息获取或保存失败"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"服务器内部错误: {str(e)}")

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
        start_date_obj = None
        end_date_obj = None
        
        if start_date:
            from datetime import datetime
            start_date_obj = datetime.strptime(start_date, "%Y%m%d").date()
        
        if end_date:
            from datetime import datetime
            end_date_obj = datetime.strptime(end_date, "%Y%m%d").date()
        
        trade_calendar_service.sync_trade_calendar(start_date=start_date_obj, end_date=end_date_obj)
        return {"message": "交易日历同步完成"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"服务器内部错误: {str(e)}")

@router.post("/daily-update")
def daily_update():
    """每日更新股票数据"""
    try:
        result = stock_service.daily_update()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"服务器内部错误: {str(e)}")