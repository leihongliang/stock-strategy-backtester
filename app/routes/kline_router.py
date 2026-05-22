from fastapi import APIRouter, HTTPException, Query
from app.services.stock_service import StockService
from app.config.settings import settings
from datetime import datetime

router = APIRouter(prefix="/api/kline", tags=["kline"])
stock_service = StockService()


@router.get("/stocks")
def search_stocks(q: str = Query("", description="搜索关键词（股票代码或名称）")):
    """搜索股票列表

    按股票代码或名称模糊匹配，返回最多 20 条结果。
    """
    try:
        companies = stock_service.repo.company_collection.find(
            {
                "$or": [
                    {"sec_code": {"$regex": q, "$options": "i"}},
                    {"sec_name": {"$regex": q, "$options": "i"}},
                ]
            },
            {"_id": 0, "sec_code": 1, "sec_name": 1, "market": 1},
            limit=20,
        )
        results = []
        for doc in companies:
            results.append(
                {
                    "sec_code": str(doc.get("sec_code", "")).zfill(6),
                    "sec_name": doc.get("sec_name", ""),
                    "market": doc.get("market", ""),
                }
            )
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"搜索失败: {str(e)}")


@router.get("/data")
def get_kline_data(
    sec_code: str = Query(..., description="股票代码，如 600519"),
    start_date: str = Query(None, description="开始日期 YYYY-MM-DD"),
    end_date: str = Query(None, description="结束日期 YYYY-MM-DD"),
):
    """获取 K 线数据

    返回指定股票在指定时间段内的 OHLCV 数据，价格单位为元。
    不传日期范围则返回全部数据。
    """
    try:
        sec_code = sec_code.zfill(6)
        raw = stock_service.repo.get_stock_prices(sec_code)

        if not raw:
            raise HTTPException(status_code=404, detail=f"未找到股票 {sec_code} 的数据")

        # 解析日期范围
        start_dt = datetime.fromisoformat(start_date) if start_date else None
        end_dt = datetime.fromisoformat(end_date) if end_date else None

        candles = []
        for trade_date, o, h, l, c, vol, amt in raw:
            # trade_date 可能是 datetime 或 date
            if hasattr(trade_date, "date"):
                d = trade_date.date()
            else:
                d = trade_date

            if start_dt and d < start_dt.date():
                continue
            if end_dt and d > end_dt.date():
                continue

            candles.append(
                {
                    "date": d.isoformat(),
                    "open": round(o / 100, 2),
                    "high": round(h / 100, 2),
                    "low": round(l / 100, 2),
                    "close": round(c / 100, 2),
                    "volume": vol,
                }
            )

        return {"sec_code": sec_code, "candles": candles}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取数据失败: {str(e)}")
