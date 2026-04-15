from pydantic import BaseModel
from datetime import date


class TradeCalendar(BaseModel):
    """交易日历数据模型
    
    用于表示A股市场的交易日历信息。
    数据存储在数据库的trade_calendar表/集合中。
    """
    trade_date: date  # 交易日期
    is_trading_day: bool  # 是否为交易日（True：交易日，False：非交易日）
    
    def to_mongo_doc(self):
        """转换为MongoDB文档格式
        
        Returns:
            dict: MongoDB文档格式的字典，trade_date使用datetime类型
        """
        from datetime import datetime
        trade_dt = datetime(
            self.trade_date.year,
            self.trade_date.month,
            self.trade_date.day
        )
        return {
            'trade_date': trade_dt,
            'is_trading_day': self.is_trading_day
        }
