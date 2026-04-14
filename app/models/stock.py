from pydantic import BaseModel
from datetime import date, datetime


class StockDailyPrice(BaseModel):
    """股票每日价格数据模型
    
    用于表示股票的每日K线数据，包括开盘价、收盘价、最高价、最低价等信息。
    数据存储在数据库的stock_daily_price表/集合中。
    """
    trade_date: date  # 交易日期
    sec_code: int     # 股票代码（数字格式，如600000）
    open: int         # 开盘价（单位：分）
    high: int         # 最高价（单位：分）
    low: int          # 最低价（单位：分）
    close: int        # 收盘价（单位：分）
    pre_close: int    # 昨收价（单位：分）
    change: int       # 涨跌额（单位：分）
    pct_chg: int      # 涨跌幅（单位：0.01%）
    volume: int       # 成交量（单位：股）
    amount: int       # 成交额（单位：分）
    adjfactor: int    # 复权因子（单位：万分）
    st_status: int    # ST状态（0：非ST，1：ST）
    trade_status: int # 交易状态（1：正常交易，0：停牌）
    
    def to_tuple(self):
        """转换为元组格式
        
        返回一个元组，包含所有字段的值，用于ClickHouse插入。
        
        Returns:
            tuple: 包含所有字段值的元组
        """
        return (
            self.trade_date,
            self.sec_code,
            self.open,
            self.high,
            self.low,
            self.close,
            self.pre_close,
            self.change,
            self.pct_chg,
            self.volume,
            self.amount,
            self.adjfactor,
            self.st_status,
            self.trade_status
        )
    
    def to_dict(self):
        """转换为字典格式
        
        返回一个字典，包含所有字段的值，用于MongoDB插入。
        
        Returns:
            dict: 包含所有字段值的字典
        """
        return {
            'trade_date': self.trade_date.isoformat(),  # 转换为ISO格式字符串
            'sec_code': self.sec_code,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'pre_close': self.pre_close,
            'change': self.change,
            'pct_chg': self.pct_chg,
            'volume': self.volume,
            'amount': self.amount,
            'adjfactor': self.adjfactor,
            'st_status': self.st_status,
            'trade_status': self.trade_status
        }
    
    def to_mongo_doc(self):
        """转换为MongoDB文档格式
        
        返回一个适合直接写入MongoDB的文档，trade_date使用datetime类型
        以便MongoDB原生时间查询，并以(sec_code, trade_date)组合作为_id
        保证幂等插入（upsert）时的唯一性。
        
        Returns:
            dict: 可直接传入insert_one / replace_one 的MongoDB文档
        """
        trade_dt = datetime(
            self.trade_date.year,
            self.trade_date.month,
            self.trade_date.day
        )
        return {
            'trade_date': trade_dt,
            'sec_code': self.sec_code,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'pre_close': self.pre_close,
            'change': self.change,
            'pct_chg': self.pct_chg,
            'volume': self.volume,
            'amount': self.amount,
            'adjfactor': self.adjfactor,
            'st_status': self.st_status,
            'trade_status': self.trade_status
        }

class StockPatternRequest(BaseModel):
    """股票涨跌模式查询请求模型
    
    用于接收前端传入的涨跌模式查询参数。
    """
    pattern: str      # 涨跌模式字符串，如"010111"
    start_date: str = None  # 开始日期（可选）
    end_date: str = None    # 结束日期（可选）

class StockPatternResponse(BaseModel):
    """股票涨跌模式查询响应模型
    
    用于返回匹配的股票信息。
    """
    code: str               # 股票代码
    name: str               # 股票名称
    market: str             # 市场（上海/深圳）
    pattern: str            # 涨跌模式
    period: str             # 匹配时间段
    start_price: float      # 起始价格
    end_price: float        # 结束价格
    price_change: float     # 价格变动
    price_change_percent: float  # 价格变动百分比
