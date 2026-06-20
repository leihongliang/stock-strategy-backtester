from pydantic import BaseModel
from datetime import date, datetime
from enum import Enum


class Market(Enum):
    """市场枚举"""
    SH = "SH"  # 上海
    SZ = "SZ"  # 深圳


class SecType(Enum):
    """证券类型枚举"""
    STOCK = "stock"  # 股票
    FUND = "fund"    # 基金
    INDEX = "index"  # 指数


class StockCompany(BaseModel):
    """A股公司/基金信息模型
    
    用于表示A股公司或基金的基本信息，包括代码、名称、市场等。
    数据存储在数据库的stock_company集合中。
    """
    sec_code: str
    sec_name: str
    market: Market
    industry: str
    listing_date: date | None = None
    sec_type: SecType = SecType.STOCK
    
    def to_mongo_doc(self):
        """转换为MongoDB文档格式"""
        listing_dt = None
        if self.listing_date:
            listing_dt = datetime(
                self.listing_date.year,
                self.listing_date.month,
                self.listing_date.day
            )
        return {
            'sec_code': self.sec_code,
            'sec_name': self.sec_name,
            'market': self.market.value,
            'industry': self.industry,
            'listing_date': listing_dt,
            'sec_type': self.sec_type.value
        }
