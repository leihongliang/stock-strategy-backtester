from pydantic import BaseModel
from datetime import date, datetime

class StockCompany(BaseModel):
    """A股公司信息模型
    
    用于表示A股公司的基本信息，包括股票代码、名称、市场等。
    数据存储在数据库的stock_company集合中。
    """
    sec_code: str     # 股票代码（字符串格式，如"600000"）
    sec_name: str     # 股票名称
    market: str       # 市场（SH/SZ）
    industry: str     # 行业
    listing_date: date  # 上市日期
    
    def to_mongo_doc(self):
        """转换为MongoDB文档格式
        
        Returns:
            dict: 可直接传入insert_one / replace_one 的MongoDB文档
        """
        listing_dt = datetime(
            self.listing_date.year,
            self.listing_date.month,
            self.listing_date.day
        )
        return {
            'sec_code': self.sec_code,
            'sec_name': self.sec_name,
            'market': self.market,
            'industry': self.industry,
            'listing_date': listing_dt
        }
