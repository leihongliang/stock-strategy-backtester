import akshare as ak
import time
import os
import pandas as pd
import datetime

from pandas import DataFrame

from app.models.company import StockCompany
from app.models.stock import StockDailyPrice


class AkShareProvider:
    """AkShare数据源提供者
    
    负责从AkShare获取股票数据。
    """
    
    @staticmethod
    def get_daily_k_data(stock_code, start_date, end_date) -> DataFrame | None:
        """获取股票日K线数据
        
        从AkShare获取指定股票的日K线数据。
        
        Args:
            stock_code: 股票代码
            start_date: 开始日期，格式为"YYYYMMDD"
            end_date: 结束日期，格式为"YYYYMMDD"
            
        Returns:
            tuple: (stock_code, k_data)，其中k_data为获取的K线数据
        """
        try:
            # 禁用所有代理，避免VPN或系统代理导致的连接问题
            # os.environ['no_proxy'] = '*'
            # os.environ['HTTP_PROXY'] = ''
            # os.environ['HTTPS_PROXY'] = ''
            # os.environ['http_proxy'] = ''
            # os.environ['https_proxy'] = ''
            
            # 添加重试机制，增加等待时间
            for i in range(3):
                try:
                    print(f"尝试从AkShare获取{stock_code}日K线数据 (尝试 {i+1}/3)...")
                    k_data = ak.stock_zh_a_hist(symbol=stock_code, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
                    print(f"从AkShare获取{stock_code}日K线数据成功，共 {len(k_data)} 条记录")
                    # 成功后增加等待时间，降低请求频率
                    time.sleep(3)
                    return k_data
                except Exception as e:
                    print(f"从AkShare获取{stock_code}日K线数据失败 (尝试 {i+1}/3): {e}")
                    # 增加等待时间，避免频繁请求
                    time.sleep(5 * (i + 1))
            print(f"从AkShare获取{stock_code}日K线数据失败，已达到最大重试次数")
            return None
        except Exception as e:
            print(f"从AkShare获取{stock_code}日K线数据失败: {e}")
            return None

    @staticmethod
    def get_all_a_stocks() -> list[StockCompany]:
        """Get all A-share stocks information"""
        company_list = []
        
        # Process Shanghai stocks
        try:
            stock_info_sh = ak.stock_info_sh_name_code()
            for _, row in stock_info_sh.iterrows():
                # Get stock code
                sec_code = str(row.get('证券代码', ''))
                if not sec_code:
                    continue
                # 确保股票代码为6位字符串
                sec_code = sec_code.zfill(6)
                
                # Get stock name
                sec_name = row.get('证券简称', '')
                
                # Get market
                market = 'SH'

                # Get industry
                industry = row.get('所属行业', '')
                
                # Get listing date
                listing_date_value = row.get('上市日期')
                listing_date = pd.to_datetime(listing_date_value).date()
                
                # Create StockCompany object
                company = StockCompany(
                    sec_code=sec_code,
                    sec_name=sec_name,
                    market=market,
                    industry=industry,
                    listing_date=listing_date
                )
                company_list.append(company)
        except Exception as e:
            print(f"处理上海股票数据失败: {e}")
        
        # Process Shenzhen stocks
        try:
            stock_info_sz = ak.stock_info_sz_name_code()
            for _, row in stock_info_sz.iterrows():
                # Get stock code
                sec_code = str(row.get('A股代码', ''))
                if not sec_code:
                    continue
                # 确保股票代码为6位字符串
                sec_code = sec_code.zfill(6)
                
                # Get stock name
                sec_name = row.get('A股简称', '')
                market = 'SZ'
                
                # Get industry
                industry = row.get('所属行业', '')
                
                # Get listing date
                listing_date_value = row.get('A股上市日期', '')
                listing_date = pd.to_datetime(listing_date_value).date()
                
                # Create StockCompany object
                company = StockCompany(
                    sec_code=sec_code,
                    sec_name=sec_name,
                    market=market,
                    industry=industry,
                    listing_date=listing_date
                )
                company_list.append(company)
        except Exception as e:
            print(f"处理深圳股票数据失败: {e}")
        
        return company_list
