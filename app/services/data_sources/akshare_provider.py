import akshare as ak
import time
import os
import pandas as pd
import datetime

from pandas import DataFrame


class AkShareProvider:
    """AkShare数据源提供者
    
    负责从AkShare获取股票、基金、指数行情数据。
    """
    
    @staticmethod
    def get_daily_k_data(stock_code: str, start_date: str, end_date: str) -> DataFrame | None:
        """获取股票日K线数据
        
        从AkShare获取指定股票的日K线数据。
        
        Args:
            stock_code: 股票代码
            start_date: 开始日期，格式为"YYYYMMDD"
            end_date: 结束日期，格式为"YYYYMMDD"
            
        Returns:
            DataFrame: K线数据
        """
        try:
            for i in range(3):
                try:
                    print(f"尝试从AkShare获取{stock_code}日K线数据 (尝试 {i+1}/3)...")
                    k_data = ak.stock_zh_a_hist(symbol=stock_code, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
                    print(f"从AkShare获取{stock_code}日K线数据成功，共 {len(k_data)} 条记录")
                    time.sleep(3)
                    return k_data
                except Exception as e:
                    print(f"从AkShare获取{stock_code}日K线数据失败 (尝试 {i+1}/3): {e}")
                    time.sleep(5 * (i + 1))
            print(f"从AkShare获取{stock_code}日K线数据失败，已达到最大重试次数")
            return None
        except Exception as e:
            print(f"从AkShare获取{stock_code}日K线数据失败: {e}")
            return None
    
    @staticmethod
    def get_index_daily_k_data(index_code: str, start_date: str | None = None, end_date: str | None = None) -> DataFrame | None:
        """获取指数日K线数据
        
        从AkShare(新浪)获取指定指数的日K线数据。
        
        Args:
            index_code: 指数代码，如"000905"(中证500)，会自动添加市场前缀
            start_date: 开始日期，格式为"YYYYMMDD"（可选）
            end_date: 结束日期，格式为"YYYYMMDD"（可选）
            
        Returns:
            DataFrame: K线数据
        """
        try:
            # 添加市场前缀
            if not index_code.startswith(('sh', 'sz')):
                if index_code.startswith(('000', '880', '9')):
                    index_code = f'sh{index_code}'
                else:
                    index_code = f'sz{index_code}'
            
            for i in range(3):
                try:
                    print(f"尝试从AkShare获取{index_code}指数日K线数据 (尝试 {i+1}/3)...")
                    k_data = ak.stock_zh_index_daily(symbol=index_code)
                    print(f"从AkShare获取{index_code}指数日K线数据成功，共 {len(k_data)} 条记录")
                    
                    # 如果指定了日期范围，进行过滤
                    if start_date or end_date:
                        k_data['date'] = pd.to_datetime(k_data['date'])
                        if start_date:
                            k_data = k_data[k_data['date'] >= pd.to_datetime(start_date)]
                        if end_date:
                            k_data = k_data[k_data['date'] <= pd.to_datetime(end_date)]
                    
                    time.sleep(3)
                    return k_data
                except Exception as e:
                    print(f"从AkShare获取{index_code}指数日K线数据失败 (尝试 {i+1}/3): {e}")
                    time.sleep(5 * (i + 1))
            print(f"从AkShare获取{index_code}指数日K线数据失败，已达到最大重试次数")
            return None
        except Exception as e:
            print(f"从AkShare获取{index_code}指数日K线数据失败: {e}")
            return None
    
    @staticmethod
    def get_etf_daily_k_data(etf_code: str, start_date: str | None = None, end_date: str | None = None) -> DataFrame | None:
        """获取ETF日K线数据
        
        从AkShare(新浪)获取指定ETF的日K线数据。
        
        Args:
            etf_code: ETF代码，如"510500"(中证500ETF)，会自动添加市场前缀
            start_date: 开始日期，格式为"YYYYMMDD"（可选）
            end_date: 结束日期，格式为"YYYYMMDD"（可选）
            
        Returns:
            DataFrame: K线数据
        """
        try:
            # 添加市场前缀
            if not etf_code.startswith(('sh', 'sz')):
                if etf_code.startswith(('50', '51', '58')):
                    etf_code = f'sh{etf_code}'
                else:
                    etf_code = f'sz{etf_code}'
            
            for i in range(3):
                try:
                    print(f"尝试从AkShare获取{etf_code} ETF日K线数据 (尝试 {i+1}/3)...")
                    k_data = ak.fund_etf_hist_sina(symbol=etf_code)
                    print(f"从AkShare获取{etf_code} ETF日K线数据成功，共 {len(k_data)} 条记录")
                    
                    # 如果指定了日期范围，进行过滤
                    if start_date or end_date:
                        k_data['date'] = pd.to_datetime(k_data['date'])
                        if start_date:
                            k_data = k_data[k_data['date'] >= pd.to_datetime(start_date)]
                        if end_date:
                            k_data = k_data[k_data['date'] <= pd.to_datetime(end_date)]
                    
                    time.sleep(3)
                    return k_data
                except Exception as e:
                    print(f"从AkShare获取{etf_code} ETF日K线数据失败 (尝试 {i+1}/3): {e}")
                    time.sleep(5 * (i + 1))
            print(f"从AkShare获取{etf_code} ETF日K线数据失败，已达到最大重试次数")
            return None
        except Exception as e:
            print(f"从AkShare获取{etf_code} ETF日K线数据失败: {e}")
            return None
