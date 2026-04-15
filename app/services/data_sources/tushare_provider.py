import time
import os
import pandas as pd
from app.models.stock import StockDailyPrice
from app.utils.log import logger

# 尝试导入tushare
try:
    import tushare as ts
    tushare_available = True
    # 设置token
    ts.set_token('00ae9e22ce521c0853e2f15c329b4ccded0f9bae3f15bf3f7f6e6b9c')
    # 初始化pro接口
    pro = ts.pro_api()
    logger.info("Tushare初始化成功")
except ImportError:
    tushare_available = False
    pro = None
    logger.info("Tushare未安装，将无法使用Tushare数据源")
except Exception as e:
    tushare_available = False
    pro = None
    logger.error(f"Tushare初始化失败: {e}")

class TushareProvider:
    """Tushare数据源提供者
    
    负责从Tushare获取股票数据。
    """
    
    @staticmethod
    def is_available():
        """检查Tushare是否可用
        
        Returns:
            bool: Tushare是否可用
        """
        return tushare_available
    
    @staticmethod
    def get_daily_k_data(stock_code, start_date, end_date) -> list[StockDailyPrice]:
        """获取股票日K线数据
        
        从Tushare获取指定股票的日K线数据，并转换为StockDailyPrice对象列表。
        
        Args:
            stock_code: 股票代码
            start_date: 开始日期，格式为"YYYYMMDD"
            end_date: 结束日期，格式为"YYYYMMDD"
            
        Returns:
            tuple: (stock_code, stock_price_list)，其中stock_price_list为StockDailyPrice对象列表
        """
        global pro
        
        if not tushare_available or pro is None:
            logger.error("Tushare未安装或初始化失败，无法从Tushare获取数据")
            return []
        
        try:
            # 禁用所有代理，避免VPN或系统代理导致的连接问题
            os.environ['no_proxy'] = '*'
            os.environ['HTTP_PROXY'] = ''
            os.environ['HTTPS_PROXY'] = ''
            os.environ['http_proxy'] = ''
            os.environ['https_proxy'] = ''
            
            # 添加重试机制，增加等待时间
            for i in range(3):
                try:
                    logger.info(f"尝试从Tushare获取{stock_code}日K线数据 (尝试 {i+1}/3)...")

                    # 转换股票代码格式
                    if stock_code.startswith("SH"):
                        ts_code = stock_code.replace("SH", "") + ".SH"
                    elif stock_code.startswith("SZ"):
                        ts_code = stock_code.replace("SZ", "") + ".SZ"
                    else:
                        # 尝试自动判断股票代码所属市场
                        # 6开头的股票是上海市场，0或3开头的股票是深圳市场
                        if stock_code.startswith("6"):
                            ts_code = stock_code + ".SH"
                        elif stock_code.startswith("0") or stock_code.startswith("3"):
                            ts_code = stock_code + ".SZ"
                        else:
                            ts_code = stock_code

                    # 获取数据
                    k_data = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)

                    # 转换为StockDailyPrice对象列表
                    stock_price_list = []
                    if not k_data.empty:
                        # 按日期排序
                        k_data = k_data.sort_values('trade_date')
                        # 重置索引
                        k_data = k_data.reset_index(drop=True)

                        # 移除股票代码前缀，保持为字符串
                        sec_code = stock_code.replace('SH', '').replace('SZ', '')
                        # 确保股票代码为6位字符串
                        sec_code = sec_code.zfill(6)

                        for _, row in k_data.iterrows():
                            # 将价格乘以100转换为整数
                            open_price = int(float(row['open']) * 100)
                            high_price = int(float(row['high']) * 100)
                            low_price = int(float(row['low']) * 100)
                            close_price = int(float(row['close']) * 100)

                            # 昨收价、涨跌额和涨跌幅
                            pre_close = int(float(row['pre_close']) * 100)
                            change = int(float(row['change']) * 100)
                            pct_chg = int(float(row['pct_chg']) * 100)  # 转换为0.01%

                            # 成交量和成交额
                            volume = int(row['vol'])
                            amount = int(row['amount'])

                            # 其他字段使用默认值
                            adjfactor = 10000  # 默认调整因子
                            st_status = 0  # 非ST
                            trade_status = 1  # 正常交易

                            # 创建StockDailyPrice对象
                            stock_price = StockDailyPrice(
                                trade_date=pd.to_datetime(row['trade_date']).date(),
                                sec_code=sec_code,
                                open=open_price,
                                high=high_price,
                                low=low_price,
                                close=close_price,
                                pre_close=pre_close,
                                change=change,
                                pct_chg=pct_chg,
                                volume=volume,
                                amount=amount,
                                adjfactor=adjfactor,
                                st_status=st_status,
                                trade_status=trade_status
                            )

                            stock_price_list.append(stock_price)

                    logger.info(f"从Tushare获取{stock_code}日K线数据成功，共 {len(k_data)} 条记录")
                    # 成功后增加等待时间，降低请求频率
                    time.sleep(3)
                    return stock_price_list
                except Exception as e:
                    logger.error(f"从Tushare获取{stock_code}日K线数据失败 (尝试 {i+1}/3): {e}")
                    # 增加等待时间，避免频繁请求
                    time.sleep(5 * (i + 1))
            logger.error(f"从Tushare获取{stock_code}日K线数据失败，已达到最大重试次数")
            return stock_code, None
        except Exception as e:
            logger.error(f"从Tushare获取{stock_code}日K线数据失败: {e}")
            return None
    
    @staticmethod
    def get_daily_k_data_batch(stock_codes, start_date, end_date) -> list[StockDailyPrice]:
        """批量获取股票日K线数据
        
        从Tushare批量获取多只股票的日K线数据，并转换为StockDailyPrice对象列表。
        
        Args:
            stock_codes: 股票代码列表
            start_date: 开始日期，格式为"YYYYMMDD"
            end_date: 结束日期，格式为"YYYYMMDD"
            
        Returns:
            list[StockDailyPrice]: 所有股票的日K线数据列表
        """
        global pro
        
        if not tushare_available or pro is None:
            logger.error("Tushare未安装或初始化失败，无法从Tushare获取数据")
            return []
        
        try:
            # 禁用所有代理，避免VPN或系统代理导致的连接问题
            os.environ['no_proxy'] = '*'
            os.environ['HTTP_PROXY'] = ''
            os.environ['HTTPS_PROXY'] = ''
            os.environ['http_proxy'] = ''
            os.environ['https_proxy'] = ''
            

            try:
                logger.info(f"尝试从Tushare批量获取{len(stock_codes)}只股票的{start_date}-{end_date}日K线数据")

                # 转换股票代码格式并合并为逗号分隔的字符串
                ts_codes = []
                stock_code_map = {}
                for stock_code in stock_codes:
                    if stock_code.startswith("SH"):
                        ts_code = stock_code.replace("SH", "") + ".SH"
                    elif stock_code.startswith("SZ"):
                        ts_code = stock_code.replace("SZ", "") + ".SZ"
                    else:
                        # 尝试自动判断股票代码所属市场
                        # 6开头的股票是上海市场，0或3开头的股票是深圳市场
                        if stock_code.startswith("6"):
                            ts_code = stock_code + ".SH"
                        elif stock_code.startswith("0") or stock_code.startswith("3"):
                            ts_code = stock_code + ".SZ"
                        else:
                            ts_code = stock_code
                    ts_codes.append(ts_code)
                    stock_code_map[ts_code] = stock_code

                # 合并为逗号分隔的字符串
                ts_code_str = ",".join(ts_codes)

                # 获取数据
                k_data = pro.daily(ts_code=ts_code_str, start_date=start_date, end_date=end_date)

                # 转换为StockDailyPrice对象列表
                stock_price_list = []
                if not k_data.empty:
                    # 按日期排序
                    k_data = k_data.sort_values(['ts_code', 'trade_date'])
                    # 重置索引
                    k_data = k_data.reset_index(drop=True)

                    for _, row in k_data.iterrows():
                        # 获取原始股票代码
                        ts_code = row['ts_code']
                        stock_code = stock_code_map.get(ts_code, ts_code)

                        # 移除股票代码前缀，保持为字符串
                        sec_code = stock_code.replace('SH', '').replace('SZ', '').replace('.SH', '').replace('.SZ', '')
                        # 确保股票代码为6位字符串
                        sec_code = sec_code.zfill(6)

                        # 将价格乘以100转换为整数
                        open_price = int(float(row['open']) * 100)
                        high_price = int(float(row['high']) * 100)
                        low_price = int(float(row['low']) * 100)
                        close_price = int(float(row['close']) * 100)

                        # 昨收价、涨跌额和涨跌幅
                        pre_close = int(float(row['pre_close']) * 100)
                        change = int(float(row['change']) * 100)
                        pct_chg = int(float(row['pct_chg']) * 100)  # 转换为0.01%

                        # 成交量和成交额
                        volume = int(row['vol'])
                        amount = int(row['amount'])

                        # 其他字段使用默认值
                        adjfactor = 10000  # 默认调整因子
                        st_status = 0  # 非ST
                        trade_status = 1  # 正常交易

                        # 创建StockDailyPrice对象
                        stock_price = StockDailyPrice(
                            trade_date=pd.to_datetime(row['trade_date']).date(),
                            sec_code=sec_code,
                            open=open_price,
                            high=high_price,
                            low=low_price,
                            close=close_price,
                            pre_close=pre_close,
                            change=change,
                            pct_chg=pct_chg,
                            volume=volume,
                            amount=amount,
                            adjfactor=adjfactor,
                            st_status=st_status,
                            trade_status=trade_status
                        )

                        stock_price_list.append(stock_price)

                logger.info(f"从Tushare批量获取{len(stock_codes)}只股票的{start_date}-{end_date}日K线数据成功，共 {len(k_data)} 条记录")
                # 成功后增加等待时间，降低请求频率
                time.sleep(1)
                return stock_price_list
            except Exception as e:
                logger.error(f"从Tushare批量获取股票日K线数据失败")
                return []
        except Exception as e:
            logger.error(f"从Tushare批量获取股票日K线数据失败: {e}")
            return []
    
    @staticmethod
    def get_hsgt_stocks(trade_date):
        """获取沪港通/深港通股票列表
        
        从Tushare获取指定日期的沪港通/深港通股票列表。
        
        Args:
            trade_date: 交易日期，格式为"YYYYMMDD"
            
        Returns:
            pd.DataFrame: 沪港通/深港通股票列表
        """
        global pro
        
        if not tushare_available or pro is None:
            logger.error("Tushare未安装或初始化失败，无法从Tushare获取数据")
            return None
        
        try:
            # 禁用所有代理，避免VPN或系统代理导致的连接问题
            os.environ['no_proxy'] = '*'
            os.environ['HTTP_PROXY'] = ''
            os.environ['HTTPS_PROXY'] = ''
            os.environ['http_proxy'] = ''
            os.environ['https_proxy'] = ''
            
            # 添加重试机制
            for i in range(3):
                try:
                    logger.info(f"尝试从Tushare获取{trade_date}沪港通/深港通股票列表 (尝试 {i+1}/3)...")
                    
                    # 获取数据
                    df = pro.stock_hsgt(trade_date=trade_date, type='HK_SZ')
                    
                    logger.info(f"从Tushare获取{trade_date}沪港通/深港通股票列表成功，共 {len(df)} 条记录")
                    # 成功后增加等待时间，降低请求频率
                    time.sleep(3)
                    return df
                except Exception as e:
                    logger.error(f"从Tushare获取沪港通/深港通股票列表失败 (尝试 {i+1}/3): {e}")
                    # 增加等待时间，避免频繁请求
                    time.sleep(5 * (i + 1))
            logger.error(f"从Tushare获取沪港通/深港通股票列表失败，已达到最大重试次数")
            return None
        except Exception as e:
            logger.error(f"从Tushare获取沪港通/深港通股票列表失败: {e}")
            return None
