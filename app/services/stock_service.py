import pandas as pd
import time
from datetime import datetime, date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import akshare as ak
from app.repositories.clickhouse import ClickHouseRepository
from app.repositories.mongodb import MongoDBRepository
from app.models.stock import StockDailyPrice
from app.models.company import StockCompany
from app.models.trade_calendar import TradeCalendar
from app.services.data_sources.akshare_provider import AkShareProvider
from app.services.data_sources.tushare_provider import TushareProvider
from app.config.settings import settings
from app.utils.log import logger

class StockService:
    """股票服务
    
    处理股票相关的业务逻辑，包括数据获取、涨跌模式生成、股票查询等。
    """
    
    def __init__(self):
        # 根据配置选择数据库类型
        if settings.DATABASE_TYPE == "mongodb":
            # 初始化MongoDB仓库
            self.repo = MongoDBRepository(
                host=settings.MONGODB_HOST,
                port=settings.MONGODB_PORT,
                database=settings.MONGODB_DATABASE
            )
            logger.info("使用MongoDB作为数据库")
        # else:
        #     # 初始化ClickHouse仓库
        #     self.repo = ClickHouseRepository()
        #     logger.info("使用ClickHouse作为数据库")
    
    def get_all_a_stocks(self, data_source="akshare") -> list[StockCompany]:
        """获取所有A股股票列表
        
        从AkShare获取上海和深圳交易所的A股股票列表。
        """
        try:
            if data_source == "akshare":
                return AkShareProvider.get_all_a_stocks()
        except Exception as e:
            logger.error(f"获取A股公司数据失败: {e}")
            return []
    
    def get_daily_k_data(self, stock_code, start_date, end_date, data_source="akshare") -> tuple:
        """获取股票日K线数据
        
        从指定数据源获取指定股票的日K线数据。
        
        Args:
            stock_code: 股票代码
            start_date: 开始日期，格式为"YYYYMMDD"
            end_date: 结束日期，格式为"YYYYMMDD"
            data_source: 数据源，可选值为"akshare"或"tushare"
            
        Returns:
            tuple: (stock_code, k_data)，其中k_data为获取的K线数据
        """
        try:
            if data_source == "tushare":
                # 从tushare获取数据
                k_data = TushareProvider.get_daily_k_data(stock_code, start_date, end_date)
                return stock_code, k_data
            else:
                # 从akshare获取数据
                k_data = AkShareProvider.get_daily_k_data(stock_code, start_date, end_date)
                return stock_code, k_data
        except Exception as e:
            logger.error(f"获取{stock_code}日K线数据失败: {e}")
            return stock_code, None
    
    def get_daily_k_data_batch(self, stock_codes, start_date, end_date, data_source="akshare") -> list[StockDailyPrice]:
        """批量获取股票日K线数据
        
        从指定数据源批量获取多只股票的日K线数据。
        对于Tushare数据源，会将股票代码用逗号分隔，一次性获取多只股票的数据。
        对于AkShare数据源，会循环获取每只股票的数据。
        
        Args:
            stock_codes: 股票代码列表
            start_date: 开始日期，格式为"YYYYMMDD"
            end_date: 结束日期，格式为"YYYYMMDD"
            data_source: 数据源，可选值为"akshare"或"tushare"
            
        Returns:
            list[StockDailyPrice]: 所有股票的日K线数据列表
        """
        try:
            if data_source == "tushare":
                # 从tushare批量获取数据
                return TushareProvider.get_daily_k_data_batch(stock_codes, start_date, end_date)
            else:
                # 从akshare循环获取数据
                all_data = []
                for stock_code in stock_codes:
                    data = AkShareProvider.get_daily_k_data(stock_code, start_date, end_date)
                    if data is not None and not data.empty:
                        # 计算昨收价、涨跌额和涨跌幅
                        data['pre_close'] = data['收盘'].shift(1)
                        data['pre_close'].fillna(data['开盘'].iloc[0], inplace=True)
                        data['change'] = data['收盘'] - data['pre_close']
                        data['pct_chg'] = (data['change'] / data['pre_close']) * 100
                        
                        # 移除股票代码前缀，转换为数字
                        sec_code = int(stock_code.replace('SH', '').replace('SZ', ''))
                        
                        for _, row in data.iterrows():
                            # 将价格乘以100转换为整数
                            open_price = int(float(row['开盘']) * 100)
                            high_price = int(float(row['最高']) * 100)
                            low_price = int(float(row['最低']) * 100)
                            close_price = int(float(row['收盘']) * 100)
                            
                            # 计算昨收价、涨跌额和涨跌幅
                            pre_close = int(float(row['pre_close']) * 100)
                            change = int(float(row['change']) * 100)
                            pct_chg = int(float(row['pct_chg']) * 100)  # 转换为0.01%
                            
                            # 成交量和成交额
                            volume = int(row['成交量'])
                            amount = int(row['成交额'])
                            
                            # 其他字段使用默认值
                            adjfactor = 10000  # 默认调整因子
                            st_status = 0  # 非ST
                            trade_status = 1  # 正常交易
                            
                            # 创建StockDailyPrice对象
                            stock_price = StockDailyPrice(
                                trade_date=pd.to_datetime(row['日期']).date(),
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
                            
                            all_data.append(stock_price)
                return all_data
        except Exception as e:
            logger.error(f"批量获取股票日K线数据失败: {e}")
            return []
    
    def save_stock_data(self):
        """获取所有A股近一年日K线数据并保存到ClickHouse

        从AkShare获取所有A股的近一年日K线数据，并保存到ClickHouse的stock_daily_price表中。
        """
        # 初始化数据库
        if not self.repo.init_tables():
            logger.error("数据库初始化失败，无法保存数据")
            return False
        
        all_stocks = self.get_all_a_stocks()
        logger.info(f"总共有 {len(all_stocks)} 只A股股票")
        
        # 计算日期范围（近一年）
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")
        
        # 使用线程池并发获取数据，降低线程池大小以减少请求频率
        max_workers = 5  # 线程池大小，从10减少到5
        processed_count = 0
        saved_count = 0
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            futures = []
            for index, stock in enumerate(all_stocks):
                # 处理StockCompany对象
                if hasattr(stock, 'sec_code'):
                    # 格式化为6位数字代码
                    stock_code = f"{stock.sec_code:06d}"
                else:
                    # 兼容旧格式
                    stock_code = stock['代码']
                futures.append(executor.submit(self.get_daily_k_data, stock_code, start_date, end_date))
                # 每提交一个任务，增加等待时间，降低请求频率
                time.sleep(0.5)
            
            # 处理完成的任务
            for future in as_completed(futures):
                stock_code, k_data = future.result()
                processed_count += 1
                
                if k_data is not None:
                    # 准备插入数据
                    insert_data = []
                    # 移除股票代码前缀，转换为数字
                    sec_code = int(stock_code.replace('SH', '').replace('SZ', ''))
                    
                    # 检查数据源类型，使用不同的字段名
                    is_tushare_data = 'ts_code' in k_data.columns
                    
                    if is_tushare_data:
                        # 使用tushare的原始字段名
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
                            
                            insert_data.append(stock_price)
                    else:
                        # 使用akshare的字段名
                        # 计算昨收价、涨跌额和涨跌幅
                        k_data['pre_close'] = k_data['收盘'].shift(1)
                        k_data['pre_close'].fillna(k_data['开盘'].iloc[0], inplace=True)
                        k_data['change'] = k_data['收盘'] - k_data['pre_close']
                        k_data['pct_chg'] = (k_data['change'] / k_data['pre_close']) * 100
                        
                        for _, row in k_data.iterrows():
                            # 将价格乘以100转换为整数
                            open_price = int(float(row['开盘']) * 100)
                            high_price = int(float(row['最高']) * 100)
                            low_price = int(float(row['最低']) * 100)
                            close_price = int(float(row['收盘']) * 100)
                            
                            # 计算昨收价、涨跌额和涨跌幅
                            pre_close = int(float(row['pre_close']) * 100)
                            change = int(float(row['change']) * 100)
                            pct_chg = int(float(row['pct_chg']) * 100)  # 转换为0.01%
                            
                            # 成交量和成交额
                            volume = int(row['成交量'])
                            amount = int(row['成交额'])
                            
                            # 其他字段使用默认值
                            adjfactor = 10000  # 默认调整因子
                            st_status = 0  # 非ST
                            trade_status = 1  # 正常交易
                            
                            # 创建StockDailyPrice对象
                            stock_price = StockDailyPrice(
                                trade_date=pd.to_datetime(row['日期']).date(),
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
                            
                            insert_data.append(stock_price)
                    
                    # 插入数据到ClickHouse
                    if insert_data:
                        self.repo.save_stock_prices(insert_data)
                        saved_count += 1
                        # 每保存10只股票打印一次
                        if saved_count % 10 == 0:
                            print(f"已保存 {saved_count} 只股票的日K线数据")
                            # 每保存10只股票后增加等待时间，降低请求频率
                            time.sleep(5)
            
            # 每处理100只股票打印一次进度
            if processed_count % 100 == 0:
                print(f"已处理 {processed_count} 只股票，已保存 {saved_count} 只股票的数据")
                # 每处理100只股票后增加等待时间，降低请求频率
                time.sleep(10)
        
        print(f"数据获取完成，共处理 {processed_count} 只股票，成功保存 {saved_count} 只股票的日K线数据")
    
    def load_stock_data(self, stock_code):
        """从ClickHouse加载股票数据
        
        从ClickHouse的stock_daily_price表中加载指定股票的数据。
        """
        # 移除股票代码前缀，转换为数字
        sec_code = int(stock_code.replace('SH', '').replace('SZ', ''))
        
        # 从数据库获取数据
        result = self.repo.get_stock_prices(sec_code)
        
        if not result:
            return None
        
        # 转换为DataFrame
        columns = ['日期', '开盘', '最高', '最低', '收盘', '成交量', '成交额']
        k_data = pd.DataFrame(result, columns=columns)
        
        # 将价格除以100转换回小数
        k_data['开盘'] = k_data['开盘'] / 100
        k_data['最高'] = k_data['最高'] / 100
        k_data['最低'] = k_data['最低'] / 100
        k_data['收盘'] = k_data['收盘'] / 100
        
        k_data['日期'] = pd.to_datetime(k_data['日期'])
        
        return k_data
    
    def generate_trend_pattern(self, k_data, start_index, weeks):
        """根据周K线数据生成涨跌模式字符串
        
        根据股票的周K线数据，生成指定长度的涨跌模式字符串。
        """
        if k_data is None or len(k_data) < start_index + weeks:
            return ""
        
        trend_pattern = ""
        for i in range(start_index, start_index + weeks):
            if k_data.iloc[i]['收盘'] > k_data.iloc[i]['开盘']:
                trend_pattern += "1"  # 涨
            else:
                trend_pattern += "0"  # 跌
        
        return trend_pattern
    
    def find_stocks_by_pattern(self, pattern, start_date=None, end_date=None):
        """根据涨跌模式查找匹配的股票
        
        根据用户输入的涨跌模式，在指定时间范围内查找匹配的股票。
        """
        all_stocks = self.get_all_a_stocks()
        logger.info(f"总共有 {len(all_stocks)} 只A股股票")
        
        matching_stocks = []
        
        # 遍历所有股票
        for index, stock in enumerate(all_stocks):
            # 处理StockCompany对象
            if hasattr(stock, 'sec_code'):
                # 格式化为6位数字代码
                stock_code = f"{stock.sec_code:06d}"
                stock_name = stock.sec_name
                market = stock.market
            else:
                # 兼容旧格式
                stock_code = stock['代码']
                stock_name = stock['名称']
                market = stock['市场']
            
            # 从ClickHouse加载数据
            k_data = self.load_stock_data(stock_code)
            
            if k_data is not None:
                # 筛选时间范围内的数据
                if start_date and end_date:
                    start_date_dt = pd.to_datetime(start_date)
                    end_date_dt = pd.to_datetime(end_date)
                    filtered_data = k_data[(k_data['日期'] >= start_date_dt) & (k_data['日期'] <= end_date_dt)]
                else:
                    filtered_data = k_data
                
                # 检查是否有足够的数据
                if len(filtered_data) >= len(pattern):
                    # 遍历所有可能的连续n周组合
                    for i in range(len(filtered_data) - len(pattern) + 1):
                        # 生成涨跌模式
                        trend_pattern = self.generate_trend_pattern(filtered_data, i, len(pattern))
                        
                        # 检查是否匹配
                        if trend_pattern == pattern:
                            # 获取匹配的时间段
                            match_start_date = filtered_data.iloc[i]['日期'].strftime('%Y-%m-%d')
                            match_end_date = filtered_data.iloc[i + len(pattern) - 1]['日期'].strftime('%Y-%m-%d')
                            
                            # 获取股价信息
                            start_price = filtered_data.iloc[i]['开盘']
                            end_price = filtered_data.iloc[i + len(pattern) - 1]['收盘']
                            price_change = end_price - start_price
                            price_change_percent = (price_change / start_price) * 100
                            
                            matching_stocks.append({
                                'code': stock_code,
                                'name': stock_name,
                                'market': market,
                                'pattern': trend_pattern,
                                'period': f"{match_start_date} 至 {match_end_date}",
                                'start_price': start_price,
                                'end_price': end_price,
                                'price_change': price_change,
                                'price_change_percent': price_change_percent
                            })
                            # 找到一个匹配后就停止当前股票的搜索
                            break
            
            # 每处理100只股票打印一次进度
            if (index + 1) % 100 == 0:
                logger.info(f"已处理 {index + 1} 只股票，找到 {len(matching_stocks)} 只匹配的股票")
        
        return matching_stocks
    
    def update_single_stock_data(self, stock_code, start_date, end_date, data_source="akshare"):
        """更新单只股票的日K线数据

        从指定数据源获取指定股票的日K线数据，并保存到ClickHouse的stock_daily_price表中。

        Args:
            stock_code: 股票代码
            start_date: 开始日期，格式为"YYYYMMDD"
            end_date: 结束日期，格式为"YYYYMMDD"
            data_source: 数据源，可选值为"akshare"或"tushare"

        Returns:
            bool: 是否更新成功
        """
        # 初始化数据库
        if not self.repo.init_tables():
            print("数据库初始化失败，无法保存数据")
            return False
        
        # 获取股票数据
        k_data = self.get_daily_k_data(stock_code, start_date, end_date, data_source)
        
        if k_data is not None:
            self.repo.save_stock_prices(k_data)
            print(f"成功从{data_source}更新股票 {stock_code} 的日K线数据，共 {len(k_data)} 条记录")
            return True
        else:
            print(f"从{data_source}获取股票 {stock_code} 的日K线数据失败")
            return False
    
    def check_data_exists(self):
        """检查数据是否存在
        
        检查ClickHouse的stock_daily_price表中是否有数据。
        """
        return self.repo.get_stock_count() > 0
    
    def get_hsgt_stocks(self, trade_date):
        """获取沪港通/深港通股票列表
        
        从Tushare获取指定日期的沪港通/深港通股票列表。
        
        Args:
            trade_date: 交易日期，格式为"YYYYMMDD"
            
        Returns:
            list: 沪港通/深港通股票列表
        """
        from app.services.data_sources.tushare_provider import TushareProvider
        
        try:
            df = TushareProvider.get_hsgt_stocks(trade_date)
            if df is not None:
                # 转换为字典列表
                result = df.to_dict('records')
                return result
            else:
                return []
        except Exception as e:
            logger.error(f"获取沪港通/深港通股票列表失败: {e}")
            return []
    
    def save_stock_companies(self):
        """获取所有A股公司信息并保存到数据库
        
        从AkShare获取所有A股公司的基本信息，包括股票代码、名称、市场、行业和上市日期，
        并保存到数据库的stock_company集合中。
        """
        # 初始化数据库
        if not self.repo.init_tables():
            logger.error("数据库初始化失败，无法保存数据")
            return False
        
        # 获取所有A股股票列表
        all_stocks = self.get_all_a_stocks()
        logger.info(f"总共有 {len(all_stocks)} 只A股股票")
        
        # 保存公司数据到数据库
        if all_stocks:
            success = self.repo.save_stock_companies(all_stocks)
            if success:
                logger.info(f"成功保存 {len(all_stocks)} 家A股公司信息")
                return True
            else:
                logger.error("保存A股公司信息失败")
                return False
        else:
            logger.error("没有获取到A股公司信息")
            return False
    
    def get_stock_companies_from_db(self):
        """从数据库获取所有A股公司信息
        
        Returns:
            list: A股公司信息列表
        """
        try:
            return self.repo.get_stock_companies()
        except Exception as e:
            logger.error(f"从数据库获取A股公司信息失败: {e}")
            return []
    
    def sync_stock_data_in_range(self, start_date, end_date, stock_codes=None, data_source="tushare"):
        """同步固定时间范围内的股票数据到数据库
        
        从数据库获取股票列表，然后按天同步指定时间范围内的股票数据。
        对于Tushare数据源，使用批量获取方法以减少API调用次数，避免触发 rate limit。
        
        Args:
            start_date: 开始日期，格式为"YYYYMMDD"
            end_date: 结束日期，格式为"YYYYMMDD"
            stock_codes: 股票代码列表，为None时同步所有股票
            data_source: 数据源，可选值为"akshare"或"tushare"
            
        Returns:
            dict: 同步结果
        """
        # 初始化数据库
        if not self.repo.init_tables():
            logger.error("数据库初始化失败，无法保存数据")
            return {"message": "数据库初始化失败", "success": False}
        
        # 从数据库获取股票列表
        stock_companies = self.get_stock_companies_from_db()
        if not stock_companies:
            logger.error("从数据库获取股票列表失败")
            return {"message": "从数据库获取股票列表失败", "success": False}
        
        # 筛选股票代码
        stocks_to_sync = []
        for company in stock_companies:
            if isinstance(company, dict):
                # 从字典中获取股票代码
                if 'sec_code' in company:
                    stock_code = str(company['sec_code'])
                    # 格式化为6位数字代码
                    stock_code = f"{int(stock_code):06d}"
                    if stock_codes is None or stock_code in stock_codes:
                        stocks_to_sync.append(stock_code)
            else:
                # 处理对象类型
                if hasattr(company, 'sec_code'):
                    stock_code = f"{company.sec_code:06d}"
                    if stock_codes is None or stock_code in stock_codes:
                        stocks_to_sync.append(stock_code)
        
        if not stocks_to_sync:
            logger.error("没有股票需要同步")
            return {"message": "没有股票需要同步", "success": False}
        
        logger.info(f"开始同步 {len(stocks_to_sync)} 只股票的数据，时间范围: {start_date} 至 {end_date}")
        
        # 获取交易日历数据
        trade_calendar_data = self.repo.get_trade_calendar()
        if not trade_calendar_data:
            logger.error("获取交易日历数据失败，无法确定交易日")
            return {"message": "获取交易日历数据失败", "success": False}
        
        # 生成日期范围
        try:
            start_dt = datetime.strptime(start_date, "%Y%m%d")
            end_dt = datetime.strptime(end_date, "%Y%m%d")
            start_date_obj = start_dt.date()
            end_date_obj = end_dt.date()
        except Exception as e:
            logger.error(f"日期格式错误: {e}")
            return {"message": "日期格式错误", "success": False}
        
        # 过滤出指定范围内的交易日
        trading_days = []
        for item in trade_calendar_data:
            trade_date = date.fromisoformat(item['trade_date'])
            if (trade_date >= start_date_obj and 
                trade_date <= end_date_obj and 
                item['is_trading_day']):
                trading_days.append(trade_date)
        
        # 排序交易日
        trading_days.sort()
        
        # 检查是否有交易日
        if not trading_days:
            logger.info("指定日期范围内没有交易日")
            return {
                "message": "指定日期范围内没有交易日",
                "success": True,
                "processed_days": 0,
                "saved_days": 0,
                "total_records": 0
            }
        
        # 生成交易日范围
        date_range = pd.DatetimeIndex([pd.Timestamp(day) for day in trading_days])
        
        # 同步数据
        processed_days = 0
        saved_count = 0
        total_records = 0
        
        logger.info(f"指定日期范围内共有 {len(trading_days)} 个交易日")
        
        for single_date in date_range:
            try:
                # 格式化日期为YYYYMMDD
                date_str = single_date.strftime("%Y%m%d")
                logger.info(f"开始同步 {date_str} 的数据")
                
                # 按1000个股票一批进行处理
                batch_size = 1000
                total_chunks = (len(stocks_to_sync) + batch_size - 1) // batch_size
                day_records = 0
                day_success = False
                
                for i in range(0, len(stocks_to_sync), batch_size):
                    # 提取当前批次的股票代码
                    chunk = stocks_to_sync[i:i+batch_size]
                    chunk_start = i + 1
                    chunk_end = min(i + batch_size, len(stocks_to_sync))
                    logger.info(f"处理第 {i//batch_size + 1}/{total_chunks} 批股票，范围: {chunk_start}-{chunk_end}")
                    
                    # 使用批量方法获取数据
                    k_data = self.get_daily_k_data_batch(chunk, date_str, date_str, data_source)
                    
                    if k_data and len(k_data) > 0:
                        # 保存数据
                        success = self.repo.save_stock_prices(k_data)
                        if success:
                            day_records += len(k_data)
                            total_records += len(k_data)
                            logger.info(f"成功同步第 {i//batch_size + 1} 批数据，共 {len(k_data)} 条记录")
                            day_success = True
                        else:
                            logger.error(f"同步第 {i//batch_size + 1} 批数据失败")
                    else:
                        logger.error(f"获取第 {i//batch_size + 1} 批数据失败")
                    
                    # 每批处理后增加等待时间，降低请求频率
                    # Tushare基础积分每分钟可调用500次，每次6000条数据
                    # 为安全起见，每批调用后等待1秒
                    # time.sleep(1)
                
                if day_success:
                    saved_count += 1
                    logger.info(f"成功同步 {date_str} 的数据，共 {day_records} 条记录")
                else:
                    logger.error(f"同步 {date_str} 的数据失败")
                
                processed_days += 1
                
                # 每处理5天增加额外等待
                if processed_days % 5 == 0:
                    logger.info(f"已处理 {processed_days} 天，成功保存 {saved_count} 天的数据")
                    # time.sleep(2)
                    
            except Exception as e:
                logger.error(f"处理 {single_date.strftime('%Y%m%d')} 时出错: {e}")
                processed_days += 1
        
        logger.info(f"同步完成，共处理 {processed_days} 天，成功保存 {saved_count} 天的数据，共 {total_records} 条记录")
        return {
            "message": f"同步完成，共处理 {processed_days} 天，成功保存 {saved_count} 天的数据，共 {total_records} 条记录",
            "success": True,
            "processed_days": processed_days,
            "saved_days": saved_count,
            "total_records": total_records
        }
    
    def sync_trade_calendar(self):
        """同步交易日历数据到数据库
        
        从AkShare获取A股交易日历数据，并保存到数据库。
        按5年分段获取数据，确保覆盖1990-12-19到当天的范围，并补充1992-05-04为交易日。
        只同步数据库中不存在的新数据。
        
        Returns:
            dict: 同步结果
        """
        # 初始化数据库
        if not self.repo.init_tables():
            logger.error("数据库初始化失败，无法保存数据")
            return {"message": "数据库初始化失败", "success": False}
        
        try:
            logger.info("开始从AkShare获取交易日历数据")
            
            # 获取数据库中最新的交易日日期
            latest_db_date = self.repo.get_latest_trade_date()
            
            # 定义日期范围和分段
            start_date = date(1990, 12, 19)
            end_date = date.today()  # 使用当天日期作为结束日期
            segment_years = 5
            
            # 如果数据库已有数据，则从最新日期的下一天开始
            if latest_db_date:
                logger.info(f"数据库中最新的交易日日期: {latest_db_date}")
                start_date = latest_db_date + timedelta(days=1)
                if start_date > end_date:
                    logger.info("数据库数据已经是最新的，无需同步")
                    return {
                        "message": "数据库数据已经是最新的，无需同步",
                        "success": True,
                        "count": 0,
                        "latest_date": latest_db_date.isoformat()
                    }
            else:
                logger.info("数据库中没有交易日历数据，将同步完整数据")
            
            # 生成5年分段
            segments = []
            current_start = start_date
            while current_start <= end_date:
                current_end = date(min(current_start.year + segment_years - 1, end_date.year), 12, 31)
                if current_end > end_date:
                    current_end = end_date
                segments.append((current_start, current_end))
                current_start = date(current_start.year + segment_years, 1, 1)
            
            # 存储所有数据
            all_trade_dates = set()
            trade_calendar_list = []
            
            # 从AkShare获取完整的交易日历数据
            tool_trade_date_hist_sina_df = ak.tool_trade_date_hist_sina()
            logger.info(f"从AkShare获取完整交易日历数据成功，共 {len(tool_trade_date_hist_sina_df)} 条记录")
            
            # 转换日期格式
            tool_trade_date_hist_sina_df['trade_date'] = pd.to_datetime(tool_trade_date_hist_sina_df['trade_date']).dt.date
            
            # 过滤出需要同步的新数据
            new_data_df = tool_trade_date_hist_sina_df[tool_trade_date_hist_sina_df['trade_date'] >= start_date]
            logger.info(f"需要同步的新数据: {len(new_data_df)} 条记录")
            
            # 分段处理数据
            for i, (segment_start, segment_end) in enumerate(segments):
                logger.info(f"处理第 {i+1}/{len(segments)} 段数据: {segment_start} 至 {segment_end}")
                
                # 过滤当前分段的数据
                segment_df = new_data_df[(new_data_df['trade_date'] >= segment_start) & (new_data_df['trade_date'] <= segment_end)]
                
                logger.info(f"第 {i+1} 段处理成功，共 {len(segment_df)} 条记录")
                
                # 处理数据
                for _, row in segment_df.iterrows():
                    trade_date = row['trade_date']
                    all_trade_dates.add(trade_date)
                    is_trading_day = bool(row['is_trading_day']) if 'is_trading_day' in row else True
                    
                    trade_calendar = TradeCalendar(
                        trade_date=trade_date,
                        is_trading_day=is_trading_day
                    )
                    trade_calendar_list.append(trade_calendar)
            
            # 补充1992-05-04为交易日（如果在同步范围内）
            special_date = date(1992, 5, 4)
            if special_date >= start_date and special_date not in all_trade_dates:
                logger.info(f"补充 {special_date} 为交易日")
                trade_calendar = TradeCalendar(
                    trade_date=special_date,
                    is_trading_day=True
                )
                trade_calendar_list.append(trade_calendar)
                all_trade_dates.add(special_date)
            elif special_date < start_date:
                logger.info(f"{special_date} 不在同步范围内，跳过")
            else:
                logger.info(f"{special_date} 已经是交易日")
            
            # 保存到数据库
            if trade_calendar_list:
                success = self.repo.save_trade_calendar(trade_calendar_list)
                if success:
                    logger.info(f"成功保存 {len(trade_calendar_list)} 条交易日历数据")
                    return {
                        "message": f"成功同步 {len(trade_calendar_list)} 条交易日历数据，覆盖{start_date}到{end_date}范围",
                        "success": True,
                        "count": len(trade_calendar_list),
                        "segments": len(segments),
                        "start_date": start_date.isoformat(),
                        "end_date": end_date.isoformat()
                    }
                else:
                    logger.error("保存交易日历数据失败")
                    return {"message": "保存交易日历数据失败", "success": False}
            else:
                logger.info("没有需要同步的新数据")
                return {
                    "message": "没有需要同步的新数据",
                    "success": True,
                    "count": 0,
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat()
                }
                
        except Exception as e:
            logger.error(f"同步交易日历数据失败: {e}")
            return {"message": f"同步交易日历数据失败: {str(e)}", "success": False}
    
    def validate_strategy(self, strategy_name, start_date=None, end_date=None):
        """根据策略从历史数据中找到符合的股票及其时间段区间，并验证之后几天的股票涨幅，计算策略的正确率
        
        Args:
            strategy_name: 策略名称，目前支持 "strategy1"
            start_date: 开始日期，格式为"YYYY-MM-DD"
            end_date: 结束日期，格式为"YYYY-MM-DD"
            
        Returns:
            dict: 包含符合条件的股票列表和策略正确率的结果
        """
        from app.services.strategies import validate_strategy
        return validate_strategy(self, strategy_name, start_date, end_date)
