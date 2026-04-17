import pandas as pd
import time
from datetime import datetime, date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import akshare as ak
from app.repositories.mongodb import MongoDBRepository
from app.models.stock import StockDailyPrice
from app.models.company import StockCompany
from app.models.trade_calendar import TradeCalendar
from app.services.data_sources.akshare_provider import AkShareProvider
from app.services.data_sources.tushare_provider import TushareProvider
from app.config.settings import settings
from app.utils.log import logger
from app.services.strategies import validate_strategy
class StockService:
    """股票服务
    
    处理股票相关的业务逻辑，包括数据获取、涨跌模式生成、股票查询等。
    """
    
    def __init__(self):
        # 初始化MongoDB仓库
        self.repo = MongoDBRepository(
            host=settings.MONGODB_HOST,
            port=settings.MONGODB_PORT,
            database=settings.MONGODB_DATABASE
        )
        logger.info("使用MongoDB作为数据库")
    
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
                        data['pre_close'] = data['close'].shift(1)
                        data['pre_close'].fillna(data['open'].iloc[0], inplace=True)
                        data['change'] = data['close'] - data['pre_close']
                        data['pct_chg'] = (data['change'] / data['pre_close']) * 100
                        
                        # 移除股票代码前缀，保持为字符串
                        sec_code = stock_code.replace('SH', '').replace('SZ', '')
                        # 确保股票代码为6位字符串
                        sec_code = sec_code.zfill(6)
                        
                        for _, row in data.iterrows():
                            # 将价格乘以100转换为整数
                            open_price = int(float(row['open']) * 100)
                            high_price = int(float(row['high']) * 100)
                            low_price = int(float(row['low']) * 100)
                            close_price = int(float(row['close']) * 100)
                            
                            pre_close = int(float(row['pre_close']) * 100)
                            change = int(float(row['change']) * 100)
                            pct_chg = int(float(row['pct_chg']) * 100)
                            
                            volume = int(row['volume'])
                            amount = int(row['amount'])
                            
                            # 其他字段使用默认值
                            adjfactor = 10000  # 默认调整因子
                            st_status = 0  # 非ST
                            trade_status = 1  # 正常交易
                            
                            # 创建StockDailyPrice对象
                            stock_price = StockDailyPrice(
                                trade_date=pd.to_datetime(row['date']).date(),
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


    def load_stock_data(self, stock_code: str) -> pd.DataFrame | None:
        """从MongoDB加载股票数据
        
        从MongoDB的stock_daily_price集合中加载指定股票的数据。
        """
        # 移除股票代码前缀，保持为字符串
        sec_code = stock_code.replace('SH', '').replace('SZ', '')
        # 确保股票代码为6位字符串
        sec_code = sec_code.zfill(6)
        
        # 从数据库获取数据
        result = self.repo.get_stock_prices(sec_code)
        
        if not result:
            return None
        
        # 转换为DataFrame
        columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'amount']
        k_data = pd.DataFrame(result, columns=columns)

        k_data['open'] = k_data['open'] / 100
        k_data['high'] = k_data['high'] / 100
        k_data['low'] = k_data['low'] / 100
        k_data['close'] = k_data['close'] / 100

        k_data['date'] = pd.to_datetime(k_data['date'])
        k_data = k_data.sort_values('date').reset_index(drop=True)

        k_data['pct_change'] = k_data['close'].pct_change() * 100
        k_data['price_change'] = k_data['close'].diff()
        k_data['volume_change'] = k_data['volume'].pct_change()
        k_data['intraday_change'] = (k_data['close'] - k_data['open']) / k_data['open'] * 100

        k_data['is_red'] = k_data['close'] > k_data['open']
        k_data['is_green'] = k_data['close'] < k_data['open']
        k_data['is_doji'] = k_data['close'] == k_data['open']

        k_data['ma5'] = k_data['close'].rolling(window=5).mean()
        k_data['ma10'] = k_data['close'].rolling(window=10).mean()
        k_data['ma20'] = k_data['close'].rolling(window=20).mean()

        return k_data

    def save_stock_companies(self):
        """获取当天A股公司信息并保存到数据库
        
        从数据库读取所有A股公司的基本信息，然后进行全量更新，
        保存到数据库的stock_company集合中。
        如果数据库中没有数据，则从外部数据源获取并保存。
        """
        
        # 从数据库获取所有A股公司信息
        all_stocks = self.get_stock_companies_from_db()
        logger.info(f"从数据库获取到 {len(all_stocks)} 只A股股票")
        
        # 保存公司数据到数据库（全量更新）
        if all_stocks:
            # 转换为StockCompany对象列表
            stock_company_objects = []
            for stock in all_stocks:
                # 处理字典类型的数据
                if isinstance(stock, dict):
                    # 确保日期格式正确
                    listing_date = stock.get('listing_date')
                    if isinstance(listing_date, str):
                        listing_date = pd.to_datetime(listing_date).date()
                    elif isinstance(listing_date, datetime):
                        listing_date = listing_date.date()
                
                    # 创建StockCompany对象
                    stock_company = StockCompany(
                        sec_code=str(stock.get('sec_code')),
                        sec_name=stock.get('sec_name'),
                        market=stock.get('market'),
                        industry=stock.get('industry'),
                        listing_date=listing_date
                    )
                    stock_company_objects.append(stock_company)
            
            # 保存到数据库
            if stock_company_objects:
                success = self.repo.save_stock_companies(stock_company_objects)
                if success:
                    logger.info(f"成功更新 {len(stock_company_objects)} 家A股公司信息")
                    return True
                else:
                    logger.error("保存A股公司信息失败")
                    return False
            else:
                logger.error("没有有效的公司信息可以更新")
                return False
        else:
            # 第一次写入场景，从外部数据源获取数据
            logger.info("数据库中暂无A股公司信息，从外部数据源获取")
            all_stocks = self.get_all_a_stocks()
            logger.info(f"从外部数据源获取到 {len(all_stocks)} 只A股股票")
            
            # 保存到数据库
            if all_stocks:
                success = self.repo.save_stock_companies(all_stocks)
                if success:
                    logger.info(f"成功保存 {len(all_stocks)} 家A股公司信息（第一次写入）")
                    return True
                else:
                    logger.error("保存A股公司信息失败")
                    return False
            else:
                logger.error("从外部数据源获取A股公司信息失败")
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
    
    def get_stock_company_by_code(self, stock_code):
        """根据股票代码从数据库获取股票公司信息
        
        Args:
            stock_code: 股票代码
            
        Returns:
            dict or object: 股票公司信息，如果不存在则返回None
        """
        try:
            # 直接从数据库根据股票代码查询
            return self.repo.get_stock_company_by_code(stock_code)
        except Exception as e:
            logger.error(f"根据股票代码获取公司信息失败: {e}")
            return None
    
    def get_all_a_stocks_from_db(self):
        """从数据库的stock_daily_price表获取所有A股股票列表
        
        Returns:
            list: 股票代码列表
        """
        try:
            return self.repo.get_unique_stock_codes()
        except Exception as e:
            logger.error(f"从数据库获取A股股票列表失败: {e}")
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
                    # 确保股票代码为6位字符串
                    stock_code = stock_code.zfill(6)
                    if stock_codes is None or stock_code in stock_codes:
                        stocks_to_sync.append(stock_code)
            else:
                # 处理对象类型
                if hasattr(company, 'sec_code'):
                    stock_code = str(company.sec_code)
                    # 确保股票代码为6位字符串
                    stock_code = stock_code.zfill(6)
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
            trade_date = item['trade_date']
            # 处理datetime类型的trade_date
            if hasattr(trade_date, 'date'):
                trade_date = trade_date.date()
            # 兼容处理字符串类型的trade_date（用于旧数据）
            elif isinstance(trade_date, str):
                trade_date = date.fromisoformat(trade_date)
            
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
    
    def sync_trade_calendar(self, start_date=None, end_date=None):
        """同步交易日历数据到数据库
        
        从AkShare获取A股交易日历数据，并保存到数据库。
        只同步数据库中不存在的新数据。
        
        Args:
            start_date (date, optional): 开始日期. Defaults to None (使用当天日期).
            end_date (date, optional): 结束日期. Defaults to None (使用当年年末).
        """

        try:
            logger.info("开始从AkShare获取交易日历数据")
            
            # 获取数据库中最新和最早的交易日日期
            latest_db_date = self.repo.get_latest_trade_date()
            earliest_db_date = self.repo.get_earliest_trade_date()
            
            # 定义日期范围
            if start_date is None:
                start_date = date.today()  # 默认开始日期为当天
            if end_date is None:
                end_date = date(date.today().year, 12, 31)  # 默认结束日期为当年年末
            
            # 检查数据库中是否已有该范围内的数据
            if earliest_db_date and latest_db_date:
                logger.info(f"数据库中最早的交易日日期: {earliest_db_date}")
                logger.info(f"数据库中最新的交易日日期: {latest_db_date}")
                
                # 如果数据库中已有完整的指定范围数据，则无需同步
                if start_date >= earliest_db_date and end_date <= latest_db_date:
                    logger.info("数据库中已有指定范围内的完整数据，无需同步")
                    return
                # 如果数据库有部分数据，则从最新日期的下一天开始同步
                elif end_date > latest_db_date:
                    logger.info("数据库中有部分数据，从最新日期的下一天开始同步")
                    start_date = latest_db_date + timedelta(days=1)
                    if start_date > end_date:
                        logger.info("数据库数据已经是最新的，无需同步")
                        return
            else:
                logger.info("数据库中没有交易日历数据，将同步指定范围的完整数据")
            
            # 存储所有数据
            all_trade_dates = set()
            trade_calendar_list = []
            
            # 从AkShare获取完整的交易日历数据
            tool_trade_date_hist_sina_df = ak.tool_trade_date_hist_sina()
            logger.info(f"从AkShare获取完整交易日历数据成功，共 {len(tool_trade_date_hist_sina_df)} 条记录")
            
            # 转换日期格式
            tool_trade_date_hist_sina_df['trade_date'] = pd.to_datetime(tool_trade_date_hist_sina_df['trade_date']).dt.date
            
            # 过滤出需要同步的新数据
            new_data_df = tool_trade_date_hist_sina_df[(tool_trade_date_hist_sina_df['trade_date'] >= start_date) & (tool_trade_date_hist_sina_df['trade_date'] <= end_date)]
            logger.info(f"需要同步的新数据: {len(new_data_df)} 条记录")
            
            # 直接处理所有数据
            for _, row in new_data_df.iterrows():
                trade_date = row['trade_date']
                all_trade_dates.add(trade_date)
                is_trading_day = bool(row['is_trading_day']) if 'is_trading_day' in row else True
                
                trade_calendar = TradeCalendar(
                    trade_date=trade_date,
                    is_trading_day=is_trading_day
                )
                trade_calendar_list.append(trade_calendar)

            # 保存到数据库
            if trade_calendar_list:
                success = self.repo.save_trade_calendar(trade_calendar_list)
                if success:
                    logger.info(f"成功保存 {len(trade_calendar_list)} 条交易日历数据")
                else:
                    logger.error("保存交易日历数据失败")
            else:
                logger.info("没有需要同步的新数据")
                
        except Exception as e:
            logger.error(f"同步交易日历数据失败: {e}")
    
    def validate_strategy(self, strategy_name, start_date=None, end_date=None):
        """根据策略从历史数据中找到符合的股票及其时间段区间，并验证之后几天的股票涨幅，计算策略的正确率
        
        Args:
            strategy_name: 策略名称，目前支持 "strategy1"
            start_date: 开始日期，格式为"YYYY-MM-DD"
            end_date: 结束日期，格式为"YYYY-MM-DD"
            
        Returns:
            dict: 包含符合条件的股票列表和策略正确率的结果
        """

        return validate_strategy(self, strategy_name, start_date, end_date)
    
    def daily_update(self):
        """每日更新股票数据
        
        执行以下操作：
        1. 更新交易日历到最新的一天
        2. 更新新增的A股公司，去掉没有的
        3. 更新日K线到最新的一天
        
        Returns:
            dict: 更新结果
        """
        try:
            results = {}
            
            # 1. 更新交易日历到最新的一天
            logger.info("开始更新交易日历...")
            self.sync_trade_calendar()
            results['calendar_update'] = {"success": True, "message": "交易日历更新完成"}
            
            # 2. 更新新增的A股公司，去掉没有的
            logger.info("开始更新A股公司信息...")
            companies_result = self.save_stock_companies()
            results['companies_update'] = {"success": companies_result, "message": "A股公司信息更新完成"}
            
            # 3. 更新日K线到最新的一天
            logger.info("开始更新日K线数据...")
            # 从数据库获取stock_daily_price的最晚日期作为开始日期
            latest_price_date = self.repo.get_latest_stock_price_date()
            if latest_price_date:
                # 使用数据库中最新的股票价格日期作为开始日期
                start_date = latest_price_date.strftime("%Y%m%d")
                logger.info(f"从数据库最新的股票价格日期 {start_date} 开始更新日K线数据")
            # 使用当天作为结束日期
            end_date = datetime.now().strftime("%Y%m%d")
            logger.info(f"使用当天日期 {end_date} 作为结束日期")
            
            # 同步指定范围的股票数据
            kline_result = self.sync_stock_data_in_range(start_date, end_date, data_source="tushare")
            results['kline_update'] = kline_result
            
            logger.info("每日更新完成")
            return {"success": True, "results": results, "message": "每日更新完成"}
        except Exception as e:
            logger.error(f"每日更新失败: {e}")
            return {"success": False, "message": f"每日更新失败: {str(e)}"}