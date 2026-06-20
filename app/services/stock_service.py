import pandas as pd
from pandas import DataFrame
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
from app.services.stock_company_service import StockCompanyService
from app.services.trade_calendar_service import TradeCalendarService
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
        self.stock_company_service = StockCompanyService()
        self.trade_calendar_service = TradeCalendarService()
        logger.info("使用MongoDB作为数据库")

    def get_all_funds(self) -> list[StockCompany]:
        """获取所有基金列表"""
        return self.stock_company_service.get_all_funds()

    def get_daily_k_data(self, stock_code: str, start_date: str, end_date: str, data_source: str = "akshare") -> tuple[str, DataFrame | None]:
        """获取证券日K线数据
        
        自动查询数据库判断证券类型，选择对应的数据源。
        
        Args:
            stock_code: 证券代码
            start_date: 开始日期，格式为"YYYYMMDD"
            end_date: 结束日期，格式为"YYYYMMDD"
            data_source: 数据源，可选值为"akshare"或"tushare"（仅对股票有效）
            
        Returns:
            tuple: (stock_code, k_data)
        """
        try:
            # 查询数据库获取证券类型
            sec_type = "stock"
            company = self.stock_company_service.get_stock_company_by_code(stock_code)
            if company:
                if isinstance(company, dict):
                    sec_type = company.get('sec_type', 'stock')
                else:
                    sec_type = getattr(company, 'sec_type', 'stock')
                    if hasattr(sec_type, 'value'):
                        sec_type = sec_type.value
            
            if sec_type == "index":
                k_data = AkShareProvider.get_index_daily_k_data(stock_code, start_date, end_date)
            elif sec_type == "fund":
                k_data = AkShareProvider.get_etf_daily_k_data(stock_code, start_date, end_date)
            elif data_source == "tushare":
                k_data = TushareProvider.get_daily_k_data(stock_code, start_date, end_date)
            else:
                k_data = AkShareProvider.get_daily_k_data(stock_code, start_date, end_date)
            return stock_code, k_data
        except Exception as e:
            logger.error(f"获取{stock_code}日K线数据失败: {e}")
            return stock_code, None
    
    def get_daily_k_data_batch(self, stock_codes: list[str], start_date: str, end_date: str, data_source: str = "akshare") -> list[StockDailyPrice]:
        """批量获取证券日K线数据
        
        Args:
            stock_codes: 证券代码列表
            start_date: 开始日期，格式为"YYYYMMDD"
            end_date: 结束日期，格式为"YYYYMMDD"
            data_source: 数据源（仅对股票有效）
            
        Returns:
            list[StockDailyPrice]: 所有证券的日K线数据列表
        """
        try:
            all_data = []
            for stock_code in stock_codes:
                _, data = self.get_daily_k_data(stock_code, start_date, end_date, data_source)
                
                if data is not None and not data.empty:
                    # 计算昨收价、涨跌额和涨跌幅
                    data['pre_close'] = data['close'].shift(1)
                    data['pre_close'].fillna(data['open'].iloc[0], inplace=True)
                    data['change'] = data['close'] - data['pre_close']
                    data['pct_chg'] = (data['change'] / data['pre_close']) * 100
                    
                    # 移除股票代码前缀，保持为字符串
                    sec_code = stock_code.replace('SH', '').replace('SZ', '').replace('sh', '').replace('sz', '')
                    sec_code = sec_code.zfill(6)
                    
                    for _, row in data.iterrows():
                        open_price = int(float(row['open']) * 100)
                        high_price = int(float(row['high']) * 100)
                        low_price = int(float(row['low']) * 100)
                        close_price = int(float(row['close']) * 100)
                        
                        pre_close = int(float(row['pre_close']) * 100)
                        change = int(float(row['change']) * 100)
                        pct_chg = int(float(row['pct_chg']) * 100)
                        
                        volume = int(row['volume'])
                        amount = int(row['amount'])
                        
                        adjfactor = 10000
                        st_status = 0
                        trade_status = 1
                        
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
            logger.error(f"批量获取证券日K线数据失败: {e}")
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

        try:
            k_data['date'] = pd.to_datetime(k_data['date'])
        except Exception as e:
            logger.error(f"股票 {sec_code} 的日期转换失败: {e}")
            logger.error(f"日期列数据示例: {k_data['date'].head(10).tolist()}")
            return None
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
        k_data['ma60'] = k_data['close'].rolling(window=60).mean()
        k_data['ma120'] = k_data['close'].rolling(window=120).mean()
        k_data['ma200'] = k_data['close'].rolling(window=200).mean()

        k_data['ema12'] = k_data['close'].ewm(span=12, adjust=False).mean()
        k_data['ema26'] = k_data['close'].ewm(span=26, adjust=False).mean()
        k_data['macd'] = k_data['ema12'] - k_data['ema26']
        k_data['macd_signal'] = k_data['macd'].ewm(span=9, adjust=False).mean()
        k_data['macd_hist'] = k_data['macd'] - k_data['macd_signal']

        return k_data

    def save_stock_companies(self) -> bool:
        """全量更新A股公司信息到数据库"""
        return self.stock_company_service.save_all_stock_companies()
    
    def get_stock_companies_from_db(self) -> list:
        """从数据库获取所有公司信息"""
        return self.stock_company_service.get_stock_companies_from_db()
    
    def get_stock_company_by_code(self, stock_code: str) -> dict | StockCompany | None:
        """根据股票代码从数据库获取公司信息"""
        return self.stock_company_service.get_stock_company_by_code(stock_code)
    
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
    
    def sync_stock_data_in_range(self, start_date: str, end_date: str, stock_codes: list[str] | None = None, data_source: str = "tushare") -> dict:
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

        # 确保所有指定的股票代码都存在于数据库中
        if stock_codes:
            self.stock_company_service.ensure_stocks_exist(stock_codes)
        
        # 从数据库获取股票列表
        stock_companies = self.stock_company_service.get_stock_companies_from_db()
        logger.info(f"从数据库获取到 {len(stock_companies)} 只A股股票")
        
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
        
        # 获取交易日
        trading_days = self.trade_calendar_service.get_trading_days(start_date, end_date)
        
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
        
        logger.info(f"指定日期范围内共有 {len(trading_days)} 个交易日")
        
        # 单只股票优化：Tushare接口每次最多返回6000条数据，无需按天查询
        if len(stocks_to_sync) == 1:
            logger.info(f"单只股票优化：直接查询 {start_date} 至 {end_date} 的完整数据")
            try:
                k_data = self.get_daily_k_data_batch(stocks_to_sync, start_date, end_date, data_source)
                if k_data and len(k_data) > 0:
                    success = self.repo.save_stock_prices(k_data)
                    if success:
                        logger.info(f"成功同步 {stocks_to_sync[0]} 的数据，共 {len(k_data)} 条记录")
                        return {
                            "message": "同步成功",
                            "success": True,
                            "processed_days": len(trading_days),
                            "saved_days": 1,
                            "total_records": len(k_data)
                        }
                    else:
                        logger.error(f"保存数据失败")
                        return {"message": "保存数据失败", "success": False}
                else:
                    logger.error(f"获取数据失败")
                    return {"message": "获取数据失败", "success": False}
            except Exception as e:
                logger.error(f"同步数据时出错: {e}")
                return {"message": f"同步数据时出错: {str(e)}", "success": False}
        
        # 同步数据
        processed_days = 0
        saved_count = 0
        total_records = 0
        
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
                    logger.info(f"开始获取第 {i//batch_size + 1} 批数据，共 {len(chunk)} 只股票")
                    k_data = self.get_daily_k_data_batch(chunk, date_str, date_str, data_source)

                    if k_data and len(k_data) > 0:
                        logger.info(f"获取第 {i//batch_size + 1} 批数据成功，共 {len(k_data)} 条记录，开始保存到数据库")
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
                        logger.warning(f"获取第 {i//batch_size + 1} 批数据为空或失败")
                    
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
    
    def sync_trade_calendar(self, start_date: date | None = None, end_date: date | None = None) -> None:
        """同步交易日历数据到数据库"""
        self.trade_calendar_service.sync_trade_calendar(start_date, end_date)
    
    def validate_strategy(self, strategy_name: str, start_date: str | None = None, end_date: str | None = None) -> dict:
        """根据策略从历史数据中找到符合的股票及其时间段区间，并验证之后几天的股票涨幅，计算策略的正确率
        
        Args:
            strategy_name: 策略名称，目前支持 "strategy1"
            start_date: 开始日期，格式为"YYYY-MM-DD"
            end_date: 结束日期，格式为"YYYY-MM-DD"
            
        Returns:
            dict: 包含符合条件的股票列表和策略正确率的结果
        """

        return validate_strategy(self, strategy_name, start_date, end_date)
    
    def daily_update(self) -> dict:
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