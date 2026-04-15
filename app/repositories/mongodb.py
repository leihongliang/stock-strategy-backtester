from pymongo import MongoClient
from app.models.stock import StockDailyPrice
from app.models.company import StockCompany
from app.models.trade_calendar import TradeCalendar
import pandas as pd
from app.utils.log import logger
from datetime import datetime, date

class MongoDBRepository:
    """MongoDB数据仓库
    
    负责与MongoDB数据库交互，处理股票数据的存储和查询。
    """
    
    def __init__(self, host='localhost', port=27017, database='stock_data'):
        """初始化MongoDB连接
        
        Args:
            host: MongoDB主机地址
            port: MongoDB端口
            database: 数据库名称
        """
        try:
            self.client = MongoClient(host, port)
            self.db = self.client[database]
            self.collection = self.db['stock_daily_price']
            self.company_collection = self.db['stock_company']
            self.calendar_collection = self.db['trade_calendar']
            logger.info("MongoDB连接成功")
        except Exception as e:
            logger.error(f"MongoDB连接失败: {e}")
            self.client = None
            self.db = None
            self.collection = None
            self.company_collection = None
            self.calendar_collection = None
    
    def init_tables(self):
        """初始化MongoDB集合
        
        MongoDB不需要预定义表结构，所以这里只需要确保集合存在即可。
        
        Returns:
            bool: 初始化是否成功
        """
        if not self.client:
            logger.error("MongoDB连接失败，无法初始化")
            return False
        
        # MongoDB不需要创建表，集合会在第一次插入数据时自动创建
        logger.info("MongoDB集合初始化完成")
        return True
    
    def save_stock_prices(self, stock_price_data: list[StockDailyPrice]):
        """保存股票价格数据到MongoDB
        
        Args:
            stock_price_data: StockDailyPrice对象的列表
        """
        if not self.client or not stock_price_data:
            return False
        
        try:
            # 为每个文档添加唯一标识并执行upsert操作
            for item in stock_price_data:
                doc = item.to_mongo_doc()
                # 以(sec_code, trade_date)组合作为唯一标识
                filter_criteria = {
                    'sec_code': doc['sec_code'],
                    'trade_date': doc['trade_date']
                }
                # 使用replace_one实现upsert，存在则更新，不存在则插入
                self.collection.replace_one(filter_criteria, doc, upsert=True)
            return True
        except Exception as e:
            logger.error(f"保存股票数据失败: {e}")
            return False
    
    def save_stock_companies(self, stock_company_data: list[StockCompany]):
        """保存A股公司信息到MongoDB
        
        Args:
            stock_company_data: StockCompany对象的列表
        """
        if not self.client or not stock_company_data:
            return False
        
        try:
            # 为每个文档添加唯一标识并执行upsert操作
            for item in stock_company_data:
                doc = item.to_mongo_doc()
                # 以sec_code作为唯一标识
                filter_criteria = {
                    'sec_code': doc['sec_code']
                }
                # 使用replace_one实现upsert，存在则更新，不存在则插入
                self.company_collection.replace_one(filter_criteria, doc, upsert=True)
            return True
        except Exception as e:
            logger.error(f"保存A股公司信息失败: {e}")
            return False
    
    def get_stock_prices(self, sec_code):
        """获取股票价格数据
        
        从stock_daily_price集合中获取指定股票的K线数据。
        
        Args:
            sec_code: 股票代码（数字格式，如600000）
            
        Returns:
            list: 股票价格数据列表
        """
        if not self.client:
            return None
        
        try:
            # 查询数据
            cursor = self.collection.find({'sec_code': sec_code}, 
                                         {'_id': 0, 'trade_date': 1, 'open': 1, 'high': 1, 'low': 1, 'close': 1, 'volume': 1, 'amount': 1})
            
            # 将数据转换为列表
            result = []
            from datetime import datetime
            for doc in cursor:
                # 将trade_date转换为datetime对象用于排序
                trade_date = doc['trade_date']
                if isinstance(trade_date, str):
                    # 如果是字符串，尝试转换为datetime对象
                    try:
                        trade_date = datetime.fromisoformat(trade_date)
                    except:
                        pass
                result.append((trade_date, doc['open'], doc['high'], doc['low'], doc['close'], doc['volume'], doc['amount']))
            
            # 按日期排序
            result.sort(key=lambda x: x[0])
            
            return result
        except Exception as e:
            logger.error(f"获取股票价格数据失败: {e}")
            return None
    
    def get_stock_count(self):
        """获取股票数据数量
        
        返回stock_daily_price集合中的数据行数。
        
        Returns:
            int: 数据行数
        """
        if not self.client:
            return 0
        
        try:
            count = self.collection.count_documents({})
            return count
        except Exception as e:
            logger.error(f"获取股票数据数量失败: {e}")
            return 0
    
    def get_stock_companies(self):
        """获取所有A股公司信息
        
        Returns:
            list: A股公司信息列表
        """
        if not self.client:
            return None
        
        try:
            # 查询所有公司信息
            cursor = self.company_collection.find({}, {'_id': 0})
            # 将数据转换为列表
            result = []
            for doc in cursor:
                result.append(doc)
            return result
        except Exception as e:
            logger.error(f"获取A股公司信息失败: {e}")
            return None
    
    def get_stock_company_by_code(self, sec_code):
        """根据股票代码获取A股公司信息
        
        Args:
            sec_code: 股票代码
            
        Returns:
            dict: 股票公司信息，如果不存在则返回None
        """
        if not self.client:
            return None
        
        try:
            # 查询指定股票代码的公司信息
            doc = self.company_collection.find_one({'sec_code': sec_code}, {'_id': 0})
            return doc
        except Exception as e:
            logger.error(f"根据股票代码获取公司信息失败: {e}")
            return None
    
    def get_stock_company_count(self):
        """获取A股公司数量
        
        Returns:
            int: 公司数量
        """
        if not self.client:
            return 0
        
        try:
            count = self.company_collection.count_documents({})
            return count
        except Exception as e:
            logger.error(f"获取A股公司数量失败: {e}")
            return 0
    
    def save_trade_calendar(self, trade_calendar_data: list[TradeCalendar]):
        """保存交易日历数据到MongoDB
        
        Args:
            trade_calendar_data: TradeCalendar对象的列表
        """
        if not self.client or not trade_calendar_data:
            return False
        
        try:
            # 为每个文档添加唯一标识并执行upsert操作
            for item in trade_calendar_data:
                doc = item.to_mongo_doc()
                # 以trade_date作为唯一标识
                filter_criteria = {
                    'trade_date': doc['trade_date']
                }
                # 使用replace_one实现upsert，存在则更新，不存在则插入
                self.calendar_collection.replace_one(filter_criteria, doc, upsert=True)
            return True
        except Exception as e:
            logger.error(f"保存交易日历数据失败: {e}")
            return False
    
    def get_trade_calendar(self):
        """获取所有交易日历数据
        
        Returns:
            list: 交易日历数据列表
        """
        if not self.client:
            return None
        
        try:
            # 查询所有交易日历数据
            cursor = self.calendar_collection.find({}, {'_id': 0})
            # 将数据转换为列表
            result = []
            for doc in cursor:
                result.append(doc)
            return result
        except Exception as e:
            logger.error(f"获取交易日历数据失败: {e}")
            return None
    
    def get_trade_calendar_count(self):
        """获取交易日历数据数量
        
        Returns:
            int: 数据行数
        """
        if not self.client:
            return 0
        
        try:
            count = self.calendar_collection.count_documents({})
            return count
        except Exception as e:
            logger.error(f"获取交易日历数据数量失败: {e}")
            return 0
    
    def get_latest_trade_date(self):
        """获取最新的交易日日期
        
        Returns:
            date or None: 最新的交易日日期，如果没有数据则返回None
        """
        if not self.client:
            return None
        
        try:
            # 按trade_date降序排序，取第一条记录
            latest_doc = self.calendar_collection.find_one(
                {},
                {'trade_date': 1},
                sort=[('trade_date', -1)]
            )
            
            if latest_doc and 'trade_date' in latest_doc:
                trade_date = latest_doc['trade_date']
                # 处理datetime类型的trade_date
                if hasattr(trade_date, 'date'):
                    return trade_date.date()
                # 兼容处理字符串类型的trade_date（用于旧数据）
                try:
                    return date.fromisoformat(trade_date)
                except:
                    pass
            return None
        except Exception as e:
            logger.error(f"获取最新交易日日期失败: {e}")
            return None
    
    def get_unique_stock_codes(self):
        """获取stock_daily_price表中所有唯一的股票代码
        
        Returns:
            list: 唯一股票代码列表
        """
        if not self.client:
            return []
        
        try:
            # 使用distinct获取唯一的股票代码
            unique_codes = self.collection.distinct('sec_code')
            return unique_codes
        except Exception as e:
            logger.error(f"获取唯一股票代码失败: {e}")
            return []
    
    def get_latest_stock_price_date(self):
        """获取stock_daily_price表中的最新日期
        
        Returns:
            date or None: 最新的股票价格日期，如果没有数据则返回None
        """
        if not self.client:
            return None
        
        try:
            # 按trade_date降序排序，取第一条记录
            latest_doc = self.collection.find_one(
                {},
                {'trade_date': 1},
                sort=[('trade_date', -1)]
            )
            
            if latest_doc and 'trade_date' in latest_doc:
                trade_date = latest_doc['trade_date']
                # 处理datetime类型的trade_date
                if hasattr(trade_date, 'date'):
                    return trade_date.date()
                # 兼容处理字符串类型的trade_date（用于旧数据）
                try:
                    return date.fromisoformat(trade_date)
                except:
                    pass
            return None
        except Exception as e:
            logger.error(f"获取最新股票价格日期失败: {e}")
            return None
    
    def get_all_stock_prices(self):
        """获取stock_daily_price表的所有数据
        
        Returns:
            list: 所有股票价格数据列表
        """
        if not self.client:
            return []
        
        try:
            # 查询所有数据
            cursor = self.collection.find({}, {'_id': 0})
            # 将数据转换为列表
            result = []
            for doc in cursor:
                result.append(doc)
            return result
        except Exception as e:
            logger.error(f"获取所有股票价格数据失败: {e}")
            return []
    
    def update_sec_code_to_string(self):
        """将stock_daily_price表的所有sec_code字段从int更新为str，并补0到6位
        
        Returns:
            dict: 更新结果
        """
        if not self.client:
            return {"success": False, "message": "MongoDB连接失败"}
        
        try:
            # 获取所有数据
            all_data = self.get_all_stock_prices()
            if not all_data:
                return {"success": False, "message": "没有数据需要更新"}
            
            updated_count = 0
            for doc in all_data:
                # 检查sec_code类型
                if isinstance(doc.get('sec_code'), int):
                    # 将int转换为6位字符串并补0
                    new_sec_code = f"{doc['sec_code']:06d}"
                    # 更新数据
                    filter_criteria = {
                        'sec_code': doc['sec_code'],
                        'trade_date': doc['trade_date']
                    }
                    update_data = {
                        '$set': {'sec_code': new_sec_code}
                    }
                    self.collection.update_one(filter_criteria, update_data)
                    updated_count += 1
            
            return {
                "success": True,
                "message": f"成功更新 {updated_count} 条记录的sec_code字段",
                "updated_count": updated_count
            }
        except Exception as e:
            logger.error(f"更新sec_code字段失败: {e}")
            return {"success": False, "message": f"更新失败: {str(e)}"}
