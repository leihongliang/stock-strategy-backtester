from clickhouse_driver import Client
from app.config.settings import settings

class ClickHouseRepository:
    """ClickHouse数据仓库
    
    负责与ClickHouse数据库交互，处理股票数据的存储和查询。
    """
    
    def __init__(self):
        # 初始化时不指定database，先连接到默认数据库
        self.client = self.get_client()
    
    def get_client(self):
        """获取ClickHouse客户端连接"""
        try:
            client = Client(
                host=settings.CLICKHOUSE_HOST,
                port=settings.CLICKHOUSE_PORT,
                user=settings.CLICKHOUSE_USER,
                password=settings.CLICKHOUSE_PASSWORD
                # 不指定database，使用默认数据库
            )
            return client
        except Exception as e:
            print(f"连接ClickHouse失败: {e}")
            return None
    
    def init_tables(self):
        """初始化ClickHouse表结构
        
        只创建stock_daily_price表，用于存储股票每日K线数据。
        """
        if not self.client:
            return False
        
        try:
            # 检查连接是否正常
            print("检查ClickHouse连接...")
            self.client.execute('SELECT 1')
            print("ClickHouse连接正常")
            
            # 创建数据库
            print("尝试创建数据库stock_data...")
            self.client.execute('CREATE DATABASE IF NOT EXISTS stock_data')
            print("数据库创建成功或已存在")
            
            # 切换到stock_data数据库
            print("切换到stock_data数据库...")
            self.client.execute('USE stock_data')
            print("切换数据库成功")
            
            # 创建股票数据表格
            print("尝试创建stock_daily_price表...")
            self.client.execute('''
            CREATE TABLE IF NOT EXISTS stock_daily_price (
                trade_date Date,
                sec_code UInt32,
                open Int32,
                high Int32,
                low Int32,
                close Int32,
                pre_close Int32,
                change Int32,
                pct_chg Int32,
                volume Int64,
                amount Int64,
                adjfactor Int32,
                st_status Int16,
                trade_status Int16
            ) ENGINE = ReplacingMergeTree()
            ORDER BY (intHash32(sec_code), trade_date)
            ''')
            
            # 创建股票公司信息表格
            print("尝试创建stock_company表...")
            self.client.execute('''
            CREATE TABLE IF NOT EXISTS stock_company (
                sec_code UInt32,
                sec_name String,
                market String,
                industry String,
                listing_date Date
            ) ENGINE = ReplacingMergeTree()
            ORDER BY sec_code
            ''')
            
            print("ClickHouse表结构初始化完成")
            return True
        except Exception as e:
            print(f"初始化ClickHouse表结构失败: {e}")
            return False
    
    def save_stock_prices(self, stock_price_data):
        """保存股票价格数据
        
        将股票的每日K线数据保存到stock_daily_price表中。
        
        Args:
            stock_price_data: StockDailyPrice对象的列表
        """
        if not self.client or not stock_price_data:
            return False
        
        try:
            # 确保使用stock_data数据库
            self.client.execute('USE stock_data')
            
            # 转换为元组列表
            if hasattr(stock_price_data[0], 'to_tuple'):
                insert_data = [item.to_tuple() for item in stock_price_data]
            else:
                insert_data = stock_price_data
            
            self.client.execute('INSERT INTO stock_data.stock_daily_price (trade_date, sec_code, open, high, low, close, pre_close, change, pct_chg, volume, amount, adjfactor, st_status, trade_status) VALUES', insert_data)
            return True
        except Exception as e:
            print(f"保存股票价格数据失败: {e}")
            return False
    
    def save_stock_companies(self, stock_company_data):
        """保存A股公司信息
        
        将A股公司的基本信息保存到stock_company表中。
        
        Args:
            stock_company_data: StockCompany对象的列表
        """
        if not self.client or not stock_company_data:
            return False
        
        try:
            # 确保使用stock_data数据库
            self.client.execute('USE stock_data')
            
            # 转换为元组列表
            insert_data = []
            for item in stock_company_data:
                insert_data.append((
                    item.sec_code,
                    item.sec_name,
                    item.market,
                    item.industry,
                    item.listing_date
                ))
            
            self.client.execute('INSERT INTO stock_data.stock_company (sec_code, sec_name, market, industry, listing_date) VALUES', insert_data)
            return True
        except Exception as e:
            print(f"保存A股公司信息失败: {e}")
            return False
    
    def get_stock_prices(self, sec_code):
        """获取股票价格数据
        
        从stock_daily_price表中获取指定股票的K线数据。
        """
        if not self.client:
            return None
        
        try:
            # 确保使用stock_data数据库
            self.client.execute('USE stock_data')
            query = f"SELECT trade_date, open, high, low, close, volume, amount FROM stock_data.stock_daily_price WHERE sec_code = {sec_code} ORDER BY trade_date"
            result = self.client.execute(query)
            return result
        except Exception as e:
            print(f"获取股票价格数据失败: {e}")
            return None
    
    def get_stock_count(self):
        """获取股票数据数量
        
        返回stock_daily_price表中的数据行数。
        """
        if not self.client:
            return 0
        
        try:
            # 确保使用stock_data数据库
            self.client.execute('USE stock_data')
            result = self.client.execute('SELECT count(*) FROM stock_data.stock_daily_price')
            return result[0][0]
        except Exception as e:
            print(f"获取股票数据数量失败: {e}")
            return 0
    
    def get_stock_companies(self):
        """获取所有A股公司信息
        
        Returns:
            list: A股公司信息列表
        """
        if not self.client:
            return None
        
        try:
            # 确保使用stock_data数据库
            self.client.execute('USE stock_data')
            result = self.client.execute('SELECT sec_code, sec_name, market, industry, listing_date FROM stock_data.stock_company')
            # 转换为字典列表
            companies = []
            for row in result:
                companies.append({
                    'sec_code': row[0],
                    'sec_name': row[1],
                    'market': row[2],
                    'industry': row[3],
                    'listing_date': row[4]
                })
            return companies
        except Exception as e:
            print(f"获取A股公司信息失败: {e}")
            return None
    
    def get_stock_company_count(self):
        """获取A股公司数量
        
        Returns:
            int: 公司数量
        """
        if not self.client:
            return 0
        
        try:
            # 确保使用stock_data数据库
            self.client.execute('USE stock_data')
            result = self.client.execute('SELECT count(*) FROM stock_data.stock_company')
            return result[0][0]
        except Exception as e:
            print(f"获取A股公司数量失败: {e}")
            return 0
