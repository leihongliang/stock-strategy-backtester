import akshare as ak
import pandas as pd
from datetime import datetime, date

from app.models.company import StockCompany, Market, SecType
from app.repositories.mongodb import MongoDBRepository
from app.config.settings import settings
from app.utils.log import logger


class StockCompanyService:
    """股票/基金公司信息服务
    
    处理股票和基金公司信息的获取、保存和查询。
    """
    
    def __init__(self):
        self.repo = MongoDBRepository(
            host=settings.MONGODB_HOST,
            port=settings.MONGODB_PORT,
            database=settings.MONGODB_DATABASE
        )
    
    def get_all_a_stocks(self, sec_type: SecType = SecType.STOCK) -> list[StockCompany]:
        """从AkShare获取证券列表
        
        Args:
            sec_type: 证券类型筛选，可选值: STOCK(股票), FUND(基金), INDEX(指数)
        """
        if sec_type == SecType.FUND:
            return self.get_all_funds()
        elif sec_type == SecType.INDEX:
            return self.get_all_indices()
        
        company_list = []
        
        # 处理上海股票
        try:
            stock_info_sh = ak.stock_info_sh_name_code()
            for _, row in stock_info_sh.iterrows():
                sec_code = str(row.get('证券代码', ''))
                if not sec_code:
                    continue
                sec_code = sec_code.zfill(6)
                
                sec_name = row.get('证券简称', '')
                market = 'SH'
                industry = row.get('所属行业', '')
                
                listing_date_value = row.get('上市日期')
                listing_date = pd.to_datetime(listing_date_value).date()
                
                company = StockCompany(
                    sec_code=sec_code,
                    sec_name=sec_name,
                    market=Market.SH,
                    industry=industry,
                    listing_date=listing_date,
                    sec_type=SecType.STOCK
                )
                company_list.append(company)
        except Exception as e:
            logger.error(f"处理上海股票数据失败: {e}")
        
        # 处理深圳股票
        try:
            stock_info_sz = ak.stock_info_sz_name_code()
            for _, row in stock_info_sz.iterrows():
                sec_code = str(row.get('A股代码', ''))
                if not sec_code:
                    continue
                sec_code = sec_code.zfill(6)
                
                sec_name = row.get('A股简称', '')
                industry = row.get('所属行业', '')
                
                listing_date_value = row.get('A股上市日期', '')
                listing_date = pd.to_datetime(listing_date_value).date()
                
                company = StockCompany(
                    sec_code=sec_code,
                    sec_name=sec_name,
                    market=Market.SZ,
                    industry=industry,
                    listing_date=listing_date,
                    sec_type=SecType.STOCK
                )
                company_list.append(company)
        except Exception as e:
            logger.error(f"处理深圳股票数据失败: {e}")
        
        return company_list
    
    def get_all_funds(self) -> list[StockCompany]:
        """从AkShare获取所有基金列表"""
        company_list = []
        
        try:
            fund_info = ak.fund_name_em()
            logger.info(f"从AkShare获取基金列表成功，共 {len(fund_info)} 条记录")
            
            for _, row in fund_info.iterrows():
                sec_code = str(row.get('基金代码', ''))
                if not sec_code:
                    continue
                sec_code = sec_code.zfill(6)
                
                sec_name = row.get('基金简称', '')
                fund_type = row.get('基金类型', '')
                
                # 根据基金代码前缀判断市场
                if sec_code.startswith(('50', '51', '58')):
                    market = Market.SH
                else:
                    market = Market.SZ
                
                company = StockCompany(
                    sec_code=sec_code,
                    sec_name=sec_name,
                    market=market,
                    industry=fund_type,
                    sec_type=SecType.FUND
                )
                company_list.append(company)
        except Exception as e:
            logger.error(f"获取基金列表失败: {e}")
        
        return company_list
    
    def get_all_indices(self) -> list[StockCompany]:
        """从AkShare获取所有指数列表"""
        company_list = []
        
        try:
            index_info = ak.index_stock_info()
            logger.info(f"从AkShare获取指数列表成功，共 {len(index_info)} 条记录")
            
            for _, row in index_info.iterrows():
                sec_code = str(row.get('index_code', ''))
                if not sec_code:
                    continue
                sec_code = sec_code.zfill(6)
                
                sec_name = row.get('display_name', '')
                publish_date = row.get('publish_date')
                
                if publish_date:
                    listing_date = pd.to_datetime(publish_date).date()
                else:
                    listing_date = None
                
                # 根据指数代码前缀判断市场
                if sec_code.startswith(('000', '880', '9')):
                    market = Market.SH
                else:
                    market = Market.SZ
                
                company = StockCompany(
                    sec_code=sec_code,
                    sec_name=sec_name,
                    market=market,
                    industry='',
                    listing_date=listing_date,
                    sec_type=SecType.INDEX
                )
                company_list.append(company)
        except Exception as e:
            logger.error(f"获取指数列表失败: {e}")
        
        return company_list
    
    def get_stock_info_by_code(self, stock_code: str) -> StockCompany | None:
        """根据代码获取单只股票信息"""
        try:
            stock_code = stock_code.zfill(6)
            all_a_stocks = ak.stock_info_a_code_name()
            
            for _, row in all_a_stocks.iterrows():
                if str(row['code']).zfill(6) == stock_code:
                    name = row['name']
                    
                    if stock_code.startswith(('60', '68', '90')):
                        market = Market.SH
                    else:
                        market = Market.SZ
                    
                    return StockCompany(
                        sec_code=stock_code,
                        sec_name=name,
                        market=market,
                        industry='',
                        sec_type=SecType.STOCK
                    )
        except Exception as e:
            logger.error(f"获取股票 {stock_code} 信息失败: {e}")
        
        return None
    
    def get_fund_info_by_code(self, fund_code: str) -> StockCompany | None:
        """根据代码获取单只基金信息"""
        try:
            fund_code = fund_code.zfill(6)
            fund_info = ak.fund_name_em()
            
            for _, row in fund_info.iterrows():
                if str(row.get('基金代码', '')).zfill(6) == fund_code:
                    sec_name = row.get('基金简称', '')
                    fund_type = row.get('基金类型', '')
                    
                    if fund_code.startswith(('50', '51', '58')):
                        market = Market.SH
                    else:
                        market = Market.SZ
                    
                    return StockCompany(
                        sec_code=fund_code,
                        sec_name=sec_name,
                        market=market,
                        industry=fund_type,
                        sec_type=SecType.FUND
                    )
        except Exception as e:
            logger.error(f"获取基金 {fund_code} 信息失败: {e}")
        
        return None
    
    def get_index_info_by_code(self, index_code: str) -> StockCompany | None:
        """根据代码获取单只指数信息"""
        try:
            index_code = index_code.zfill(6)
            index_info = ak.index_stock_info()
            
            for _, row in index_info.iterrows():
                if str(row.get('index_code', '')).zfill(6) == index_code:
                    sec_name = row.get('display_name', '')
                    publish_date = row.get('publish_date')
                    
                    if publish_date:
                        listing_date = pd.to_datetime(publish_date).date()
                    else:
                        listing_date = None
                    
                    if index_code.startswith(('000', '880', '9')):
                        market = Market.SH
                    else:
                        market = Market.SZ
                    
                    return StockCompany(
                        sec_code=index_code,
                        sec_name=sec_name,
                        market=market,
                        industry='',
                        listing_date=listing_date,
                        sec_type=SecType.INDEX
                    )
        except Exception as e:
            logger.error(f"获取指数 {index_code} 信息失败: {e}")
        
        return None
    
    def get_stock_companies_from_db(self) -> list[dict]:
        """从数据库获取所有股票/基金公司信息"""
        try:
            return self.repo.get_stock_companies()
        except Exception as e:
            logger.error(f"从数据库获取公司信息失败: {e}")
            return []
    
    def get_stock_company_by_code(self, stock_code: str) -> dict | None:
        """根据股票代码从数据库获取公司信息"""
        try:
            return self.repo.get_stock_company_by_code(stock_code)
        except Exception as e:
            logger.error(f"根据股票代码获取公司信息失败: {e}")
            return None
    
    def save_stock_companies(self, companies: list[StockCompany]) -> bool:
        """保存公司信息列表到数据库"""
        if not companies:
            return False
        
        try:
            success = self.repo.save_stock_companies(companies)
            if success:
                logger.info(f"成功保存 {len(companies)} 家公司信息")
            return success
        except Exception as e:
            logger.error(f"保存公司信息失败: {e}")
            return False
    
    def save_all_stock_companies(self) -> bool:
        """全量更新A股公司信息到数据库
        
        从数据库读取所有公司信息，然后进行全量更新。
        如果数据库中没有数据，则从外部数据源获取并保存。
        """
        all_stocks = self.get_stock_companies_from_db()
        logger.info(f"从数据库获取到 {len(all_stocks)} 家公司信息")
        
        if all_stocks:
            stock_company_objects = []
            for stock in all_stocks:
                if isinstance(stock, dict):
                    listing_date = stock.get('listing_date')
                    if isinstance(listing_date, str):
                        listing_date = pd.to_datetime(listing_date).date()
                    elif isinstance(listing_date, datetime):
                        listing_date = listing_date.date()
                    
                    market_val = stock.get('market', 'SZ')
                    if isinstance(market_val, str):
                        market_val = Market(market_val)
                    
                    sec_type_val = stock.get('sec_type', 'stock')
                    if isinstance(sec_type_val, str):
                        sec_type_val = SecType(sec_type_val)
                    
                    stock_company = StockCompany(
                        sec_code=str(stock.get('sec_code')),
                        sec_name=stock.get('sec_name'),
                        market=market_val,
                        industry=stock.get('industry'),
                        listing_date=listing_date,
                        sec_type=sec_type_val
                    )
                    stock_company_objects.append(stock_company)
            
            if stock_company_objects:
                return self.save_stock_companies(stock_company_objects)
            else:
                logger.error("没有有效的公司信息可以更新")
                return False
        else:
            logger.info("数据库中暂无公司信息，从外部数据源获取")
            all_stocks = self.get_all_a_stocks()
            logger.info(f"从外部数据源获取到 {len(all_stocks)} 只A股股票")
            
            if all_stocks:
                return self.save_stock_companies(all_stocks)
            else:
                logger.error("从外部数据源获取A股公司信息失败")
                return False
    
    def ensure_stock_exists(self, stock_code: str) -> StockCompany | None:
        """确保证券代码存在于数据库中，不存在则从API获取并保存"""
        stock_code = stock_code.zfill(6)
        
        # 先从数据库查询
        existing = self.get_stock_company_by_code(stock_code)
        if existing:
            return existing
        
        # 数据库中不存在，依次尝试获取股票、基金、指数信息
        logger.info(f"证券 {stock_code} 不在数据库中，尝试从API获取")
        
        company = self.get_stock_info_by_code(stock_code)
        if not company:
            company = self.get_fund_info_by_code(stock_code)
        if not company:
            company = self.get_index_info_by_code(stock_code)
        
        if company:
            success = self.save_stock_companies([company])
            if success:
                logger.info(f"成功保存新证券 {stock_code} ({company.sec_name}) 到数据库")
                return company
        
        return None
    
    def ensure_stocks_exist(self, stock_codes: list[str]) -> list[StockCompany]:
        """确保多个证券代码存在于数据库中"""
        result = []
        missing_codes = []
        
        # 先检查数据库中已有的
        for code in stock_codes:
            code = code.zfill(6)
            existing = self.get_stock_company_by_code(code)
            if existing:
                result.append(existing)
            else:
                missing_codes.append(code)
        
        if not missing_codes:
            return result
        
        # 批量获取缺失的股票信息
        new_companies = []
        still_missing = []
        
        try:
            all_a_stocks = ak.stock_info_a_code_name()
            stock_map = {}
            for _, row in all_a_stocks.iterrows():
                stock_map[str(row['code']).zfill(6)] = row['name']
            
            for code in missing_codes:
                name = stock_map.get(code, '')
                if name:
                    if code.startswith(('60', '68', '90')):
                        market = Market.SH
                    else:
                        market = Market.SZ
                    
                    company = StockCompany(
                        sec_code=code,
                        sec_name=name,
                        market=market,
                        industry='',
                        sec_type=SecType.STOCK
                    )
                    new_companies.append(company)
                    result.append(company)
                else:
                    still_missing.append(code)
        except Exception as e:
            logger.error(f"从API获取股票信息失败: {e}")
            still_missing = missing_codes
        
        # 对仍未找到的代码，尝试获取基金信息
        still_missing_after_fund = []
        if still_missing:
            try:
                fund_info = ak.fund_name_em()
                fund_map = {}
                for _, row in fund_info.iterrows():
                    fund_map[str(row.get('基金代码', '')).zfill(6)] = {
                        'name': row.get('基金简称', ''),
                        'type': row.get('基金类型', '')
                    }
                
                for code in still_missing:
                    info = fund_map.get(code)
                    if info:
                        if code.startswith(('50', '51', '58')):
                            market = Market.SH
                        else:
                            market = Market.SZ
                        
                        company = StockCompany(
                            sec_code=code,
                            sec_name=info['name'],
                            market=market,
                            industry=info['type'],
                            sec_type=SecType.FUND
                        )
                        new_companies.append(company)
                        result.append(company)
                    else:
                        still_missing_after_fund.append(code)
            except Exception as e:
                logger.error(f"从API获取基金信息失败: {e}")
                still_missing_after_fund = still_missing
        
        # 对仍未找到的代码，尝试获取指数信息
        if still_missing_after_fund:
            try:
                index_info = ak.index_stock_info()
                index_map = {}
                for _, row in index_info.iterrows():
                    index_map[str(row.get('index_code', '')).zfill(6)] = {
                        'name': row.get('display_name', ''),
                        'date': row.get('publish_date')
                    }
                
                for code in still_missing_after_fund:
                    info = index_map.get(code)
                    if info:
                        if code.startswith(('000', '880', '9')):
                            market = Market.SH
                        else:
                            market = Market.SZ
                        
                        listing_date = None
                        if info['date']:
                            listing_date = pd.to_datetime(info['date']).date()
                        
                        company = StockCompany(
                            sec_code=code,
                            sec_name=info['name'],
                            market=market,
                            industry='',
                            listing_date=listing_date,
                            sec_type=SecType.INDEX
                        )
                        new_companies.append(company)
                        result.append(company)
                    else:
                        logger.warning(f"未找到代码 {code} 对应的股票、基金或指数信息")
            except Exception as e:
                logger.error(f"从API获取指数信息失败: {e}")
        
        # 保存所有新获取的公司信息
        if new_companies:
            self.save_stock_companies(new_companies)
        
        return result
