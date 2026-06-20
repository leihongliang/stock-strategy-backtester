import akshare as ak
import pandas as pd
from datetime import datetime, date, timedelta

from app.models.trade_calendar import TradeCalendar
from app.repositories.mongodb import MongoDBRepository
from app.config.settings import settings
from app.utils.log import logger


class TradeCalendarService:
    """交易日历服务
    
    处理交易日历数据的获取、同步和查询。
    """
    
    def __init__(self):
        self.repo = MongoDBRepository(
            host=settings.MONGODB_HOST,
            port=settings.MONGODB_PORT,
            database=settings.MONGODB_DATABASE
        )
    
    def get_trade_calendar(self) -> list[dict]:
        """从数据库获取交易日历数据"""
        try:
            return self.repo.get_trade_calendar() or []
        except Exception as e:
            logger.error(f"获取交易日历数据失败: {e}")
            return []
    
    def get_latest_trade_date(self) -> date | None:
        """获取数据库中最新的交易日期"""
        try:
            return self.repo.get_latest_trade_date()
        except Exception as e:
            logger.error(f"获取最新交易日期失败: {e}")
            return None
    
    def get_earliest_trade_date(self) -> date | None:
        """获取数据库中最早的交易日期"""
        try:
            return self.repo.get_earliest_trade_date()
        except Exception as e:
            logger.error(f"获取最早交易日期失败: {e}")
            return None
    
    def get_trading_days(self, start_date: str, end_date: str) -> list[date]:
        """获取指定日期范围内的交易日列表
        
        Args:
            start_date: 开始日期，格式为"YYYYMMDD"
            end_date: 结束日期，格式为"YYYYMMDD"
            
        Returns:
            list[date]: 交易日列表
        """
        trade_calendar_data = self.get_trade_calendar()
        if not trade_calendar_data:
            logger.error("获取交易日历数据失败，无法确定交易日")
            return []
        
        try:
            start_dt = datetime.strptime(start_date, "%Y%m%d")
            end_dt = datetime.strptime(end_date, "%Y%m%d")
            start_date_obj = start_dt.date()
            end_date_obj = end_dt.date()
        except Exception as e:
            logger.error(f"日期格式错误: {e}")
            return []
        
        trading_days = []
        for item in trade_calendar_data:
            trade_date = item['trade_date']
            if hasattr(trade_date, 'date'):
                trade_date = trade_date.date()
            elif isinstance(trade_date, str):
                trade_date = date.fromisoformat(trade_date)
            
            if (trade_date >= start_date_obj and 
                trade_date <= end_date_obj and 
                item['is_trading_day']):
                trading_days.append(trade_date)
        
        trading_days.sort()
        return trading_days
    
    def sync_trade_calendar(self, start_date: date | None = None, end_date: date | None = None) -> None:
        """同步交易日历数据到数据库
        
        从AkShare获取A股交易日历数据，并保存到数据库。
        只同步数据库中不存在的新数据。
        
        Args:
            start_date (date, optional): 开始日期. Defaults to None (使用当天日期).
            end_date (date, optional): 结束日期. Defaults to None (使用当年年末).
        """
        try:
            logger.info("开始从AkShare获取交易日历数据")
            
            latest_db_date = self.get_latest_trade_date()
            earliest_db_date = self.get_earliest_trade_date()
            
            if start_date is None:
                start_date = date.today()
            if end_date is None:
                end_date = date(date.today().year, 12, 31)
            
            if earliest_db_date and latest_db_date:
                logger.info(f"数据库中最早的交易日日期: {earliest_db_date}")
                logger.info(f"数据库中最新的交易日日期: {latest_db_date}")
                
                if start_date >= earliest_db_date and end_date <= latest_db_date:
                    logger.info("数据库中已有指定范围内的完整数据，无需同步")
                    return
                elif end_date > latest_db_date:
                    logger.info("数据库中有部分数据，从最新日期的下一天开始同步")
                    start_date = latest_db_date + timedelta(days=1)
                    if start_date > end_date:
                        logger.info("数据库数据已经是最新的，无需同步")
                        return
            else:
                logger.info("数据库中没有交易日历数据，将同步指定范围的完整数据")
            
            trade_calendar_list = []
            
            tool_trade_date_hist_sina_df = ak.tool_trade_date_hist_sina()
            logger.info(f"从AkShare获取完整交易日历数据成功，共 {len(tool_trade_date_hist_sina_df)} 条记录")
            
            tool_trade_date_hist_sina_df['trade_date'] = pd.to_datetime(tool_trade_date_hist_sina_df['trade_date']).dt.date
            
            new_data_df = tool_trade_date_hist_sina_df[
                (tool_trade_date_hist_sina_df['trade_date'] >= start_date) & 
                (tool_trade_date_hist_sina_df['trade_date'] <= end_date)
            ]
            logger.info(f"需要同步的新数据: {len(new_data_df)} 条记录")
            
            for _, row in new_data_df.iterrows():
                trade_date = row['trade_date']
                is_trading_day = bool(row['is_trading_day']) if 'is_trading_day' in row else True
                
                trade_calendar = TradeCalendar(
                    trade_date=trade_date,
                    is_trading_day=is_trading_day
                )
                trade_calendar_list.append(trade_calendar)

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
