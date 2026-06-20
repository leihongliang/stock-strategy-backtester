from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import pandas as pd

from app.utils.log import logger
from app.services.stock_company_service import StockCompanyService

# 策略注册表
STRATEGIES = {}

stock_company_service = StockCompanyService()


def _get_stock_service():
    """延迟导入避免循环依赖"""
    from app.services.stock_service import StockService
    return StockService()


stock_service = None


def _ensure_stock_service():
    global stock_service
    if stock_service is None:
        stock_service = _get_stock_service()
    return stock_service

def register_strategy(strategy_name):
    """注册策略函数的装饰器"""
    def decorator(func):
        STRATEGIES[strategy_name] = func
        return func
    return decorator

def process_stock(stock_code: str, stock_service, strategy_func, start_date, end_date) -> dict:
    """处理单个股票的策略验证
    
    Args:
        stock: 股票代码（字符串）
        stock_service: StockService实例
        strategy_func: 策略函数
        start_date: 开始日期
        end_date: 结束日期
        
    Returns:
        dict: 处理结果，包含匹配模式、总案例数和成功案例数
    """
    # 从数据库获取股票名称和市场信息
    stock_name = ""
    market = ""
    # 尝试从数据库获取股票公司信息
    company = stock_company_service.get_stock_company_by_code(stock_code)
    if company:
        stock_name = company.get('sec_name', '')
        market = company.get('market', '')
    
    # 只处理主板股票：沪市600/601/603/605开头，深市000开头
    stock_code_str = str(stock_code)
    valid_prefixes = ('600', '601', '603', '605', '000')
    if not any(stock_code_str.startswith(p) for p in valid_prefixes):
        logger.debug(f"跳过非主板股票: {stock_code} - {stock_name}")
        return {"matches": [], "total_cases": 0, "successful_cases": 0}
    
    logger.debug(f"处理股票: {stock_code} - {stock_name}")
    
    # Load data from database匹配时间段
    k_data = _ensure_stock_service().load_stock_data(stock_code)
    
    if k_data is not None:
        logger.debug(f"成功加载 {stock_code} 的历史数据，共 {len(k_data)} 条记录")
        
        # Filter data within time range
        if start_date and end_date:
            start_date_dt = pd.to_datetime(start_date)
            end_date_dt = pd.to_datetime(end_date)
            filtered_data = k_data[(k_data['date'] >= start_date_dt) & (k_data['date'] <= end_date_dt)]
            logger.debug(f"时间范围筛选后，剩余 {len(filtered_data)} 条记录")
        else:
            filtered_data = k_data
        
        # Apply strategy
        logger.debug(f"应用策略到 {stock_code}")
        strategy_result = strategy_func(filtered_data, stock_code, stock_name, market)
        
        # Log results
        if strategy_result:
            match_count = len(strategy_result['matches'])
            if match_count > 0:
                # logger.info(f"股票 {stock_code} - {stock_name} 找到 {match_count} 个匹配模式")
                for match in strategy_result['matches']:
                    period = match.get('period', match.get('golden_cross_date', '未知时间段'))
                    # 计算涨幅信息
                    buy_day_open = match.get('abnormal_up_day_open', match.get('golden_cross_price', 0))
                    # 尝试从数据中计算5日、10日、20日涨幅
                    five_day_increase = match.get('5_day_increase', 'N/A')
                    ten_day_increase = match.get('10_day_increase', 'N/A')
                    twenty_day_increase = match.get('20_day_increase', 'N/A')
                    
                    # 格式化涨幅值，保留两位小数
                    def format_increase(value):
                        if isinstance(value, (int, float)):
                            return f"{value:.2f}%"
                        return "N/A"
                    
                    # 打印详细日志（一行）
                    logger.info(
                        f"  - 股票代码: {stock_code}, 股票名称: {stock_name}, "
                        f"匹配时间段: {period}, "
                        f"异动阳线后第四天开盘买入价格: {buy_day_open}, "
                        f"5日后涨幅: {format_increase(five_day_increase)}, "
                        f"10日后涨幅: {format_increase(ten_day_increase)}, "
                        f"20日涨幅: {format_increase(twenty_day_increase)}"
                    )
        
        return strategy_result
    else:
        logger.warning(f"无法加载股票 {stock_code} 的历史数据")
        return None

def validate_strategy(strategy_name: str, start_date: str | None = None, end_date: str | None = None) -> dict:
    """基于历史数据验证股票策略
    
    参数:
        strategy_name: 策略名称
        start_date: 开始日期，格式为"YYYY-MM-DD"
        end_date: 结束日期，格式为"YYYY-MM-DD"
        
    返回:
        dict: 包含匹配股票和策略准确率的结果
    """
    # 检查策略是否存在
    if strategy_name not in STRATEGIES:
        raise ValueError(f"策略 '{strategy_name}' 未注册")
    
    logger.info(f"开始验证策略: {strategy_name}")
    logger.info(f"验证时间范围: {start_date} 至 {end_date}")
    
    all_stocks = _ensure_stock_service().get_all_a_stocks_from_db()
    logger.info(f"获取到 {len(all_stocks)} 只A股股票")
    
    matching_stocks = []
    total_cases = 0
    successful_cases = 0
    
    # 获取策略函数
    strategy_func = STRATEGIES[strategy_name]
    logger.info(f"使用策略函数: {strategy_func.__name__}")
    
    # 使用ThreadPoolExecutor进行并行处理
    max_workers = 200  # 可根据系统资源调整
    logger.info(f"使用 {max_workers} 个线程进行并行处理")
    
    processed_count = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务
        futures = {
            executor.submit(
                process_stock, stock, strategy_func, start_date, end_date
            ): stock for stock in all_stocks
        }
        
        # 处理完成的任务
        for future in as_completed(futures):
            result = future.result()
            processed_count += 1
            
            if result:
                matching_stocks.extend(result['matches'])
                total_cases += result['total_cases']
                successful_cases += result['successful_cases']
            
            # 每处理100只股票打印一次进度
            # if processed_count % 100 == 0:
            #     logger.info(f"已处理 {processed_count} 只股票，找到 {len(matching_stocks)} 个匹配模式")
    
    # 计算准确率
    accuracy = (successful_cases / total_cases * 100) if total_cases > 0 else 0
    logger.info(f"策略验证完成")
    logger.info(f"总案例数: {total_cases}")
    logger.info(f"成功案例数: {successful_cases}")
    logger.info(f"策略准确率: {accuracy:.2f}%")
    logger.info(f"找到 {len(matching_stocks)} 个匹配模式")
    
    return {
        'matching_stocks': matching_stocks,
        'total_cases': total_cases,
        'successful_cases': successful_cases,
        'accuracy': accuracy
    }

def validate_513_strategy(
    start_date: str | None = None,
    end_date: str | None = None,
    consecutive_days: int = 4,
    verification_days: int = 3,
    stock_codes: list[str] | None = None
) -> dict:
    """验证513战法（可自定义连续上涨天数和后续验证天数）
    
    参数:
        start_date: 开始日期，格式为"YYYY-MM-DD"
        end_date: 结束日期，格式为"YYYY-MM-DD"
        consecutive_days: 连续上涨天数，默认4天
        verification_days: 后续验证天数，默认3天
        stock_codes: 股票代码列表，为None时验证所有股票
        
    返回:
        dict: 包含匹配股票和策略准确率的结果
    """
    logger.info(f"开始验证513战法")
    logger.info(f"验证时间范围: {start_date} 至 {end_date}")
    logger.info(f"连续上涨天数: {consecutive_days}")
    logger.info(f"后续验证天数: {verification_days}")

    if stock_codes is None:
        stocks = _ensure_stock_service().get_all_a_stocks_from_db()
        logger.info(f"获取到 {len(stocks)} 只A股股票")
    else:
        stocks = stock_codes
    
    matching_stocks = []
    total_cases = 0
    successful_cases = 0
    
    # 使用ThreadPoolExecutor进行并行处理
    max_workers = 400  # 可根据系统资源调整
    logger.info(f"使用 {max_workers} 个线程进行并行处理")
    
    processed_count = 0
    
    def process_stock_with_params(stock_code: str) -> Optional[dict]:
        """处理单个股票的513策略验证（带参数）"""
        try:
            # 从数据库获取股票名称和市场信息
            stock_name = ""
            market = ""
            # 尝试从数据库获取股票公司信息
            company = stock_company_service.get_stock_company_by_code(stock_code)
            if company:
                stock_name = company.get('sec_name', '')
                market = company.get('market', '')
            
            # 只处理主板股票：沪市600/601/603/605开头，深市000开头
            stock_code_str = str(stock_code)
            valid_prefixes = ('600', '601', '603', '605', '000')
            if not any(stock_code_str.startswith(p) for p in valid_prefixes):
                logger.debug(f"跳过非主板股票: {stock_code} - {stock_name}")
                return None
            
            logger.debug(f"处理股票: {stock_code} - {stock_name}")
            
            # Load data from database
            k_data = _ensure_stock_service().load_stock_data(stock_code)
            
            if k_data is not None:
                logger.debug(f"成功加载 {stock_code} 的历史数据，共 {len(k_data)} 条记录")
                
                # Filter data within time range
                if start_date and end_date:
                    start_date_dt = pd.to_datetime(start_date)
                    end_date_dt = pd.to_datetime(end_date)
                    filtered_data = k_data[(k_data['date'] >= start_date_dt) & (k_data['date'] <= end_date_dt)]
                    logger.debug(f"时间范围筛选后，剩余 {len(filtered_data)} 条记录")
                else:
                    filtered_data = k_data
                
                # Apply strategy with custom parameters
                logger.debug(f"应用513策略到 {stock_code}")
                strategy_result = strategy_513(filtered_data, stock_code, stock_name, market, consecutive_days, verification_days)
                
                # Log results
                if strategy_result:
                    match_count = len(strategy_result['matches'])
                    if match_count > 0:
                        for match in strategy_result['matches']:
                            period = match.get('period', '未知时间段')
                            abnormal_up_day_date = match.get('abnormal_up_day_date', '未知日期')
                            # 计算涨幅信息
                            buy_day_open = match.get('buy_day_open', 0)
                            # 尝试从数据中计算5日、10日、20日涨幅
                            five_day_increase = match.get('5_day_increase', 'N/A')
                            ten_day_increase = match.get('10_day_increase', 'N/A')
                            twenty_day_increase = match.get('20_day_increase', 'N/A')
                            
                            # 格式化涨幅值，保留两位小数
                            def format_increase(value):
                                if isinstance(value, (int, float)):
                                    return f"{value:.2f}%"
                                return "N/A"
                            
                            # 打印详细日志（一行）
                            logger.info(
                                f"  - 股票代码: {stock_code}, 股票名称: {stock_name}, "
                                f"异动日: {abnormal_up_day_date}, "
                                f"买入价格: {buy_day_open}, "
                                f"5日后涨幅: {format_increase(five_day_increase)}, "
                                f"10日后涨幅: {format_increase(ten_day_increase)}, "
                                f"20日涨幅: {format_increase(twenty_day_increase)}"
                            )
                
                return strategy_result
            else:
                logger.warning(f"无法加载股票 {stock_code} 的历史数据")
                return None
        except Exception as e:
            logger.error(f"处理股票 {stock_code} 时发生错误: {type(e).__name__}: {str(e)}")
            import traceback
            logger.error(f"错误堆栈: {traceback.format_exc()}")
            return None
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务
        futures = {executor.submit(process_stock_with_params, stock): stock for stock in stocks}
        
        # 处理完成的任务
        for future in as_completed(futures):
            result = future.result()
            processed_count += 1
            
            if result:
                matching_stocks.extend(result['matches'])
                total_cases += result['total_cases']
                successful_cases += result['successful_cases']
            
            # 每处理100只股票打印一次进度
            # if processed_count % 100 == 0:
            #     logger.info(f"已处理 {processed_count} 只股票，找到 {len(matching_stocks)} 个匹配模式")
    
    # 计算准确率
    accuracy = (successful_cases / total_cases * 100) if total_cases > 0 else 0
    logger.info(f"513战法验证完成")
    logger.info(f"总案例数: {total_cases}")
    logger.info(f"成功案例数: {successful_cases}")
    logger.info(f"策略准确率: {accuracy:.2f}%")
    logger.info(f"找到 {len(matching_stocks)} 个匹配模式")
    
    return {
        'matching_stocks': matching_stocks,
        'total_cases': total_cases,
        'successful_cases': successful_cases,
        'accuracy': accuracy,
        'consecutive_days': consecutive_days,
        'verification_days': verification_days
    }

@register_strategy('strategy1')
def strategy1(filtered_data, stock_code, stock_name, market):
    """策略1实现: 4连阳 + 1根异常放量阳线 + 3天验证"""
    matches = []
    total_cases = 0
    successful_cases = 0
    
    # 检查是否有足够的数据
    if len(filtered_data) >= 8:  # 至少需要4连阳 + 1根异常放量阳线 + 3天验证
        # 遍历所有可能的连续组合
        for i in range(len(filtered_data) - 7):  # 4连阳 + 1根异常放量阳线 + 3天验证
            # 检查连续阳线条件（至少4天，允许一根小阴线）
            consecutive_up_days = 0
            has_small_red = False
            valid_up_trend = True
            
            for j in range(i, i + 4):
                if j >= len(filtered_data):
                    valid_up_trend = False
                    break
                
                # 计算每日涨跌
                if filtered_data.iloc[j]['close'] > filtered_data.iloc[j]['open']:
                    consecutive_up_days += 1
                else:
                    close = filtered_data.iloc[j]['close']
                    open_price = filtered_data.iloc[j]['open']
                    pre_close = filtered_data.iloc[j-1]['close'] if j > i else open_price
                    
                    # 小阴线定义: 跌幅不超过1%，且收盘价仍高于前一天收盘价
                    if (open_price - close) / open_price <= 0.01 and close > pre_close:
                        has_small_red = True
                    else:
                        valid_up_trend = False
                        break
            
            if valid_up_trend and (consecutive_up_days >= 4 or (consecutive_up_days >= 3 and has_small_red)):
                # 检查第5天是否为放量阳线
                abnormal_up_day_index = i + 4
                if abnormal_up_day_index < len(filtered_data):
                    abnormal_up_day = filtered_data.iloc[abnormal_up_day_index]
                    previous_day = filtered_data.iloc[abnormal_up_day_index - 1]
                    
                    # 检查是否为大阳线（涨幅≥3%）
                    if (abnormal_up_day['close'] - abnormal_up_day['open']) / abnormal_up_day['open'] >= 0.03:
                        if abnormal_up_day['high'] > abnormal_up_day['close'] and (abnormal_up_day['high'] - abnormal_up_day['close']) / abnormal_up_day['open'] >= 0.01:
                            if abnormal_up_day['volume'] >= previous_day['volume'] * 2:
                                # 验证接下来的3天
                                valid_verification = True
                                abnormal_up_day_open = abnormal_up_day['open']
                                
                                for j in range(1, 4):
                                    verification_index = abnormal_up_day_index + j
                                    if verification_index >= len(filtered_data):
                                        valid_verification = False
                                        break
                                    
                                    # 检查是否跌破异常放量阳线的开盘价
                                if filtered_data.iloc[verification_index]['low'] < abnormal_up_day_open:
                                        valid_verification = False
                                        break
                                
                                # 计算3天涨幅
                                if abnormal_up_day_index + 3 < len(filtered_data):
                                    verification_end_day = filtered_data.iloc[abnormal_up_day_index + 3]
                                    increase = (verification_end_day['close'] - abnormal_up_day['close']) / abnormal_up_day['close'] * 100
                                else:
                                    increase = None
                                    valid_verification = False
                                
                                # 计算5日、10日、20日涨幅
                                five_day_increase = None
                                ten_day_increase = None
                                twenty_day_increase = None
                                
                                # 计算5日涨幅（从异动阳线后第四天开盘买入，即异动阳线后第3天）
                                if abnormal_up_day_index + 4 < len(filtered_data):
                                    five_day_end = filtered_data.iloc[abnormal_up_day_index + 4]
                                    five_day_increase = (five_day_end['close'] - abnormal_up_day['open']) / abnormal_up_day['open'] * 100
                                
                                # 计算10日涨幅
                                if abnormal_up_day_index + 9 < len(filtered_data):
                                    ten_day_end = filtered_data.iloc[abnormal_up_day_index + 9]
                                    ten_day_increase = (ten_day_end['close'] - abnormal_up_day['open']) / abnormal_up_day['open'] * 100
                                
                                # 计算20日涨幅
                                if abnormal_up_day_index + 19 < len(filtered_data):
                                    twenty_day_end = filtered_data.iloc[abnormal_up_day_index + 19]
                                    twenty_day_increase = (twenty_day_end['close'] - abnormal_up_day['open']) / abnormal_up_day['open'] * 100
                                
                                # 记录结果
                                total_cases += 1
                                if valid_verification:
                                    successful_cases += 1
                                
                                # 添加到匹配列表
                                match_start_date = filtered_data.iloc[i]['date'].strftime('%Y-%m-%d')
                                match_end_date = abnormal_up_day['date'].strftime('%Y-%m-%d')
                                
                                matches.append({
                                    'code': stock_code,
                                    'name': stock_name,
                                    'market': market,
                                    'period': f"{match_start_date} 至 {match_end_date}",
                                    'abnormal_up_day_date': abnormal_up_day['date'].strftime('%Y-%m-%d'),
                                    'abnormal_up_day_open': abnormal_up_day['open'],
                                    'abnormal_up_day_close': abnormal_up_day['close'],
                                    '3_day_verification_result': '成功' if valid_verification else '失败',
                                    '3_day_increase': increase,
                                    '5_day_increase': five_day_increase,
                                    '10_day_increase': ten_day_increase,
                                    '20_day_increase': twenty_day_increase
                                })
    
    return {
        'matches': matches,
        'total_cases': total_cases,
        'successful_cases': successful_cases
    }



ALL_STRATEGIES = ['ma5_ma20_cross', 'ma5_ma20_cross_ma60', 'ma5_ma20_cross_ma120',
                  'price_breakout_20w_10w', 'buy_and_hold',
                  'macd_cross', 'macd_cross_ma60', 'macd_cross_ma120']

def multi_strategy_backtest(
    stock_codes: list[str],
    start_date: str,
    end_date: str,
    strategies: list[str] | None = None
) -> dict:
    """多策略回测
    
    对指定股票列表和时间段运行多个策略，分析每个策略结束后的收益。
    
    Args:
        stock_codes: 股票代码列表
        start_date: 开始日期，格式为"YYYY-MM-DD"
        end_date: 结束日期，格式为"YYYY-MM-DD"
        strategies: 策略名称列表，为None时运行所有预设策略
        
    Returns:
        dict: 包含每个股票的回测结果，按股票分组
    """
    if strategies is None:
        strategies = ALL_STRATEGIES
    
    logger.info(f"开始多策略回测")
    logger.info(f"股票数量: {len(stock_codes)}")
    logger.info(f"时间范围: {start_date} 至 {end_date}")
    logger.info(f"策略列表: {strategies}")
    
    results = {}
    
    for strategy_name in strategies:
        if strategy_name not in STRATEGIES:
            logger.warning(f"策略 '{strategy_name}' 未注册，跳过")
            continue
        
        logger.info(f"正在回测策略: {strategy_name}")
        
        strategy_func = STRATEGIES[strategy_name]
        
        max_workers = 200
        
        def process_single_stock(stock_code):
            try:
                stock_name = ""
                market = ""
                company = stock_company_service.get_stock_company_by_code(stock_code)
                if company:
                    stock_name = company.get('sec_name', '')
                    market = company.get('market', '')
                
                k_data = _ensure_stock_service().load_stock_data(stock_code)
                
                if k_data is not None:
                    start_date_dt = pd.to_datetime(start_date)
                    end_date_dt = pd.to_datetime(end_date)
                    filtered_data = k_data[(k_data['date'] >= start_date_dt) & (k_data['date'] <= end_date_dt)]
                    
                    if len(filtered_data) > 0:
                        strategy_result = strategy_func(filtered_data, stock_code, stock_name, market)
                        return {
                            'stock_code': stock_code,
                            'stock_name': stock_name,
                            'market': market,
                            'strategy_name': strategy_name,
                            'strategy_result': strategy_result
                        }
                return None
            except Exception as e:
                logger.error(f"处理股票 {stock_code} 时发生错误: {str(e)}")
                return None
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(process_single_stock, stock): stock for stock in stock_codes}
            
            for future in as_completed(futures):
                result = future.result()
                if result:
                    stock_code = result['stock_code']
                    strategy_result = result['strategy_result']
                    
                    if stock_code not in results:
                        results[stock_code] = {
                            'stock_code': stock_code,
                            'stock_name': result['stock_name'],
                            'market': result['market'],
                            'strategies': {}
                        }
                    
                    if strategy_result is None:
                        continue
                    
                    total_cases = strategy_result.get('total_cases', 0)
                    successful_cases = strategy_result.get('successful_cases', 0)
                    win_rate = (successful_cases / total_cases * 100) if total_cases > 0 else 0
                    
                    all_matches = strategy_result.get('matches', [])
                    avg_profit = 0
                    total_profit = 0
                    if all_matches:
                        profit_sum = sum(m.get('profit_ratio', 0) for m in all_matches if isinstance(m.get('profit_ratio'), (int, float)))
                        total_profit = profit_sum
                        avg_profit = profit_sum / len(all_matches) if all_matches else 0
                        
                        compound_return = 1.0
                        for m in all_matches:
                            if isinstance(m.get('profit_ratio'), (int, float)):
                                compound_return *= (1 + m.get('profit_ratio', 0) / 100)
                        total_return_rate = (compound_return - 1) * 100
                    else:
                        total_return_rate = 0
                    
                    results[stock_code]['strategies'][strategy_name] = {
                        'strategy_name': strategy_name,
                        'strategy_display_name': get_strategy_display_name(strategy_name),
                        'total_trades': total_cases,
                        'successful_trades': successful_cases,
                        'win_rate': win_rate,
                        'total_return_rate': total_return_rate,
                        'total_profit': total_profit,
                        'average_profit': avg_profit,
                        'matches': all_matches
                    }
    
    for stock_code in results:
        logger.info(f"股票 {stock_code} ({results[stock_code]['stock_name']}) 回测完成")
    
    return results

def get_strategy_display_name(strategy_name):
    """获取策略的中文显示名称"""
    display_names = {
        'ma5_ma20_cross': 'MA5/MA20金叉死叉',
        'ma5_ma20_cross_ma60': 'MA5/MA20金叉死叉(MA60过滤)',
        'ma5_ma20_cross_ma120': 'MA5/MA20金叉死叉(MA120过滤)',
        'price_breakout_20w_10w': '价格突破20周最高买跌破10周最低卖',
        'buy_and_hold': '买入持有策略',
        'macd_cross': 'MACD金叉买死叉卖'
    }
    return display_names.get(strategy_name, strategy_name)

@register_strategy('strategy2')
def strategy2(filtered_data, stock_code, stock_name, market):
    """策略2实现: 简单金叉策略"""
    matches = []
    total_cases = 0
    successful_cases = 0
    
    # 检查是否有足够的数据
    if len(filtered_data) >= 20:
        filtered_data['MA5'] = filtered_data['ma5']
        filtered_data['MA20'] = filtered_data['ma20']
        
        # 遍历数据查找金叉
        for i in range(1, len(filtered_data)):
            # 检查金叉（MA5上穿MA20）
            if filtered_data.iloc[i-1]['MA5'] <= filtered_data.iloc[i-1]['MA20'] and \
               filtered_data.iloc[i]['MA5'] > filtered_data.iloc[i]['MA20']:
                # 记录金叉
                total_cases += 1
                
                # 验证接下来的5天
                valid_verification = True
                golden_cross_price = filtered_data.iloc[i]['close']
                
                for j in range(1, 6):
                    verification_index = i + j
                    if verification_index >= len(filtered_data):
                        valid_verification = False
                        break
                    
                    # 检查价格是否保持在金叉价格之上
                    if filtered_data.iloc[verification_index]['low'] < golden_cross_price * 0.98:
                        valid_verification = False
                        break
                
                # 计算5日、10日、20日涨幅
                five_day_increase = None
                ten_day_increase = None
                twenty_day_increase = None
                
                # 计算5日涨幅（从金叉日之后开始计算）
                if i + 5 < len(filtered_data):
                    five_day_end = filtered_data.iloc[i + 5]
                    five_day_increase = (five_day_end['close'] - golden_cross_price) / golden_cross_price * 100
                
                # 计算10日涨幅
                if i + 10 < len(filtered_data):
                    ten_day_end = filtered_data.iloc[i + 10]
                    ten_day_increase = (ten_day_end['close'] - golden_cross_price) / golden_cross_price * 100
                
                # 计算20日涨幅
                if i + 20 < len(filtered_data):
                    twenty_day_end = filtered_data.iloc[i + 20]
                    twenty_day_increase = (twenty_day_end['close'] - golden_cross_price) / golden_cross_price * 100
                
                if valid_verification:
                    successful_cases += 1
                
                golden_cross_date = filtered_data.iloc[i]['date'].strftime('%Y-%m-%d')
                
                matches.append({
                    'code': stock_code,
                    'name': stock_name,
                    'market': market,
                    'golden_cross_date': golden_cross_date,
                    'golden_cross_price': golden_cross_price,
                    'verification_result': '成功' if valid_verification else '失败',
                    '5_day_increase': five_day_increase,
                    '10_day_increase': ten_day_increase,
                    '20_day_increase': twenty_day_increase
                })
    
    return {
        'matches': matches,
        'total_cases': total_cases,
        'successful_cases': successful_cases
    }





@register_strategy('price_breakout_20w_10w')
@register_strategy('rising_surge_3')
def strategy_513(filtered_data, stock_code, stock_name, market, consecutive_days=4, verification_days=3):
    """513战法实现: 连续上涨≥N天（允许夹一根小阴线），出现放量大阳线，后续M天不跌破异动阳线的开盘价

    Args:
        filtered_data: 过滤后的股票数据
        stock_code: 股票代码
        stock_name: 股票名称
        market: 市场
        consecutive_days: 连续上涨天数，默认4天
        verification_days: 后续验证天数，默认3天
    """
    matches = []
    total_cases = 0
    successful_cases = 0
    # 异动阳线的涨幅
    bullish_threshold=3, 
    # 上影线长度阈值
    upper_shadow_threshold=2
    

    df = filtered_data.copy()
    required_days = consecutive_days + 1 + verification_days

    if len(df) >= required_days:
        is_small_green = df['is_green'] & (df['intraday_change'] >= -1) & (df['intraday_change'] < 0) & (df['close'] > df['close'].shift(1))
        is_bullish = df['intraday_change'] >= bullish_threshold
        is_heavy_volume = df['volume'] >= df['volume'].shift(1) * 2
        
        # 确保每天涨幅不过大，避免急拉
        is_slow_rise = (df['intraday_change'] >= 0) & (df['intraday_change'] < 4)  # 每天涨幅不超过3%

        rolling_reds = df['is_red'].rolling(window=consecutive_days).sum()
        rolling_small_greens = is_small_green.rolling(window=consecutive_days).sum()
        rolling_slow_rises = is_slow_rise.rolling(window=consecutive_days).sum()

        valid_up_trend = ((rolling_reds == consecutive_days) | \
                         ((rolling_reds >= consecutive_days - 1) & (rolling_small_greens == 1))) & \
                        (rolling_slow_rises == consecutive_days)  # 确保所有天都是缓慢上涨

        has_upper_shadow = df['high'] > df['close']
        upper_shadow_pct = (df['high'] - df['close']) / df['open'] * 100

        is_abnormal_up_day = is_bullish & has_upper_shadow & (upper_shadow_pct >= upper_shadow_threshold) & is_heavy_volume

        signal_trigger = valid_up_trend.shift(1) & is_abnormal_up_day

        # 动态计算未来verification_days天的最低价的最小值（使用列表推导式提高性能）
        future_lows = pd.concat([df['low'].shift(-d) for d in range(1, verification_days + 1)], axis=1).min(axis=1)
        valid_verification = future_lows >= df['open']

        signal_indices = df[signal_trigger].index

        for idx in signal_indices:
            i = df.index.get_loc(idx)
            abnormal_day = df.iloc[i]

            next_days_mask = df.index > idx
            next_days = df[next_days_mask].head(verification_days)
            
            if len(next_days) == verification_days:
                valid_verification = (next_days['low'] >= abnormal_day['open']).all() and (next_days['volume'] < abnormal_day['volume']).all()
            else:
                valid_verification = False

            total_cases += 1
            if valid_verification:
                successful_cases += 1

                end_day = next_days.iloc[-1] if len(next_days) == verification_days else None
                buy_day_idx_in_df = df.index.get_loc(idx) + verification_days + 1
                
                if end_day is not None:
                    increase = (end_day['close'] - abnormal_day['close']) / abnormal_day['close'] * 100
                else:
                    increase = None

                buy_day = df.iloc[buy_day_idx_in_df] if buy_day_idx_in_df < len(df) else None
                buy_day_open = buy_day['open'] if buy_day is not None else None
                buy_day_label = buy_day.name if buy_day is not None else None

                future_5_day = df.loc[buy_day_label:].iloc[5] if buy_day_label is not None and len(df.loc[buy_day_label:]) > 5 else None
                future_10_day = df.loc[buy_day_label:].iloc[10] if buy_day_label is not None and len(df.loc[buy_day_label:]) > 10 else None
                future_20_day = df.loc[buy_day_label:].iloc[20] if buy_day_label is not None and len(df.loc[buy_day_label:]) > 20 else None

                matches.append({
                    'code': stock_code,
                    'name': stock_name,
                    'market': market,
                    'period': f"{df.iloc[i]['date'].strftime('%Y-%m-%d')} 至 {abnormal_day['date'].strftime('%Y-%m-%d')}",
                    'abnormal_up_day_date': abnormal_day['date'].strftime('%Y-%m-%d'),
                    'abnormal_up_day_open': abnormal_day['open'],
                    'abnormal_up_day_close': abnormal_day['close'],
                    f'{verification_days}_day_verification_result': '成功' if valid_verification else '失败',
                    f'{verification_days}_day_increase': increase,
                    'buy_day_date': buy_day['date'].strftime('%Y-%m-%d') if buy_day is not None else None,
                    'buy_day_open': buy_day_open,
                    '5_day_increase': (future_5_day['close'] - buy_day_open) / buy_day_open * 100 if future_5_day is not None and buy_day_open else None,
                    '10_day_increase': (future_10_day['close'] - buy_day_open) / buy_day_open * 100 if future_10_day is not None and buy_day_open else None,
                    '20_day_increase': (future_20_day['close'] - buy_day_open) / buy_day_open * 100 if future_20_day is not None and buy_day_open else None
                })

    return {
        'matches': matches,
        'total_cases': total_cases,
        'successful_cases': successful_cases
    }

@register_strategy('ma5_ma20_cross')
def ma5_ma20_cross_strategy(filtered_data, stock_code, stock_name, market, ma_filter=None):
    """MA5/MA20金叉死叉策略
    
    买入条件：MA5上穿MA20（金叉）
    卖出条件：MA5下穿MA20（死叉）
    ma_filter: 均线过滤参数，可选 60 或 120，表示股票必须处于该周期均线上方才买入
                为 None 时表示不进行均线过滤
    计算从买入到卖出的收益率
    """
    matches = []
    total_cases = 0
    successful_cases = 0
    
    if len(filtered_data) < 5:
        return {'matches': matches, 'total_cases': total_cases, 'successful_cases': successful_cases}
    
    df = filtered_data.copy()
    df['MA5'] = df['ma5']
    df['MA20'] = df['ma20']
    
    df['MA_FILTER'] = df['ma60'] if ma_filter == 60 else (df['ma120'] if ma_filter == 120 else None)

    position = None

    for i in range(1, len(df)):
        if df.iloc[i-1]['MA5'] <= df.iloc[i-1]['MA20'] and df.iloc[i]['MA5'] > df.iloc[i]['MA20']:
            if position is None:
                can_buy = True
                if ma_filter is not None and df.iloc[i]['close'] < df.iloc[i]['MA_FILTER']:
                        can_buy = False
                if can_buy:
                    position = {
                        'buy_date': df.iloc[i]['date'].strftime('%Y-%m-%d'),
                        'buy_price': df.iloc[i]['close'],
                        'buy_index': i
                    }
        elif df.iloc[i-1]['MA5'] >= df.iloc[i-1]['MA20'] and df.iloc[i]['MA5'] < df.iloc[i]['MA20']:
            if position is not None:
                sell_price = df.iloc[i]['close']
                sell_date = df.iloc[i]['date'].strftime('%Y-%m-%d')
                buy_price = position['buy_price']
                
                total_cases += 1
                profit_ratio = (sell_price - buy_price) / buy_price * 100
                
                if profit_ratio > 0:
                    successful_cases += 1
                
                holding_days = i - position['buy_index']
                
                matches.append({
                    'code': stock_code,
                    'name': stock_name,
                    'market': market,
                    'buy_date': position['buy_date'],
                    'buy_price': buy_price,
                    'sell_date': sell_date,
                    'sell_price': sell_price,
                    'profit_ratio': profit_ratio,
                    'holding_days': holding_days,
                    'result': '盈利' if profit_ratio > 0 else '亏损'
                })
                position = None

    return {
        'matches': matches,
        'total_cases': total_cases,
        'successful_cases': successful_cases
    }



@register_strategy('ma5_ma20_cross_ma60')
def ma5_ma20_cross_ma60_strategy(filtered_data, stock_code, stock_name, market):
    """MA5/MA20金叉死叉策略（MA60过滤）
    
    买入条件：MA5上穿MA20（金叉）且股价高于MA60
    卖出条件：MA5下穿MA20（死叉）
    计算从买入到卖出的收益率
    """
    return ma5_ma20_cross_strategy(filtered_data, stock_code, stock_name, market, ma_filter=60)


@register_strategy('ma5_ma20_cross_ma120')
def ma5_ma20_cross_ma120_strategy(filtered_data, stock_code, stock_name, market):
    """MA5/MA20金叉死叉策略（MA120过滤）
    
    买入条件：MA5上穿MA20（金叉）且股价高于MA120
    卖出条件：MA5下穿MA20（死叉）
    计算从买入到卖出的收益率
    """
    return ma5_ma20_cross_strategy(filtered_data, stock_code, stock_name, market, ma_filter=120)

@register_strategy('price_breakout_20w_10w')
def price_breakout_20w_10w_strategy(filtered_data, stock_code, stock_name, market):
    """价格突破前20周最高点买，跌破10周最低点卖策略
    
    买入条件：当前价格突破前20周（100个交易日）的最高价
    卖出条件：当前价格跌破前10周（50个交易日）的最低价
    计算从买入到卖出的收益率
    """
    matches = []
    total_cases = 0
    successful_cases = 0
    
    if len(filtered_data) < 100:
        return {'matches': matches, 'total_cases': total_cases, 'successful_cases': successful_cases}
    
    df = filtered_data.copy()
    
    position = None
    
    for i in range(100, len(df)):
        current_high_100 = df.iloc[i-100:i]['high'].max()
        current_low_50 = df.iloc[i-50:i]['low'].min()
        current_price = df.iloc[i]['close']
        current_date = df.iloc[i]['date'].strftime('%Y-%m-%d')
        
        if position is None:
            if current_price > current_high_100:
                position = {
                    'buy_date': current_date,
                    'buy_price': current_price,
                    'buy_index': i,
                    'breakout_high': current_high_100
                }
        else:
            if current_price < current_low_50:
                sell_price = current_price
                sell_date = current_date
                buy_price = position['buy_price']
                
                total_cases += 1
                profit_ratio = (sell_price - buy_price) / buy_price * 100
                
                if profit_ratio > 0:
                    successful_cases += 1
                
                holding_days = i - position['buy_index']
                
                matches.append({
                    'code': stock_code,
                    'name': stock_name,
                    'market': market,
                    'buy_date': position['buy_date'],
                    'buy_price': buy_price,
                    'breakout_high': position['breakout_high'],
                    'sell_date': sell_date,
                    'sell_price': sell_price,
                    'stop_loss_low': current_low_50,
                    'profit_ratio': profit_ratio,
                    'holding_days': holding_days,
                    'result': '盈利' if profit_ratio > 0 else '亏损'
                })
                position = None
    
    return {
        'matches': matches,
        'total_cases': total_cases,
        'successful_cases': successful_cases
    }

@register_strategy('buy_and_hold')
def buy_and_hold_strategy(filtered_data, stock_code, stock_name, market):
    """买入持有策略
    
    在时间起点买入，时间终点卖出，不做任何操作。
    """
    matches = []
    total_cases = 0
    successful_cases = 0
    
    if len(filtered_data) < 2:
        return {'matches': matches, 'total_cases': total_cases, 'successful_cases': successful_cases}
    
    buy_row = filtered_data.iloc[0]
    sell_row = filtered_data.iloc[-1]
    
    buy_date = buy_row['date'].strftime('%Y-%m-%d')
    buy_price = buy_row['close']
    sell_date = sell_row['date'].strftime('%Y-%m-%d')
    sell_price = sell_row['close']
    
    total_cases += 1
    profit_ratio = (sell_price - buy_price) / buy_price * 100
    
    if profit_ratio > 0:
        successful_cases += 1
    
    holding_days = (filtered_data.iloc[-1]['date'] - filtered_data.iloc[0]['date']).days
    
    matches.append({
        'code': stock_code,
        'name': stock_name,
        'market': market,
        'buy_date': buy_date,
        'buy_price': buy_price,
        'sell_date': sell_date,
        'sell_price': sell_price,
        'profit_ratio': profit_ratio,
        'holding_days': holding_days,
        'result': '盈利' if profit_ratio > 0 else '亏损'
    })
    
    return {
        'matches': matches,
        'total_cases': total_cases,
        'successful_cases': successful_cases
    }


@register_strategy('macd_cross')
def macd_cross_strategy(filtered_data, stock_code, stock_name, market, ma_filter=None):
    """MACD金叉买死叉卖策略
    
    买入条件：MACD柱由负转正（DIF从下方穿过DEA）
    卖出条件：MACD柱由正转负（DIF从上方穿过DEA）
    ma_filter: 均线过滤参数，可选 60 或 120，表示股票必须处于该周期均线上方才买入
                为 None 时表示不进行均线过滤
    计算从买入到卖出的收益率
    """
    matches = []
    total_cases = 0
    successful_cases = 0
    
    if len(filtered_data) < 2:
        return {'matches': matches, 'total_cases': total_cases, 'successful_cases': successful_cases}
    
    df = filtered_data.copy()
    
    if 'macd' not in df.columns or 'macd_signal' not in df.columns:
        df['ema12'] = df['close'].ewm(span=12, adjust=False).mean()
        df['ema26'] = df['close'].ewm(span=26, adjust=False).mean()
        df['macd'] = df['ema12'] - df['ema26']
        df['macd_signal'] = df['macd'].ewm(span=9, adjust=False).mean()
    
    df['macd_hist'] = df['macd'] - df['macd_signal']

    df['MA_FILTER'] = df['ma60'] if ma_filter == 60 else (df['ma120'] if ma_filter == 120 else None)

    position = None

    for i in range(1, len(df)):
        prev_macd_hist = df.iloc[i-1]['macd_hist']
        curr_macd_hist = df.iloc[i]['macd_hist']

        if prev_macd_hist <= 0 and curr_macd_hist > 0:
            if position is None and df.iloc[i]['macd'] > 0:
                can_buy = True
                if ma_filter is not None and df.iloc[i]['close'] < df.iloc[i]['MA_FILTER']:
                        can_buy = False
                if can_buy:
                    position = {
                        'buy_date': df.iloc[i]['date'].strftime('%Y-%m-%d'),
                        'buy_price': df.iloc[i]['close'],
                        'buy_index': i,
                        'macd_hist_at_buy': curr_macd_hist
                    }
        elif prev_macd_hist >= 0 and curr_macd_hist < 0:
            if position is not None:
                sell_price = df.iloc[i]['close']
                sell_date = df.iloc[i]['date'].strftime('%Y-%m-%d')
                buy_price = position['buy_price']
                
                total_cases += 1
                profit_ratio = (sell_price - buy_price) / buy_price * 100
                
                if profit_ratio > 0:
                    successful_cases += 1
                
                holding_days = i - position['buy_index']
                
                matches.append({
                    'code': stock_code,
                    'name': stock_name,
                    'market': market,
                    'buy_date': position['buy_date'],
                    'buy_price': buy_price,
                    'sell_date': sell_date,
                    'sell_price': sell_price,
                    'profit_ratio': profit_ratio,
                    'holding_days': holding_days,
                    'result': '盈利' if profit_ratio > 0 else '亏损'
                })
                position = None
    
    return {
        'matches': matches,
        'total_cases': total_cases,
        'successful_cases': successful_cases
    }


@register_strategy('macd_cross_ma60')
def macd_cross_ma60_strategy(filtered_data, stock_code, stock_name, market):
    """MACD金叉买死叉卖策略（MA60过滤）

    买入条件：MACD柱由负转正（DIF从下方穿过DEA）且股价高于MA60
    卖出条件：MACD柱由正转负（DIF从上方穿过DEA）
    计算从买入到卖出的收益率
    """
    return macd_cross_strategy(filtered_data, stock_code, stock_name, market, ma_filter=60)


@register_strategy('macd_cross_ma120')
def macd_cross_ma120_strategy(filtered_data, stock_code, stock_name, market):
    """MACD金叉买死叉卖策略（MA120过滤）

    买入条件：MACD柱由负转正（DIF从下方穿过DEA）且股价高于MA120
    卖出条件：MACD柱由正转负（DIF从上方穿过DEA）
    计算从买入到卖出的收益率
    """
    return macd_cross_strategy(filtered_data, stock_code, stock_name, market, ma_filter=120)


@register_strategy('macd_rejuvenation')
def macd_rejuvenation_strategy(filtered_data, stock_code, stock_name, market):
    """回春战法: MACD(10,20,9)参数下的特殊形态

    条件1: MACD金叉到第一个死叉，涨幅≥40%
    条件2: 死叉后出现的第一个金叉，在0轴附近
    条件3: 股价在60日均线之上，60日均线向上
    买点: 即将金叉时买入，需有明显放量
    """
    matches = []
    total_cases = 0
    successful_cases = 0

    if len(filtered_data) < 30:
        return {'matches': matches, 'total_cases': total_cases, 'successful_cases': successful_cases}

    df = filtered_data.copy()

    # 计算自定义MACD(10,20,9)
    df['ema10'] = df['close'].ewm(span=10, adjust=False).mean()
    df['ema20'] = df['close'].ewm(span=20, adjust=False).mean()
    df['dif'] = df['ema10'] - df['ema20']
    df['dea'] = df['dif'].ewm(span=9, adjust=False).mean()
    df['macd_hist'] = (df['dif'] - df['dea']) * 2

    df = df.reset_index(drop=True)

    golden_cross_idx = None

    for i in range(1, len(df)):
        prev_hist = df.iloc[i - 1]['macd_hist']
        curr_hist = df.iloc[i]['macd_hist']

        # 金叉: MACD柱由负转正
        if prev_hist <= 0 and curr_hist > 0:
            if golden_cross_idx is None:
                golden_cross_idx = i
            continue

        # 死叉: MACD柱由正转负
        if prev_hist >= 0 and curr_hist < 0 and golden_cross_idx is not None:
            death_cross_idx = i

            # 条件1: 金叉到死叉涨幅≥40%
            gc_close = df.iloc[golden_cross_idx]['close']
            dc_close = df.iloc[death_cross_idx]['close']
            gain_pct = (dc_close - gc_close) / gc_close * 100

            if gain_pct < 40:
                golden_cross_idx = None
                continue

            # 条件1满足：金叉到死叉涨幅≥40%，先作为预选股票输出
            # 检查死叉后是否已有新的金叉出现，以及金叉是否在零轴附近
            has_next_golden = False
            next_golden_date = None
            is_near_zero = None
            macd_value = None
            current_dif = df.iloc[-1]['dif']

            for j in range(death_cross_idx + 1, len(df)):
                if df.iloc[j - 1]['macd_hist'] <= 0 and df.iloc[j]['macd_hist'] > 0:
                    has_next_golden = True
                    next_golden_date = df.iloc[j]['date'].strftime('%Y-%m-%d')
                    # 计算金叉是否在零轴附近
                    macd_ffat_gc = df.iloc[j]['macd']
                    macd_value = round(macd_at_gc, 4)
                    recent_range = df.iloc[max(0, death_cross_idx - 20):death_cross_idx + 1]['macd'].max() - \
                                   df.iloc[max(0, death_cross_idx - 20):death_cross_idx + 1]['macd'].min()
                    threshold = max(recent_range * 0.2, 0.01)
                    is_near_zero = bool(abs(macd_at_gc) <= threshold)
                    break

            matches.append({
                'code': stock_code,
                'name': stock_name,
                'market': market,
                'golden_cross_date': df.iloc[golden_cross_idx]['date'].strftime('%Y-%m-%d'),
                'death_cross_date': df.iloc[death_cross_idx]['date'].strftime('%Y-%m-%d'),
                'death_cross_idx': death_cross_idx,
                'gain_gc_to_dc': round(gain_pct, 2),
                'has_next_golden_cross': has_next_golden,
                'next_golden_cross_date': next_golden_date,
                'macd_at_next_golden': macd_value,
                'dif_to_zero': round(current_dif, 4),
                'is_near_zero_axis': is_near_zero,
                'is_prescreened': True,
            })

            # 寻找死叉后第一个金叉
            next_golden_idx = None
            for j in range(death_cross_idx + 1, len(df)):
                if df.iloc[j - 1]['macd_hist'] <= 0 and df.iloc[j]['macd_hist'] > 0:
                    next_golden_idx = j
                    break

            if next_golden_idx is None:
                golden_cross_idx = None
                continue

            # 条件2: 金叉在0轴附近
            macd_at_gc = df.iloc[next_golden_idx]['macd']
            recent_range = df.iloc[max(0, death_cross_idx - 20):death_cross_idx + 1]['macd'].max() - \
                           df.iloc[max(0, death_cross_idx - 20):death_cross_idx + 1]['macd'].min()
            threshold = max(recent_range * 0.2, 0.01)
            if abs(macd_at_gc) > threshold:
                golden_cross_idx = None
                continue

            # 条件3: 股价在MA60之上，MA60向上（数据不足时跳过此条件）
            if 'ma60' in df.columns:
                ma60_val = df.iloc[next_golden_idx]['ma60']
                ma60_5ago = df.iloc[max(0, next_golden_idx - 5)]['ma60']
                if not pd.isna(ma60_val) and not pd.isna(ma60_5ago):
                    price = df.iloc[next_golden_idx]['close']
                    if price < ma60_val:
                        golden_cross_idx = None
                        continue
                    if ma60_val <= ma60_5ago:
                        golden_cross_idx = None
                        continue

            # 放量检查: 金叉日成交量≥5日均量的1.5倍
            if next_golden_idx >= 5:
                avg_vol = df.iloc[next_golden_idx - 5:next_golden_idx]['volume'].mean()
                if avg_vol > 0 and df.iloc[next_golden_idx]['volume'] < avg_vol * 1.5:
                    golden_cross_idx = None
                    continue

            total_cases += 1

            # 所有条件满足，更新为完整回春战法匹配
            matches[-1].update({
                'rejuvenation_date': df.iloc[next_golden_idx]['date'].strftime('%Y-%m-%d'),
                'macd_at_rejuvenation': round(macd_at_gc, 4),
                'is_prescreened': False,
            })

            golden_cross_idx = None

    # 按照死叉日期降序排列，死叉越晚排在越前面
    matches.sort(key=lambda x: x.get('death_cross_idx', 0), reverse=True)

    # 移除排序用的临时字段
    for match in matches:
        match.pop('death_cross_idx', None)

    return {
        'matches': matches,
        'total_cases': total_cases,
        'successful_cases': successful_cases
    }


def validate_macd_rejuvenation(start_date: str | None = None, end_date: str | None = None, stock_codes: list[str] | None = None) -> dict:
    """验证回春战法

    在指定时间段内扫描所有股票，寻找符合回春战法条件的股票。

    Args:
        start_date: 开始日期，格式为"YYYY-MM-DD"
        end_date: 结束日期，格式为"YYYY-MM-DD"
        stock_codes: 股票代码列表，为None时验证所有股票

    Returns:
        dict: 包含匹配股票和策略准确率的结果
    """
    if stock_codes is None:
        stocks = _ensure_stock_service().get_all_a_stocks_from_db()
        logger.info(f"获取到 {len(stocks)} 只A股股票")
    else:
        stocks = stock_codes

    matching_stocks = []
    total_cases = 0
    successful_cases = 0

    max_workers = 400
    logger.info(f"使用 {max_workers} 个线程进行并行处理")

    processed_count = 0

    def process_stock_rejuvenation(stock_code: str) -> Optional[dict]:
        try:
            stock_name = ""
            market = ""
            company = stock_company_service.get_stock_company_by_code(stock_code)
            if company:
                stock_name = company.get('sec_name', '')
                market = company.get('market', '')

            stock_code_str = str(stock_code)
            if not (stock_code_str.startswith('600') or stock_code_str.startswith('601') or
                    stock_code_str.startswith('603') or stock_code_str.startswith('605') or
                    stock_code_str.startswith('000')):
                return None

            k_data = _ensure_stock_service().load_stock_data(stock_code)
            if k_data is None:
                return None

            # 过滤日期范围
            if start_date and end_date:
                start_date_dt = pd.to_datetime(start_date)
                end_date_dt = pd.to_datetime(end_date)
                filtered_data = k_data[(k_data['date'] >= start_date_dt) & (k_data['date'] <= end_date_dt)]
            else:
                filtered_data = k_data

            strategy_result = macd_rejuvenation_strategy(filtered_data, stock_code, stock_name, market)

            if strategy_result and strategy_result['matches']:
                for match in strategy_result['matches']:
                    if match.get('is_prescreened', False):
                        if match.get('has_next_golden_cross'):
                            next_gc_status = f"已出现({match['next_golden_cross_date']})"
                            zero_axis_status = f", 零轴附近: {'是' if match.get('is_near_zero_axis') else '否'}(MACD={match.get('macd_at_next_golden', 'N/A')})"
                        else:
                            next_gc_status = "未出现"
                            zero_axis_status = f", 快线到零轴距离={match.get('dif_to_zero', 'N/A')}"
                        logger.info(
                            f"  - 预选股票: {stock_code} {stock_name}, "
                            f"金叉→死叉日期: {match['golden_cross_date']}→{match['death_cross_date']}, "
                            f"金叉→死叉涨幅: {match['gain_gc_to_dc']}%, "
                            f"下一个金叉: {next_gc_status}{zero_axis_status}"
                        )
                    else:
                        logger.info(
                            f"  - 回春战法: {stock_code} {stock_name}, "
                            f"金叉→死叉日期: {match['golden_cross_date']}→{match['death_cross_date']}, "
                            f"金叉→死叉涨幅: {match['gain_gc_to_dc']}%, "
                            f"回春日: {match['rejuvenation_date']}"
                        )

            return strategy_result
        except Exception as e:
            logger.error(f"处理股票 {stock_code} 时发生错误: {type(e).__name__}: {str(e)}")
            return None

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_stock_rejuvenation, stock): stock for stock in stocks}

        for future in as_completed(futures):
            result = future.result()
            processed_count += 1

            if result:
                matching_stocks.extend(result['matches'])
                total_cases += result['total_cases']
                successful_cases += result['successful_cases']

    accuracy = (successful_cases / total_cases * 100) if total_cases > 0 else 0
    logger.info(f"回春战法验证完成")
    logger.info(f"总案例数: {total_cases}")
    logger.info(f"成功案例数: {successful_cases}")
    logger.info(f"策略准确率: {accuracy:.2f}%")
    logger.info(f"找到 {len(matching_stocks)} 个匹配模式")

    # 按死叉日期降序排列，死叉越晚排在越前面
    matching_stocks.sort(key=lambda x: x.get('death_cross_date', ''), reverse=True)

    return {
        'matching_stocks': matching_stocks,
        'total_cases': total_cases,
        'successful_cases': successful_cases,
        'accuracy': accuracy
    }
