from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

from app.utils.log import logger

# 策略注册表
STRATEGIES = {}

def register_strategy(strategy_name):
    """注册策略函数的装饰器"""
    def decorator(func):
        STRATEGIES[strategy_name] = func
        return func
    return decorator

def process_stock(stock, stock_service, strategy_func, start_date, end_date):
    """处理单个股票的策略验证
    
    Args:
        stock: 股票数据
        stock_service: StockService实例
        strategy_func: 策略函数
        start_date: 开始日期
        end_date: 结束日期
        
    Returns:
        dict: 处理结果，包含匹配模式、总案例数和成功案例数
    """
    # Process stock code (from database)
    if isinstance(stock, (str, int)):
        # Format as 6-digit code
        stock_code = f"{int(stock):06d}"
        # 从数据库获取股票名称和市场信息
        stock_name = ""
        market = ""
        # 尝试从数据库获取股票公司信息
        company = stock_service.get_stock_company_by_code(stock_code)
        if company:
            if isinstance(company, dict):
                stock_name = company.get('sec_name', '')
                market = company.get('market', '')
            else:
                stock_name = company.sec_name
                market = company.market
    # Process StockCompany object
    elif hasattr(stock, 'sec_code'):
        # Format as 6-digit code
        stock_code = f"{stock.sec_code:06d}"
        stock_name = stock.sec_name
        market = stock.market
    else:
        # Compatible with old format
        stock_code = stock['代码']
        stock_name = stock['名称']
        market = stock['市场']
    
    logger.debug(f"处理股票: {stock_code} - {stock_name}")
    
    # Load data from database
    k_data = stock_service.load_stock_data(stock_code)
    
    if k_data is not None:
        logger.debug(f"成功加载 {stock_code} 的历史数据，共 {len(k_data)} 条记录")
        
        # Filter data within time range
        if start_date and end_date:
            start_date_dt = pd.to_datetime(start_date)
            end_date_dt = pd.to_datetime(end_date)
            filtered_data = k_data[(k_data['日期'] >= start_date_dt) & (k_data['日期'] <= end_date_dt)]
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
                    logger.info(f"  - 股票代码: {stock_code}, 股票名称: {stock_name}, 匹配时间段: {period}, 异动阳线后第四天开盘买入价格: {buy_day_open}, 5日后涨幅: {format_increase(five_day_increase)}, 10日后涨幅: {format_increase(ten_day_increase)}, 20日涨幅: {format_increase(twenty_day_increase)}")
        
        return strategy_result
    else:
        logger.warning(f"无法加载股票 {stock_code} 的历史数据")
        return None

def validate_strategy(stock_service, strategy_name, start_date=None, end_date=None):
    """基于历史数据验证股票策略
    
    参数:
        stock_service: StockService实例
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
    
    all_stocks = stock_service.get_all_a_stocks_from_db()
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
        futures = {executor.submit(process_stock, stock, stock_service, strategy_func, start_date, end_date): stock for stock in all_stocks}
        
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

def validate_513_strategy(stock_service, start_date=None, end_date=None, consecutive_days=4, verification_days=3, stock_codes=None):
    """验证513战法（可自定义连续上涨天数和后续验证天数）
    
    参数:
        stock_service: StockService实例
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
    
    if stock_codes:
        # 验证指定的股票
        stocks = []
        for code in stock_codes:
            company = stock_service.get_stock_company_by_code(code)
            if company:
                stocks.append(company)
        logger.info(f"获取到 {len(stocks)} 只指定股票")
    else:
        # 验证所有A股股票
        stocks = stock_service.get_all_a_stocks_from_db()
        logger.info(f"获取到 {len(stocks)} 只A股股票")
    
    matching_stocks = []
    total_cases = 0
    successful_cases = 0
    
    # 使用ThreadPoolExecutor进行并行处理
    max_workers = 400  # 可根据系统资源调整
    logger.info(f"使用 {max_workers} 个线程进行并行处理")
    
    processed_count = 0
    
    def process_stock_with_params(stock):
        """处理单个股票的513策略验证（带参数）"""
        # Process stock code (from database)
        if isinstance(stock, (str, int)):
            # Format as 6-digit code
            stock_code = f"{int(stock):06d}"
            # 从数据库获取股票名称和市场信息
            stock_name = ""
            market = ""
            # 尝试从数据库获取股票公司信息
            company = stock_service.get_stock_company_by_code(stock_code)
            if company:
                if isinstance(company, dict):
                    stock_name = company.get('sec_name', '')
                    market = company.get('market', '')
                else:
                    stock_name = company.sec_name
                    market = company.market
        # Process StockCompany object
        elif hasattr(stock, 'sec_code'):
            # Format as 6-digit code
            stock_code = f"{stock.sec_code:06d}"
            stock_name = stock.sec_name
            market = stock.market
        else:
            # Compatible with old format
            stock_code = stock['代码']
            stock_name = stock['名称']
            market = stock['市场']
        
        logger.debug(f"处理股票: {stock_code} - {stock_name}")
        
        # Load data from database
        k_data = stock_service.load_stock_data(stock_code)
        
        if k_data is not None:
            logger.debug(f"成功加载 {stock_code} 的历史数据，共 {len(k_data)} 条记录")
            
            # Filter data within time range
            if start_date and end_date:
                import pandas as pd
                start_date_dt = pd.to_datetime(start_date)
                end_date_dt = pd.to_datetime(end_date)
                filtered_data = k_data[(k_data['日期'] >= start_date_dt) & (k_data['日期'] <= end_date_dt)]
                logger.debug(f"时间范围筛选后，剩余 {len(filtered_data)} 条记录")
            else:
                filtered_data = k_data
            
            # Apply strategy with custom parameters
            logger.debug(f"应用513策略到 {stock_code}")
            from app.services.strategies import strategy_513
            strategy_result = strategy_513(filtered_data, stock_code, stock_name, market, consecutive_days, verification_days)
            
            # Log results
            if strategy_result:
                match_count = len(strategy_result['matches'])
                if match_count > 0:
                    for match in strategy_result['matches']:
                        period = match.get('period', '未知时间段')
                        # 计算涨幅信息
                        buy_day_open = match.get('abnormal_up_day_open', 0)
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
                        logger.info(f"  - 股票代码: {stock_code}, 股票名称: {stock_name}, 匹配时间段: {period}, 异动阳线后第四天开盘买入价格: {buy_day_open}, 5日后涨幅: {format_increase(five_day_increase)}, 10日后涨幅: {format_increase(ten_day_increase)}, 20日涨幅: {format_increase(twenty_day_increase)}")
            
            return strategy_result
        else:
            logger.warning(f"无法加载股票 {stock_code} 的历史数据")
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
                if filtered_data.iloc[j]['收盘'] > filtered_data.iloc[j]['开盘']:
                    consecutive_up_days += 1
                else:
                    # 检查是否为小阴线
                    close = filtered_data.iloc[j]['收盘']
                    open_price = filtered_data.iloc[j]['开盘']
                    pre_close = filtered_data.iloc[j-1]['收盘'] if j > i else open_price
                    
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
                    if (abnormal_up_day['收盘'] - abnormal_up_day['开盘']) / abnormal_up_day['开盘'] >= 0.03:
                        # 检查是否有上影线（从最高价到收盘价的回落）
                        if abnormal_up_day['最高'] > abnormal_up_day['收盘'] and (abnormal_up_day['最高'] - abnormal_up_day['收盘']) / abnormal_up_day['开盘'] >= 0.01:
                            # 检查成交量是否≥前一天的2倍
                            if abnormal_up_day['成交量'] >= previous_day['成交量'] * 2:
                                # 验证接下来的3天
                                valid_verification = True
                                abnormal_up_day_open = abnormal_up_day['开盘']
                                
                                for j in range(1, 4):
                                    verification_index = abnormal_up_day_index + j
                                    if verification_index >= len(filtered_data):
                                        valid_verification = False
                                        break
                                    
                                    # 检查是否跌破异常放量阳线的开盘价
                                    if filtered_data.iloc[verification_index]['最低'] < abnormal_up_day_open:
                                        valid_verification = False
                                        break
                                
                                # 计算3天涨幅
                                if abnormal_up_day_index + 3 < len(filtered_data):
                                    verification_end_day = filtered_data.iloc[abnormal_up_day_index + 3]
                                    increase = (verification_end_day['收盘'] - abnormal_up_day['收盘']) / abnormal_up_day['收盘'] * 100
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
                                    five_day_increase = (five_day_end['收盘'] - abnormal_up_day['开盘']) / abnormal_up_day['开盘'] * 100
                                
                                # 计算10日涨幅
                                if abnormal_up_day_index + 9 < len(filtered_data):
                                    ten_day_end = filtered_data.iloc[abnormal_up_day_index + 9]
                                    ten_day_increase = (ten_day_end['收盘'] - abnormal_up_day['开盘']) / abnormal_up_day['开盘'] * 100
                                
                                # 计算20日涨幅
                                if abnormal_up_day_index + 19 < len(filtered_data):
                                    twenty_day_end = filtered_data.iloc[abnormal_up_day_index + 19]
                                    twenty_day_increase = (twenty_day_end['收盘'] - abnormal_up_day['开盘']) / abnormal_up_day['开盘'] * 100
                                
                                # 记录结果
                                total_cases += 1
                                if valid_verification:
                                    successful_cases += 1
                                
                                # 添加到匹配列表
                                match_start_date = filtered_data.iloc[i]['日期'].strftime('%Y-%m-%d')
                                match_end_date = abnormal_up_day['日期'].strftime('%Y-%m-%d')
                                
                                matches.append({
                                    'code': stock_code,
                                    'name': stock_name,
                                    'market': market,
                                    'period': f"{match_start_date} 至 {match_end_date}",
                                    'abnormal_up_day_date': abnormal_up_day['日期'].strftime('%Y-%m-%d'),
                                    'abnormal_up_day_open': abnormal_up_day['开盘'],
                                    'abnormal_up_day_close': abnormal_up_day['收盘'],
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

@register_strategy('strategy2')
def strategy2(filtered_data, stock_code, stock_name, market):
    """策略2实现: 简单金叉策略"""
    matches = []
    total_cases = 0
    successful_cases = 0
    
    # 检查是否有足够的数据
    if len(filtered_data) >= 20:
        # 计算移动平均线
        filtered_data['MA5'] = filtered_data['收盘'].rolling(window=5).mean()
        filtered_data['MA20'] = filtered_data['收盘'].rolling(window=20).mean()
        
        # 遍历数据查找金叉
        for i in range(1, len(filtered_data)):
            # 检查金叉（MA5上穿MA20）
            if filtered_data.iloc[i-1]['MA5'] <= filtered_data.iloc[i-1]['MA20'] and \
               filtered_data.iloc[i]['MA5'] > filtered_data.iloc[i]['MA20']:
                # 记录金叉
                total_cases += 1
                
                # 验证接下来的5天
                valid_verification = True
                golden_cross_price = filtered_data.iloc[i]['收盘']
                
                for j in range(1, 6):
                    verification_index = i + j
                    if verification_index >= len(filtered_data):
                        valid_verification = False
                        break
                    
                    # 检查价格是否保持在金叉价格之上
                    if filtered_data.iloc[verification_index]['最低'] < golden_cross_price * 0.98:
                        valid_verification = False
                        break
                
                # 计算5日、10日、20日涨幅
                five_day_increase = None
                ten_day_increase = None
                twenty_day_increase = None
                
                # 计算5日涨幅（从金叉日之后开始计算）
                if i + 5 < len(filtered_data):
                    five_day_end = filtered_data.iloc[i + 5]
                    five_day_increase = (five_day_end['收盘'] - golden_cross_price) / golden_cross_price * 100
                
                # 计算10日涨幅
                if i + 10 < len(filtered_data):
                    ten_day_end = filtered_data.iloc[i + 10]
                    ten_day_increase = (ten_day_end['收盘'] - golden_cross_price) / golden_cross_price * 100
                
                # 计算20日涨幅
                if i + 20 < len(filtered_data):
                    twenty_day_end = filtered_data.iloc[i + 20]
                    twenty_day_increase = (twenty_day_end['收盘'] - golden_cross_price) / golden_cross_price * 100
                
                if valid_verification:
                    successful_cases += 1
                
                # 添加到匹配列表
                golden_cross_date = filtered_data.iloc[i]['日期'].strftime('%Y-%m-%d')
                
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

@register_strategy('strategy_513')
@register_strategy('rising_surge_3')  # 英文名字

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
    
    # 检查是否有足够的数据
    required_days = consecutive_days + 1 + verification_days  # 连续上涨天数 + 1根异常放量阳线 + 验证天数
    if len(filtered_data) >= required_days:
        # 遍历所有可能的连续组合
        for i in range(len(filtered_data) - required_days + 1):
            # 检查连续阳线条件（至少consecutive_days天，允许一根小阴线）
            consecutive_up_days = 0
            has_small_red = False
            valid_up_trend = True
            
            for j in range(i, i + consecutive_days):
                if j >= len(filtered_data):
                    valid_up_trend = False
                    break
                
                # 计算每日涨跌
                if filtered_data.iloc[j]['收盘'] > filtered_data.iloc[j]['开盘']:
                    consecutive_up_days += 1
                else:
                    # 检查是否为小阴线
                    close = filtered_data.iloc[j]['收盘']
                    open_price = filtered_data.iloc[j]['开盘']
                    pre_close = filtered_data.iloc[j-1]['收盘'] if j > i else open_price
                    
                    # 小阴线定义: 跌幅不超过1%，且收盘价仍高于前一天收盘价
                    if (open_price - close) / open_price <= 0.01 and close > pre_close:
                        has_small_red = True
                    else:
                        valid_up_trend = False
                        break
            
            if valid_up_trend and (consecutive_up_days >= consecutive_days or (consecutive_up_days >= consecutive_days - 1 and has_small_red)):
                # 检查第consecutive_days+1天是否为放量阳线
                abnormal_up_day_index = i + consecutive_days
                if abnormal_up_day_index < len(filtered_data):
                    abnormal_up_day = filtered_data.iloc[abnormal_up_day_index]
                    previous_day = filtered_data.iloc[abnormal_up_day_index - 1]
                    
                    # 检查是否为大阳线（涨幅≥3%）
                    if (abnormal_up_day['收盘'] - abnormal_up_day['开盘']) / abnormal_up_day['开盘'] >= 0.03:
                        # 检查是否有上影线（从最高价到收盘价的回落）
                        if abnormal_up_day['最高'] > abnormal_up_day['收盘'] and (abnormal_up_day['最高'] - abnormal_up_day['收盘']) / abnormal_up_day['开盘'] >= 0.01:
                            # 检查成交量是否≥前一天的2倍
                            if abnormal_up_day['成交量'] >= previous_day['成交量'] * 2:
                                # 验证接下来的verification_days天
                                valid_verification = True
                                abnormal_up_day_open = abnormal_up_day['开盘']
                                
                                for j in range(1, verification_days + 1):
                                    verification_index = abnormal_up_day_index + j
                                    if verification_index >= len(filtered_data):
                                        valid_verification = False
                                        break
                                    
                                    # 检查是否跌破异常放量阳线的开盘价
                                    if filtered_data.iloc[verification_index]['最低'] < abnormal_up_day_open:
                                        valid_verification = False
                                        break
                                
                                # 计算verification_days天涨幅
                                if abnormal_up_day_index + verification_days < len(filtered_data):
                                    verification_end_day = filtered_data.iloc[abnormal_up_day_index + verification_days]
                                    increase = (verification_end_day['收盘'] - abnormal_up_day['收盘']) / abnormal_up_day['收盘'] * 100
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
                                    five_day_increase = (five_day_end['收盘'] - abnormal_up_day['开盘']) / abnormal_up_day['开盘'] * 100
                                
                                # 计算10日涨幅
                                if abnormal_up_day_index + 9 < len(filtered_data):
                                    ten_day_end = filtered_data.iloc[abnormal_up_day_index + 9]
                                    ten_day_increase = (ten_day_end['收盘'] - abnormal_up_day['开盘']) / abnormal_up_day['开盘'] * 100
                                
                                # 计算20日涨幅
                                if abnormal_up_day_index + 19 < len(filtered_data):
                                    twenty_day_end = filtered_data.iloc[abnormal_up_day_index + 19]
                                    twenty_day_increase = (twenty_day_end['收盘'] - abnormal_up_day['开盘']) / abnormal_up_day['开盘'] * 100
                                
                                # 记录结果
                                total_cases += 1
                                if valid_verification:
                                    successful_cases += 1
                                
                                # 添加到匹配列表
                                match_start_date = filtered_data.iloc[i]['日期'].strftime('%Y-%m-%d')
                                match_end_date = abnormal_up_day['日期'].strftime('%Y-%m-%d')
                                
                                matches.append({
                                    'code': stock_code,
                                    'name': stock_name,
                                    'market': market,
                                    'period': f"{match_start_date} 至 {match_end_date}",
                                    'abnormal_up_day_date': abnormal_up_day['日期'].strftime('%Y-%m-%d'),
                                    'abnormal_up_day_open': abnormal_up_day['开盘'],
                                    'abnormal_up_day_close': abnormal_up_day['收盘'],
                                    f'{verification_days}_day_verification_result': '成功' if valid_verification else '失败',
                                    f'{verification_days}_day_increase': increase,
                                    '5_day_increase': five_day_increase,
                                    '10_day_increase': ten_day_increase,
                                    '20_day_increase': twenty_day_increase
                                })
    
    return {
        'matches': matches,
        'total_cases': total_cases,
        'successful_cases': successful_cases
    }
