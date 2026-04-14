import pandas as pd
from app.services.stock_service import StockService
from app.utils.log import logger

# Strategy registry
STRATEGIES = {}

def register_strategy(strategy_name):
    """Decorator to register a strategy function"""
    def decorator(func):
        STRATEGIES[strategy_name] = func
        return func
    return decorator

def validate_strategy(stock_service, strategy_name, start_date=None, end_date=None):
    """Validate stock strategy based on historical data
    
    Args:
        stock_service: StockService instance
        strategy_name: Strategy name
        start_date: Start date in "YYYY-MM-DD" format
        end_date: End date in "YYYY-MM-DD" format
        
    Returns:
        dict: Result containing matching stocks and strategy accuracy
    """
    # Check if strategy exists
    if strategy_name not in STRATEGIES:
        raise ValueError(f"Strategy '{strategy_name}' is not registered")
    
    all_stocks = stock_service.get_all_a_stocks()
    logger.info(f"Total A-shares: {len(all_stocks)}")
    
    matching_stocks = []
    total_cases = 0
    successful_cases = 0
    
    # Get the strategy function
    strategy_func = STRATEGIES[strategy_name]
    
    # Iterate through all stocks
    for index, stock in enumerate(all_stocks):
        # Process StockCompany object
        if hasattr(stock, 'sec_code'):
            # Format as 6-digit code
            stock_code = f"{stock.sec_code:06d}"
            stock_name = stock.sec_name
            market = stock.market
        else:
            # Compatible with old format
            stock_code = stock['代码']
            stock_name = stock['名称']
            market = stock['市场']
        
        # Load data from database
        k_data = stock_service.load_stock_data(stock_code)
        
        if k_data is not None:
            # Filter data within time range
            if start_date and end_date:
                start_date_dt = pd.to_datetime(start_date)
                end_date_dt = pd.to_datetime(end_date)
                filtered_data = k_data[(k_data['日期'] >= start_date_dt) & (k_data['日期'] <= end_date_dt)]
            else:
                filtered_data = k_data
            
            # Apply strategy
            strategy_result = strategy_func(filtered_data, stock_code, stock_name, market)
            
            # Update results
            if strategy_result:
                matching_stocks.extend(strategy_result['matches'])
                total_cases += strategy_result['total_cases']
                successful_cases += strategy_result['successful_cases']
        
        # Print progress every 100 stocks
        if (index + 1) % 100 == 0:
            logger.info(f"Processed {index + 1} stocks, found {len(matching_stocks)} matching stocks")
    
    # Calculate accuracy
    accuracy = (successful_cases / total_cases * 100) if total_cases > 0 else 0
    
    return {
        'matching_stocks': matching_stocks,
        'total_cases': total_cases,
        'successful_cases': successful_cases,
        'accuracy': accuracy
    }

@register_strategy('strategy1')
def strategy1(filtered_data, stock_code, stock_name, market):
    """Strategy 1 implementation: 4 consecutive up days + 1 abnormal up day + 3 days verification"""
    matches = []
    total_cases = 0
    successful_cases = 0
    
    # Check if there is enough data
    if len(filtered_data) >= 8:  # At least 4 consecutive up days + 1 abnormal up day + 3 days verification
        # Iterate through all possible consecutive combinations
        for i in range(len(filtered_data) - 7):  # 4 consecutive up days + 1 abnormal up day + 3 days verification
            # Check consecutive up days condition (at least 4 days, allow one small阴线)
            consecutive_up_days = 0
            has_small_red = False
            valid_up_trend = True
            
            for j in range(i, i + 4):
                if j >= len(filtered_data):
                    valid_up_trend = False
                    break
                
                # Calculate daily change
                if filtered_data.iloc[j]['收盘'] > filtered_data.iloc[j]['开盘']:
                    consecutive_up_days += 1
                else:
                    # Check if it's a small negative line
                    close = filtered_data.iloc[j]['收盘']
                    open_price = filtered_data.iloc[j]['开盘']
                    pre_close = filtered_data.iloc[j-1]['收盘'] if j > i else open_price
                    
                    # Small negative line definition: drop no more than 1%, and close still higher than previous day's close
                    if (open_price - close) / open_price <= 0.01 and close > pre_close:
                        has_small_red = True
                    else:
                        valid_up_trend = False
                        break
            
            if valid_up_trend and (consecutive_up_days >= 4 or (consecutive_up_days >= 3 and has_small_red)):
                # Check if the 5th day is a large volume up day
                abnormal_up_day_index = i + 4
                if abnormal_up_day_index < len(filtered_data):
                    abnormal_up_day = filtered_data.iloc[abnormal_up_day_index]
                    previous_day = filtered_data.iloc[abnormal_up_day_index - 1]
                    
                    # Check if it's a large up day (increase ≥3%)
                    if (abnormal_up_day['收盘'] - abnormal_up_day['开盘']) / abnormal_up_day['开盘'] >= 0.03:
                        # Check if there is an upper shadow (high to close drop)
                        if abnormal_up_day['最高'] > abnormal_up_day['收盘'] and (abnormal_up_day['最高'] - abnormal_up_day['收盘']) / abnormal_up_day['开盘'] >= 0.01:
                            # Check if volume is ≥2 times previous day's volume
                            if abnormal_up_day['成交量'] >= previous_day['成交量'] * 2:
                                # Verify next 3 days
                                valid_verification = True
                                abnormal_up_day_open = abnormal_up_day['开盘']
                                
                                for j in range(1, 4):
                                    verification_index = abnormal_up_day_index + j
                                    if verification_index >= len(filtered_data):
                                        valid_verification = False
                                        break
                                    
                                    # Check if it breaks below the abnormal up day's open price
                                    if filtered_data.iloc[verification_index]['最低'] < abnormal_up_day_open:
                                        valid_verification = False
                                        break
                                
                                # Calculate 3-day increase
                                if abnormal_up_day_index + 3 < len(filtered_data):
                                    verification_end_day = filtered_data.iloc[abnormal_up_day_index + 3]
                                    increase = (verification_end_day['收盘'] - abnormal_up_day['收盘']) / abnormal_up_day['收盘'] * 100
                                else:
                                    increase = None
                                    valid_verification = False
                                
                                # Record results
                                total_cases += 1
                                if valid_verification:
                                    successful_cases += 1
                                
                                # Add to matching list
                                match_start_date = filtered_data.iloc[i]['日期'].strftime('%Y-%m-%d')
                                match_end_date = abnormal_up_day['日期'].strftime('%Y-%m-%d')
                                
                                matches.append({
                                    'code': stock_code,
                                    'name': stock_name,
                                    'market': market,
                                    'period': f"{match_start_date} to {match_end_date}",
                                    'abnormal_up_day_date': abnormal_up_day['日期'].strftime('%Y-%m-%d'),
                                    'abnormal_up_day_open': abnormal_up_day['开盘'],
                                    'abnormal_up_day_close': abnormal_up_day['收盘'],
                                    '3_day_verification_result': 'Success' if valid_verification else 'Failure',
                                    '3_day_increase': increase
                                })
    
    return {
        'matches': matches,
        'total_cases': total_cases,
        'successful_cases': successful_cases
    }

@register_strategy('strategy2')
def strategy2(filtered_data, stock_code, stock_name, market):
    """Strategy 2 implementation: Simple golden cross strategy"""
    matches = []
    total_cases = 0
    successful_cases = 0
    
    # Check if there is enough data
    if len(filtered_data) >= 20:
        # Calculate moving averages
        filtered_data['MA5'] = filtered_data['收盘'].rolling(window=5).mean()
        filtered_data['MA20'] = filtered_data['收盘'].rolling(window=20).mean()
        
        # Iterate through data to find golden cross
        for i in range(1, len(filtered_data)):
            # Check for golden cross (MA5 crosses above MA20)
            if filtered_data.iloc[i-1]['MA5'] <= filtered_data.iloc[i-1]['MA20'] and \
               filtered_data.iloc[i]['MA5'] > filtered_data.iloc[i]['MA20']:
                # Record golden cross
                total_cases += 1
                
                # Verify next 5 days
                valid_verification = True
                golden_cross_price = filtered_data.iloc[i]['收盘']
                
                for j in range(1, 6):
                    verification_index = i + j
                    if verification_index >= len(filtered_data):
                        valid_verification = False
                        break
                    
                    # Check if price remains above golden cross price
                    if filtered_data.iloc[verification_index]['最低'] < golden_cross_price * 0.98:
                        valid_verification = False
                        break
                
                if valid_verification:
                    successful_cases += 1
                
                # Add to matching list
                golden_cross_date = filtered_data.iloc[i]['日期'].strftime('%Y-%m-%d')
                
                matches.append({
                    'code': stock_code,
                    'name': stock_name,
                    'market': market,
                    'golden_cross_date': golden_cross_date,
                    'golden_cross_price': golden_cross_price,
                    'verification_result': 'Success' if valid_verification else 'Failure'
                })
    
    return {
        'matches': matches,
        'total_cases': total_cases,
        'successful_cases': successful_cases
    }
