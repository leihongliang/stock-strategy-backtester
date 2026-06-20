from fastapi import APIRouter, HTTPException
from app.models.stock import StrategyValidationRequest, MultiStrategyBacktestRequest
import datetime
from app.services.strategies import (
    validate_strategy,
    validate_513_strategy as validate_513,
    multi_strategy_backtest,
    validate_macd_rejuvenation
)

router = APIRouter(prefix="/api/stocks", tags=["stocks-strategy"])

@router.post("/strategy/validate")
def validate_strategy_endpoint(request: StrategyValidationRequest):
    """根据策略从历史数据中找到符合的股票及其时间段区间，并验证之后几天的股票涨幅，计算策略的正确率
    
    目前支持的策略：
    - strategy1: 至少连续上涨≥4天（允许夹一根小阴线），出现放量大阳线，后续3天不跌破异动阳线的开盘价
    - strategy_513: 513战法，可自定义连续上涨天数和后续验证天数
    - rising_surge_3: 513战法的英文名称
    """
    try:
        result = validate_strategy(
            request.strategy_name,
            request.start_date,
            request.end_date
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"服务器内部错误: {str(e)}")

@router.post("/strategy/backtest")
def backtest_multi_strategy(request: MultiStrategyBacktestRequest):
    """多策略回测接口
    
    输入一组股票代码和时间段，运行预设的所有策略，分析每个策略结束后的收益。
    
    预设策略：
    - ma5_ma20_cross: MA5/MA20金叉死叉（MA5上穿MA20买，下穿MA20卖）
    - price_breakout_20w_10w: 价格突破20周最高买，跌破10周最低卖
    
    Args:
        request: 多策略回测请求
    """
    try:
        result = multi_strategy_backtest(
            request.stock_codes,
            request.start_date,
            request.end_date,
            request.strategies
        )
        
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"服务器内部错误: {str(e)}")

@router.post("/strategy/validate/513")
def validate_513_strategy(request: StrategyValidationRequest, consecutive_days: int = 4, verification_days: int = 3):
    """验证513战法（可自定义连续上涨天数和后续验证天数）
    
    Args:
        request: 策略验证请求
        consecutive_days: 连续上涨天数，默认4天
        verification_days: 后续验证天数，默认3天
    """
    try:
        if not request.start_date or not request.end_date:
            end_date = datetime.date.today()
            start_date = end_date - datetime.timedelta(days=7)
            request.start_date = start_date.strftime("%Y-%m-%d")
            request.end_date = end_date.strftime("%Y-%m-%d")
        
        result = validate_513(
            request.start_date,
            request.end_date,
            consecutive_days,
            verification_days,
            request.stock_codes
        )

        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"服务器内部错误: {str(e)}")

@router.post("/strategy/validate/macd-rejuvenation")
def validate_macd_rejuvenation_strategy(request: StrategyValidationRequest):
    """回春战法验证接口

    在指定时间段内扫描所有股票，寻找符合回春战法条件的股票。

    回春战法三要素（MACD参数10,20,9）：
    1. MACD金叉到第一个死叉，涨幅≥40%
    2. 死叉后出现的第一个金叉，在0轴附近
    3. 股价在60日均线之上，60日均线向上

    买点：即将金叉时买入，需有明显放量

    Args:
        request: 策略验证请求，包含start_date、end_date和可选的stock_codes
    """
    try:
        result = validate_macd_rejuvenation(
            request.start_date,
            request.end_date,
            request.stock_codes
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"服务器内部错误: {str(e)}")