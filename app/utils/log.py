import logging
import os
from logging.handlers import RotatingFileHandler

# 日志配置
def setup_logger():
    """设置日志配置"""
    # 创建日志目录
    log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'logs')
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # 日志文件路径
    log_file = os.path.join(log_dir, 'stock_finder.log')
    
    # 创建日志记录器
    logger = logging.getLogger('stock_finder')
    logger.setLevel(logging.INFO)
    
    # 创建文件处理器，设置日志文件大小和备份数量
    file_handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5)
    file_handler.setLevel(logging.INFO)
    
    # 创建控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # 设置日志格式
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # 添加处理器到日志记录器
    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
    
    return logger

# 创建全局日志记录器
logger = setup_logger()
