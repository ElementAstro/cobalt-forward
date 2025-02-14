from loguru import logger
import sys
from pathlib import Path

def setup_logging(log_path: str = "logs"):
    """配置 loguru 日志系统"""
    # 创建日志目录
    log_dir = Path(log_path)
    log_dir.mkdir(exist_ok=True)

    # 移除默认处理器
    logger.remove()

    # 添加控制台处理器
    logger.add(sys.stderr, 
              format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
              level="INFO")

    # 添加文件处理器 - 常规日志
    logger.add(f"{log_path}/app.log",
              rotation="500 MB",
              retention="10 days",
              compression="zip",
              format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
              level="INFO")

    # 添加文件处理器 - 错误日志
    logger.add(f"{log_path}/error.log",
              rotation="100 MB",
              retention="30 days",
              compression="zip",
              format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
              level="ERROR")

    return logger
