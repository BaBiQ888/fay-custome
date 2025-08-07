import logging
import sys


def setup_asr_logging():
    """配置ASR相关的日志"""

    # 创建格式化器
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 配置ASR WebSocket服务器日志
    asr_logger = logging.getLogger('asr.asr_ws_server')
    asr_logger.setLevel(logging.INFO)

    # 创建控制台处理器
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    asr_logger.addHandler(console_handler)

    # 创建文件处理器
    file_handler = logging.FileHandler('logs/asr_server.log', encoding='utf-8')
    file_handler.setFormatter(formatter)
    asr_logger.addHandler(file_handler)

    return asr_logger


# 在启动时调用
if __name__ == "__main__":
    setup_asr_logging()
