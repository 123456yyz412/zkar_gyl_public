import logging
import os
from pathlib import Path
from datetime import datetime
from typing import Optional
from skjAPI.shukuajing_config.skj_config import log_win_path,log_linux_path

class SKJLogger:
    def __init__(
        self,
        name: str = "shukuajing",
        log_dir: str = "./logs",
        level: int = logging.INFO
    ):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

        # 创建日志目录
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)

        # 设置文件名格式
        log_file = self.log_dir / f"{name}/{datetime.now().strftime('%Y%m%d')}.log"

        # 通用格式
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
        )

        # 文件处理器
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

        # 控制台处理器（可选）
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

    def get_logger(self):
        # 返回类属性，该属性是logging类的实例对象
        return self.logger

# 全局日志实例
logger = SKJLogger('数跨境',log_dir= log_win_path).get_logger()

"""
(1):        logger.info(f"成功获取项目列表，数据量：{len(data.get('data', []))}")

(2)
# 原print语句
# print(f"Error fetching API tables: {e}")

# 改为结构化日志
logger.error(
    "接口请求异常 | 功能:获取表数据 | 项目:%s | 错误类型:%s | 详情:%s",
    project_name,
    type(e).__name__,
    str(e)
)
"""
