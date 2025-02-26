from app.plugin.plugin_system import Plugin, PluginMetadata
from typing import Dict, Any
from loguru import logger

PLUGIN_METADATA = PluginMetadata(
    name="DataProcessor",
    version="1.0.0",
    author="System",
    description="数据处理插件",
    dependencies=[],
    load_priority=50
)


class DataProcessor(Plugin):
    async def initialize(self):
        """初始化插件"""
        self.register_event_handler("data.received", self.process_data)
        self.register_hook("data_transform", self.transform_data)
        logger.info("DataProcessor plugin initialized")

    async def shutdown(self):
        """关闭插件"""
        logger.info("DataProcessor plugin shutdown")

    async def process_data(self, data: Dict[str, Any]):
        """处理接收到的数据"""
        try:
            # 数据处理逻辑
            processed_data = {
                "timestamp": data.get("timestamp"),
                "processed": True,
                "value": data.get("value", 0) * 2
            }
            return processed_data
        except Exception as e:
            logger.error(f"Data processing error: {e}")
            return None

    async def transform_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """数据转换钩子"""
        return {
            "transformed": True,
            "original": data,
            "timestamp": data.get("timestamp")
        }

    async def get_stats(self) -> Dict[str, Any]:
        """获取插件统计信息"""
        return {
            "processed_count": 0,
            "error_count": 0
        }
