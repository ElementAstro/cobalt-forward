from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
from dataclasses import dataclass
import time

@dataclass
class ComponentMetrics:
    """组件性能指标"""
    start_time: float = time.time()
    message_count: int = 0
    error_count: int = 0
    processing_times: list = None
    
    def __post_init__(self):
        self.processing_times = []
    
    def record_message(self, processing_time: float):
        self.message_count += 1
        self.processing_times.append(processing_time)
        if len(self.processing_times) > 1000:
            self.processing_times.pop(0)
            
    def record_error(self):
        self.error_count += 1
        
    @property
    def avg_processing_time(self) -> float:
        return sum(self.processing_times) / len(self.processing_times) if self.processing_times else 0.0

class BaseComponent(ABC):
    """核心组件基类"""
    
    def __init__(self):
        self._message_bus = None
        self._metrics = ComponentMetrics()
        self._transformers = []
        
    async def set_message_bus(self, message_bus):
        self._message_bus = message_bus
        
    async def register_transformer(self, transformer):
        self._transformers.append(transformer)
        
    @abstractmethod
    async def handle_message(self, topic: str, data: Any) -> None:
        pass
        
    async def _transform_data(self, data: Any) -> Any:
        """应用所有转换器"""
        start_time = time.time()
        try:
            result = data
            for transformer in self._transformers:
                result = await transformer.transform(result)
            processing_time = time.time() - start_time
            self._metrics.record_message(processing_time)
            return result
        except Exception as e:
            self._metrics.record_error()
            raise

    @property
    def metrics(self) -> Dict[str, Any]:
        """获取组件指标"""
        return {
            "message_count": self._metrics.message_count,
            "error_count": self._metrics.error_count,
            "avg_processing_time": self._metrics.avg_processing_time,
            "uptime": time.time() - self._metrics.start_time
        }
