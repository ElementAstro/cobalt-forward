import pytest
import asyncio
import time
import psutil
import os
import gc
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Any
from app.core.base import BaseComponent
from app.core.shared_services import SharedServices, ServiceType
import logging
import statistics
import json
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PerformanceTestComponent(BaseComponent):
    """性能测试专用组件"""
    def __init__(self, name: str = "perf_test"):
        super().__init__(name)
        self.processed_count = 0
        self.processing_times = []
        
    async def handle_message(self, topic: str, data: Any):
        start_time = time.time()
        # 模拟消息处理
        await asyncio.sleep(0.001)  # 1ms处理时间
        self.processed_count += 1
        self.processing_times.append(time.time() - start_time)

class PerformanceMetrics:
    """性能指标收集器"""
    def __init__(self):
        self.start_time = time.time()
        self.end_time = 0
        self.operations = []
        self.memory_samples = []
        self.cpu_samples = []
        self.process = psutil.Process()

    def record_operation(self, duration: float):
        """记录操作耗时"""
        self.operations.append(duration)

    def sample_system_metrics(self):
        """采样系统指标"""
        self.memory_samples.append(self.process.memory_info().rss)
        self.cpu_samples.append(self.process.cpu_percent())

    def complete(self):
        """完成性能收集"""
        self.end_time = time.time()

    def get_report(self) -> Dict[str, Any]:
        """生成性能报告"""
        duration = self.end_time - self.start_time
        ops = len(self.operations)
        
        return {
            'duration_seconds': duration,
            'total_operations': ops,
            'operations_per_second': ops / duration if duration > 0 else 0,
            'latency': {
                'min_ms': min(self.operations) * 1000 if self.operations else 0,
                'max_ms': max(self.operations) * 1000 if self.operations else 0,
                'avg_ms': statistics.mean(self.operations) * 1000 if self.operations else 0,
                'p95_ms': statistics.quantiles(self.operations, n=20)[18] * 1000 if len(self.operations) >= 20 else 0,
                'p99_ms': statistics.quantiles(self.operations, n=100)[98] * 1000 if len(self.operations) >= 100 else 0
            },
            'memory': {
                'min_mb': min(self.memory_samples) / (1024 * 1024),
                'max_mb': max(self.memory_samples) / (1024 * 1024),
                'avg_mb': statistics.mean(self.memory_samples) / (1024 * 1024)
            },
            'cpu': {
                'min_percent': min(self.cpu_samples),
                'max_percent': max(self.cpu_samples),
                'avg_percent': statistics.mean(self.cpu_samples)
            }
        }

    def save_report(self, filename: str):
        """保存性能报告到文件"""
        report = self.get_report()
        report['timestamp'] = datetime.now().isoformat()
        
        with open(filename, 'w') as f:
            json.dump(report, f, indent=2)

@pytest.fixture
async def perf_component():
    """组件性能测试夹具"""
    comp = PerformanceTestComponent()
    await comp.start()
    yield comp
    await comp.stop()

@pytest.fixture
def metrics():
    """性能指标收集器夹具"""
    return PerformanceMetrics()

async def generate_test_messages(count: int) -> List[tuple]:
    """生成测试消息"""
    return [
        (f"test.topic.{i}", {"id": i, "data": "test" * 100})
        for i in range(count)
    ]

@pytest.mark.asyncio
async def test_message_processing_performance(perf_component, metrics):
    """测试消息处理性能"""
    MESSAGE_COUNT = 10000
    BATCH_SIZE = 100
    
    # 生成测试消息
    messages = await generate_test_messages(MESSAGE_COUNT)
    
    # 分批处理消息
    for i in range(0, MESSAGE_COUNT, BATCH_SIZE):
        batch = messages[i:i+BATCH_SIZE]
        batch_start = time.time()
        
        # 并发处理批次消息
        tasks = [
            perf_component.handle_message(topic, data)
            for topic, data in batch
        ]
        await asyncio.gather(*tasks)
        
        metrics.record_operation(time.time() - batch_start)
        metrics.sample_system_metrics()
        
        # 允许系统短暂休息
        await asyncio.sleep(0.01)
    
    metrics.complete()
    report = metrics.get_report()
    
    # 验证性能指标
    assert report['total_operations'] == MESSAGE_COUNT // BATCH_SIZE
    assert report['operations_per_second'] > 50  # 期望至少每秒处理50批
    assert report['latency']['avg_ms'] < 100  # 平均延迟小于100ms
    assert report['latency']['p99_ms'] < 200  # 99分位延迟小于200ms

@pytest.mark.asyncio
async def test_concurrent_component_lifecycle(metrics):
    """测试并发组件生命周期性能"""
    COMPONENT_COUNT = 100
    
    # 创建多个组件
    components = [PerformanceTestComponent(f"perf_{i}") for i in range(COMPONENT_COUNT)]
    
    # 测试并发启动
    start_time = time.time()
    await asyncio.gather(*(comp.start() for comp in components))
    metrics.record_operation(time.time() - start_time)
    
    # 测试并发停止
    start_time = time.time()
    await asyncio.gather(*(comp.stop() for comp in components))
    metrics.record_operation(time.time() - start_time)
    
    metrics.complete()
    report = metrics.get_report()
    
    # 验证性能指标
    assert report['latency']['avg_ms'] < 500  # 平均启动/停止时间小于500ms

@pytest.mark.asyncio
async def test_shared_services_performance(perf_component, metrics):
    """测试共享服务性能"""
    OPERATION_COUNT = 10000
    
    # 注册测试转换器和验证器
    def test_transformer(data: Dict) -> Dict:
        return {'transformed': data}
    
    def test_validator(data: Dict) -> bool:
        return isinstance(data, dict)
    
    perf_component.shared_services.register_transformer('test', test_transformer)
    perf_component.shared_services.register_validator('test', test_validator)
    
    # 测试转换性能
    for _ in range(OPERATION_COUNT):
        start_time = time.time()
        await perf_component.shared_services.transform_data({'test': 'data'}, 'test')
        metrics.record_operation(time.time() - start_time)
        
        if _ % 100 == 0:  # 每100次操作采样一次系统指标
            metrics.sample_system_metrics()
    
    metrics.complete()
    report = metrics.get_report()
    
    # 验证性能指标
    assert report['operations_per_second'] > 1000  # 期望每秒至少1000次转换
    assert report['latency']['p99_ms'] < 10  # 99分位延迟小于10ms

@pytest.mark.asyncio
async def test_memory_usage(perf_component, metrics):
    """测试内存使用"""
    ITERATION_COUNT = 1000
    
    # 收集初始内存使用
    initial_memory = psutil.Process().memory_info().rss
    
    # 执行密集操作
    for _ in range(ITERATION_COUNT):
        data = {'test': 'x' * 1000}  # 1KB的数据
        await perf_component._transform_data(data)
        
        if _ % 10 == 0:
            metrics.sample_system_metrics()
            gc.collect()  # 强制垃圾回收
    
    metrics.complete()
    report = metrics.get_report()
    
    # 验证内存使用
    memory_increase = max(metrics.memory_samples) - initial_memory
    assert memory_increase / (1024 * 1024) < 100  # 内存增长不超过100MB

@pytest.mark.asyncio
async def test_cpu_load(perf_component, metrics):
    """测试CPU负载"""
    DURATION = 5  # 测试5秒
    INTERVAL = 0.1  # 每0.1秒采样一次
    
    async def cpu_intensive_task():
        """CPU密集型任务"""
        for _ in range(1000):
            data = {'test': 'x' * 1000}
            await perf_component._transform_data(data)
    
    # 启动多个CPU密集型任务
    tasks = [cpu_intensive_task() for _ in range(os.cpu_count() or 4)]
    
    # 监控CPU使用
    start_time = time.time()
    while time.time() - start_time < DURATION:
        metrics.sample_system_metrics()
        await asyncio.sleep(INTERVAL)
    
    await asyncio.gather(*tasks)
    metrics.complete()
    
    report = metrics.get_report()
    # 验证CPU使用率不超过预期
    assert report['cpu']['avg_percent'] < 80  # 平均CPU使用率不超过80%

@pytest.mark.asyncio
async def test_network_io_simulation(perf_component, metrics):
    """模拟网络IO性能测试"""
    MESSAGE_SIZE = 1024 * 1024  # 1MB消息
    MESSAGE_COUNT = 100
    
    # 模拟网络延迟
    async def simulate_network_latency():
        await asyncio.sleep(0.05)  # 模拟50ms网络延迟
    
    # 处理大型消息
    for _ in range(MESSAGE_COUNT):
        start_time = time.time()
        
        # 模拟发送大型消息
        data = {'payload': 'x' * MESSAGE_SIZE}
        await simulate_network_latency()
        await perf_component.handle_message('test.large', data)
        
        metrics.record_operation(time.time() - start_time)
        metrics.sample_system_metrics()
    
    metrics.complete()
    report = metrics.get_report()
    
    # 验证网络性能指标
    assert report['operations_per_second'] > 10  # 期望每秒至少处理10个大型消息
    assert report['latency']['avg_ms'] < 100  # 平均延迟小于100ms

def test_save_performance_report(metrics):
    """测试性能报告保存"""
    # 生成一些测试数据
    for _ in range(100):
        metrics.record_operation(0.001)
        metrics.sample_system_metrics()
    
    metrics.complete()
    
    # 保存报告
    report_file = "performance_report.json"
    metrics.save_report(report_file)
    
    # 验证报告文件
    assert os.path.exists(report_file)
    with open(report_file, 'r') as f:
        report = json.load(f)
    
    assert 'timestamp' in report
    assert 'duration_seconds' in report
    assert 'total_operations' in report
    assert 'latency' in report
    assert 'memory' in report
    assert 'cpu' in report
    
    # 清理测试文件
    os.remove(report_file)

if __name__ == '__main__':
    pytest.main(['-v', __file__])