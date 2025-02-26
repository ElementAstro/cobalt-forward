import pytest
import asyncio
import time
from app.core.base import BaseComponent
from app.core.shared_services import SharedServices, ServiceType
from typing import Any, Dict
import logging

# 配置日志
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class TestComponent(BaseComponent):
    """测试用组件类"""
    def __init__(self, name: str = "test_component"):
        super().__init__(name)
        self.started = False
        self.stopped = False
        self.messages = []
        self.transform_count = 0

    async def _start_impl(self):
        self.started = True

    async def _stop_impl(self):
        self.stopped = True

    async def handle_message(self, topic: str, data: Any):
        self.messages.append((topic, data))

    async def _health_check_impl(self):
        if not self.started:
            raise RuntimeError("Component not started")

class TestTransformer:
    """测试用转换器"""
    async def transform(self, data: Any) -> Any:
        if isinstance(data, dict):
            data['transformed'] = True
        return data

@pytest.fixture
async def component():
    """组件测试夹具"""
    comp = TestComponent()
    yield comp
    if comp.is_running():
        await comp.stop()

@pytest.mark.asyncio
async def test_component_lifecycle(component):
    """测试组件生命周期管理"""
    assert not component.is_running()
    assert not component.started
    assert not component.stopped

    # 测试启动
    await component.start()
    assert component.is_running()
    assert component.started
    assert component._start_time > 0

    # 测试停止
    await component.stop()
    assert not component.is_running()
    assert component.stopped
    assert component.metrics['uptime'] > 0

@pytest.mark.asyncio
async def test_transformer_integration(component):
    """测试转换器集成"""
    transformer = TestTransformer()
    component.add_transformer(transformer)

    # 测试数据转换
    data = {'test': 'data'}
    transformed_data = await component._transform_data(data)
    
    assert transformed_data['transformed']
    assert component.metrics['message_count'] == 1
    assert len(component.metrics['processing_times']) == 1

@pytest.mark.asyncio
async def test_error_handling(component):
    """测试错误处理"""
    error_handled = False

    async def error_handler(error):
        nonlocal error_handled
        error_handled = True

    # 注册错误处理器
    component.shared_services.register_error_handler(
        'RuntimeError',
        error_handler
    )

    # 触发错误
    with pytest.raises(RuntimeError):
        await component._transform_data(None)

    assert error_handled
    assert component.metrics['error_count'] == 1
    assert component.metrics['last_error'] is not None

@pytest.mark.asyncio
async def test_lifecycle_hooks(component):
    """测试生命周期钩子"""
    hooks_called = []

    async def before_start_hook(comp):
        hooks_called.append('before_start')

    async def after_start_hook(comp):
        hooks_called.append('after_start')

    async def before_stop_hook(comp):
        hooks_called.append('before_stop')

    async def after_stop_hook(comp):
        hooks_called.append('after_stop')

    # 添加钩子
    component.add_lifecycle_hook('before_start', before_start_hook)
    component.add_lifecycle_hook('after_start', after_start_hook)
    component.add_lifecycle_hook('before_stop', before_stop_hook)
    component.add_lifecycle_hook('after_stop', after_stop_hook)

    # 执行生命周期
    await component.start()
    await component.stop()

    assert hooks_called == ['before_start', 'after_start', 'before_stop', 'after_stop']

@pytest.mark.asyncio
async def test_health_check(component):
    """测试健康检查"""
    # 未启动时的健康检查
    health_status = await component.check_health()
    assert not health_status['healthy']
    assert not health_status['running']

    # 启动后的健康检查
    await component.start()
    health_status = await component.check_health()
    assert health_status['healthy']
    assert health_status['running']
    assert health_status['uptime'] > 0

@pytest.mark.asyncio
async def test_metrics_collection(component):
    """测试指标收集"""
    await component.start()

    # 生成一些测试数据
    for i in range(5):
        data = {'test': i}
        await component._transform_data(data)
        await asyncio.sleep(0.1)

    metrics = component.metrics
    shared_metrics = metrics['shared_services']

    # 验证组件指标
    assert metrics['message_count'] == 5
    assert len(metrics['processing_times']) == 5
    assert metrics['avg_processing_time'] > 0

    # 验证共享服务指标
    assert ServiceType.TRANSFORM.name in shared_metrics
    transform_metrics = shared_metrics[ServiceType.TRANSFORM.name]
    assert transform_metrics['calls'] > 0
    assert 'avg_time' in transform_metrics

@pytest.mark.asyncio
async def test_message_handling(component):
    """测试消息处理"""
    await component.start()

    # 发送测试消息
    test_messages = [
        ('test.topic1', {'data': 1}),
        ('test.topic2', {'data': 2}),
    ]

    for topic, data in test_messages:
        await component.handle_message(topic, data)

    # 验证消息处理
    assert len(component.messages) == len(test_messages)
    for (topic, data), (handled_topic, handled_data) in zip(test_messages, component.messages):
        assert topic == handled_topic
        assert data == handled_data

@pytest.mark.asyncio
async def test_shared_services_integration(component):
    """测试共享服务集成"""
    # 测试数据转换服务
    def test_transformer(data):
        return {'transformed': data}

    component.shared_services.register_transformer('test', test_transformer)
    result = await component.shared_services.transform_data({'test': 'data'}, 'test')
    assert 'transformed' in result

    # 测试数据验证服务
    def test_validator(data):
        return isinstance(data, dict)

    component.shared_services.register_validator('test', test_validator)
    is_valid = await component.shared_services.validate_data({'test': 'data'}, 'test')
    assert is_valid

    # 验证指标收集
    metrics = component.shared_services.get_metrics()
    assert ServiceType.TRANSFORM.name in metrics
    assert ServiceType.VALIDATION.name in metrics

def test_utility_functions():
    """测试共享服务工具函数"""
    services = SharedServices()

    # 测试深度合并字典
    dict1 = {'a': 1, 'b': {'c': 2}}
    dict2 = {'b': {'d': 3}, 'e': 4}
    merged = services.deep_merge_dicts(dict1, dict2)
    assert merged == {'a': 1, 'b': {'c': 2, 'd': 3}, 'e': 4}

    # 测试嵌套值获取和设置
    data = {'a': {'b': {'c': 1}}}
    value = services.get_nested_value(data, 'a.b.c')
    assert value == 1

    services.set_nested_value(data, 'a.b.d', 2)
    assert data['a']['b']['d'] == 2

    # 测试时间格式化
    duration = services.format_duration(3661.123)
    assert 'h' in duration
    assert 'm' in duration
    assert 's' in duration
    assert 'ms' in duration

if __name__ == '__main__':
    pytest.main(['-v', __file__])