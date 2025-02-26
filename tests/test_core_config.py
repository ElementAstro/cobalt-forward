import pytest
import os
import tempfile
import yaml
from app.core.core_config import (
    CoreConfig,
    SharedServicesConfig,
    ComponentConfig,
    LogLevel,
    load_config
)

@pytest.fixture
def sample_config_dict():
    """示例配置字典"""
    return {
        'env': 'testing',
        'debug': True,
        'shared_services': {
            'log_level': 'DEBUG',
            'max_concurrent_transformations': 200,
            'metrics_history_size': 2000
        },
        'default_component': {
            'name': 'test_component',
            'startup_timeout': 60.0,
            'max_message_size': 2048576
        }
    }

@pytest.fixture
def temp_config_file(sample_config_dict):
    """临时配置文件"""
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.yaml') as f:
        yaml.dump(sample_config_dict, f)
        temp_path = f.name
    
    yield temp_path
    
    # 清理临时文件
    if os.path.exists(temp_path):
        os.unlink(temp_path)

def test_default_config():
    """测试默认配置"""
    config = CoreConfig()
    
    assert config.env == 'development'
    assert not config.debug
    assert isinstance(config.shared_services, SharedServicesConfig)
    assert isinstance(config.default_component, ComponentConfig)
    assert config.validate()

def test_config_from_dict(sample_config_dict):
    """测试从字典创建配置"""
    config = CoreConfig.from_dict(sample_config_dict)
    
    assert config.env == 'testing'
    assert config.debug is True
    assert config.shared_services.log_level == LogLevel.DEBUG
    assert config.shared_services.max_concurrent_transformations == 200
    assert config.shared_services.metrics_history_size == 2000
    assert config.default_component.name == 'test_component'
    assert config.default_component.startup_timeout == 60.0
    assert config.default_component.max_message_size == 2048576

def test_config_to_dict():
    """测试配置转换为字典"""
    config = CoreConfig()
    config_dict = config.to_dict()
    
    assert isinstance(config_dict, dict)
    assert 'env' in config_dict
    assert 'shared_services' in config_dict
    assert 'default_component' in config_dict
    
    # 验证嵌套配置
    assert isinstance(config_dict['shared_services'], dict)
    assert isinstance(config_dict['default_component'], dict)

def test_config_validation():
    """测试配置验证"""
    config = CoreConfig()
    assert config.validate()
    
    # 测试无效环境
    invalid_config = CoreConfig()
    invalid_config.env = 'invalid'
    assert not invalid_config.validate()
    
    # 测试无效限制值
    invalid_config = CoreConfig()
    invalid_config.max_components = -1
    assert not invalid_config.validate()

def test_environment_overrides(monkeypatch):
    """测试环境变量覆盖"""
    # 设置环境变量
    monkeypatch.setenv('APP_CORE_DEBUG', 'true')
    monkeypatch.setenv('APP_CORE_MAX_COMPONENTS', '200')
    monkeypatch.setenv('APP_CORE_OPERATION_TIMEOUT', '45.5')
    
    config = CoreConfig()
    overrides = config.get_environment_overrides()
    
    assert overrides['debug'] is True
    assert overrides['max_components'] == 200
    assert overrides['operation_timeout'] == 45.5

def test_load_config_from_file(temp_config_file):
    """测试从文件加载配置"""
    config = load_config(temp_config_file)
    
    assert config.env == 'testing'
    assert config.debug is True
    assert config.shared_services.log_level == LogLevel.DEBUG
    assert config.validate()

def test_load_config_with_env_override(temp_config_file, monkeypatch):
    """测试加载配置时的环境变量覆盖"""
    monkeypatch.setenv('APP_CORE_DEBUG', 'false')
    
    config = load_config(temp_config_file)
    assert config.debug is False

def test_shared_services_config():
    """测试共享服务配置"""
    config = SharedServicesConfig(
        log_level=LogLevel.INFO,
        max_concurrent_transformations=150,
        metrics_sampling_interval=2.0
    )
    
    assert config.log_level == LogLevel.INFO
    assert config.max_concurrent_transformations == 150
    assert config.metrics_sampling_interval == 2.0

def test_component_config():
    """测试组件配置"""
    config = ComponentConfig(
        name="custom_component",
        enabled=True,
        startup_timeout=45.0,
        processing_threads=8
    )
    
    assert config.name == "custom_component"
    assert config.enabled is True
    assert config.startup_timeout == 45.0
    assert config.processing_threads == 8

def test_config_type_conversion(monkeypatch):
    """测试配置类型转换"""
    monkeypatch.setenv('APP_CORE_MAX_COMPONENTS', '100')
    monkeypatch.setenv('APP_CORE_DEBUG', 'true')
    monkeypatch.setenv('APP_CORE_OPERATION_TIMEOUT', '30.5')
    
    config = load_config()
    
    assert isinstance(config.max_components, int)
    assert isinstance(config.debug, bool)
    assert isinstance(config.shared_services.operation_timeout, float)

def test_invalid_config_file():
    """测试无效配置文件处理"""
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.yaml') as f:
        f.write("invalid: yaml: content")
        temp_path = f.name
    
    try:
        with pytest.raises(yaml.YAMLError):
            load_config(temp_path)
    finally:
        os.unlink(temp_path)

def test_missing_config_file():
    """测试缺失配置文件处理"""
    non_existent_path = "/path/to/nonexistent/config.yaml"
    
    # 应该返回默认配置
    config = load_config(non_existent_path)
    assert isinstance(config, CoreConfig)
    assert config.validate()

def test_config_merge():
    """测试配置合并"""
    base_config = CoreConfig()
    override_dict = {
        'debug': True,
        'shared_services': {
            'log_level': 'DEBUG',
            'max_retries': 5
        }
    }
    
    merged_config = CoreConfig.from_dict({
        **base_config.to_dict(),
        **override_dict
    })
    
    assert merged_config.debug is True
    assert merged_config.shared_services.log_level == LogLevel.DEBUG
    assert merged_config.shared_services.max_retries == 5
    # 确保其他配置保持不变
    assert merged_config.max_components == base_config.max_components

def test_config_edge_cases():
    """测试边界情况"""
    # 测试空字典
    empty_config = CoreConfig.from_dict({})
    assert empty_config.validate()
    
    # 测试None值
    none_config = CoreConfig.from_dict({
        'shared_services': None,
        'default_component': None
    })
    assert isinstance(none_config.shared_services, SharedServicesConfig)
    assert isinstance(none_config.default_component, ComponentConfig)
    
    # 测试超大值
    large_config = CoreConfig.from_dict({
        'max_components': 1000000,
        'system_memory_limit': 1000000000000
    })
    assert large_config.validate()

if __name__ == '__main__':
    pytest.main(['-v', __file__])