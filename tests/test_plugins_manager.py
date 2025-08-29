"""
Tests for plugin manager implementation.

测试插件管理器实现功能。
"""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import Mock, AsyncMock, patch
from typing import List, Dict, Any, Optional

from cobalt_forward.plugins.manager import PluginManager
from cobalt_forward.core.interfaces.plugins import IPlugin


# 测试用的插件类
class MockPlugin(IPlugin):
    """测试插件"""
    
    def __init__(self, name: str = "test_plugin", dependencies: Optional[List[str]] = None):
        self._name = name
        self._dependencies = dependencies or []
        self._version = "1.0.0"
        self.initialized = False
        self.started = False
        self.stopped = False

    @property
    def name(self) -> str:
        return self._name

    @property
    def version(self) -> str:
        return self._version

    @property
    def dependencies(self) -> List[str]:
        return self._dependencies

    @property
    def metadata(self) -> Dict[str, Any]:
        return {"name": self._name, "version": self._version}

    @property
    def permissions(self) -> set[str]:
        return set()

    async def initialize(self, context: Any) -> None:
        self.initialized = True

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def shutdown(self) -> None:
        self.stopped = True

    async def configure(self, config: Dict[str, Any]) -> None:
        pass

    async def check_health(self) -> Dict[str, Any]:
        return {
            'healthy': True,
            'status': 'running' if self.started else 'stopped'
        }


class FailingPlugin(IPlugin):
    """失败的插件"""
    
    @property
    def name(self) -> str:
        return "failing_plugin"

    @property
    def version(self) -> str:
        return "1.0.0"

    @property
    def dependencies(self) -> List[str]:
        return []

    @property
    def metadata(self) -> Dict[str, Any]:
        return {"name": "failing_plugin", "version": "1.0.0"}

    @property
    def permissions(self) -> set[str]:
        return set()

    async def initialize(self, context: Any) -> None:
        raise Exception("Initialize failed")

    async def start(self) -> None:
        raise Exception("Start failed")

    async def stop(self) -> None:
        raise Exception("Stop failed")

    async def shutdown(self) -> None:
        raise Exception("Shutdown failed")

    async def configure(self, config: Dict[str, Any]) -> None:
        raise Exception("Configure failed")

    async def check_health(self) -> Dict[str, Any]:
        raise Exception("Health check failed")


@pytest.mark.asyncio
class MockPluginManager:
    """测试PluginManager类"""

    def setup_method(self) -> None:
        """测试前设置"""
        self.config = {
            'plugin_directory': 'test_plugins',
            'auto_load': False,
            'sandbox_enabled': True,
            'allowed_modules': ['os', 'sys'],
            'blocked_modules': ['subprocess']
        }
        self.container = Mock()
        self.plugin_manager = PluginManager(self.config, self.container)

    async def test_plugin_manager_init(self) -> None:
        """测试插件管理器初始化"""
        assert self.plugin_manager.name == "PluginManager"
        assert self.plugin_manager.version == "1.0.0"
        assert self.plugin_manager._running is False
        assert len(self.plugin_manager._plugins) == 0
        assert self.plugin_manager._plugin_directory == 'test_plugins'
        assert self.plugin_manager._auto_load is False
        assert self.plugin_manager._sandbox_enabled is True

    async def test_plugin_manager_start(self) -> None:
        """测试插件管理器启动"""
        # Mock依赖
        config_manager = Mock()
        event_bus = Mock()
        self.container.resolve.side_effect = lambda name: {
            "ConfigManager": config_manager,
            "IEventBus": event_bus
        }[name]
        
        await self.plugin_manager.start()
        
        assert self.plugin_manager._running is True
        assert self.plugin_manager._plugin_context is not None

    async def test_plugin_manager_start_dependency_failure(self) -> None:
        """测试插件管理器启动时依赖解析失败"""
        self.container.resolve.side_effect = Exception("Dependency not found")
        
        await self.plugin_manager.start()
        
        assert self.plugin_manager._running is True
        assert self.plugin_manager._plugin_context is None

    async def test_plugin_manager_stop(self) -> None:
        """测试插件管理器停止"""
        await self.plugin_manager.start()
        
        # 添加一些插件
        plugin = MockPlugin()
        self.plugin_manager._plugins["test"] = plugin
        
        with patch.object(self.plugin_manager, 'unload_plugin') as mock_unload:
            await self.plugin_manager.stop()
            
            mock_unload.assert_called_once_with("test")
        
        assert self.plugin_manager._running is False

    async def test_plugin_manager_configure(self) -> None:
        """测试插件管理器配置"""
        new_config = {
            'plugin_directory': 'new_plugins',
            'auto_load': True
        }
        
        await self.plugin_manager.configure(new_config)
        
        assert self.plugin_manager._plugin_directory == 'new_plugins'
        assert self.plugin_manager._auto_load is True

    async def test_plugin_manager_check_health_all_healthy(self) -> None:
        """测试插件管理器健康检查（所有插件健康）"""
        plugin1 = MockPlugin("plugin1")
        plugin2 = MockPlugin("plugin2")
        
        self.plugin_manager._plugins = {
            "plugin1": plugin1,
            "plugin2": plugin2
        }
        self.plugin_manager._running = True
        
        health = await self.plugin_manager.check_health()
        
        assert health['healthy'] is True
        assert health['status'] == 'running'
        assert health['details']['loaded_plugins'] == 2
        assert 'plugin1' in health['details']['plugins']
        assert 'plugin2' in health['details']['plugins']

    async def test_load_plugin_success(self) -> None:
        """测试成功加载插件"""
        plugin_path = "/path/to/plugin.py"
        
        with patch.object(self.plugin_manager, '_load_plugin_module') as mock_load_module, \
             patch.object(self.plugin_manager, '_find_plugin_class') as mock_find_class, \
             patch.object(self.plugin_manager, '_check_dependencies') as mock_check_deps:
            
            # Setup mocks
            mock_module = Mock()
            mock_load_module.return_value = mock_module
            
            mock_plugin_class = Mock(return_value=MockPlugin())
            mock_find_class.return_value = mock_plugin_class
            
            mock_check_deps.return_value = True
            
            result = await self.plugin_manager.load_plugin(plugin_path)
            
            assert result is True
            assert "test_plugin" in self.plugin_manager._plugins
            assert self.plugin_manager._plugin_paths["test_plugin"] == plugin_path

    async def test_load_plugin_no_plugin_class(self) -> None:
        """测试加载插件时找不到插件类"""
        plugin_path = "/path/to/plugin.py"
        
        with patch.object(self.plugin_manager, '_load_plugin_module') as mock_load_module, \
             patch.object(self.plugin_manager, '_find_plugin_class') as mock_find_class:
            
            mock_module = Mock()
            mock_load_module.return_value = mock_module
            mock_find_class.return_value = None
            
            result = await self.plugin_manager.load_plugin(plugin_path)
            
            assert result is False

    async def test_unload_plugin_success(self) -> None:
        """测试成功卸载插件"""
        plugin = MockPlugin()
        self.plugin_manager._plugins["test_plugin"] = plugin
        self.plugin_manager._plugin_paths["test_plugin"] = "/path/to/plugin.py"
        
        result = await self.plugin_manager.unload_plugin("test_plugin")
        
        assert result is True
        assert "test_plugin" not in self.plugin_manager._plugins
        assert "test_plugin" not in self.plugin_manager._plugin_paths
        assert plugin.stopped is True

    async def test_unload_plugin_not_found(self) -> None:
        """测试卸载不存在的插件"""
        result = await self.plugin_manager.unload_plugin("nonexistent")
        
        assert result is False

    def test_get_plugin(self) -> None:
        """测试获取插件"""
        plugin = MockPlugin()
        self.plugin_manager._plugins["test_plugin"] = plugin
        
        result = self.plugin_manager.get_plugin("test_plugin")
        assert result == plugin
        
        result = self.plugin_manager.get_plugin("nonexistent")
        assert result is None

    def test_get_all_plugins(self) -> None:
        """测试获取所有插件"""
        plugin1 = MockPlugin("plugin1")
        plugin2 = MockPlugin("plugin2")
        
        self.plugin_manager._plugins = {
            "plugin1": plugin1,
            "plugin2": plugin2
        }
        
        result = self.plugin_manager.get_all_plugins()
        
        assert len(result) == 2
        assert result["plugin1"] == plugin1
        assert result["plugin2"] == plugin2

    async def test_discover_plugins(self) -> None:
        """测试插件发现"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # 创建测试插件文件
            plugin_file1 = Path(temp_dir) / "plugin1.py"
            plugin_file2 = Path(temp_dir) / "plugin2.py"
            plugin_file1.write_text("# Plugin 1")
            plugin_file2.write_text("# Plugin 2")
            
            # 创建__init__.py文件（应该被忽略）
            init_file = Path(temp_dir) / "__init__.py"
            init_file.write_text("# Init file")
            
            result = await self.plugin_manager.discover_plugins(temp_dir)
            
            assert len(result) == 2
            assert str(plugin_file1) in result
            assert str(plugin_file2) in result
            assert str(init_file) not in result

    async def test_discover_plugins_nonexistent_directory(self) -> None:
        """测试在不存在的目录中发现插件"""
        result = await self.plugin_manager.discover_plugins("/nonexistent/directory")
        
        assert len(result) == 0

    def test_is_plugin_loaded(self) -> None:
        """测试检查插件是否已加载"""
        plugin = MockPlugin()
        self.plugin_manager._plugins["test_plugin"] = plugin
        
        assert self.plugin_manager.is_plugin_loaded("test_plugin") is True
        assert self.plugin_manager.is_plugin_loaded("nonexistent") is False

    async def test_get_plugin_dependencies(self) -> None:
        """测试获取插件依赖"""
        plugin = MockPlugin(dependencies=["dep1", "dep2"])
        self.plugin_manager._plugins["test_plugin"] = plugin
        
        result = await self.plugin_manager.get_plugin_dependencies("test_plugin")
        
        assert result == ["dep1", "dep2"]

    async def test_get_plugin_dependencies_not_found(self) -> None:
        """测试获取不存在插件的依赖"""
        result = await self.plugin_manager.get_plugin_dependencies("nonexistent")
        
        assert result == []

    @patch('importlib.util.spec_from_file_location')
    @patch('importlib.util.module_from_spec')
    async def test_load_plugin_module(self, mock_module_from_spec: Mock, mock_spec_from_file: Mock) -> None:
        """测试加载插件模块"""
        plugin_path = "/path/to/plugin.py"
        
        # Setup mocks
        mock_spec = Mock()
        mock_loader = Mock()
        mock_spec.loader = mock_loader
        mock_spec_from_file.return_value = mock_spec
        
        mock_module = Mock()
        mock_module_from_spec.return_value = mock_module
        
        result = await self.plugin_manager._load_plugin_module(plugin_path)
        
        assert result == mock_module
        mock_spec_from_file.assert_called_once_with("plugin", plugin_path)
        mock_loader.exec_module.assert_called_once_with(mock_module)

    def test_find_plugin_class(self) -> None:
        """测试在模块中查找插件类"""
        # 创建模拟插件类
        class MockPluginClass(IPlugin):
            @property
            def name(self) -> str:
                return "test"
            
            @property
            def version(self) -> str:
                return "1.0.0"
            
            @property
            def dependencies(self) -> List[str]:
                return []
            
            @property
            def metadata(self) -> Dict[str, Any]:
                return {}
            
            @property
            def permissions(self) -> set[str]:
                return set()
            
            async def initialize(self, context: Any) -> None:
                pass
            
            async def start(self) -> None:
                pass
            
            async def stop(self) -> None:
                pass
            
            async def shutdown(self) -> None:
                pass
            
            async def configure(self, config: Dict[str, Any]) -> None:
                pass
            
            async def check_health(self) -> Dict[str, Any]:
                return {}
        
        # 创建mock模块并设置属性
        mock_module = Mock()
        mock_module.MockPluginClass = MockPluginClass
        mock_module.SomeOtherClass = str
        
        with patch('builtins.dir', return_value=['MockPluginClass', 'SomeOtherClass']):
            result = self.plugin_manager._find_plugin_class(mock_module)
            
            # 验证找到了插件类
            assert result == MockPluginClass

    def test_find_plugin_class_not_found(self) -> None:
        """测试在模块中找不到插件类"""
        mock_module = Mock()
        mock_module.SomeClass = str
        mock_module.some_function = lambda: None
        
        with patch('builtins.dir', return_value=['SomeClass', 'some_function']):
            result = self.plugin_manager._find_plugin_class(mock_module)
            
            assert result is None

    async def test_check_dependencies_satisfied(self) -> None:
        """测试检查插件依赖满足"""
        # 先加载依赖插件
        dep_plugin = MockPlugin("dependency")
        self.plugin_manager._plugins["dependency"] = dep_plugin
        
        # 创建有依赖的插件
        plugin = MockPlugin(dependencies=["dependency"])
        
        result = await self.plugin_manager._check_dependencies(plugin)
        
        assert result is True

    async def test_check_dependencies_not_satisfied(self) -> None:
        """测试检查插件依赖不满足"""
        plugin = MockPlugin(dependencies=["missing_dependency"])
        
        result = await self.plugin_manager._check_dependencies(plugin)
        
        assert result is False


class TestErrorHandling:
    """测试错误处理"""

    @pytest.mark.asyncio
    async def test_plugin_manager_invalid_config(self) -> None:
        """测试插件管理器使用无效配置"""
        container = Mock()
        
        # 测试缺少必要配置
        config: Dict[str, Any] = {}
        manager = PluginManager(config, container)
        
        assert manager._plugin_directory == 'plugins'  # 默认值
        assert manager._auto_load is True  # 默认值

    @pytest.mark.asyncio
    async def test_load_plugin_file_not_found(self) -> None:
        """测试加载不存在的插件文件"""
        manager = PluginManager({}, Mock())
        
        result = await manager.load_plugin("/nonexistent/plugin.py")
        
        assert result is False