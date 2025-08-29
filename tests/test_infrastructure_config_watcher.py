"""
Tests for configuration file watcher.

测试配置文件监控功能。
"""

import pytest
import asyncio
import tempfile
from pathlib import Path
from unittest.mock import Mock, AsyncMock, patch
from typing import Any, Callable, Optional

from cobalt_forward.infrastructure.config.watcher import (
    ConfigFileHandler,
    ConfigWatcher,
    PollingConfigWatcher,
    create_config_watcher,
    IConfigWatcher,
    WATCHDOG_AVAILABLE
)


class MockFileSystemEvent:
    """Mock文件系统事件"""
    
    def __init__(self, src_path: str, dest_path: Optional[str] = None, is_directory: bool = False) -> None:
        self.src_path = src_path
        self.dest_path = dest_path or src_path
        self.is_directory = is_directory


class TestConfigFileHandler:
    """测试ConfigFileHandler类"""

    def setup_method(self) -> None:
        """测试前设置"""
        self.config_path = Path("/test/config.yaml")
        self.reload_callback: Mock = Mock()
        self.handler = ConfigFileHandler(
            config_path=self.config_path,
            reload_callback=self.reload_callback,
            debounce_delay=0.1
        )

    def test_handler_init(self) -> None:
        """测试处理器初始化"""
        assert self.handler.config_path == self.config_path
        assert self.handler.reload_callback == self.reload_callback
        assert self.handler.debounce_delay == 0.1
        assert self.handler.last_modified == 0.0
        assert self.handler._pending_task is None

    @patch('time.time')
    @patch('asyncio.create_task')
    def test_on_modified_file_match(self, mock_create_task: Mock, mock_time: Mock) -> None:
        """测试文件修改事件匹配"""
        mock_time.return_value = 100.0
        mock_task = Mock()
        mock_create_task.return_value = mock_task
        
        event = MockFileSystemEvent(str(self.config_path))
        
        self.handler.on_modified(event)  # type: ignore[arg-type]
        
        assert self.handler.last_modified == 100.0
        mock_create_task.assert_called_once()

    def test_on_modified_file_no_match(self) -> None:
        """测试文件修改事件不匹配"""
        event = MockFileSystemEvent("/other/file.yaml")
        
        self.handler.on_modified(event)  # type: ignore[arg-type]
        
        assert self.handler.last_modified == 0.0

    def test_on_modified_directory_event(self) -> None:
        """测试目录事件被忽略"""
        event = MockFileSystemEvent(str(self.config_path), is_directory=True)
        
        self.handler.on_modified(event)  # type: ignore[arg-type]
        
        assert self.handler.last_modified == 0.0

    @pytest.mark.asyncio
    async def test_debounced_reload_sync_callback(self) -> None:
        """测试防抖重载（同步回调）"""
        self.reload_callback.return_value = None
        
        with patch('asyncio.sleep') as mock_sleep:
            await self.handler._debounced_reload()
            
            mock_sleep.assert_called_once_with(0.1)
            self.reload_callback.assert_called_once()

    @pytest.mark.asyncio
    async def test_debounced_reload_async_callback(self) -> None:
        """测试防抖重载（异步回调）"""
        reload_callback = AsyncMock()
        
        handler = ConfigFileHandler(
            config_path=self.config_path,
            reload_callback=reload_callback,
            debounce_delay=0.1
        )
        
        with patch('asyncio.sleep') as mock_sleep:
            await handler._debounced_reload()
            
            mock_sleep.assert_called_once_with(0.1)
            reload_callback.assert_called_once()

    @pytest.mark.asyncio
    async def test_debounced_reload_cancelled(self) -> None:
        """测试防抖重载被取消"""
        with patch('asyncio.sleep', side_effect=asyncio.CancelledError()):
            await self.handler._debounced_reload()
            
            # 应该不会调用回调
            self.reload_callback.assert_not_called()


@pytest.mark.asyncio
class TestConfigWatcher:
    """测试ConfigWatcher类"""

    def setup_method(self) -> None:
        """测试前设置"""
        self.config_path = Path("/test/config.yaml")
        self.reload_callback: Mock = Mock()

    @patch('cobalt_forward.infrastructure.config.watcher.WATCHDOG_AVAILABLE', True)
    def test_watcher_init(self) -> None:
        """测试监控器初始化"""
        watcher = ConfigWatcher(
            config_path=self.config_path,
            reload_callback=self.reload_callback,
            debounce_delay=0.5
        )
        
        assert watcher.config_path == self.config_path.resolve()
        assert watcher.reload_callback == self.reload_callback
        assert watcher.debounce_delay == 0.5
        assert watcher._running is False

    @patch('cobalt_forward.infrastructure.config.watcher.WATCHDOG_AVAILABLE', False)
    def test_watcher_init_no_watchdog(self) -> None:
        """测试无watchdog时初始化"""
        with pytest.raises(ImportError, match="watchdog library is required"):
            ConfigWatcher(
                config_path=self.config_path,
                reload_callback=self.reload_callback
            )

    @patch('cobalt_forward.infrastructure.config.watcher.WATCHDOG_AVAILABLE', True)
    @patch('cobalt_forward.infrastructure.config.watcher.Observer')
    @patch('pathlib.Path.exists')
    async def test_watcher_start_success(self, mock_exists: Mock, mock_observer_class: Mock) -> None:
        """测试监控器成功启动"""
        mock_observer = Mock()
        mock_observer_class.return_value = mock_observer
        mock_exists.return_value = True
        
        watcher = ConfigWatcher(
            config_path=self.config_path,
            reload_callback=self.reload_callback
        )
        
        await watcher.start()
        
        assert watcher._running is True
        assert watcher._handler is not None
        assert watcher._observer == mock_observer
        mock_observer.schedule.assert_called_once()
        mock_observer.start.assert_called_once()

    @patch('cobalt_forward.infrastructure.config.watcher.WATCHDOG_AVAILABLE', True)
    @patch('pathlib.Path.exists')
    async def test_watcher_start_file_not_exists(self, mock_exists: Mock) -> None:
        """测试监控器启动时文件不存在"""
        mock_exists.return_value = False
        
        watcher = ConfigWatcher(
            config_path=self.config_path,
            reload_callback=self.reload_callback
        )
        
        await watcher.start()
        
        assert watcher._running is False


@pytest.mark.asyncio
class TestPollingConfigWatcher:
    """测试PollingConfigWatcher类"""

    def setup_method(self) -> None:
        """测试前设置"""
        self.config_path = Path("/test/config.yaml")
        self.reload_callback: Mock = Mock()

    def test_polling_watcher_init(self) -> None:
        """测试轮询监控器初始化"""
        watcher = PollingConfigWatcher(
            config_path=self.config_path,
            reload_callback=self.reload_callback,
            poll_interval=2.0
        )
        
        assert watcher.config_path == self.config_path.resolve()
        assert watcher.reload_callback == self.reload_callback
        assert watcher.poll_interval == 2.0
        assert watcher._running is False

    async def test_polling_watcher_start(self) -> None:
        """测试轮询监控器启动"""
        watcher = PollingConfigWatcher(
            config_path=self.config_path,
            reload_callback=self.reload_callback,
            poll_interval=0.1
        )
        
        with patch.object(watcher, '_poll_loop') as mock_poll_loop:
            mock_poll_loop.return_value = asyncio.create_task(asyncio.sleep(0))
            
            await watcher.start()
            
            assert watcher._running is True
            assert watcher._task is not None

    def test_polling_watcher_is_running(self) -> None:
        """测试轮询监控器运行状态检查"""
        watcher = PollingConfigWatcher(
            config_path=self.config_path,
            reload_callback=self.reload_callback
        )
        
        # 初始状态
        assert watcher.is_running() is False
        
        # 设置为运行状态
        mock_task = Mock()
        mock_task.done.return_value = False
        watcher._running = True
        watcher._task = mock_task
        
        assert watcher.is_running() is True
        
        # 任务完成
        mock_task.done.return_value = True
        assert watcher.is_running() is False


class TestCreateConfigWatcher:
    """测试配置监控器工厂函数"""

    def setup_method(self) -> None:
        """测试前设置"""
        self.config_path = Path("/test/config.yaml")
        self.reload_callback: Mock = Mock()

    @patch('cobalt_forward.infrastructure.config.watcher.WATCHDOG_AVAILABLE', True)
    def test_create_config_watcher_default(self) -> None:
        """测试创建默认配置监控器"""
        watcher = create_config_watcher(
            config_path=self.config_path,
            reload_callback=self.reload_callback
        )
        
        assert isinstance(watcher, ConfigWatcher)

    @patch('cobalt_forward.infrastructure.config.watcher.WATCHDOG_AVAILABLE', False)
    def test_create_config_watcher_no_watchdog(self) -> None:
        """测试无watchdog时创建配置监控器"""
        watcher = create_config_watcher(
            config_path=self.config_path,
            reload_callback=self.reload_callback
        )
        
        assert isinstance(watcher, PollingConfigWatcher)


class TestIntegration:
    """集成测试"""

    @pytest.mark.asyncio
    async def test_real_file_watching_simulation(self) -> None:
        """测试真实文件监控模拟"""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.yaml"
            config_path.write_text("key: value1")
            
            callback_called = asyncio.Event()
            call_count = 0
            
            async def reload_callback() -> None:
                nonlocal call_count
                call_count += 1
                callback_called.set()
            
            # 使用轮询监控器进行测试
            watcher = PollingConfigWatcher(
                config_path=config_path,
                reload_callback=reload_callback,
                poll_interval=0.05
            )
            
            try:
                await watcher.start()
                
                # 等待一点时间
                await asyncio.sleep(0.1)
                
                # 修改文件
                config_path.write_text("key: value2")
                
                # 等待回调被调用
                await asyncio.wait_for(callback_called.wait(), timeout=1.0)
                
                assert call_count > 0
                
            finally:
                await watcher.stop()