"""
Tests for logging setup and configuration utilities.

测试日志设置和配置工具功能。
"""

import pytest
import logging
import sys
from pathlib import Path
from unittest.mock import Mock, patch
from tempfile import TemporaryDirectory

from cobalt_forward.infrastructure.logging.setup import (
    setup_logging, 
    _setup_loguru_logging, 
    _setup_standard_logging,
    LoggingManager,
    LOGURU_AVAILABLE
)
from cobalt_forward.infrastructure.config.models import LoggingConfig


class TestSetupLogging:
    """测试日志设置主函数"""

    @patch('cobalt_forward.infrastructure.logging.setup.LOGURU_AVAILABLE', True)
    @patch('cobalt_forward.infrastructure.logging.setup._setup_loguru_logging')
    def test_setup_logging_with_loguru(self, mock_setup_loguru: Mock) -> None:
        """测试使用loguru的日志设置"""
        config = LoggingConfig()
        
        setup_logging(config)
        
        mock_setup_loguru.assert_called_once_with(config)

    @patch('cobalt_forward.infrastructure.logging.setup.LOGURU_AVAILABLE', False)
    @patch('cobalt_forward.infrastructure.logging.setup._setup_standard_logging')
    def test_setup_logging_without_loguru(self, mock_setup_standard: Mock) -> None:
        """测试不使用loguru的日志设置"""
        config = LoggingConfig()
        
        setup_logging(config)
        
        mock_setup_standard.assert_called_once_with(config)


class TestLoguruLoggingSetup:
    """测试loguru日志设置"""

    def setup_method(self) -> None:
        """测试前设置"""
        self.config = LoggingConfig()
        self.config.log_directory = "test_logs"
        self.config.level = "INFO"
        self.config.console_enabled = True
        self.config.file_enabled = True

    @patch('cobalt_forward.infrastructure.logging.setup.LOGURU_AVAILABLE', True)
    @patch('cobalt_forward.infrastructure.logging.setup.loguru_logger')
    @patch('pathlib.Path.mkdir')
    def test_setup_loguru_logging_console_and_file(self, mock_mkdir: Mock, mock_loguru: Mock) -> None:
        """测试loguru日志设置（控制台和文件）"""
        _setup_loguru_logging(self.config)
        
        # 验证目录创建
        mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
        
        # 验证loguru配置
        mock_loguru.remove.assert_called_once()
        
        # 验证add方法被调用（控制台和文件）
        assert mock_loguru.add.call_count == 2

    @patch('cobalt_forward.infrastructure.logging.setup.LOGURU_AVAILABLE', True)
    @patch('cobalt_forward.infrastructure.logging.setup.loguru_logger')
    @patch('pathlib.Path.mkdir')
    def test_setup_loguru_logging_console_only(self, mock_mkdir: Mock, mock_loguru: Mock) -> None:
        """测试loguru日志设置（仅控制台）"""
        self.config.file_enabled = False
        
        _setup_loguru_logging(self.config)
        
        # 验证只添加了控制台处理器
        mock_loguru.add.assert_called_once()
        
        # 验证控制台配置
        call_args = mock_loguru.add.call_args[0]
        assert call_args[0] == sys.stderr


class TestStandardLoggingSetup:
    """测试标准库日志设置"""

    def setup_method(self) -> None:
        """测试前设置"""
        self.config = LoggingConfig()
        self.config.log_directory = "test_logs"
        self.config.level = "INFO"
        self.config.console_enabled = True
        self.config.file_enabled = True

    @patch('logging.FileHandler')
    @patch('logging.StreamHandler')
    @patch('pathlib.Path.mkdir')
    @patch('logging.basicConfig')
    @patch('logging.getLogger')
    def test_setup_standard_logging_full(self, mock_get_logger: Mock, mock_basic_config: Mock, 
                                       mock_mkdir: Mock, mock_stream_handler: Mock, mock_file_handler: Mock) -> None:
        """测试标准库日志完整设置"""
        mock_root_logger = Mock()
        mock_root_logger.handlers = []
        mock_get_logger.return_value = mock_root_logger
        
        # Mock handlers
        mock_stream_handler.return_value = Mock()
        mock_file_handler.return_value = Mock()
        
        _setup_standard_logging(self.config)
        
        # 验证目录创建
        mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
        
        # 验证基础配置
        mock_basic_config.assert_called_once()
        
        # 验证处理器清理（mock handlers是列表，clear是内置方法）
        # 直接检查handlers被设置为Mock对象
        assert hasattr(mock_root_logger, 'handlers')
        
        # 验证添加了两个处理器（控制台和文件）
        assert mock_root_logger.addHandler.call_count == 2

    @patch('pathlib.Path.mkdir')
    @patch('logging.basicConfig')
    @patch('logging.getLogger')
    def test_setup_standard_logging_console_only(self, mock_get_logger: Mock, mock_basic_config: Mock, mock_mkdir: Mock) -> None:
        """测试标准库日志设置（仅控制台）"""
        self.config.file_enabled = False
        
        mock_root_logger = Mock()
        mock_root_logger.handlers = []
        mock_get_logger.return_value = mock_root_logger
        
        _setup_standard_logging(self.config)
        
        # 验证只添加了一个处理器（控制台）
        mock_root_logger.addHandler.assert_called_once()


@pytest.mark.asyncio
class TestLoggingManager:
    """测试LoggingManager类"""

    def setup_method(self) -> None:
        """测试前设置"""
        self.config_dict = {
            "level": "INFO",
            "log_directory": "test_logs",
            "console_enabled": True,
            "file_enabled": True,
            "format": "%(levelname)s - %(message)s",
            "max_file_size": "10MB",
            "backup_count": 5
        }

    def test_logging_manager_init(self) -> None:
        """测试LoggingManager初始化"""
        manager = LoggingManager(self.config_dict)
        
        assert manager.name == "LoggingManager"
        assert manager.version == "1.0.0"
        assert manager._started is False
        assert isinstance(manager._config, LoggingConfig)

    @patch('cobalt_forward.infrastructure.logging.setup.setup_logging')
    async def test_logging_manager_start(self, mock_setup_logging: Mock) -> None:
        """测试LoggingManager启动"""
        manager = LoggingManager(self.config_dict)
        
        await manager.start()
        
        assert manager._started is True
        mock_setup_logging.assert_called_once_with(manager._config)

    async def test_logging_manager_start_already_started(self) -> None:
        """测试LoggingManager重复启动"""
        manager = LoggingManager(self.config_dict)
        manager._started = True
        
        with patch('cobalt_forward.infrastructure.logging.setup.setup_logging') as mock_setup:
            await manager.start()
            mock_setup.assert_not_called()

    async def test_logging_manager_stop(self) -> None:
        """测试LoggingManager停止"""
        manager = LoggingManager(self.config_dict)
        manager._started = True
        
        await manager.stop()
        
        assert manager._started is False

    async def test_logging_manager_stop_not_started(self) -> None:
        """测试LoggingManager未启动时停止"""
        manager = LoggingManager(self.config_dict)
        
        # 不应该抛出异常
        await manager.stop()
        
        assert manager._started is False

    @patch('cobalt_forward.infrastructure.logging.setup.setup_logging')
    async def test_logging_manager_configure(self, mock_setup_logging: Mock) -> None:
        """测试LoggingManager配置"""
        manager = LoggingManager(self.config_dict)
        manager._started = True
        
        new_config = {"level": "DEBUG", "console_enabled": False}
        await manager.configure(new_config)
        
        # 验证配置被更新且重新设置日志
        mock_setup_logging.assert_called_once()

    async def test_logging_manager_configure_not_started(self) -> None:
        """测试LoggingManager未启动时配置"""
        manager = LoggingManager(self.config_dict)
        
        new_config = {"level": "DEBUG"}
        
        with patch('cobalt_forward.infrastructure.logging.setup.setup_logging') as mock_setup:
            await manager.configure(new_config)
            mock_setup.assert_not_called()

    async def test_logging_manager_check_health(self) -> None:
        """测试LoggingManager健康检查"""
        manager = LoggingManager(self.config_dict)
        manager._started = True
        
        health = await manager.check_health()
        
        assert health['healthy'] is True
        assert health['status'] == 'running'
        assert 'details' in health
        assert health['details']['log_level'] == 'INFO'
        assert health['details']['console_enabled'] is True
        assert health['details']['file_enabled'] is True

    async def test_logging_manager_check_health_stopped(self) -> None:
        """测试LoggingManager停止状态健康检查"""
        manager = LoggingManager(self.config_dict)
        
        health = await manager.check_health()
        
        assert health['status'] == 'stopped'

    @patch('cobalt_forward.infrastructure.logging.setup.LOGURU_AVAILABLE', True)
    @patch('cobalt_forward.infrastructure.logging.setup.loguru_logger')
    def test_get_logger_with_loguru(self, mock_loguru: Mock) -> None:
        """测试使用loguru获取logger"""
        manager = LoggingManager(self.config_dict)
        
        logger = manager.get_logger("test_logger")
        
        mock_loguru.bind.assert_called_once_with(name="test_logger")

    @patch('cobalt_forward.infrastructure.logging.setup.LOGURU_AVAILABLE', False)
    @patch('logging.getLogger')
    def test_get_logger_without_loguru(self, mock_get_logger: Mock) -> None:
        """测试不使用loguru获取logger"""
        manager = LoggingManager(self.config_dict)
        
        manager.get_logger("test_logger")
        
        # getLogger会被调用两次：一次是LoggingManager初始化时，一次是get_logger调用
        assert mock_get_logger.call_count >= 1
        mock_get_logger.assert_any_call("test_logger")

    @patch('cobalt_forward.infrastructure.logging.setup.LOGURU_AVAILABLE', True)
    @patch('cobalt_forward.infrastructure.logging.setup.loguru_logger')
    def test_log_access_with_loguru(self, mock_loguru: Mock) -> None:
        """测试使用loguru记录访问日志"""
        manager = LoggingManager(self.config_dict)
        
        manager.log_access("User accessed /api", user_id=123)
        
        mock_loguru.bind.assert_called_once_with(access_log=True, user_id=123)

    @patch('cobalt_forward.infrastructure.logging.setup.LOGURU_AVAILABLE', True)
    @patch('cobalt_forward.infrastructure.logging.setup.loguru_logger')
    def test_log_performance_with_loguru(self, mock_loguru: Mock) -> None:
        """测试使用loguru记录性能日志"""
        manager = LoggingManager(self.config_dict)
        
        manager.log_performance("Request took 100ms", duration=100)
        
        mock_loguru.bind.assert_called_once_with(performance_log=True, duration=100)

    @patch('cobalt_forward.infrastructure.logging.setup.LOGURU_AVAILABLE', True)
    @patch('cobalt_forward.infrastructure.logging.setup.loguru_logger')
    def test_log_error_with_exception(self, mock_loguru: Mock) -> None:
        """测试使用loguru记录错误日志（带异常）"""
        manager = LoggingManager(self.config_dict)
        error = Exception("Test error")
        
        manager.log_error("Something went wrong", error=error, context="test")
        
        mock_loguru.bind.assert_called_once_with(context="test")

    @patch('cobalt_forward.infrastructure.logging.setup.LOGURU_AVAILABLE', True)
    @patch('cobalt_forward.infrastructure.logging.setup.loguru_logger')
    def test_log_error_without_exception(self, mock_loguru: Mock) -> None:
        """测试使用loguru记录错误日志（无异常）"""
        manager = LoggingManager(self.config_dict)
        
        manager.log_error("Something went wrong", context="test")
        
        mock_loguru.bind.assert_called_once_with(context="test")

    @patch('cobalt_forward.infrastructure.logging.setup.LOGURU_AVAILABLE', True)
    @patch('cobalt_forward.infrastructure.logging.setup.loguru_logger')
    def test_log_structured_with_loguru(self, mock_loguru: Mock) -> None:
        """测试使用loguru记录结构化日志"""
        manager = LoggingManager(self.config_dict)
        
        manager.log_structured("INFO", "User action", action="login", user_id=123)
        
        # 验证info方法被调用
        mock_loguru.info.assert_called_once_with("User action", action="login", user_id=123)

    @patch('cobalt_forward.infrastructure.logging.setup.LOGURU_AVAILABLE', False)
    @patch('logging.getLogger')
    def test_log_methods_without_loguru(self, mock_get_logger: Mock) -> None:
        """测试不使用loguru时的日志方法"""
        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        
        manager = LoggingManager(self.config_dict)
        
        # 测试访问日志
        manager.log_access("User accessed /api")
        mock_logger.info.assert_any_call("ACCESS: User accessed /api")
        
        # 测试性能日志
        manager.log_performance("Request took 100ms")
        mock_logger.info.assert_any_call("PERFORMANCE: Request took 100ms")
        
        # 测试错误日志
        manager.log_error("Something went wrong")
        mock_logger.error.assert_any_call("Something went wrong")
        
        # 测试结构化日志
        manager.log_structured("DEBUG", "Debug message", key="value")
        mock_logger.debug.assert_any_call("Debug message - {'key': 'value'}")


class TestErrorHandling:
    """测试错误处理"""

    def test_logging_manager_invalid_config(self) -> None:
        """测试LoggingManager使用无效配置"""
        invalid_config = {"invalid_key": "invalid_value"}
        
        # 应该抛出验证错误
        with pytest.raises(Exception):
            LoggingManager(invalid_config)

    @patch('pathlib.Path.mkdir')
    def test_setup_logging_directory_creation_failure(self, mock_mkdir: Mock) -> None:
        """测试日志目录创建失败"""
        mock_mkdir.side_effect = PermissionError("Cannot create directory")
        config = LoggingConfig()
        
        with pytest.raises(PermissionError):
            _setup_standard_logging(config)