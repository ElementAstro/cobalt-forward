"""
Tests for the main entry point and CLI commands.

测试主入口文件和CLI命令的功能。
"""

import asyncio
import pytest
from unittest.mock import Mock, patch, AsyncMock
from pathlib import Path
from typer.testing import CliRunner

from cobalt_forward.main import cli, run_application, shutdown_handler
from cobalt_forward.infrastructure.config.models import ApplicationConfig


class TestMainCLI:
    """测试主CLI功能"""

    def setup_method(self) -> None:
        """测试前设置"""
        self.runner = CliRunner()

    def test_cli_help_command(self) -> None:
        """测试CLI帮助命令"""
        result = self.runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "High-performance TCP-WebSocket forwarder" in result.output

    @patch('cobalt_forward.main.ConfigLoader')
    @patch('cobalt_forward.main.setup_logging')
    @patch('cobalt_forward.main.asyncio.run')
    def test_start_command_basic(self, mock_run: Mock, mock_setup_logging: Mock, mock_config_loader: Mock) -> None:
        """测试基本启动命令"""
        # Mock配置加载器
        mock_loader = Mock()
        mock_config = ApplicationConfig()
        mock_loader.load_config.return_value = mock_config
        mock_config_loader.return_value = mock_loader

        # 运行命令
        result = self.runner.invoke(cli, ["start"])
        
        # 验证调用
        assert result.exit_code == 0
        mock_config_loader.assert_called_once()
        mock_setup_logging.assert_called_once()
        mock_run.assert_called_once()

    @patch('cobalt_forward.main.ConfigLoader')
    @patch('cobalt_forward.main.setup_logging')
    @patch('cobalt_forward.main.asyncio.run')
    def test_start_command_with_options(self, mock_run: Mock, mock_setup_logging: Mock, mock_config_loader: Mock) -> None:
        """测试带选项的启动命令"""
        # Mock配置加载器
        mock_loader = Mock()
        mock_config = ApplicationConfig()
        mock_loader.load_config.return_value = mock_config
        mock_config_loader.return_value = mock_loader

        # 运行带选项的命令
        result = self.runner.invoke(cli, [
            "start", 
            "--config", "test.yaml",
            "--host", "0.0.0.0",
            "--port", "9000",
            "--log-level", "DEBUG",
            "--debug"
        ])
        
        # 验证配置被正确覆盖
        assert result.exit_code == 0
        assert mock_config.websocket.host == "0.0.0.0"
        assert mock_config.websocket.port == 9000
        assert mock_config.logging.level == "DEBUG"
        assert mock_config.debug is True

    @patch('cobalt_forward.main.ConfigLoader')
    @patch('cobalt_forward.main.setup_logging')
    @patch('cobalt_forward.main.uvicorn.run')
    def test_dev_command(self, mock_uvicorn_run: Mock, mock_setup_logging: Mock, mock_config_loader: Mock) -> None:
        """测试开发模式命令"""
        # Mock配置加载器
        mock_loader = Mock()
        mock_config = ApplicationConfig()
        mock_loader.load_config.return_value = mock_config
        mock_config_loader.return_value = mock_loader

        # 运行开发命令
        result = self.runner.invoke(cli, ["dev", "--port", "8080", "--no-reload"])
        
        # 验证调用
        assert result.exit_code == 0
        mock_uvicorn_run.assert_called_once()
        # 验证开发模式配置
        assert mock_config.debug is True
        assert mock_config.environment == "development"
        assert mock_config.logging.level == "DEBUG"

    @patch('cobalt_forward.main.ConfigLoader')
    def test_init_config_command(self, mock_config_loader: Mock) -> None:
        """测试初始化配置命令"""
        mock_loader = Mock()
        mock_config_loader.return_value = mock_loader
        
        # 运行初始化配置命令
        result = self.runner.invoke(cli, ["init-config", "--output", "test_config.yaml"])
        
        # 验证调用
        assert result.exit_code == 0
        mock_loader.save_config.assert_called_once()

    @patch('cobalt_forward.main.ConfigLoader')
    def test_validate_config_command_success(self, mock_config_loader: Mock) -> None:
        """测试配置验证命令成功情况"""
        mock_loader = Mock()
        mock_config = ApplicationConfig()
        mock_loader.load_config.return_value = mock_config
        mock_config_loader.return_value = mock_loader
        
        # 运行配置验证命令
        result = self.runner.invoke(cli, ["validate-config", "config.yaml"])
        
        # 验证调用
        assert result.exit_code == 0
        assert "Configuration file config.yaml is valid" in result.output
        mock_loader.load_config.assert_called_once_with("config.yaml")

    @patch('cobalt_forward.main.ConfigLoader')
    def test_validate_config_command_failure(self, mock_config_loader: Mock) -> None:
        """测试配置验证命令失败情况"""
        mock_loader = Mock()
        mock_loader.load_config.side_effect = Exception("Invalid config")
        mock_config_loader.return_value = mock_loader
        
        # 运行配置验证命令
        result = self.runner.invoke(cli, ["validate-config", "invalid.yaml"])
        
        # 验证失败
        assert result.exit_code == 1
        assert "Configuration validation failed" in result.output

    @patch('cobalt_forward.main.aiohttp.ClientSession')
    def test_health_check_command_success(self, mock_client_session: Mock) -> None:
        """测试健康检查命令成功情况"""
        # 创建 mock 响应
        mock_response = Mock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={"status": "healthy"})
        
        # 创建 mock session，使其正确处理 async with
        mock_session = Mock()
        mock_get_context = AsyncMock()
        mock_get_context.__aenter__ = AsyncMock(return_value=mock_response)
        mock_get_context.__aexit__ = AsyncMock(return_value=None)
        mock_session.get.return_value = mock_get_context
        
        # 设置 ClientSession context manager
        mock_session_context = AsyncMock()
        mock_session_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session_context.__aexit__ = AsyncMock(return_value=None)
        mock_client_session.return_value = mock_session_context
        
        # 运行健康检查命令
        result = self.runner.invoke(cli, ["health-check", "--host", "localhost", "--port", "8000"])
        
        # 验证成功
        assert result.exit_code == 0
        assert "Server is healthy" in result.output

    @patch('cobalt_forward.main.aiohttp.ClientSession')
    def test_health_check_command_failure(self, mock_client_session: Mock) -> None:
        """测试健康检查命令失败情况"""
        # 创建 mock 响应失败
        mock_response = Mock()
        mock_response.status = 500
        
        # 创建 mock session，使其正确处理 async with
        mock_session = Mock()
        mock_get_context = AsyncMock()
        mock_get_context.__aenter__ = AsyncMock(return_value=mock_response)
        mock_get_context.__aexit__ = AsyncMock(return_value=None)
        mock_session.get.return_value = mock_get_context
        
        # 设置 ClientSession context manager
        mock_session_context = AsyncMock()
        mock_session_context.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session_context.__aexit__ = AsyncMock(return_value=None)
        mock_client_session.return_value = mock_session_context
        
        # 运行健康检查命令
        result = self.runner.invoke(cli, ["health-check"])
        
        # 验证失败
        assert result.exit_code == 1


@pytest.mark.asyncio
class TestApplicationRuntime:
    """测试应用运行时功能"""

    @patch('cobalt_forward.main.Container')
    @patch('cobalt_forward.main.ApplicationStartup')
    @patch('cobalt_forward.main.create_app')
    @patch('cobalt_forward.main.uvicorn.Server')
    async def test_run_application_success(self, mock_server_class: Mock, mock_create_app: Mock, 
                                         mock_startup_class: Mock, mock_container_class: Mock) -> None:
        """测试应用成功运行"""
        # Setup mocks
        mock_container = Mock()
        mock_container_class.return_value = mock_container
        
        mock_startup = AsyncMock()
        mock_startup_class.return_value = mock_startup
        
        mock_app = Mock()
        mock_create_app.return_value = mock_app
        
        mock_server = AsyncMock()
        mock_server_class.return_value = mock_server
        
        config = ApplicationConfig()
        
        # 运行应用
        await run_application(config)
        
        # 验证调用序列
        mock_startup.configure_services.assert_called_once()
        mock_startup.start_application.assert_called_once()
        mock_create_app.assert_called_once_with(mock_container, config)
        mock_server.serve.assert_called_once()
        mock_startup.stop_application.assert_called()

    @patch('cobalt_forward.main.Container')
    @patch('cobalt_forward.main.ApplicationStartup')
    async def test_run_application_startup_failure(self, mock_startup_class: Mock, mock_container_class: Mock) -> None:
        """测试应用启动失败"""
        # Setup mocks
        mock_container = Mock()
        mock_container_class.return_value = mock_container
        
        mock_startup = AsyncMock()
        mock_startup.configure_services.side_effect = Exception("Startup failed")
        mock_startup_class.return_value = mock_startup
        
        config = ApplicationConfig()
        
        # 验证异常被抛出
        with pytest.raises(Exception, match="Startup failed"):
            await run_application(config)
        
        # 验证清理被调用
        mock_startup.stop_application.assert_called_once()

    @patch('cobalt_forward.main.logger')
    async def test_shutdown_handler_success(self, mock_logger: Mock) -> None:
        """测试成功关闭处理器"""
        mock_startup = AsyncMock()
        
        await shutdown_handler(mock_startup)
        
        mock_startup.stop_application.assert_called_once()
        mock_logger.info.assert_called_with("Application shutdown completed")

    @patch('cobalt_forward.main.logger')
    async def test_shutdown_handler_failure(self, mock_logger: Mock) -> None:
        """测试关闭处理器失败"""
        mock_startup = AsyncMock()
        mock_startup.stop_application.side_effect = Exception("Shutdown failed")
        
        await shutdown_handler(mock_startup)
        
        mock_logger.error.assert_called_with("Error during shutdown: Shutdown failed")


class TestMainFunction:
    """测试主函数"""

    @patch('cobalt_forward.main.cli')
    def test_main_function(self, mock_cli: Mock) -> None:
        """测试main函数调用CLI"""
        from cobalt_forward.main import main
        
        main()
        
        mock_cli.assert_called_once()