"""
Tests for command dispatcher implementation.

测试命令调度器实现功能。
"""

import pytest
import asyncio
import time
from unittest.mock import Mock, AsyncMock
from typing import Any, List, Dict

from cobalt_forward.core.services.command_dispatcher import CommandDispatcher
from cobalt_forward.core.domain.commands import Command, CommandResult, CommandStatus
from cobalt_forward.core.interfaces.commands import (
    ICommandHandler, ICommandValidator, ICommandInterceptor
)


# 测试用的命令类
class MockCommand(Command[str]):
    """测试命令"""
    
    def __init__(self, name: str = "test_command", data: str = "test") -> None:
        super().__init__(name=name, data=data)


class AnotherMockCommand(Command[int]):
    """另一个测试命令"""
    
    def __init__(self, name: str = "another_command", data: int = 42) -> None:
        super().__init__(name=name, data=data)


# 测试用的处理器类
class MockCommandHandler(ICommandHandler[MockCommand, str]):
    """测试命令处理器"""
    
    def __init__(self, can_handle_result: bool = True, handle_result: str = "success") -> None:
        self.can_handle_result = can_handle_result
        self.handle_result = handle_result
        self.handle_called = False
    
    @property
    def supported_commands(self) -> List[type]:
        return [MockCommand]
    
    def can_handle(self, command: Command[Any]) -> bool:
        return self.can_handle_result
    
    async def handle(self, command: Command[MockCommand]) -> CommandResult[str]:
        self.handle_called = True
        return CommandResult.success(
            command_id=command.command_id,
            result=self.handle_result,
            execution_time=0.1
        )


class FailingCommandHandler(ICommandHandler[MockCommand, str]):
    """失败的命令处理器"""
    
    @property
    def supported_commands(self) -> List[type]:
        return [MockCommand]
    
    def can_handle(self, command: Command[Any]) -> bool:
        return True
    
    async def handle(self, command: Command[MockCommand]) -> CommandResult[str]:
        raise Exception("Handler failed")


# 测试用的验证器类
class MockCommandValidator(ICommandValidator):
    """测试命令验证器"""
    
    def __init__(self, validation_result: bool = True) -> None:
        self.validation_result = validation_result
        self.validate_called = False
    
    async def validate(self, command: Command[Any]) -> bool:
        self.validate_called = True
        return self.validation_result


# 测试用的拦截器类
class MockCommandInterceptor(ICommandInterceptor):
    """测试命令拦截器"""
    
    def __init__(self) -> None:
        self.before_called = False
        self.after_called = False
        self.error_called = False
    
    async def before_handle(self, command: Command[Any]) -> Command[Any]:
        self.before_called = True
        return command
    
    async def after_handle(self, command: Command[Any], result: CommandResult[Any]) -> CommandResult[Any]:
        self.after_called = True
        return result
    
    async def on_error(self, command: Command[Any], error: Exception) -> CommandResult[Any] | None:
        self.error_called = True
        return None


@pytest.mark.asyncio
class MockCommandDispatcher:
    """测试CommandDispatcher类"""

    def setup_method(self) -> None:
        """测试前设置"""
        self.dispatcher = CommandDispatcher()

    async def test_dispatcher_init(self) -> None:
        """测试调度器初始化"""
        assert self.dispatcher.name == "CommandDispatcher"
        assert self.dispatcher.version == "1.0.0"
        assert self.dispatcher._running is False
        assert len(self.dispatcher._handlers) == 0
        assert len(self.dispatcher._validators) == 0
        assert len(self.dispatcher._interceptors) == 0

    async def test_dispatcher_start_stop(self) -> None:
        """测试调度器启动和停止"""
        # 测试启动
        await self.dispatcher.start()
        assert self.dispatcher._running is True
        
        # 测试重复启动
        await self.dispatcher.start()  # 应该不会有问题
        
        # 测试停止
        await self.dispatcher.stop()
        assert self.dispatcher._running is False
        
        # 测试重复停止
        await self.dispatcher.stop()  # 应该不会有问题

    async def test_dispatcher_configure(self) -> None:
        """测试调度器配置"""
        config = {"some_setting": "value"}
        
        # 配置应该不会抛出异常
        await self.dispatcher.configure(config)

    async def test_dispatcher_check_health(self) -> None:
        """测试调度器健康检查"""
        health = await self.dispatcher.check_health()
        
        assert health['healthy'] is True
        assert health['status'] == 'stopped'
        assert 'details' in health
        assert health['details']['handlers_count'] == 0
        assert health['details']['validators_count'] == 0
        assert health['details']['interceptors_count'] == 0
        
        # 启动后检查
        await self.dispatcher.start()
        health = await self.dispatcher.check_health()
        assert health['status'] == 'running'

    async def test_register_handler(self) -> None:
        """测试注册处理器"""
        handler = MockCommandHandler()
        
        await self.dispatcher.register_handler(handler)
        
        assert MockCommand in self.dispatcher._handlers
        assert handler in self.dispatcher._handlers[MockCommand]
        
        metrics = await self.dispatcher.get_metrics()
        assert metrics['handlers_count'] == 1

    async def test_unregister_handler(self) -> None:
        """测试注销处理器"""
        handler = MockCommandHandler()
        
        # 先注册
        await self.dispatcher.register_handler(handler)
        assert len(self.dispatcher._handlers[MockCommand]) == 1
        
        # 注销
        result = await self.dispatcher.unregister_handler(handler)
        assert result is True
        assert len(self.dispatcher._handlers[MockCommand]) == 0
        
        # 再次注销（应该返回False）
        result = await self.dispatcher.unregister_handler(handler)
        assert result is False

    async def test_get_handlers(self) -> None:
        """测试获取处理器"""
        handler1 = MockCommandHandler()
        handler2 = MockCommandHandler()
        
        await self.dispatcher.register_handler(handler1)
        await self.dispatcher.register_handler(handler2)
        
        handlers = await self.dispatcher.get_handlers(MockCommand)
        assert len(handlers) == 2
        assert handler1 in handlers
        assert handler2 in handlers

    async def test_get_metrics(self) -> None:
        """测试获取指标"""
        metrics = await self.dispatcher.get_metrics()
        
        assert 'commands_dispatched' in metrics
        assert 'commands_completed' in metrics
        assert 'commands_failed' in metrics
        assert 'handlers_count' in metrics
        assert 'avg_processing_time' in metrics
        assert 'processing_times' in metrics

    def test_add_validator(self) -> None:
        """测试添加验证器"""
        validator = MockCommandValidator()
        
        self.dispatcher.add_validator(validator)
        
        assert validator in self.dispatcher._validators

    def test_add_interceptor(self) -> None:
        """测试添加拦截器"""
        interceptor = MockCommandInterceptor()
        
        self.dispatcher.add_interceptor(interceptor)
        
        assert interceptor in self.dispatcher._interceptors

    async def test_dispatch_success(self) -> None:
        """测试成功调度命令"""
        await self.dispatcher.start()
        
        handler = MockCommandHandler()
        await self.dispatcher.register_handler(handler)
        
        command = MockCommand()
        result = await self.dispatcher.dispatch(command)
        
        assert result.is_success is True
        assert result.result == "success"
        assert result.command_id == command.command_id
        assert handler.handle_called is True
        
        # 检查指标
        metrics = await self.dispatcher.get_metrics()
        assert metrics['commands_dispatched'] == 1
        assert metrics['commands_completed'] == 1
        assert metrics['commands_failed'] == 0

    async def test_dispatch_not_running(self) -> None:
        """测试调度器未运行时调度命令"""
        command = MockCommand()
        
        with pytest.raises(RuntimeError, match="Command dispatcher is not running"):
            await self.dispatcher.dispatch(command)

    async def test_dispatch_no_handler(self) -> None:
        """测试没有处理器时调度命令"""
        await self.dispatcher.start()
        
        command = MockCommand()
        result = await self.dispatcher.dispatch(command)
        
        assert result.is_success is False
        assert "No handler found" in str(result.error)
        
        # 检查指标
        metrics = await self.dispatcher.get_metrics()
        assert metrics['commands_failed'] == 1

    async def test_dispatch_handler_cannot_handle(self) -> None:
        """测试处理器无法处理命令"""
        await self.dispatcher.start()
        
        handler = MockCommandHandler(can_handle_result=False)
        await self.dispatcher.register_handler(handler)
        
        command = MockCommand()
        result = await self.dispatcher.dispatch(command)
        
        assert result.is_success is False
        assert "No suitable handler found" in str(result.error)

    async def test_dispatch_handler_failure(self) -> None:
        """测试处理器处理失败"""
        await self.dispatcher.start()
        
        handler = FailingCommandHandler()
        await self.dispatcher.register_handler(handler)
        
        command = MockCommand()
        result = await self.dispatcher.dispatch(command)
        
        assert result.is_success is False
        assert "Handler failed" in str(result.error)
        
        # 检查指标
        metrics = await self.dispatcher.get_metrics()
        assert metrics['commands_failed'] == 1

    async def test_dispatch_with_validation(self) -> None:
        """测试带验证的命令调度"""
        await self.dispatcher.start()
        
        handler = MockCommandHandler()
        validator = MockCommandValidator()
        
        await self.dispatcher.register_handler(handler)
        self.dispatcher.add_validator(validator)
        
        command = MockCommand()
        result = await self.dispatcher.dispatch(command)
        
        assert result.is_success is True
        assert validator.validate_called is True

    async def test_dispatch_validation_failure(self) -> None:
        """测试验证失败的命令调度"""
        await self.dispatcher.start()
        
        handler = MockCommandHandler()
        validator = MockCommandValidator(validation_result=False)
        
        await self.dispatcher.register_handler(handler)
        self.dispatcher.add_validator(validator)
        
        command = MockCommand()
        result = await self.dispatcher.dispatch(command)
        
        assert result.is_success is False
        assert "Command validation failed" in str(result.error)
        assert validator.validate_called is True
        assert handler.handle_called is False

    async def test_dispatch_with_interceptors(self) -> None:
        """测试带拦截器的命令调度"""
        await self.dispatcher.start()
        
        handler = MockCommandHandler()
        interceptor = MockCommandInterceptor()
        
        await self.dispatcher.register_handler(handler)
        self.dispatcher.add_interceptor(interceptor)
        
        command = MockCommand()
        result = await self.dispatcher.dispatch(command)
        
        assert result.is_success is True
        assert interceptor.before_called is True
        assert interceptor.after_called is True
        assert interceptor.error_called is False

    async def test_dispatch_interceptor_error_handling(self) -> None:
        """测试拦截器错误处理"""
        await self.dispatcher.start()
        
        handler = FailingCommandHandler()
        interceptor = MockCommandInterceptor()
        
        await self.dispatcher.register_handler(handler)
        self.dispatcher.add_interceptor(interceptor)
        
        command = MockCommand()
        result = await self.dispatcher.dispatch(command)
        
        assert result.is_success is False
        assert interceptor.before_called is True
        assert interceptor.error_called is True

    async def test_dispatch_multiple_handlers(self) -> None:
        """测试多个处理器的命令调度"""
        await self.dispatcher.start()
        
        # 第一个处理器无法处理
        handler1 = MockCommandHandler(can_handle_result=False)
        # 第二个处理器可以处理
        handler2 = MockCommandHandler(can_handle_result=True, handle_result="handler2")
        
        await self.dispatcher.register_handler(handler1)
        await self.dispatcher.register_handler(handler2)
        
        command = MockCommand()
        result = await self.dispatcher.dispatch(command)
        
        assert result.is_success is True
        assert result.result == "handler2"
        assert handler2.handle_called is True

    async def test_processing_time_metrics(self) -> None:
        """测试处理时间指标"""
        await self.dispatcher.start()
        
        handler = MockCommandHandler()
        await self.dispatcher.register_handler(handler)
        
        # 调度多个命令
        for _ in range(5):
            command = MockCommand()
            await self.dispatcher.dispatch(command)
        
        metrics = await self.dispatcher.get_metrics()
        assert len(metrics['processing_times']) == 5
        assert metrics['avg_processing_time'] >= 0  # 允许为0，因为测试执行很快

    async def test_processing_time_limit(self) -> None:
        """测试处理时间限制（最多保留1000个）"""
        await self.dispatcher.start()
        
        handler = MockCommandHandler()
        await self.dispatcher.register_handler(handler)
        
        # 模拟大量处理时间记录
        self.dispatcher._metrics['processing_times'] = [0.1] * 1500
        
        command = MockCommand()
        await self.dispatcher.dispatch(command)
        
        # 应该只保留最后1000个
        assert len(self.dispatcher._metrics['processing_times']) == 1000

    async def test_stop_clears_resources(self) -> None:
        """测试停止时清理资源"""
        await self.dispatcher.start()
        
        handler = MockCommandHandler()
        validator = MockCommandValidator()
        interceptor = MockCommandInterceptor()
        
        await self.dispatcher.register_handler(handler)
        self.dispatcher.add_validator(validator)
        self.dispatcher.add_interceptor(interceptor)
        
        await self.dispatcher.stop()
        
        assert len(self.dispatcher._handlers) == 0
        assert len(self.dispatcher._validators) == 0
        assert len(self.dispatcher._interceptors) == 0


class TestErrorHandling:
    """测试错误处理"""

    @pytest.mark.asyncio
    async def test_validator_exception(self) -> None:
        """测试验证器抛出异常"""
        dispatcher = CommandDispatcher()
        await dispatcher.start()
        
        handler = MockCommandHandler()
        await dispatcher.register_handler(handler)
        
        # 创建会抛出异常的验证器
        class FailingValidator(ICommandValidator):
            async def validate(self, command: Command[Any]) -> bool:
                raise Exception("Validator error")
        
        validator = FailingValidator()
        dispatcher.add_validator(validator)
        
        command = MockCommand()
        result = await dispatcher.dispatch(command)
        
        assert result.is_success is False
        assert "Validator error" in str(result.error)

    @pytest.mark.asyncio
    async def test_interceptor_exception(self) -> None:
        """测试拦截器抛出异常"""
        dispatcher = CommandDispatcher()
        await dispatcher.start()
        
        handler = MockCommandHandler()
        await dispatcher.register_handler(handler)
        
        # 创建会抛出异常的拦截器
        class FailingInterceptor(ICommandInterceptor):
            async def before_handle(self, command: Command[Any]) -> Command[Any]:
                raise Exception("Interceptor error")
            
            async def after_handle(self, command: Command[Any], result: CommandResult[Any]) -> CommandResult[Any]:
                return result
            
            async def on_error(self, command: Command[Any], error: Exception) -> CommandResult[Any] | None:
                return None
        
        interceptor = FailingInterceptor()
        dispatcher.add_interceptor(interceptor)
        
        command = MockCommand()
        result = await dispatcher.dispatch(command)
        
        assert result.is_success is False
        assert "Interceptor error" in str(result.error)

    @pytest.mark.asyncio
    async def test_command_execution_timeout(self) -> None:
        """测试命令执行超时（模拟）"""
        dispatcher = CommandDispatcher()
        await dispatcher.start()
        
        # 创建慢速处理器
        class SlowHandler(ICommandHandler[MockCommand, str]):
            @property
            def supported_commands(self) -> List[type]:
                return [MockCommand]
            
            def can_handle(self, command: Command[Any]) -> bool:
                return True
            
            async def handle(self, command: Command[MockCommand]) -> CommandResult[str]:
                await asyncio.sleep(0.1)  # 模拟慢速处理
                return CommandResult.success(
                    command_id=command.command_id,
                    result="slow_success",
                    execution_time=0.1
                )
        
        handler = SlowHandler()
        await dispatcher.register_handler(handler)
        
        command = MockCommand()
        result = await dispatcher.dispatch(command)
        
        # 即使慢，也应该成功
        assert result.is_success is True
        assert result.execution_time is not None and result.execution_time >= 0.1