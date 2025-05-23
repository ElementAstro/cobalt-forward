import inspect
import logging
import time
from enum import Enum, auto
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Tuple, Union, TypeVar, Type

from app.core.exceptions import MiddlewareException

logger = logging.getLogger(__name__)

# 中间件处理阶段
class MiddlewareStage(Enum):
    PRE_PROCESS = auto()    # 预处理阶段
    PROCESS = auto()        # 处理阶段
    POST_PROCESS = auto()   # 后处理阶段
    ERROR = auto()          # 错误处理阶段

# 中间件优先级
class MiddlewarePriority(Enum):
    HIGHEST = 0    # 最高优先级
    HIGH = 10      # 高优先级
    NORMAL = 20    # 普通优先级
    LOW = 30       # 低优先级
    LOWEST = 40    # 最低优先级

# 中间件类型
T = TypeVar('T')

class Middleware:
    """
    中间件基类
    
    提供消息处理生命周期的钩子方法，可用于：
    - 消息过滤、验证和转换
    - 添加元数据
    - 性能监控
    - 安全检查
    - 日志记录
    - 错误处理
    """
    
    def __init__(self, name: str = None, priority: MiddlewarePriority = MiddlewarePriority.NORMAL):
        """
        初始化中间件
        
        Args:
            name: 中间件名称，默认为类名
            priority: 中间件执行优先级
        """
        self.name = name or self.__class__.__name__
        self.priority = priority
        self._next_middleware = None
        self._enabled = True
        logger.debug(f"创建中间件: {self.name}, 优先级: {self.priority.name}")
    
    @property
    def enabled(self) -> bool:
        """中间件是否启用"""
        return self._enabled
    
    @enabled.setter
    def enabled(self, value: bool) -> None:
        """设置中间件启用状态"""
        self._enabled = value
        logger.debug(f"中间件 {self.name} 状态变更为: {'启用' if value else '禁用'}")
    
    async def pre_process(self, message: Any, context: Dict[str, Any] = None) -> Tuple[Any, Dict[str, Any]]:
        """
        预处理阶段，在消息被处理之前调用
        
        Args:
            message: 要处理的消息
            context: 消息上下文信息
            
        Returns:
            可能修改后的消息和上下文
        """
        # 默认实现只返回原始消息和上下文
        return message, context or {}
    
    async def process(self, message: Any, context: Dict[str, Any] = None) -> Tuple[Any, Dict[str, Any]]:
        """
        处理阶段，主要消息处理逻辑
        
        Args:
            message: 要处理的消息
            context: 消息上下文信息
            
        Returns:
            处理后的消息和上下文
        """
        # 默认实现调用下一个中间件
        if self._next_middleware and self._next_middleware.enabled:
            return await self._next_middleware.process(message, context or {})
        return message, context or {}
    
    async def post_process(self, message: Any, context: Dict[str, Any] = None) -> Tuple[Any, Dict[str, Any]]:
        """
        后处理阶段，在消息被处理之后调用
        
        Args:
            message: 处理后的消息
            context: 消息上下文信息
            
        Returns:
            可能进一步修改的消息和上下文
        """
        # 默认实现只返回处理后的消息和上下文
        return message, context or {}
    
    async def error_handler(self, error: Exception, original_message: Any, context: Dict[str, Any] = None) -> Tuple[Any, Dict[str, Any]]:
        """
        错误处理，当处理过程中出现异常时调用
        
        Args:
            error: 发生的异常
            original_message: 原始消息
            context: 消息上下文信息
            
        Returns:
            处理错误后的消息和上下文
        """
        # 默认实现是重新抛出异常
        logger.error(f"中间件 {self.name} 处理错误: {error}")
        raise error
    
    def set_next(self, middleware: 'Middleware') -> 'Middleware':
        """
        设置下一个要执行的中间件，形成责任链
        
        Args:
            middleware: 下一个中间件
            
        Returns:
            下一个中间件的引用
        """
        self._next_middleware = middleware
        return middleware


class MiddlewareManager:
    """
    中间件管理器
    
    管理一系列中间件的注册、排序和执行
    """
    
    def __init__(self):
        """初始化中间件管理器"""
        self._middlewares: Dict[str, Middleware] = {}
        self._middleware_chain: Optional[Middleware] = None
        self._chain_built = False
        self._metrics: Dict[str, Dict[str, float]] = {}
        self._error_handlers: List[Callable] = []
        logger.debug("初始化中间件管理器")
    
    def add(self, middleware: Middleware) -> None:
        """
        添加中间件
        
        Args:
            middleware: 要添加的中间件实例
        """
        if middleware.name in self._middlewares:
            logger.warning(f"中间件 {middleware.name} 已存在，将被替换")
        
        self._middlewares[middleware.name] = middleware
        self._chain_built = False
        logger.debug(f"添加中间件: {middleware.name}, 优先级: {middleware.priority.name}")
    
    def remove(self, middleware_name: str) -> bool:
        """
        移除中间件
        
        Args:
            middleware_name: 要移除的中间件名称
            
        Returns:
            是否成功移除
        """
        if middleware_name in self._middlewares:
            del self._middlewares[middleware_name]
            self._chain_built = False
            logger.debug(f"移除中间件: {middleware_name}")
            return True
        return False
    
    def get(self, middleware_name: str) -> Optional[Middleware]:
        """
        获取中间件实例
        
        Args:
            middleware_name: 中间件名称
            
        Returns:
            中间件实例，不存在则返回None
        """
        return self._middlewares.get(middleware_name)
    
    def clear(self) -> None:
        """清空所有中间件"""
        self._middlewares.clear()
        self._middleware_chain = None
        self._chain_built = False
        logger.debug("清空所有中间件")
    
    def _build_chain(self) -> None:
        """构建中间件责任链"""
        # 按优先级排序中间件
        sorted_middlewares = sorted(
            self._middlewares.values(),
            key=lambda m: m.priority.value
        )
        
        if not sorted_middlewares:
            self._middleware_chain = None
            self._chain_built = True
            return
        
        # 构建责任链
        self._middleware_chain = sorted_middlewares[0]
        current = self._middleware_chain
        
        for middleware in sorted_middlewares[1:]:
            current = current.set_next(middleware)
        
        self._chain_built = True
        middleware_names = [m.name for m in sorted_middlewares]
        logger.debug(f"构建中间件责任链完成，顺序: {' -> '.join(middleware_names)}")
    
    async def process(self, message: Any, context: Dict[str, Any] = None) -> Tuple[Any, Dict[str, Any]]:
        """
        执行完整的中间件处理流程
        
        Args:
            message: 要处理的消息
            context: 消息上下文
            
        Returns:
            处理后的消息和上下文
        """
        if not self._chain_built:
            self._build_chain()
        
        if not self._middleware_chain:
            return message, context or {}
        
        start_time = time.time()
        processed_message = message
        current_context = context or {}
        
        try:
            # 预处理阶段
            for middleware in self._middlewares.values():
                if not middleware.enabled:
                    continue
                
                stage_start = time.time()
                processed_message, current_context = await middleware.pre_process(
                    processed_message, current_context
                )
                stage_end = time.time()
                
                # 记录指标
                self._record_metric(middleware.name, MiddlewareStage.PRE_PROCESS, stage_end - stage_start)
            
            # 处理阶段
            processed_message, current_context = await self._middleware_chain.process(
                processed_message, current_context
            )
            
            # 后处理阶段（逆序执行）
            for middleware in reversed(list(self._middlewares.values())):
                if not middleware.enabled:
                    continue
                
                stage_start = time.time()
                processed_message, current_context = await middleware.post_process(
                    processed_message, current_context
                )
                stage_end = time.time()
                
                # 记录指标
                self._record_metric(middleware.name, MiddlewareStage.POST_PROCESS, stage_end - stage_start)
            
        except Exception as e:
            logger.error(f"中间件处理异常: {e}")
            
            # 调用错误处理器
            for error_handler in self._error_handlers:
                try:
                    await error_handler(e, message, current_context)
                except Exception as handler_error:
                    logger.error(f"错误处理器异常: {handler_error}")
            
            # 错误处理阶段
            for middleware in self._middlewares.values():
                if not middleware.enabled:
                    continue
                
                try:
                    stage_start = time.time()
                    processed_message, current_context = await middleware.error_handler(
                        e, message, current_context
                    )
                    stage_end = time.time()
                    
                    # 记录指标
                    self._record_metric(middleware.name, MiddlewareStage.ERROR, stage_end - stage_start)
                    
                    # 如果某个中间件处理了错误，不再继续传播
                    if current_context.get('error_handled', False):
                        break
                        
                except Exception as handler_error:
                    logger.error(f"中间件 {middleware.name} 错误处理器异常: {handler_error}")
        
        end_time = time.time()
        total_time = end_time - start_time
        
        # 记录总处理时间
        if 'total' not in self._metrics:
            self._metrics['total'] = {}
        self._metrics['total'][MiddlewareStage.PROCESS.name] = total_time
        
        # 处理警告
        if total_time > 0.1:  # 如果处理时间超过100ms，记录警告
            logger.warning(f"中间件处理耗时较长: {total_time:.4f}s")
        
        return processed_message, current_context
    
    def _record_metric(self, middleware_name: str, stage: MiddlewareStage, duration: float) -> None:
        """记录中间件性能指标"""
        if middleware_name not in self._metrics:
            self._metrics[middleware_name] = {}
        
        self._metrics[middleware_name][stage.name] = duration
    
    def get_metrics(self) -> Dict[str, Dict[str, float]]:
        """获取中间件性能指标"""
        return self._metrics
    
    def reset_metrics(self) -> None:
        """重置性能指标"""
        self._metrics.clear()
    
    def add_error_handler(self, handler: Callable) -> None:
        """
        添加错误处理器
        
        Args:
            handler: 错误处理函数，接收异常、原始消息和上下文作为参数
        """
        self._error_handlers.append(handler)
    
    def disable_middleware(self, middleware_name: str) -> bool:
        """
        禁用某个中间件
        
        Args:
            middleware_name: 中间件名称
            
        Returns:
            是否成功禁用
        """
        middleware = self.get(middleware_name)
        if middleware:
            middleware.enabled = False
            return True
        return False
    
    def enable_middleware(self, middleware_name: str) -> bool:
        """
        启用某个中间件
        
        Args:
            middleware_name: 中间件名称
            
        Returns:
            是否成功启用
        """
        middleware = self.get(middleware_name)
        if middleware:
            middleware.enabled = True
            return True
        return False
    
    def get_middleware_names(self) -> List[str]:
        """获取所有中间件名称"""
        return list(self._middlewares.keys())
    
    def get_enabled_middleware_names(self) -> List[str]:
        """获取所有启用的中间件名称"""
        return [name for name, middleware in self._middlewares.items() if middleware.enabled]
    
    def contains(self, middleware_name: str) -> bool:
        """
        检查是否包含指定中间件
        
        Args:
            middleware_name: 中间件名称
            
        Returns:
            是否包含
        """
        return middleware_name in self._middlewares
    
    def count(self) -> int:
        """获取中间件总数"""
        return len(self._middlewares)
    
    def register_by_type(self, middleware_class: Type[Middleware], **kwargs) -> Middleware:
        """
        通过类型注册中间件
        
        Args:
            middleware_class: 中间件类
            **kwargs: 传递给中间件构造函数的参数
            
        Returns:
            创建的中间件实例
        """
        middleware = middleware_class(**kwargs)
        self.add(middleware)
        return middleware


# 预定义的中间件

class LoggingMiddleware(Middleware):
    """日志记录中间件"""
    
    def __init__(self, log_level: int = logging.INFO, name: str = "LoggingMiddleware",
                 priority: MiddlewarePriority = MiddlewarePriority.HIGHEST):
        """
        初始化日志中间件
        
        Args:
            log_level: 日志级别
            name: 中间件名称
            priority: 中间件优先级
        """
        super().__init__(name=name, priority=priority)
        self.log_level = log_level
    
    async def pre_process(self, message: Any, context: Dict[str, Any] = None) -> Tuple[Any, Dict[str, Any]]:
        """记录请求日志"""
        ctx = context or {}
        logger.log(self.log_level, f"处理消息: {message}")
        
        # 添加处理开始时间
        ctx['process_start_time'] = time.time()
        return message, ctx
    
    async def post_process(self, message: Any, context: Dict[str, Any] = None) -> Tuple[Any, Dict[str, Any]]:
        """记录响应日志"""
        ctx = context or {}
        start_time = ctx.get('process_start_time', 0)
        
        if start_time:
            duration = time.time() - start_time
            logger.log(self.log_level, f"消息处理完成，耗时: {duration:.4f}s")
        else:
            logger.log(self.log_level, "消息处理完成")
        
        return message, ctx
    
    async def error_handler(self, error: Exception, original_message: Any, context: Dict[str, Any] = None) -> Tuple[Any, Dict[str, Any]]:
        """记录错误日志"""
        ctx = context or {}
        logger.error(f"消息处理错误: {error}, 原始消息: {original_message}")
        
        # 添加错误信息到上下文，但不处理错误
        ctx['error'] = str(error)
        ctx['error_type'] = error.__class__.__name__
        
        # 继续抛出异常
        raise error


class ValidationMiddleware(Middleware):
    """消息验证中间件"""
    
    def __init__(self, validators: List[Callable] = None, name: str = "ValidationMiddleware",
                 priority: MiddlewarePriority = MiddlewarePriority.HIGH,
                 stop_on_first_error: bool = True):
        """
        初始化验证中间件
        
        Args:
            validators: 验证器列表，每个验证器是一个函数，接收消息作为参数，返回(是否有效, 错误信息)
            name: 中间件名称
            priority: 中间件优先级
            stop_on_first_error: 是否在第一个错误时停止验证
        """
        super().__init__(name=name, priority=priority)
        self.validators = validators or []
        self.stop_on_first_error = stop_on_first_error
    
    def add_validator(self, validator: Callable) -> None:
        """添加验证器"""
        self.validators.append(validator)
    
    async def pre_process(self, message: Any, context: Dict[str, Any] = None) -> Tuple[Any, Dict[str, Any]]:
        """验证消息"""
        ctx = context or {}
        errors = []
        
        # 运行所有验证器
        for validator in self.validators:
            try:
                is_valid, error_message = validator(message)
                if not is_valid:
                    errors.append(error_message)
                    if self.stop_on_first_error:
                        break
            except Exception as e:
                errors.append(f"验证器异常: {e}")
                if self.stop_on_first_error:
                    break
        
        # 如果有错误，抛出异常
        if errors:
            error_message = "; ".join(errors)
            ctx['validation_errors'] = errors
            raise MiddlewareException(f"消息验证失败: {error_message}")
        
        return message, ctx


class SecurityMiddleware(Middleware):
    """安全中间件"""
    
    def __init__(self, checkers: List[Callable] = None, name: str = "SecurityMiddleware",
                 priority: MiddlewarePriority = MiddlewarePriority.HIGHEST):
        """
        初始化安全中间件
        
        Args:
            checkers: 安全检查器列表
            name: 中间件名称
            priority: 中间件优先级
        """
        super().__init__(name=name, priority=priority)
        self.checkers = checkers or []
    
    def add_checker(self, checker: Callable) -> None:
        """添加安全检查器"""
        self.checkers.append(checker)
    
    async def pre_process(self, message: Any, context: Dict[str, Any] = None) -> Tuple[Any, Dict[str, Any]]:
        """执行安全检查"""
        ctx = context or {}
        
        # 运行所有安全检查器
        for checker in self.checkers:
            try:
                is_safe, security_issue = checker(message, ctx)
                if not is_safe:
                    logger.warning(f"安全检查失败: {security_issue}")
                    raise MiddlewareException(f"安全检查失败: {security_issue}")
            except Exception as e:
                if not isinstance(e, MiddlewareException):
                    logger.error(f"安全检查器异常: {e}")
                    raise MiddlewareException(f"安全检查器异常: {e}")
                raise
        
        return message, ctx


class CachingMiddleware(Middleware):
    """缓存中间件"""
    
    def __init__(self, cache_size: int = 100, ttl: int = 300, name: str = "CachingMiddleware",
                 priority: MiddlewarePriority = MiddlewarePriority.NORMAL):
        """
        初始化缓存中间件
        
        Args:
            cache_size: 缓存容量
            ttl: 缓存时间，单位秒
            name: 中间件名称
            priority: 中间件优先级
        """
        super().__init__(name=name, priority=priority)
        self.cache: Dict[str, Tuple[Any, float]] = {}  # 键是缓存键，值是(缓存数据, 过期时间)
        self.cache_size = cache_size
        self.ttl = ttl
        self.hits = 0
        self.misses = 0
    
    def _generate_cache_key(self, message: Any) -> str:
        """生成缓存键"""
        # 默认使用消息的字符串表示作为键
        # 对于复杂消息类型，子类应该重写此方法
        return str(message)
    
    def _is_cacheable(self, message: Any) -> bool:
        """判断消息是否可缓存"""
        # 默认所有消息都可缓存
        # 子类可以重写此方法以实现更详细的缓存策略
        return True
    
    async def pre_process(self, message: Any, context: Dict[str, Any] = None) -> Tuple[Any, Dict[str, Any]]:
        """检查缓存"""
        ctx = context or {}
        
        # 如果消息不可缓存，直接返回
        if not self._is_cacheable(message):
            return message, ctx
        
        # 尝试从缓存获取结果
        cache_key = self._generate_cache_key(message)
        ctx['cache_key'] = cache_key
        
        current_time = time.time()
        
        # 清理过期缓存
        expired_keys = [k for k, (_, expiry) in self.cache.items() if expiry < current_time]
        for k in expired_keys:
            del self.cache[k]
        
        # 检查缓存
        if cache_key in self.cache:
            cached_result, expiry = self.cache[cache_key]
            if expiry >= current_time:
                # 缓存命中
                self.hits += 1
                ctx['cache_hit'] = True
                ctx['from_cache'] = True
                ctx['cache_expiry'] = expiry
                
                # 返回缓存的结果
                return cached_result, ctx
        
        # 缓存未命中
        self.misses += 1
        ctx['cache_hit'] = False
        
        return message, ctx
    
    async def post_process(self, message: Any, context: Dict[str, Any] = None) -> Tuple[Any, Dict[str, Any]]:
        """更新缓存"""
        ctx = context or {}
        
        # 如果已经是从缓存中获取的，不再更新缓存
        if ctx.get('from_cache', False):
            return message, ctx
        
        # 如果消息不可缓存或没有缓存键，直接返回
        if not self._is_cacheable(message) or 'cache_key' not in ctx:
            return message, ctx
        
        # 计算过期时间
        expiry = time.time() + self.ttl
        
        # 检查缓存大小，如果超过限制，移除最旧的缓存
        if len(self.cache) >= self.cache_size:
            # 按过期时间排序，移除将最早过期的项
            oldest_key = sorted(self.cache.items(), key=lambda x: x[1][1])[0][0]
            del self.cache[oldest_key]
        
        # 更新缓存
        self.cache[ctx['cache_key']] = (message, expiry)
        ctx['cache_updated'] = True
        ctx['cache_expiry'] = expiry
        
        return message, ctx
    
    def get_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息"""
        total = self.hits + self.misses
        hit_rate = self.hits / total if total > 0 else 0
        
        return {
            'size': len(self.cache),
            'capacity': self.cache_size,
            'hits': self.hits,
            'misses': self.misses,
            'hit_rate': hit_rate,
            'ttl': self.ttl
        }
    
    def clear_cache(self) -> None:
        """清空缓存"""
        self.cache.clear()
        logger.debug("缓存已清空")
    
    def set_ttl(self, ttl: int) -> None:
        """设置缓存存活时间"""
        self.ttl = ttl
        logger.debug(f"缓存TTL已更新: {ttl}秒")


class RateLimitingMiddleware(Middleware):
    """速率限制中间件"""
    
    def __init__(self, rate_limit: int = 100, window_size: int = 60, 
                 name: str = "RateLimitingMiddleware",
                 priority: MiddlewarePriority = MiddlewarePriority.HIGHEST):
        """
        初始化速率限制中间件
        
        Args:
            rate_limit: 在窗口期内允许的最大请求数
            window_size: 窗口期大小，单位秒
            name: 中间件名称
            priority: 中间件优先级
        """
        super().__init__(name=name, priority=priority)
        self.rate_limit = rate_limit
        self.window_size = window_size
        self.request_timestamps: Dict[str, List[float]] = {}  # 客户端ID -> 请求时间戳列表
    
    def _get_client_id(self, context: Dict[str, Any]) -> str:
        """从上下文获取客户端ID"""
        # 默认使用IP地址作为客户端ID
        # 子类可以重写此方法以使用其他标识
        return context.get('client_ip', 'unknown')
    
    async def pre_process(self, message: Any, context: Dict[str, Any] = None) -> Tuple[Any, Dict[str, Any]]:
        """检查速率限制"""
        ctx = context or {}
        
        # 获取客户端ID
        client_id = self._get_client_id(ctx)
        
        # 当前时间
        current_time = time.time()
        
        # 获取该客户端的请求时间戳列表
        if client_id not in self.request_timestamps:
            self.request_timestamps[client_id] = []
        
        timestamps = self.request_timestamps[client_id]
        
        # 清理过期时间戳
        cutoff_time = current_time - self.window_size
        self.request_timestamps[client_id] = [ts for ts in timestamps if ts > cutoff_time]
        
        # 检查是否超过速率限制
        if len(self.request_timestamps[client_id]) >= self.rate_limit:
            logger.warning(f"客户端 {client_id} 请求速率超限: {len(self.request_timestamps[client_id])}/{self.rate_limit}")
            raise MiddlewareException(f"请求速率超限，请稍后再试")
        
        # 记录当前请求
        self.request_timestamps[client_id].append(current_time)
        
        return message, ctx
    
    def set_rate_limit(self, rate_limit: int) -> None:
        """更新速率限制"""
        self.rate_limit = rate_limit
        logger.debug(f"速率限制已更新: {rate_limit}/窗口期")
    
    def set_window_size(self, window_size: int) -> None:
        """更新窗口期大小"""
        self.window_size = window_size
        logger.debug(f"窗口期大小已更新: {window_size}秒")
    
    def get_client_rate(self, client_id: str) -> int:
        """获取指定客户端的当前请求速率"""
        if client_id not in self.request_timestamps:
            return 0
        
        # 清理过期时间戳
        current_time = time.time()
        cutoff_time = current_time - self.window_size
        self.request_timestamps[client_id] = [ts for ts in timestamps if ts > cutoff_time]
        
        return len(self.request_timestamps[client_id])
    
    def reset_client(self, client_id: str) -> None:
        """重置指定客户端的请求记录"""
        if client_id in self.request_timestamps:
            del self.request_timestamps[client_id]
            logger.debug(f"已重置客户端 {client_id} 的请求记录")
    
    def reset_all(self) -> None:
        """重置所有客户端的请求记录"""
        self.request_timestamps.clear()
        logger.debug("已重置所有客户端的请求记录")


# 中间件装饰器
def middleware(middleware_class: Type[Middleware] = None, **middleware_kwargs):
    """
    中间件装饰器，将函数转换为具有中间件功能的函数
    
    Args:
        middleware_class: 中间件类
        **middleware_kwargs: 传递给中间件构造函数的参数
    
    Returns:
        装饰后的函数
    """
    def decorator(func):
        middleware_instance = None
        manager = MiddlewareManager()
        
        if middleware_class:
            middleware_instance = middleware_class(**middleware_kwargs)
            manager.add(middleware_instance)
        
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # 从函数签名中提取上下文参数
            context = {}
            sig = inspect.signature(func)
            
            for param_name, param in sig.parameters.items():
                if param_name == 'context' and param_name not in kwargs:
                    # 如果函数有context参数但没有传入，创建空的上下文
                    kwargs['context'] = context
                elif param_name in kwargs:
                    # 如果参数已经传入，可以加入到上下文
                    context[param_name] = kwargs[param_name]
            
            # 构造消息
            if len(args) > 0:
                message = args[0]  # 假设第一个位置参数是消息
            else:
                message = kwargs.get('message')  # 或者使用命名参数
            
            # 通过中间件处理
            processed_message, processed_context = await manager.process(message, context)
            
            # 更新kwargs中的context
            if 'context' in kwargs:
                kwargs['context'] = processed_context
            
            # 调用原始函数
            if len(args) > 0:
                new_args = (processed_message,) + args[1:]
                return await func(*new_args, **kwargs)
            else:
                kwargs['message'] = processed_message
                return await func(**kwargs)
        
        # 附加中间件管理器
        wrapper.middleware_manager = manager
        
        # 便捷方法
        wrapper.add_middleware = manager.add
        wrapper.get_metrics = manager.get_metrics
        wrapper.reset_metrics = manager.reset_metrics
        
        return wrapper
    
    # 支持无参数调用装饰器
    if callable(middleware_class) and not isinstance(middleware_class, type):
        func = middleware_class
        middleware_class = None
        return decorator(func)
    
    return decorator
