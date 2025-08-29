"""
Dependency injection container for managing service lifecycles and dependencies.

This module provides a lightweight dependency injection container that supports
singleton and transient service lifetimes, automatic dependency resolution,
and interface-based service registration.
"""

import inspect
import logging
from abc import ABC, abstractmethod
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Type, TypeVar, Union, get_type_hints

logger = logging.getLogger(__name__)

T = TypeVar('T')


class ServiceLifetime(Enum):
    """Service lifetime management options."""
    SINGLETON = auto()  # Single instance shared across application
    TRANSIENT = auto()  # New instance created each time
    SCOPED = auto()     # Single instance per scope (future enhancement)


class ServiceRegistration:
    """Registration information for a service."""

    def __init__(self,
                 service_type: Type[T],
                 implementation: Union[Type[T], Callable[[], T], T],
                 lifetime: ServiceLifetime = ServiceLifetime.SINGLETON,
                 dependencies: Optional[List[Type[Any]]] = None):
        self.service_type = service_type
        self.implementation = implementation
        self.lifetime = lifetime
        self.dependencies = dependencies or []
        self.instance: Optional[T] = None

        # If implementation is already an instance, treat as singleton
        if not (inspect.isclass(implementation) or callable(implementation)):
            self.instance = implementation
            self.lifetime = ServiceLifetime.SINGLETON


class IContainer(ABC):
    """Interface for dependency injection containers."""

    @abstractmethod
    def register(self,
                 service_type: Type[T],
                 implementation: Union[Type[T], Callable[[], T], T],
                 lifetime: ServiceLifetime = ServiceLifetime.SINGLETON) -> None:
        """
        Register a service with the container.

        Args:
            service_type: Interface or base type
            implementation: Implementation class, factory function, or instance
            lifetime: Service lifetime management
        """
        pass

    @abstractmethod
    def register_instance(self, service_type: Type[T], instance: T) -> None:
        """
        Register a specific instance as a singleton.

        Args:
            service_type: Interface or base type
            instance: Service instance
        """
        pass

    @abstractmethod
    def resolve(self, service_type: Type[T]) -> T:
        """
        Resolve a service instance.

        Args:
            service_type: Type to resolve

        Returns:
            Service instance

        Raises:
            ServiceNotRegisteredException: If service not registered
            ServiceResolutionException: If service cannot be resolved
        """
        pass

    @abstractmethod
    def try_resolve(self, service_type: Type[T]) -> Optional[T]:
        """
        Try to resolve a service instance without raising exceptions.

        Args:
            service_type: Type to resolve

        Returns:
            Service instance or None if not found
        """
        pass

    @abstractmethod
    def is_registered(self, service_type: Type[T]) -> bool:
        """
        Check if a service type is registered.

        Args:
            service_type: Type to check

        Returns:
            True if registered
        """
        pass


class ServiceNotRegisteredException(Exception):
    """Raised when trying to resolve an unregistered service."""
    pass


class ServiceResolutionException(Exception):
    """Raised when service resolution fails."""
    pass


class CircularDependencyException(Exception):
    """Raised when circular dependencies are detected."""
    pass


class Container(IContainer):
    """
    Lightweight dependency injection container.

    Supports automatic constructor injection, singleton and transient lifetimes,
    and circular dependency detection.
    """

    def __init__(self) -> None:
        self._services: Dict[Type[Any], ServiceRegistration] = {}
        self._resolution_stack: List[Type[Any]] = []

    def register(self,
                 service_type: Type[T],
                 implementation: Union[Type[T], Callable[[], T], T],
                 lifetime: ServiceLifetime = ServiceLifetime.SINGLETON) -> None:
        """Register a service with the container."""
        logger.debug(
            f"Registering service: {service_type.__name__} -> {implementation}")

        # Analyze dependencies if implementation is a class
        dependencies = []
        if inspect.isclass(implementation):
            dependencies = self._analyze_dependencies(implementation)

        registration = ServiceRegistration(
            service_type=service_type,
            implementation=implementation,
            lifetime=lifetime,
            dependencies=dependencies
        )

        self._services[service_type] = registration
        logger.info(
            f"Registered {service_type.__name__} with {lifetime.name} lifetime")

    def register_instance(self, service_type: Type[T], instance: T) -> None:
        """Register a specific instance as a singleton."""
        self.register(service_type, instance, ServiceLifetime.SINGLETON)

    def resolve(self, service_type: Type[T]) -> T:
        """Resolve a service instance."""
        if service_type in self._resolution_stack:
            cycle = " -> ".join([t.__name__ for t in self._resolution_stack] +
                                [service_type.__name__])
            raise CircularDependencyException(
                f"Circular dependency detected: {cycle}")

        if service_type not in self._services:
            raise ServiceNotRegisteredException(
                f"Service {service_type.__name__} is not registered")

        registration = self._services[service_type]

        # Return existing singleton instance
        if registration.lifetime == ServiceLifetime.SINGLETON and registration.instance is not None:
            return registration.instance  # type: ignore[return-value]

        # Create new instance
        self._resolution_stack.append(service_type)
        try:
            instance = self._create_instance(registration)

            # Store singleton instance
            if registration.lifetime == ServiceLifetime.SINGLETON:
                registration.instance = instance

            return instance  # type: ignore[no-any-return]

        except Exception as e:
            raise ServiceResolutionException(
                f"Failed to resolve {service_type.__name__}: {str(e)}") from e

        finally:
            self._resolution_stack.pop()

    def try_resolve(self, service_type: Type[T]) -> Optional[T]:
        """Try to resolve a service instance without raising exceptions."""
        try:
            return self.resolve(service_type)
        except (ServiceNotRegisteredException, ServiceResolutionException, CircularDependencyException):
            return None

    def is_registered(self, service_type: Type[T]) -> bool:
        """Check if a service type is registered."""
        return service_type in self._services

    def get_registrations(self) -> Dict[Type[Any], ServiceRegistration]:
        """Get all service registrations (for debugging)."""
        return self._services.copy()

    def _create_instance(self, registration: ServiceRegistration) -> Any:
        """Create an instance from a service registration."""
        implementation = registration.implementation

        # If implementation is already an instance
        if not (inspect.isclass(implementation) or callable(implementation)):
            return implementation

        # If implementation is a factory function
        if callable(implementation) and not inspect.isclass(implementation):
            return implementation()

        # If implementation is a class, resolve constructor dependencies
        if inspect.isclass(implementation):
            constructor_args = []
            
            # Get constructor signature to handle optional parameters
            constructor = implementation.__init__
            signature = inspect.signature(constructor)
            type_hints = get_type_hints(constructor)
            
            for param_name, param in signature.parameters.items():
                if param_name == 'self':
                    continue
                    
                # Handle parameters with default values (optional dependencies)
                if param.default is not inspect.Parameter.empty:
                    # Try to resolve the dependency, but use default if not available
                    if param_name in type_hints:
                        param_type = type_hints[param_name]
                        # Handle Optional types
                        if hasattr(param_type, '__origin__') and param_type.__origin__ is Union:
                            args = param_type.__args__
                            if len(args) == 2 and type(None) in args:
                                non_none_type = next(arg for arg in args if arg is not type(None))
                                dep_instance = self.try_resolve(non_none_type)
                                constructor_args.append(dep_instance)  # Could be None
                                continue
                        
                        # Non-optional dependency with default value
                        dep_instance = self.try_resolve(param_type)
                        if dep_instance is not None:
                            constructor_args.append(dep_instance)
                        else:
                            constructor_args.append(param.default)
                    else:
                        constructor_args.append(param.default)
                else:
                    # Required dependency, must be resolved
                    for dep_type in registration.dependencies:
                        dep_instance = self.resolve(dep_type)
                        constructor_args.append(dep_instance)
                    break  # All required dependencies handled

            return implementation(*constructor_args)

        raise ServiceResolutionException(
            f"Cannot create instance from {implementation}")

    def _analyze_dependencies(self, implementation_class: Type[Any]) -> List[Type[Any]]:
        """Analyze constructor dependencies of a class."""
        try:
            # Get constructor signature
            constructor = implementation_class.__init__
            signature = inspect.signature(constructor)

            # Get type hints
            type_hints = get_type_hints(constructor)

            dependencies = []
            for param_name, param in signature.parameters.items():
                # Skip 'self' parameter
                if param_name == 'self':
                    continue

                # Skip parameters with default values for Optional types
                if param.default is not inspect.Parameter.empty:
                    continue

                # Get parameter type from type hints
                if param_name in type_hints:
                    param_type = type_hints[param_name]
                    
                    # Handle Optional types (Union[T, None])
                    if hasattr(param_type, '__origin__') and param_type.__origin__ is Union:
                        args = param_type.__args__
                        if len(args) == 2 and type(None) in args:
                            # This is Optional[T], extract the non-None type
                            non_none_type = next(arg for arg in args if arg is not type(None))
                            dependencies.append(non_none_type)
                        else:
                            # Regular Union type, not Optional
                            dependencies.append(param_type)
                    else:
                        dependencies.append(param_type)
                elif param.annotation != inspect.Parameter.empty:
                    dependencies.append(param.annotation)

            return dependencies

        except Exception as e:
            logger.warning(
                f"Failed to analyze dependencies for {implementation_class.__name__}: {e}")
            return []
