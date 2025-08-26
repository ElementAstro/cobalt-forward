"""
Tests for the dependency injection container.

This module tests the core dependency injection functionality including
service registration, resolution, and lifecycle management.
"""

import pytest
from typing import Protocol

from cobalt_forward.application.container import Container, ServiceLifetime, ServiceNotRegisteredException


class ITestService(Protocol):
    """Test service interface."""
    def get_value(self) -> str: ...


class TestService:
    """Test service implementation."""
    def __init__(self) -> None:
        self.value = "test"
    
    def get_value(self) -> str:
        return self.value


class TestServiceWithDependency:
    """Test service with dependency."""
    def __init__(self, dependency: ITestService) -> None:
        self.dependency = dependency
    
    def get_combined_value(self) -> str:
        return f"combined_{self.dependency.get_value()}"


class TestContainer:
    """Test cases for the dependency injection container."""
    
    def test_register_and_resolve_singleton(self) -> None:
        """Test singleton service registration and resolution."""
        container = Container()
        
        # Register service
        container.register(ITestService, TestService, ServiceLifetime.SINGLETON)
        
        # Resolve service twice
        service1 = container.resolve(ITestService)
        service2 = container.resolve(ITestService)
        
        # Should be the same instance
        assert service1 is service2
        assert service1.get_value() == "test"
    
    def test_register_and_resolve_transient(self) -> None:
        """Test transient service registration and resolution."""
        container = Container()
        
        # Register service
        container.register(ITestService, TestService, ServiceLifetime.TRANSIENT)
        
        # Resolve service twice
        service1 = container.resolve(ITestService)
        service2 = container.resolve(ITestService)
        
        # Should be different instances
        assert service1 is not service2
        assert service1.get_value() == "test"
        assert service2.get_value() == "test"
    
    def test_register_instance(self):
        """Test instance registration."""
        container = Container()
        instance = TestService()
        
        # Register instance
        container.register_instance(ITestService, instance)
        
        # Resolve service
        resolved = container.resolve(ITestService)
        
        # Should be the same instance
        assert resolved is instance
    
    def test_dependency_injection(self):
        """Test automatic dependency injection."""
        container = Container()
        
        # Register dependencies
        container.register(ITestService, TestService, ServiceLifetime.SINGLETON)
        container.register(TestServiceWithDependency, TestServiceWithDependency, ServiceLifetime.SINGLETON)
        
        # Resolve service with dependency
        service = container.resolve(TestServiceWithDependency)
        
        # Should have dependency injected
        assert service.get_combined_value() == "combined_test"
    
    def test_service_not_registered_exception(self):
        """Test exception when resolving unregistered service."""
        container = Container()
        
        with pytest.raises(ServiceNotRegisteredException):
            container.resolve(ITestService)
    
    def test_try_resolve_returns_none(self):
        """Test try_resolve returns None for unregistered service."""
        container = Container()
        
        result = container.try_resolve(ITestService)
        assert result is None
    
    def test_is_registered(self):
        """Test service registration checking."""
        container = Container()
        
        # Initially not registered
        assert not container.is_registered(ITestService)
        
        # Register service
        container.register(ITestService, TestService)
        
        # Now registered
        assert container.is_registered(ITestService)
    
    def test_factory_function_registration(self):
        """Test registration with factory function."""
        container = Container()
        
        def create_service() -> ITestService:
            service = TestService()
            service.value = "factory_created"
            return service
        
        # Register factory
        container.register(ITestService, create_service)
        
        # Resolve service
        service = container.resolve(ITestService)
        assert service.get_value() == "factory_created"


@pytest.mark.asyncio
class TestContainerAsync:
    """Async test cases for container functionality."""
    
    async def test_container_with_async_components(self):
        """Test container with async component lifecycle."""
        container = Container()
        
        # Register service
        container.register(ITestService, TestService)
        
        # Resolve and verify
        service = container.resolve(ITestService)
        assert service.get_value() == "test"
