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


class TestContainerEdgeCases:
    """Test edge cases and advanced scenarios for the container."""

    def test_register_same_service_twice_overwrites(self) -> None:
        """Test that registering the same service twice overwrites the first registration."""
        container = Container()

        # Register first implementation
        container.register(ITestService, TestService, ServiceLifetime.SINGLETON)
        service1 = container.resolve(ITestService)

        # Register second implementation (should overwrite)
        class AnotherTestService:
            def get_value(self) -> str:
                return "another"

        container.register(ITestService, AnotherTestService, ServiceLifetime.SINGLETON)
        service2 = container.resolve(ITestService)

        # Should get the new implementation
        assert service2.get_value() == "another"
        assert service1 is not service2

    def test_resolve_unregistered_service_raises_exception(self) -> None:
        """Test that resolving unregistered service raises exception."""
        container = Container()

        with pytest.raises(ServiceNotRegisteredException):
            container.resolve(ITestService)

    def test_try_resolve_unregistered_service_returns_none(self) -> None:
        """Test that try_resolve returns None for unregistered service."""
        container = Container()

        result = container.try_resolve(ITestService)
        assert result is None

    def test_register_instance_with_none_value(self) -> None:
        """Test registering None as an instance."""
        container = Container()

        container.register_instance(ITestService, None)
        result = container.resolve(ITestService)
        assert result is None

    def test_register_factory_function(self) -> None:
        """Test registering a factory function."""
        container = Container()

        def create_service() -> TestService:
            service = TestService()
            service.value = "factory_created"
            return service

        container.register(ITestService, create_service, ServiceLifetime.TRANSIENT)

        service1 = container.resolve(ITestService)
        service2 = container.resolve(ITestService)

        assert service1.get_value() == "factory_created"
        assert service2.get_value() == "factory_created"
        assert service1 is not service2  # Transient should create new instances

    def test_register_factory_function_singleton(self) -> None:
        """Test registering a factory function with singleton lifetime."""
        container = Container()

        call_count = 0
        def create_service() -> TestService:
            nonlocal call_count
            call_count += 1
            service = TestService()
            service.value = f"factory_{call_count}"
            return service

        container.register(ITestService, create_service, ServiceLifetime.SINGLETON)

        service1 = container.resolve(ITestService)
        service2 = container.resolve(ITestService)

        assert service1.get_value() == "factory_1"
        assert service2.get_value() == "factory_1"
        assert service1 is service2  # Singleton should return same instance
        assert call_count == 1  # Factory should only be called once

    def test_complex_dependency_chain(self) -> None:
        """Test resolving complex dependency chains."""
        container = Container()

        class ServiceA:
            def get_name(self) -> str:
                return "A"

        class ServiceB:
            def __init__(self, service_a: ServiceA) -> None:
                self.service_a = service_a

            def get_name(self) -> str:
                return f"B-{self.service_a.get_name()}"

        class ServiceC:
            def __init__(self, service_b: ServiceB, service_a: ServiceA) -> None:
                self.service_b = service_b
                self.service_a = service_a

            def get_name(self) -> str:
                return f"C-{self.service_b.get_name()}-{self.service_a.get_name()}"

        # Register services
        container.register(ServiceA, ServiceA, ServiceLifetime.SINGLETON)
        container.register(ServiceB, ServiceB, ServiceLifetime.SINGLETON)
        container.register(ServiceC, ServiceC, ServiceLifetime.SINGLETON)

        # Resolve complex service
        service_c = container.resolve(ServiceC)
        assert service_c.get_name() == "C-B-A-A"

        # Verify singleton behavior - same ServiceA instance used everywhere
        service_a_direct = container.resolve(ServiceA)
        assert service_c.service_a is service_a_direct
        assert service_c.service_b.service_a is service_a_direct

    def test_mixed_lifetimes_dependency_chain(self) -> None:
        """Test dependency chain with mixed lifetimes."""
        container = Container()

        class SingletonService:
            def __init__(self) -> None:
                self.id = id(self)

        class TransientService:
            def __init__(self, singleton: SingletonService) -> None:
                self.singleton = singleton
                self.id = id(self)

        container.register(SingletonService, SingletonService, ServiceLifetime.SINGLETON)
        container.register(TransientService, TransientService, ServiceLifetime.TRANSIENT)

        # Resolve multiple times
        transient1 = container.resolve(TransientService)
        transient2 = container.resolve(TransientService)

        # Transient services should be different instances
        assert transient1 is not transient2
        assert transient1.id != transient2.id

        # But they should share the same singleton dependency
        assert transient1.singleton is transient2.singleton
        assert transient1.singleton.id == transient2.singleton.id

    def test_service_with_optional_dependencies(self) -> None:
        """Test service with optional dependencies using try_resolve."""
        container = Container()

        class ServiceWithOptionalDep:
            def __init__(self) -> None:
                # Simulate optional dependency injection
                self.optional_service = container.try_resolve(ITestService)

            def has_dependency(self) -> bool:
                return self.optional_service is not None

        # Register service without the optional dependency
        container.register(ServiceWithOptionalDep, ServiceWithOptionalDep)

        service = container.resolve(ServiceWithOptionalDep)
        assert service.has_dependency() is False

        # Now register the optional dependency
        container.register(ITestService, TestService)

        # Create new service instance (this would need manual re-creation in real scenario)
        service2 = ServiceWithOptionalDep()
        assert service2.has_dependency() is True

    def test_container_state_isolation(self) -> None:
        """Test that different container instances are isolated."""
        container1 = Container()
        container2 = Container()

        # Register different services in each container
        container1.register(ITestService, TestService)

        class AnotherService:
            def get_value(self) -> str:
                return "another"

        container2.register(ITestService, AnotherService)

        # Verify isolation
        service1 = container1.resolve(ITestService)
        service2 = container2.resolve(ITestService)

        assert service1.get_value() == "test"
        assert service2.get_value() == "another"

        # Verify one container doesn't affect the other
        assert container1.try_resolve(AnotherService) is None
        assert container2.try_resolve(TestService) is None
