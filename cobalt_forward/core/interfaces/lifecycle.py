"""
Lifecycle management interfaces for components that need startup/shutdown behavior.

These interfaces provide a consistent way to manage component lifecycles
and health monitoring across the application.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional


class IStartable(ABC):
    """Interface for components that can be started."""
    
    @abstractmethod
    async def start(self) -> None:
        """
        Start the component.
        
        This method should initialize any resources needed by the component
        and prepare it for normal operation.
        
        Raises:
            Exception: If the component fails to start.
        """
        pass


class IStoppable(ABC):
    """Interface for components that can be stopped."""
    
    @abstractmethod
    async def stop(self) -> None:
        """
        Stop the component gracefully.
        
        This method should clean up any resources used by the component
        and ensure a graceful shutdown.
        
        Raises:
            Exception: If the component fails to stop cleanly.
        """
        pass


class IHealthCheckable(ABC):
    """Interface for components that can report their health status."""
    
    @abstractmethod
    async def check_health(self) -> Dict[str, Any]:
        """
        Check the health status of the component.
        
        Returns:
            Dict containing health information with at least:
            - 'healthy': bool indicating if component is healthy
            - 'status': str describing current status
            - 'details': Dict with additional health details
            
        Example:
            {
                'healthy': True,
                'status': 'running',
                'details': {
                    'uptime': 3600,
                    'connections': 5,
                    'last_error': None
                }
            }
        """
        pass


class IConfigurable(ABC):
    """Interface for components that can be configured."""
    
    @abstractmethod
    async def configure(self, config: Dict[str, Any]) -> None:
        """
        Configure the component with the provided configuration.
        
        Args:
            config: Configuration dictionary
            
        Raises:
            ValueError: If configuration is invalid
        """
        pass


class IComponent(IStartable, IStoppable, IHealthCheckable, IConfigurable):
    """
    Base interface for all major system components.
    
    Combines all lifecycle interfaces into a single interface that
    components can implement to provide full lifecycle management.
    """
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Get the component name."""
        pass
    
    @property
    @abstractmethod
    def version(self) -> str:
        """Get the component version."""
        pass
