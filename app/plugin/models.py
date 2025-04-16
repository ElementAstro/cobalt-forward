"""
Plugin system models and data structures.

This module defines the data models used by the plugin system, including
plugin metadata, configuration schemas, and plugin states.
"""
from enum import Enum, auto
from typing import Dict, List, Any, Optional, Set, Union
from dataclasses import dataclass, field
import datetime
import uuid
import semver
from pydantic import BaseModel, Field, validator


class PluginState(str, Enum):
    """
    Enumeration of possible plugin states.
    """
    UNLOADED = "unloaded"     # Plugin has been discovered but not loaded
    LOADING = "loading"       # Plugin is currently being loaded
    ACTIVE = "active"         # Plugin is loaded and active
    INACTIVE = "inactive"     # Plugin is loaded but inactive (disabled)
    ERROR = "error"           # Plugin failed to load or encountered an error
    PENDING = "pending"       # Plugin is waiting for dependencies
    UPDATING = "updating"     # Plugin is being updated
    INCOMPATIBLE = "incompatible"  # Plugin is incompatible with current version


class PluginCategory(str, Enum):
    """
    Categorization of plugins by function.
    """
    INTEGRATION = "integration"   # Integrates with external systems
    UTILITY = "utility"           # Provides utility functions
    PROCESSOR = "processor"       # Processes data
    UI = "ui"                     # UI components or enhancements
    SECURITY = "security"         # Security-related plugins
    ANALYTICS = "analytics"       # Data analytics
    STORAGE = "storage"           # Storage-related plugins
    OTHER = "other"               # Uncategorized


@dataclass
class PluginMetadata:
    """
    Metadata for a plugin.
    """
    name: str                            # Unique plugin identifier
    display_name: str                    # Human-readable name
    version: str                         # Semantic version
    description: str                     # Plugin description
    author: str = "Unknown"              # Plugin author
    email: Optional[str] = None          # Author email
    website: Optional[str] = None        # Plugin website
    license: str = "Proprietary"         # License information
    category: PluginCategory = PluginCategory.OTHER  # Plugin category
    tags: List[str] = field(default_factory=list)    # Tags for searching
    dependencies: List[str] = field(default_factory=list)  # Required plugins
    conflicts: List[str] = field(default_factory=list)     # Conflicting plugins
    min_app_version: Optional[str] = None  # Minimum application version
    max_app_version: Optional[str] = None  # Maximum application version
    config_schema: Dict[str, Any] = field(default_factory=dict)  # JSON schema for config
    icon: Optional[str] = None           # Icon for UI
    load_priority: int = 100             # Loading priority (lower loads first)
    state: PluginState = PluginState.UNLOADED  # Current state
    creation_date: datetime.datetime = field(default_factory=datetime.datetime.now)
    last_updated: Optional[datetime.datetime] = None
    uuid: str = field(default_factory=lambda: str(uuid.uuid4()))
    
    def __post_init__(self):
        """Validate and normalize metadata after initialization."""
        # Validate version
        try:
            semver.VersionInfo.parse(self.version)
        except ValueError:
            # If not a valid semver, try to normalize it
            if self.version.count('.') == 1:
                self.version = f"{self.version}.0"
            else:
                # Default to a basic version if parsing fails
                self.version = "0.1.0"
        
        # Normalize name
        self.name = self.name.strip().lower().replace(' ', '_')
        
        # Set display name if not provided
        if not hasattr(self, 'display_name') or not self.display_name:
            self.display_name = self.name.replace('_', ' ').title()
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert metadata to dictionary.
        
        Returns:
            Dictionary of plugin metadata
        """
        return {
            "name": self.name,
            "display_name": self.display_name,
            "version": self.version,
            "description": self.description,
            "author": self.author,
            "email": self.email,
            "website": self.website,
            "license": self.license,
            "category": self.category.value,
            "tags": self.tags,
            "dependencies": self.dependencies,
            "conflicts": self.conflicts,
            "min_app_version": self.min_app_version,
            "max_app_version": self.max_app_version,
            "icon": self.icon,
            "load_priority": self.load_priority,
            "state": self.state.value,
            "uuid": self.uuid
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'PluginMetadata':
        """
        Create metadata from dictionary.
        
        Args:
            data: Dictionary of metadata values
            
        Returns:
            PluginMetadata instance
        """
        # Convert string category to enum
        if "category" in data and isinstance(data["category"], str):
            try:
                data["category"] = PluginCategory(data["category"])
            except ValueError:
                data["category"] = PluginCategory.OTHER
                
        # Convert string state to enum
        if "state" in data and isinstance(data["state"], str):
            try:
                data["state"] = PluginState(data["state"])
            except ValueError:
                data["state"] = PluginState.UNLOADED
                
        # Handle datetime fields if they're strings
        for dt_field in ["creation_date", "last_updated"]:
            if dt_field in data and isinstance(data[dt_field], str):
                try:
                    data[dt_field] = datetime.datetime.fromisoformat(data[dt_field])
                except (ValueError, TypeError):
                    if dt_field == "creation_date":
                        data[dt_field] = datetime.datetime.now()
                    else:
                        data[dt_field] = None
        
        return cls(**data)
    
    def is_compatible_with(self, other_version: str) -> bool:
        """
        Check if plugin is compatible with a specific version.
        
        Args:
            other_version: Version to check compatibility with
            
        Returns:
            True if compatible, False otherwise
        """
        try:
            app_version = semver.VersionInfo.parse(other_version)
            
            # Check minimum version constraint
            if self.min_app_version:
                min_version = semver.VersionInfo.parse(self.min_app_version)
                if app_version < min_version:
                    return False
                    
            # Check maximum version constraint
            if self.max_app_version:
                max_version = semver.VersionInfo.parse(self.max_app_version)
                if app_version > max_version:
                    return False
                    
            return True
        except ValueError:
            # If version parsing fails, assume incompatible
            return False


class PluginConfigSchema(BaseModel):
    """
    Schema definition for plugin configuration.
    """
    title: str = Field(..., description="Config schema title")
    description: str = Field(default="", description="Schema description")
    type: str = Field(default="object", description="JSON Schema type")
    properties: Dict[str, Dict[str, Any]] = Field(
        default_factory=dict, 
        description="Properties definition"
    )
    required: List[str] = Field(
        default_factory=list,
        description="List of required property names"
    )
    default: Dict[str, Any] = Field(
        default_factory=dict,
        description="Default configuration values"
    )
    
    class Config:
        schema_extra = {
            "example": {
                "title": "My Plugin Config",
                "description": "Configuration for my awesome plugin",
                "type": "object",
                "properties": {
                    "enabled": {
                        "type": "boolean",
                        "description": "Enable the plugin",
                        "default": True
                    },
                    "api_key": {
                        "type": "string",
                        "description": "API key for external service"
                    },
                    "retry_count": {
                        "type": "integer",
                        "description": "Number of retry attempts",
                        "minimum": 1,
                        "maximum": 10,
                        "default": 3
                    }
                },
                "required": ["api_key"],
                "default": {
                    "enabled": True,
                    "retry_count": 3
                }
            }
        }


class PluginManifest(BaseModel):
    """
    Plugin manifest file structure.
    """
    name: str = Field(..., description="Plugin unique identifier")
    display_name: str = Field(..., description="Human-readable name")
    version: str = Field(..., description="Semantic version")
    description: str = Field(..., description="Plugin description")
    main: str = Field(..., description="Main plugin file")
    author: str = Field(default="Unknown", description="Plugin author")
    email: Optional[str] = Field(default=None, description="Author email")
    website: Optional[str] = Field(default=None, description="Plugin website")
    license: str = Field(default="Proprietary", description="License information")
    category: str = Field(default="other", description="Plugin category")
    tags: List[str] = Field(default_factory=list, description="Tags for searching")
    dependencies: List[str] = Field(default_factory=list, description="Required plugins")
    conflicts: List[str] = Field(default_factory=list, description="Conflicting plugins")
    min_app_version: Optional[str] = Field(default=None, description="Minimum app version")
    max_app_version: Optional[str] = Field(default=None, description="Maximum app version")
    config_schema: Optional[Dict[str, Any]] = Field(default=None, description="JSON schema for config")
    icon: Optional[str] = Field(default=None, description="Icon for UI")
    load_priority: int = Field(default=100, description="Loading priority (lower loads first)")
    
    @validator('version')
    def validate_version(cls, v):
        """Validate semantic version."""
        try:
            semver.VersionInfo.parse(v)
            return v
        except ValueError:
            if v.count('.') == 1:
                return f"{v}.0"
            raise ValueError("Invalid semantic version")
    
    @validator('category')
    def validate_category(cls, v):
        """Validate plugin category."""
        try:
            PluginCategory(v)
            return v
        except ValueError:
            return "other"
    
    def to_metadata(self) -> PluginMetadata:
        """
        Convert manifest to plugin metadata.
        
        Returns:
            PluginMetadata instance
        """
        # Convert string category to enum
        try:
            category = PluginCategory(self.category)
        except ValueError:
            category = PluginCategory.OTHER
            
        return PluginMetadata(
            name=self.name,
            display_name=self.display_name,
            version=self.version,
            description=self.description,
            author=self.author,
            email=self.email,
            website=self.website,
            license=self.license,
            category=category,
            tags=self.tags,
            dependencies=self.dependencies,
            conflicts=self.conflicts,
            min_app_version=self.min_app_version,
            max_app_version=self.max_app_version,
            config_schema=self.config_schema or {},
            icon=self.icon,
            load_priority=self.load_priority,
            state=PluginState.UNLOADED
        )
    
    class Config:
        schema_extra = {
            "example": {
                "name": "my_awesome_plugin",
                "display_name": "My Awesome Plugin",
                "version": "1.0.0",
                "description": "This plugin does awesome things",
                "main": "plugin.py",
                "author": "John Doe",
                "email": "john@example.com",
                "website": "https://example.com/plugins/my-awesome-plugin",
                "license": "MIT",
                "category": "utility",
                "tags": ["awesome", "utility", "example"],
                "dependencies": ["base_plugin"],
                "conflicts": ["conflicting_plugin"],
                "min_app_version": "1.0.0",
                "max_app_version": "2.0.0",
                "load_priority": 50
            }
        }


class PluginHealth(BaseModel):
    """
    Plugin health status.
    """
    status: str = Field(..., description="Health status (healthy, warning, error)")
    plugin: str = Field(..., description="Plugin name")
    version: str = Field(..., description="Plugin version")
    message: Optional[str] = Field(default=None, description="Status message")
    details: Dict[str, Any] = Field(default_factory=dict, description="Detailed status information")
    timestamp: datetime.datetime = Field(default_factory=datetime.datetime.now, description="Status timestamp")
    
    class Config:
        schema_extra = {
            "example": {
                "status": "healthy",
                "plugin": "example_plugin",
                "version": "1.0.0",
                "message": "Plugin is running normally",
                "details": {
                    "memory_usage": "12.5MB",
                    "uptime": "2h 15m",
                    "active_connections": 5
                },
                "timestamp": "2025-02-15T12:30:45.123456"
            }
        }
