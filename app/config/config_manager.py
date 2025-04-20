from datetime import datetime
from typing import Dict, Any, Optional, List, Callable, Coroutine, Union
import yaml
from pathlib import Path
import asyncio
from loguru import logger
from dataclasses import asdict
import jsonschema
import zlib
import os
import copy

from app.models.config_model import RuntimeConfig
from .file_watcher import FileWatcher
from .config_crypto import ConfigCrypto
from .config_backup import ConfigBackup

# Type aliases
ObserverCallable = Callable[[RuntimeConfig], Coroutine]


class ConfigManager:
    """
    Configuration management system that handles:
    - Loading/saving configurations with validation
    - Hot reloading and change notifications
    - Encryption and backup capabilities
    - Versioning and rollback support
    
    Supports observers pattern for change notifications to dependent components.
    """

    def __init__(self, config_path: str, encrypt: bool = False):
        """
        Initialize the configuration manager.
        
        Args:
            config_path: Path to the configuration file
            encrypt: Whether to encrypt the configuration file
        """
        self.config_path = Path(config_path).resolve()
        self.config_dir = self.config_path.parent
        self._observers: List[ObserverCallable] = []
        self._lock = asyncio.Lock()  # Lock to prevent concurrent access issues
        self._config_loaded = asyncio.Event()
        self._plugin_schemas = {}  # Registry for plugin-provided schemas

        # Ensure config directory exists
        self.config_dir.mkdir(parents=True, exist_ok=True)

        # Initialize encryption tool
        self.encrypt_enabled = encrypt
        self.crypto = ConfigCrypto(
            self.config_dir / ".config.key") if encrypt else None

        # Initialize backup tool
        self.backup_manager = ConfigBackup(self.config_dir / "backups")

        # Load configuration
        self.runtime_config: RuntimeConfig = self._load_default_config()
        self._cache = {}  # Value caching for performance
        self._version_history: List[Dict[str, Any]] = []
        self._max_history = 20  # Maximum version history size
        self._load_schema()
        self.file_watcher = FileWatcher(self.config_path, self)

        logger.info(f"ConfigManager initialized with config path: {config_path}")

    def _load_schema(self):
        """
        Load configuration validation schema.
        Provides core schema validation for basic config fields.
        """
        try:
            self.schema = {
                "type": "object",
                "properties": {
                    "version": {"type": "string"},
                    "tcp_host": {"type": "string"},
                    "tcp_port": {"type": "integer", "minimum": 1, "maximum": 65535},
                    "websocket_host": {"type": "string"},
                    "websocket_port": {"type": "integer", "minimum": 1, "maximum": 65535}
                },
                "required": ["tcp_host", "tcp_port", "websocket_host", "websocket_port"],
                # Add extensibility point for plugins
                "additionalProperties": True
            }
        except Exception as e:
            logger.error(f"Failed to load schema: {e}")
            raise

    def register_plugin_schema(self, plugin_id: str, schema_fragment: dict) -> bool:
        """
        Register plugin-specific schema fragments for validation.
        Allows plugins to extend config validation without modifying core schema.
        
        Args:
            plugin_id: Unique identifier for the plugin
            schema_fragment: JSON schema fragment to validate plugin config
            
        Returns:
            True if registration succeeded, False otherwise
        """
        try:
            logger.debug(f"Registering schema for plugin: {plugin_id}")
            if plugin_id in self._plugin_schemas:
                logger.warning(f"Schema for plugin {plugin_id} already exists, overwriting")
            
            # Validate schema fragment
            jsonschema.Draft7Validator.check_schema(schema_fragment)
            
            # Store plugin schema
            self._plugin_schemas[plugin_id] = schema_fragment
            logger.info(f"Schema for plugin {plugin_id} registered successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to register schema for plugin {plugin_id}: {e}")
            return False

    def unregister_plugin_schema(self, plugin_id: str) -> bool:
        """
        Unregister plugin schema when plugin is disabled.
        
        Args:
            plugin_id: Unique identifier for the plugin
            
        Returns:
            True if unregistration succeeded, False if plugin not found
        """
        if plugin_id in self._plugin_schemas:
            del self._plugin_schemas[plugin_id]
            logger.info(f"Schema for plugin {plugin_id} unregistered")
            return True
        logger.warning(f"No schema found for plugin {plugin_id}")
        return False

    def get_config_value(self, key: str, default=None) -> Any:
        """
        Get configuration value with caching for performance.
        
        Args:
            key: Configuration key
            default: Default value if key doesn't exist
            
        Returns:
            Configuration value or default if not found
        """
        # Check cache first for performance
        if key in self._cache:
            return self._cache[key]

        # Get value from runtime config
        value = getattr(self.runtime_config, key, default)
        self._cache[key] = value
        return value

    def get_plugin_config(self, plugin_id: str) -> Dict[str, Any]:
        """
        Get plugin-specific configuration section.
        
        Args:
            plugin_id: Unique identifier for the plugin
            
        Returns:
            Dictionary containing plugin configuration or empty dict if not found
        """
        config_data = asdict(self.runtime_config)
        plugin_config = config_data.get("plugins", {}).get(plugin_id, {})
        return plugin_config

    def _validate_config(self, config_data: dict) -> bool:
        """
        Validate configuration data against schema.
        
        Args:
            config_data: Configuration data to validate
            
        Returns:
            True if valid, False otherwise
        """
        try:
            # Validate against core schema
            jsonschema.validate(instance=config_data, schema=self.schema)
            
            # Validate against plugin schemas if plugins section exists
            if "plugins" in config_data:
                for plugin_id, plugin_schema in self._plugin_schemas.items():
                    plugin_config = config_data.get("plugins", {}).get(plugin_id)
                    if plugin_config:
                        try:
                            jsonschema.validate(
                                instance=plugin_config, 
                                schema=plugin_schema
                            )
                        except jsonschema.exceptions.ValidationError as e:
                            logger.error(f"Plugin {plugin_id} config validation failed: {e}")
                            return False
                            
            return True
        except jsonschema.exceptions.ValidationError as e:
            logger.error(f"Configuration validation failed: {e}")
            return False

    def _compress_config(self, config_data: str) -> bytes:
        """
        Compress configuration data to save space.
        
        Args:
            config_data: String configuration data
            
        Returns:
            Compressed bytes
        """
        try:
            return zlib.compress(config_data.encode(), level=9)  # Maximum compression level
        except Exception as e:
            logger.error(f"Configuration compression failed: {e}")
            return config_data.encode()  # Return uncompressed data on failure

    def _decompress_config(self, compressed_data: bytes) -> str:
        """
        Decompress configuration data.
        
        Args:
            compressed_data: Compressed configuration data
            
        Returns:
            Decompressed string data
        """
        try:
            return zlib.decompress(compressed_data).decode()
        except zlib.error:
            # Possibly uncompressed data
            logger.warning("Decompression failed, trying direct decoding")
            return compressed_data.decode()
        except Exception as e:
            logger.error(f"Configuration decompression failed: {e}")
            raise

    async def create_backup(self) -> Path:
        """
        Create configuration backup.
        
        Returns:
            Path to created backup file
        """
        metadata = {
            "version": getattr(self.runtime_config, "version", "unknown"),
            "timestamp": datetime.now().isoformat(),
            "encrypted": self.encrypt_enabled
        }
        return self.backup_manager.create_backup(self.config_path, metadata)

    async def restore_from_backup(self, backup_path: Path) -> None:
        """
        Restore configuration from backup.
        
        Args:
            backup_path: Path to backup file
        """
        async with self._lock:
            metadata = self.backup_manager.restore_backup(
                backup_path, self.config_path)
            await self.load_config()
            logger.info(f"Configuration restored from backup, version: {metadata.get('version', 'unknown')}")
            self._config_loaded.set()

    def _load_default_config(self) -> RuntimeConfig:
        """
        Load default configuration.
        
        Returns:
            Default RuntimeConfig object
        """
        logger.debug("Loading default configuration")
        config = RuntimeConfig(
            tcp_host="localhost",
            tcp_port=8080,
            websocket_host="0.0.0.0",
            websocket_port=8000,
            version=datetime.now().isoformat()
        )
        logger.info("Default configuration loaded")
        return config

    async def load_config(self) -> None:
        """
        Load configuration from file.
        """
        async with self._lock:
            try:
                if self.config_path.exists() and os.path.getsize(self.config_path) > 0:
                    logger.debug(f"Loading configuration from {self.config_path}")

                    # Read file content
                    with open(self.config_path, 'rb') as f:
                        content = f.read()

                    # Decrypt if encryption enabled
                    if self.encrypt_enabled and self.crypto:
                        content = self.crypto.decrypt_bytes(content)

                    # Decompress configuration
                    yaml_content = self._decompress_config(content)

                    # Parse YAML
                    config_data = yaml.safe_load(yaml_content)

                    # Validate configuration
                    if not self._validate_config(config_data):
                        logger.warning("Configuration validation failed, using defaults")
                        self.runtime_config = self._load_default_config()
                    else:
                        self.runtime_config = RuntimeConfig(**config_data)

                    # Clear cache
                    self._cache.clear()
                    logger.info("Configuration loaded successfully")
                else:
                    logger.warning(f"Configuration file doesn't exist or is empty: {self.config_path}, creating default")
                    self.runtime_config = self._load_default_config()
                    await self.save_config()

                self._config_loaded.set()

            except Exception as e:
                logger.error(f"Failed to load configuration: {str(e)}", exc_info=True)
                # Use default configuration on failure
                self.runtime_config = self._load_default_config()
                self._config_loaded.set()

    async def save_config(self) -> None:
        """
        Save configuration to file.
        """
        async with self._lock:
            try:
                logger.debug(f"Saving configuration to {self.config_path}")

                # Create parent directory if it doesn't exist
                self.config_path.parent.mkdir(parents=True, exist_ok=True)

                # Prepare configuration data
                config_data = asdict(self.runtime_config)

                # Validate configuration
                if not self._validate_config(config_data):
                    raise ValueError("Configuration validation failed")

                # Version control
                config_data['version'] = datetime.now().isoformat()

                # Manage version history
                self._version_history.append(copy.deepcopy(config_data))
                if len(self._version_history) > self._max_history:
                    self._version_history.pop(0)  # Remove oldest version

                # Convert to YAML
                yaml_content = yaml.dump(config_data, default_flow_style=False)

                # Compress configuration
                compressed = self._compress_config(yaml_content)

                # Encrypt if enabled
                if self.encrypt_enabled and self.crypto:
                    compressed = self.crypto.encrypt_bytes(compressed)

                # Write to temporary file then rename for atomicity
                temp_path = self.config_path.with_suffix('.tmp')
                with open(temp_path, 'wb') as f:
                    f.write(compressed)
                    f.flush()
                    os.fsync(f.fileno())  # Ensure data is written to disk

                # Rename to target file
                temp_path.replace(self.config_path)

                # Clear cache
                self._cache.clear()

                # Create automatic backup
                await self.create_backup()
                logger.info("Configuration saved successfully")

            except Exception as e:
                logger.error(f"Failed to save configuration: {str(e)}", exc_info=True)
                raise

    async def update_plugin_config(self, plugin_id: str, config: Dict[str, Any]) -> bool:
        """
        Update plugin-specific configuration section.
        
        Args:
            plugin_id: Unique identifier for the plugin
            config: New configuration for the plugin
            
        Returns:
            True if update succeeded, False otherwise
        """
        async with self._lock:
            try:
                logger.debug(f"Updating configuration for plugin: {plugin_id}")
                # Get current config
                current_config = asdict(self.runtime_config)
                
                # Initialize plugins section if it doesn't exist
                if "plugins" not in current_config:
                    current_config["plugins"] = {}
                
                # Update plugin config
                current_config["plugins"][plugin_id] = config
                
                # Validate plugin config if schema exists
                if plugin_id in self._plugin_schemas:
                    try:
                        jsonschema.validate(
                            instance=config,
                            schema=self._plugin_schemas[plugin_id]
                        )
                    except jsonschema.exceptions.ValidationError as e:
                        logger.error(f"Plugin {plugin_id} config validation failed: {e}")
                        return False
                
                # Update and save config
                self.runtime_config = RuntimeConfig(**current_config)
                await self.save_config()
                await self._notify_observers()
                
                logger.info(f"Configuration for plugin {plugin_id} updated successfully")
                return True
            except Exception as e:
                logger.error(f"Failed to update plugin {plugin_id} configuration: {e}")
                return False

    async def update_config(self, new_config: Dict[str, Any]) -> bool:
        """
        Update configuration with new values.
        
        Args:
            new_config: Dictionary of new configuration values
            
        Returns:
            True if update succeeded, False otherwise
        """
        async with self._lock:
            try:
                logger.debug(f"Updating configuration: {new_config}")
                current_config = asdict(self.runtime_config)
                current_config.update(new_config)

                # Validate updated configuration
                if not self._validate_config(current_config):
                    logger.error("Configuration update failed: invalid configuration values")
                    return False

                self.runtime_config = RuntimeConfig(**current_config)
                await self.save_config()

                logger.info("Configuration updated, notifying observers")
                await self._notify_observers()
                return True
            except Exception as e:
                logger.error(f"Configuration update failed: {e}")
                return False

    async def rollback_to_version(self, version: str) -> bool:
        """
        Rollback to specific version from history.
        
        Args:
            version: Version string to rollback to
            
        Returns:
            True if rollback succeeded, False if version not found
        """
        async with self._lock:
            try:
                for config in reversed(self._version_history):
                    if config.get('version') == version:
                        await self.update_config(config)
                        logger.info(f"Rolled back to version {version}")
                        return True
                logger.warning(f"Version {version} not found in history")
                return False
            except Exception as e:
                logger.error(f"Rollback failed: {e}")
                return False

    async def repair_config(self) -> bool:
        """
        Repair corrupted configuration.
        
        Returns:
            True if repair succeeded, False otherwise
        """
        async with self._lock:
            try:
                # Try loading latest backup
                backups = self.backup_manager.list_backups()
                if backups:
                    logger.info(f"Attempting repair from backup: {backups[0]}")
                    await self.restore_from_backup(backups[0])
                    logger.info("Configuration repaired from latest backup")
                else:
                    # If no backup, load default config
                    self.runtime_config = self._load_default_config()
                    await self.save_config()
                    logger.info("Configuration reset to default values")

                # Clear cache
                self._cache.clear()
                return True

            except Exception as e:
                logger.error(f"Configuration repair failed: {str(e)}")
                # Ensure config_loaded is set even if repair fails
                self._config_loaded.set()
                return False

    def register_observer(self, callback: ObserverCallable) -> bool:
        """
        Register configuration change observer.
        
        Args:
            callback: Async callback function to notify on changes
            
        Returns:
            True if registration succeeded, False otherwise
        """
        try:
            if callback not in self._observers:  # Prevent duplicate registration
                observer_name = getattr(callback, '__name__', str(callback))
                logger.debug(f"Registering new observer: {observer_name}")
                self._observers.append(callback)
                logger.info(f"Observer count: {len(self._observers)}")
                return True
            return False
        except Exception as e:
            logger.error(f"Failed to register observer: {e}")
            return False

    def unregister_observer(self, callback: ObserverCallable) -> bool:
        """
        Unregister configuration change observer.
        
        Args:
            callback: The callback to unregister
            
        Returns:
            True if successfully unregistered, False if not found
        """
        try:
            if callback in self._observers:
                self._observers.remove(callback)
                observer_name = getattr(callback, '__name__', str(callback))
                logger.debug(f"Observer unregistered: {observer_name}")
                return True
            return False
        except Exception as e:
            logger.error(f"Failed to unregister observer: {e}")
            return False

    async def _notify_observers(self) -> None:
        """
        Notify all observers about configuration changes.
        """
        logger.debug(f"Notifying {len(self._observers)} observers")

        # Create tasks for all observers
        tasks = []
        for observer in self._observers:
            tasks.append(self._notify_single_observer(observer))

        # Wait for all tasks to complete, without letting failures affect others
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _notify_single_observer(self, observer: ObserverCallable) -> None:
        """
        Notify a single observer with error handling.
        
        Args:
            observer: Observer callback to notify
        """
        observer_name = getattr(observer, '__name__', str(observer))
        try:
            await observer(self.runtime_config)
            logger.debug(f"Observer {observer_name} notification succeeded")
        except Exception as e:
            logger.error(f"Observer {observer_name} notification failed: {str(e)}", exc_info=True)

    async def _reload_config(self):
        """
        Reload configuration and notify observers.
        """
        async with self._lock:
            try:
                # Create backup before reloading
                await self.create_backup()

                # Load configuration
                await self.load_config()

                # Notify observers
                await self._notify_observers()
                logger.success("Configuration reload completed")
            except Exception as e:
                logger.error(f"Configuration reload failed: {e}")

    async def wait_for_config(self, timeout: float = None) -> bool:
        """
        Wait for configuration to be loaded.
        
        Args:
            timeout: Optional timeout in seconds
            
        Returns:
            True if config loaded, False if timeout occurred
        """
        try:
            if timeout:
                return await asyncio.wait_for(self._config_loaded.wait(), timeout=timeout)
            else:
                return await self._config_loaded.wait()
        except asyncio.TimeoutError:
            logger.warning(f"Timed out waiting for configuration to load after {timeout}s")
            return False

    def start_hot_reload(self):
        """
        Start hot reload support for config changes.
        """
        logger.info("Starting configuration hot reload")
        self.file_watcher.start()

    def stop_hot_reload(self):
        """
        Stop hot reload support.
        """
        self.file_watcher.stop()

    async def __aenter__(self):
        """
        Support for async context manager.
        """
        await self.load_config()
        self.start_hot_reload()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Support for async context manager.
        """
        self.stop_hot_reload()
