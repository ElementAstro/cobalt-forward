from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
import json
import yaml
import os
from ipaddress import ip_address
import socket
from pathlib import Path


@dataclass
class FTPConfig:
    """FTP client configuration class
    
    This class stores all configuration parameters for the FTP client,
    with sensible defaults. Supports loading from JSON and YAML files.
    """
    # Basic configuration
    host: str
    port: int = 21
    username: str = "anonymous"
    password: str = "anonymous@example.com"

    # Connection configuration
    timeout: int = 30
    encoding: str = 'utf-8'
    buffer_size: int = 8192
    passive_mode: bool = True
    
    # Transfer configuration
    max_retries: int = 3
    retry_delay: int = 5
    chunk_size: int = 8192
    max_concurrent_transfers: int = 4
    
    # Security configuration
    use_tls: bool = False
    cert_file: Optional[str] = None
    key_file: Optional[str] = None
    
    # Limitation configuration
    max_upload_speed: Optional[int] = None
    max_download_speed: Optional[int] = None
    allowed_file_types: List[str] = field(default_factory=list)
    
    # Server-specific configuration (for FTPServer)
    max_cons: int = 256
    max_cons_per_ip: int = 5
    masquerade_address: Optional[str] = None
    passive_ports: List[int] = field(default_factory=lambda: list(range(60000, 61000)))
    
    # Advanced options
    keepalive: bool = True
    keepalive_interval: int = 60
    binary_mode: bool = True
    debug_level: int = 0
    
    def __post_init__(self):
        """Validate configuration after initialization"""
        self._validate_config()
    
    def _validate_config(self):
        """Validate configuration parameters
        
        Raises:
            ValueError: If configuration parameters are invalid
        """
        # Validate host (try to resolve if it's a hostname)
        try:
            if not self.host:
                raise ValueError("Host cannot be empty")
            
            # Check if host is IP address
            try:
                ip_address(self.host)
            except ValueError:
                # Not an IP address, try to resolve hostname
                socket.gethostbyname(self.host)
        except socket.gaierror:
            raise ValueError(f"Invalid host: {self.host}")
        
        # Validate port
        if not isinstance(self.port, int) or self.port < 1 or self.port > 65535:
            raise ValueError(f"Invalid port: {self.port}. Must be between 1 and 65535.")
        
        # Validate timeout
        if not isinstance(self.timeout, int) or self.timeout < 1:
            raise ValueError(f"Invalid timeout: {self.timeout}. Must be positive integer.")
        
        # Validate TLS parameters
        if self.use_tls:
            if self.cert_file and not os.path.exists(self.cert_file):
                raise ValueError(f"Certificate file not found: {self.cert_file}")
            if self.key_file and not os.path.exists(self.key_file):
                raise ValueError(f"Key file not found: {self.key_file}")
    
    @classmethod
    def from_json(cls, json_file: str):
        """Load configuration from JSON file
        
        Args:
            json_file: Path to JSON configuration file
            
        Returns:
            FTPConfig instance
            
        Raises:
            FileNotFoundError: If file does not exist
            json.JSONDecodeError: If JSON is malformed
        """
        if not os.path.exists(json_file):
            raise FileNotFoundError(f"Config file not found: {json_file}")
            
        with open(json_file, 'r', encoding='utf-8') as f:
            config_data = json.load(f)
        
        # Handle allowed_file_types special case (convert to list if needed)
        if 'allowed_file_types' in config_data and config_data['allowed_file_types'] is None:
            config_data['allowed_file_types'] = []
            
        return cls(**config_data)

    @classmethod
    def from_yaml(cls, yaml_file: str):
        """Load configuration from YAML file
        
        Args:
            yaml_file: Path to YAML configuration file
            
        Returns:
            FTPConfig instance
            
        Raises:
            FileNotFoundError: If file does not exist
            yaml.YAMLError: If YAML is malformed
        """
        if not os.path.exists(yaml_file):
            raise FileNotFoundError(f"Config file not found: {yaml_file}")
            
        with open(yaml_file, 'r', encoding='utf-8') as f:
            config_data = yaml.safe_load(f)
        
        # Handle allowed_file_types special case (convert to list if needed)
        if 'allowed_file_types' in config_data and config_data['allowed_file_types'] is None:
            config_data['allowed_file_types'] = []
            
        return cls(**config_data)
        
    def to_json(self, json_file: str):
        """Save configuration to JSON file
        
        Args:
            json_file: Path to save configuration
            
        Raises:
            IOError: If file cannot be written
        """
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(os.path.abspath(json_file)), exist_ok=True)
        
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(self.to_dict(), f, indent=2)
    
    def to_yaml(self, yaml_file: str):
        """Save configuration to YAML file
        
        Args:
            yaml_file: Path to save configuration
            
        Raises:
            IOError: If file cannot be written
        """
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(os.path.abspath(yaml_file)), exist_ok=True)
        
        with open(yaml_file, 'w', encoding='utf-8') as f:
            yaml.dump(self.to_dict(), f, default_flow_style=False)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary
        
        Returns:
            Dictionary representation of configuration
        """
        result = {}
        for key, value in self.__dict__.items():
            if not key.startswith('_'):  # Skip private fields
                result[key] = value
        return result
    
    @classmethod
    def create_default(cls, host: str, save_path: Optional[str] = None):
        """Create configuration with default values
        
        Args:
            host: FTP server hostname or IP
            save_path: Optional path to save configuration
            
        Returns:
            FTPConfig instance
        """
        config = cls(host=host)
        
        if save_path:
            if save_path.endswith('.json'):
                config.to_json(save_path)
            elif save_path.endswith(('.yaml', '.yml')):
                config.to_yaml(save_path)
            else:
                # Default to JSON if extension not recognized
                config.to_json(f"{save_path}.json")
                
        return config
    
    def update(self, **kwargs):
        """Update configuration parameters
        
        Args:
            **kwargs: Parameters to update
            
        Returns:
            Updated FTPConfig instance
        """
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)
            else:
                raise AttributeError(f"Unknown configuration parameter: {key}")
                
        # Re-validate after update
        self._validate_config()
        return self
    
    @classmethod
    def merge(cls, *configs):
        """Merge multiple configurations
        
        Later configurations override earlier ones.
        
        Args:
            *configs: FTPConfig instances to merge
            
        Returns:
            New FTPConfig instance
        """
        if not configs:
            raise ValueError("No configurations to merge")
            
        merged_dict = {}
        for config in configs:
            merged_dict.update(config.to_dict())
            
        return cls(**merged_dict)


@dataclass
class FTPServerConfig(FTPConfig):
    """Configuration class specifically for FTP servers
    
    Extends the base FTPConfig with server-specific parameters.
    """
    # Server-specific configuration
    address: str = "0.0.0.0"  # Listen on all interfaces by default
    banner: str = "Welcome to FTP Server"
    anonymous_enable: bool = False
    anonymous_root: Optional[str] = None
    user_root: str = field(default_factory=lambda: str(Path.home() / "ftp"))
    
    # Security settings
    tls_certfile: Optional[str] = None
    tls_keyfile: Optional[str] = None
    require_tls: bool = False
    allow_anonymous_tls: bool = False
    
    # Performance settings
    max_connections: int = 100
    multiprocess: bool = False
    process_count: int = 0  # 0 = use CPU count
    
    def _validate_config(self):
        """Validate server configuration parameters"""
        super()._validate_config()
        
        # Validate address
        try:
            ip_address(self.address)
        except ValueError:
            raise ValueError(f"Invalid server address: {self.address}")
            
        # Validate TLS configuration
        if self.require_tls:
            if not self.tls_certfile or not self.tls_keyfile:
                raise ValueError("TLS certificate and key files are required when TLS is mandatory")
            if not os.path.exists(self.tls_certfile):
                raise ValueError(f"TLS certificate file not found: {self.tls_certfile}")
            if not os.path.exists(self.tls_keyfile):
                raise ValueError(f"TLS key file not found: {self.tls_keyfile}")
                
        # Validate anonymous configuration
        if self.anonymous_enable and self.anonymous_root:
            if not os.path.exists(self.anonymous_root):
                try:
                    os.makedirs(self.anonymous_root, exist_ok=True)
                except OSError:
                    raise ValueError(f"Cannot create anonymous root directory: {self.anonymous_root}")
                    
        # Validate user root
        if not os.path.exists(self.user_root):
            try:
                os.makedirs(self.user_root, exist_ok=True)
            except OSError:
                raise ValueError(f"Cannot create user root directory: {self.user_root}")
                
    @classmethod
    def create_default_server(cls, address: str = "0.0.0.0", port: int = 21, save_path: Optional[str] = None):
        """Create a default server configuration
        
        Args:
            address: Server address to listen on
            port: Server port to listen on
            save_path: Optional path to save configuration
            
        Returns:
            FTPServerConfig instance
        """
        config = cls(host="localhost", address=address, port=port)
        
        if save_path:
            if save_path.endswith('.json'):
                config.to_json(save_path)
            elif save_path.endswith(('.yaml', '.yml')):
                config.to_yaml(save_path)
            else:
                config.to_json(f"{save_path}.json")
                
        return config
