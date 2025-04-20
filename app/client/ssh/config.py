from dataclasses import dataclass
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field


@dataclass
class PoolConfig:
    """SSH connection pool configuration"""
    min_size: int = 2
    max_size: int = 10
    timeout: float = 30.0
    idle_timeout: int = 300
    check_interval: int = 60


@dataclass
class SSHConfig:
    """Enhanced SSH configuration class"""
    hostname: str
    username: str
    password: Optional[str] = None
    private_key_path: Optional[str] = None
    port: int = 22
    timeout: int = 10
    compress: bool = True
    keep_alive: bool = True
    keep_alive_interval: int = 30
    banner_timeout: int = 60
    auth_timeout: int = 30
    channel_timeout: int = 30
    max_retries: int = 3
    retry_interval: int = 5
    pool: Optional[PoolConfig] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary for compatibility"""
        result = {
            'hostname': self.hostname,
            'username': self.username,
            'port': self.port,
            'timeout': self.timeout,
            'compress': self.compress,
            'keep_alive': self.keep_alive,
            'keep_alive_interval': self.keep_alive_interval,
            'banner_timeout': self.banner_timeout,
            'auth_timeout': self.auth_timeout,
            'channel_timeout': self.channel_timeout,
            'max_retries': self.max_retries,
            'retry_interval': self.retry_interval,
        }
        
        if self.password:
            result['password'] = self.password
        
        if self.private_key_path:
            result['private_key_path'] = self.private_key_path
            
        if self.pool:
            result['pool'] = {
                'min_size': self.pool.min_size,
                'max_size': self.pool.max_size,
                'timeout': self.pool.timeout,
                'idle_timeout': self.pool.idle_timeout,
                'check_interval': self.pool.check_interval
            }
            
        return result
        
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'SSHConfig':
        """Create config from dictionary for compatibility"""
        pool_config = None
        if 'pool' in config_dict:
            pool_data = config_dict.pop('pool')
            pool_config = PoolConfig(**pool_data)
            
        return cls(**{k: v for k, v in config_dict.items() if k != 'pool'}, pool=pool_config)


# Pydantic models for modern API and schema validation
class PoolConfigModel(BaseModel):
    """SSH connection pool configuration (Pydantic model)"""
    min_size: int = Field(default=2, description="Minimum number of connections to keep in pool")
    max_size: int = Field(default=10, description="Maximum number of connections allowed in pool")
    timeout: float = Field(default=30.0, description="Connection timeout in seconds")
    idle_timeout: int = Field(default=300, description="Time in seconds before idle connections are closed")
    check_interval: int = Field(default=60, description="Interval in seconds to check connection health")


class SSHConfigModel(BaseModel):
    """SSH configuration (Pydantic model)"""
    hostname: str = Field(..., description="SSH server hostname or IP address")
    username: str = Field(..., description="SSH username")
    password: Optional[str] = Field(None, description="SSH password (if using password authentication)")
    private_key_path: Optional[str] = Field(None, description="Path to private key file (if using key authentication)")
    port: int = Field(22, description="SSH port number")
    timeout: int = Field(10, description="Connection timeout in seconds")
    compress: bool = Field(True, description="Enable compression")
    keep_alive: bool = Field(True, description="Enable keep-alive packets")
    keep_alive_interval: int = Field(30, description="Keep-alive interval in seconds")
    banner_timeout: int = Field(60, description="Banner timeout in seconds")
    auth_timeout: int = Field(30, description="Authentication timeout in seconds")
    channel_timeout: int = Field(30, description="Channel timeout in seconds")
    max_retries: int = Field(3, description="Maximum number of connection retry attempts")
    retry_interval: int = Field(5, description="Interval between retry attempts in seconds")
    pool: Optional[PoolConfigModel] = Field(None, description="Connection pool configuration")
    
    def to_dataclass(self) -> SSHConfig:
        """Convert Pydantic model to dataclass for backward compatibility"""
        pool_config = None
        if self.pool:
            pool_config = PoolConfig(
                min_size=self.pool.min_size,
                max_size=self.pool.max_size,
                timeout=self.pool.timeout,
                idle_timeout=self.pool.idle_timeout,
                check_interval=self.pool.check_interval
            )
            
        return SSHConfig(
            hostname=self.hostname,
            username=self.username,
            password=self.password,
            private_key_path=self.private_key_path,
            port=self.port,
            timeout=self.timeout,
            compress=self.compress,
            keep_alive=self.keep_alive,
            keep_alive_interval=self.keep_alive_interval,
            banner_timeout=self.banner_timeout,
            auth_timeout=self.auth_timeout,
            channel_timeout=self.channel_timeout,
            max_retries=self.max_retries,
            retry_interval=self.retry_interval,
            pool=pool_config
        )
