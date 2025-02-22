
from dataclasses import dataclass
from typing import Optional, List
import json
import yaml


@dataclass
class FTPConfig:
    # 基本配置
    host: str
    port: int = 21
    username: str = "anonymous"
    password: str = "anonymous@example.com"

    # 连接配置
    timeout: int = 30
    encoding: str = 'utf-8'
    buffer_size: int = 8192
    passive_mode: bool = True

    # 传输配置
    max_retries: int = 3
    retry_delay: int = 5
    chunk_size: int = 8192
    max_concurrent_transfers: int = 4

    # 安全配置
    use_tls: bool = False
    cert_file: Optional[str] = None
    key_file: Optional[str] = None

    # 限制配置
    max_upload_speed: Optional[int] = None
    max_download_speed: Optional[int] = None
    allowed_file_types: List[str] = None

    @classmethod
    def from_json(cls, json_file: str):
        """从JSON文件加载配置"""
        with open(json_file, 'r') as f:
            config_data = json.load(f)
        return cls(**config_data)

    @classmethod
    def from_yaml(cls, yaml_file: str):
        """从YAML文件加载配置"""
        with open(yaml_file, 'r') as f:
            config_data = yaml.safe_load(f)
        return cls(**config_data)
