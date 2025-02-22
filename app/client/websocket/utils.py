import ssl
from typing import Any, Optional
import json
from loguru import logger

def create_ssl_context(
    cert_path: Optional[str] = None,
    key_path: Optional[str] = None,
    ca_path: Optional[str] = None,
    verify_ssl: bool = True
) -> ssl.SSLContext:
    """创建SSL上下文"""
    ssl_context = ssl.create_default_context()
    if not verify_ssl:
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
    if cert_path and key_path:
        ssl_context.load_cert_chain(cert_path, key_path)
    if ca_path:
        ssl_context.load_verify_locations(ca_path)
    return ssl_context

def parse_message(message: str) -> Any:
    """解析消息"""
    try:
        return json.loads(message)
    except json.JSONDecodeError:
        logger.trace("Message is not JSON")
        return message

def serialize_message(message: Any) -> str:
    """序列化消息"""
    if isinstance(message, (dict, list)):
        return json.dumps(message)
    return str(message)
