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
    ssl_context = ssl.create_default_context(
        purpose=ssl.Purpose.CLIENT_AUTH if cert_path else ssl.Purpose.SERVER_AUTH)

    if not verify_ssl:
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

    if cert_path and key_path:
        ssl_context.load_cert_chain(cert_path, key_path)

    if ca_path:
        ssl_context.load_verify_locations(cafile=ca_path)

    return ssl_context


def parse_message(message: str) -> Any:
    """解析消息"""
    if not message:
        return None

    if isinstance(message, bytes):
        message = message.decode('utf-8')

    try:
        return json.loads(message)
    except json.JSONDecodeError:
        logger.trace("Message is not JSON")
        return message


def serialize_message(message: Any) -> str:
    """序列化消息"""
    if message is None:
        return ""

    if isinstance(message, (dict, list)):
        try:
            return json.dumps(message, ensure_ascii=False)
        except Exception as e:
            logger.warning(f"JSON serialization error: {e}")
            return str(message)

    return str(message)
