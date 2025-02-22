from cryptography.fernet import Fernet
from pathlib import Path
import base64
from loguru import logger

class ConfigCrypto:
    def __init__(self, key_file: Path):
        self.key_file = key_file
        self._key = self._load_or_create_key()
        self._fernet = Fernet(self._key)

    def _load_or_create_key(self) -> bytes:
        if self.key_file.exists():
            return self.key_file.read_bytes()
        else:
            key = Fernet.generate_key()
            self.key_file.write_bytes(key)
            return key

    def encrypt(self, data: str) -> str:
        return base64.b64encode(self._fernet.encrypt(data.encode())).decode()

    def decrypt(self, encrypted_data: str) -> str:
        try:
            return self._fernet.decrypt(base64.b64decode(encrypted_data)).decode()
        except Exception as e:
            logger.error(f"解密失败: {e}")
            raise
