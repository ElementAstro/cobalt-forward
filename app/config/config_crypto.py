from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from pathlib import Path
import base64
from loguru import logger
import os
import secrets


class ConfigCrypto:
    def __init__(self, key_file: Path):
        self.key_file = key_file
        self._key = self._load_or_create_key()
        self._fernet = Fernet(self._key)

    def _load_or_create_key(self) -> bytes:
        """加载或创建加密密钥"""
        try:
            if self.key_file.exists():
                logger.debug(f"从 {self.key_file} 加载加密密钥")
                key_data = self.key_file.read_bytes()
                return key_data
            else:
                logger.info(f"创建新的加密密钥: {self.key_file}")
                # 确保父目录存在
                self.key_file.parent.mkdir(parents=True, exist_ok=True)

                # 生成强密钥
                key = Fernet.generate_key()

                # 安全写入密钥文件
                temp_key_file = self.key_file.with_suffix('.tmp')
                temp_key_file.write_bytes(key)
                temp_key_file.chmod(0o600)  # 仅所有者可读写
                temp_key_file.replace(self.key_file)

                return key
        except Exception as e:
            logger.error(f"处理加密密钥时出错: {e}")
            # 如果发生错误，生成临时密钥（仅用于当前会话）
            logger.warning("使用临时会话密钥")
            return Fernet.generate_key()

    def derive_key(self, password: str, salt: bytes = None) -> bytes:
        """从密码派生密钥"""
        if salt is None:
            salt = secrets.token_bytes(16)

        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
            backend=default_backend()
        )
        key = base64.urlsafe_b64encode(kdf.derive(password.encode()))
        return key, salt

    def encrypt(self, data: str) -> str:
        """加密字符串数据，返回加密后的字符串"""
        try:
            return base64.urlsafe_b64encode(self._fernet.encrypt(data.encode())).decode()
        except Exception as e:
            logger.error(f"加密失败: {e}")
            raise

    def decrypt(self, encrypted_data: str) -> str:
        """解密字符串数据，返回解密后的字符串"""
        try:
            return self._fernet.decrypt(base64.urlsafe_b64decode(encrypted_data)).decode()
        except InvalidToken:
            logger.error("无效的加密令牌，数据可能已损坏或使用了错误的密钥")
            raise
        except Exception as e:
            logger.error(f"解密失败: {e}")
            raise

    def encrypt_bytes(self, data: bytes) -> bytes:
        """直接加密字节数据，返回加密后的字节"""
        try:
            return self._fernet.encrypt(data)
        except Exception as e:
            logger.error(f"加密字节数据失败: {e}")
            raise

    def decrypt_bytes(self, encrypted_data: bytes) -> bytes:
        """直接解密字节数据，返回解密后的字节"""
        try:
            return self._fernet.decrypt(encrypted_data)
        except InvalidToken:
            logger.error("无效的加密令牌，数据可能已损坏或使用了错误的密钥")
            raise
        except Exception as e:
            logger.error(f"解密字节数据失败: {e}")
            raise

    def rotate_key(self):
        """轮换加密密钥"""
        try:
            old_key = self._key
            new_key = Fernet.generate_key()

            # 创建新密钥的Fernet实例
            new_fernet = Fernet(new_key)

            # 备份旧密钥
            backup_path = self.key_file.with_suffix('.bak')
            if backup_path.exists():
                backup_path.unlink()
            self.key_file.rename(backup_path)

            # 写入新密钥
            self.key_file.write_bytes(new_key)
            self.key_file.chmod(0o600)  # 仅所有者可读写

            self._key = new_key
            self._fernet = new_fernet
            logger.info("加密密钥已轮换")

            return old_key  # 返回旧密钥以便需要时重新加密现有数据
        except Exception as e:
            logger.error(f"轮换密钥失败: {e}")
            raise
