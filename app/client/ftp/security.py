import hashlib
import ssl
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import jwt
import os
import time
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from websockets import SecurityError

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric.padding import OAEP, MGF1
from cryptography.hazmat.primitives.serialization import (
    load_pem_private_key,
    load_pem_public_key,
    Encoding,
    PrivateFormat,
    PublicFormat,
    NoEncryption
)
import base64
import uuid
import tempfile
from typing import Tuple, Optional, Dict, Any
from loguru import logger


class SecurityEnhancer:
    def __init__(self, key_dir: str = None):
        """初始化安全增强器

        Args:
            key_dir: 密钥存储目录
        """
        self.key_dir = key_dir or os.path.join(
            os.path.expanduser("~"), ".ftp_keys")
        if not os.path.exists(self.key_dir):
            os.makedirs(self.key_dir, exist_ok=True)

        self.session_key = Fernet.generate_key()
        self.cipher = Fernet(self.session_key)
        self.jwt_secret = os.urandom(32)
        self._init_asymmetric_keys()
        self.key_rotation_interval = 3600  # 1小时轮换一次密钥
        self._key_cache = {}
        self._init_ciphers()

    def _init_asymmetric_keys(self):
        """初始化非对称密钥对"""
        self.private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048
        )
        self.public_key = self.private_key.public_key()

    def _init_ciphers(self):
        """初始化加密器"""
        self.aes_gcm = AESGCM(AESGCM.generate_key(bit_length=256))
        self._update_key_timestamp = time.time()

    @property
    def needs_key_rotation(self) -> bool:
        """检查是否需要密钥轮换"""
        return time.time() - self._update_key_timestamp >= self.key_rotation_interval

    def create_ssl_context(self, cert_file: str, key_file: str) -> ssl.SSLContext:
        """创建SSL上下文"""
        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        context.load_cert_chain(certfile=cert_file, keyfile=key_file)
        return context

    def encrypt_file(self, file_path: str, key_path: Optional[str] = None) -> str:
        """加密文件

        Args:
            file_path: 要加密的文件路径
            key_path: 可选的公钥路径，默认使用会话密钥

        Returns:
            加密后的文件路径
        """
        encrypted_path = f"{file_path}.enc"
        try:
            with open(file_path, "rb") as f:
                data = f.read()

            if key_path:
                # 使用RSA公钥加密
                with open(key_path, "rb") as key_file:
                    public_key = load_pem_public_key(
                        key_file.read(),
                        backend=default_backend()
                    )

                # 对于大文件，使用混合加密：随机AES密钥+RSA
                file_key = Fernet.generate_key()
                file_cipher = Fernet(file_key)
                encrypted_data = file_cipher.encrypt(data)

                # 使用RSA加密文件密钥
                encrypted_key = public_key.encrypt(
                    file_key,
                    OAEP(
                        mgf=MGF1(algorithm=hashes.SHA256()),
                        algorithm=hashes.SHA256(),
                        label=None
                    )
                )

                # 将加密的密钥和数据写入文件
                with open(encrypted_path, "wb") as f:
                    f.write(len(encrypted_key).to_bytes(4, byteorder='big'))
                    f.write(encrypted_key)
                    f.write(encrypted_data)
            else:
                # 使用会话密钥加密
                encrypted_data = self.cipher.encrypt(data)
                with open(encrypted_path, "wb") as f:
                    f.write(encrypted_data)

            logger.debug(f"文件已加密: {file_path} -> {encrypted_path}")
            return encrypted_path
        except Exception as e:
            logger.error(f"加密文件失败 {file_path}: {str(e)}")
            if os.path.exists(encrypted_path):
                os.remove(encrypted_path)
            raise

    def decrypt_file(self, encrypted_path: str, output_path: Optional[str] = None,
                     private_key_path: Optional[str] = None) -> str:
        """解密文件

        Args:
            encrypted_path: 加密文件路径
            output_path: 输出文件路径
            private_key_path: 私钥路径

        Returns:
            解密后的文件路径
        """
        if not output_path:
            output_path = encrypted_path.replace('.enc', '.dec')

        try:
            with open(encrypted_path, "rb") as f:
                if private_key_path:
                    # RSA解密
                    with open(private_key_path, "rb") as key_file:
                        private_key = load_pem_private_key(
                            key_file.read(),
                            password=None,
                            backend=default_backend()
                        )

                    # 读取加密的密钥长度
                    key_len_bytes = f.read(4)
                    key_len = int.from_bytes(key_len_bytes, byteorder='big')

                    # 读取加密的密钥
                    encrypted_key = f.read(key_len)

                    # 解密文件密钥
                    file_key = private_key.decrypt(
                        encrypted_key,
                        OAEP(
                            mgf=MGF1(algorithm=hashes.SHA256()),
                            algorithm=hashes.SHA256(),
                            label=None
                        )
                    )

                    # 使用文件密钥解密文件数据
                    file_cipher = Fernet(file_key)
                    encrypted_data = f.read()
                    data = file_cipher.decrypt(encrypted_data)
                else:
                    # 使用会话密钥解密
                    encrypted_data = f.read()
                    data = self.cipher.decrypt(encrypted_data)

                with open(output_path, "wb") as f:
                    f.write(data)

                logger.debug(f"文件已解密: {encrypted_path} -> {output_path}")
                return output_path
        except Exception as e:
            logger.error(f"解密文件失败 {encrypted_path}: {str(e)}")
            if os.path.exists(output_path):
                os.remove(output_path)
            raise

    def verify_file_integrity(self, file_path: str, checksum: str) -> bool:
        """验证文件完整性"""
        calculated_checksum = self.calculate_file_checksum(file_path)
        return calculated_checksum == checksum

    def calculate_file_checksum(self, file_path: str) -> str:
        """计算文件校验和"""
        sha256_hash = hashlib.sha256()
        with open(file_path, 'rb') as file:
            for chunk in iter(lambda: file.read(4096), b''):
                sha256_hash.update(chunk)
        return sha256_hash.hexdigest()

    def generate_token(self, user_data: dict, expiry: int = 3600) -> str:
        """生成JWT令牌"""
        payload = {
            **user_data,
            'exp': time.time() + expiry
        }
        return jwt.encode(payload, self.jwt_secret, algorithm='HS256')

    def verify_token(self, token: str) -> dict:
        """验证JWT令牌"""
        try:
            return jwt.decode(token, self.jwt_secret, algorithms=['HS256'])
        except jwt.ExpiredSignatureError:
            raise SecurityError("Token has expired")
        except jwt.InvalidTokenError:
            raise SecurityError("Invalid token")

    def derive_key(self, password: str, salt: bytes = None) -> bytes:
        """从密码派生密钥"""
        if salt is None:
            salt = os.urandom(16)
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
        )
        key = kdf.derive(password.encode())
        return key, salt

    def secure_delete(self, file_path: str):
        """安全删除文件"""
        if os.path.exists(file_path):
            size = os.path.getsize(file_path)
            with open(file_path, 'wb') as f:
                f.write(os.urandom(size))
            os.remove(file_path)

    def encrypt_with_rsa(self, data: bytes) -> bytes:
        """使用RSA加密数据"""
        return self.public_key.encrypt(
            data,
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None
            )
        )

    def rotate_keys(self):
        """轮换加密密钥"""
        self.key = Fernet.generate_key()
        self.cipher_suite = Fernet(self.key)
        self.jwt_secret = os.urandom(32)
        self._init_asymmetric_keys()

    def encrypt_large_file(self, file_path: str) -> str:
        """使用AESGCM加密大文件"""
        aes_key = AESGCM.generate_key(bit_length=256)
        aesgcm = AESGCM(aes_key)
        nonce = os.urandom(12)

        output_path = f"{file_path}.aes"
        with open(file_path, 'rb') as infile, open(output_path, 'wb') as outfile:
            # 写入加密的AES密钥和nonce
            encrypted_key = self.encrypt_with_rsa(aes_key)
            outfile.write(len(encrypted_key).to_bytes(4, 'big'))
            outfile.write(encrypted_key)
            outfile.write(nonce)

            # 分块加密文件内容
            while chunk := infile.read(64 * 1024):  # 64KB chunks
                encrypted_chunk = aesgcm.encrypt(nonce, chunk, None)
                outfile.write(encrypted_chunk)

        return output_path

    def generate_key_pair(self, key_name: str = "default") -> Tuple[str, str]:
        """生成RSA密钥对

        Args:
            key_name: 密钥名称

        Returns:
            (私钥路径, 公钥路径)元组
        """
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
            backend=default_backend()
        )
        public_key = private_key.public_key()

        # 保存私钥
        private_key_path = os.path.join(self.key_dir, f"{key_name}.pem")
        with open(private_key_path, "wb") as f:
            f.write(private_key.private_bytes(
                encoding=Encoding.PEM,
                format=PrivateFormat.PKCS8,
                encryption_algorithm=NoEncryption()
            ))

        # 保存公钥
        public_key_path = os.path.join(self.key_dir, f"{key_name}.pub")
        with open(public_key_path, "wb") as f:
            f.write(public_key.public_bytes(
                encoding=Encoding.PEM,
                format=PublicFormat.SubjectPublicKeyInfo
            ))

        logger.info(f"生成的密钥对: {key_name}")
        return private_key_path, public_key_path

    def verify_file_integrity(self, file_path: str, expected_hash: str,
                              algorithm: str = 'sha256') -> bool:
        """验证文件完整性

        Args:
            file_path: 文件路径
            expected_hash: 预期的哈希值
            algorithm: 哈希算法

        Returns:
            如果哈希值匹配则返回True
        """
        actual_hash = self.calculate_file_hash(file_path, algorithm)
        result = actual_hash.lower() == expected_hash.lower()
        if not result:
            logger.warning(f"文件完整性验证失败: {file_path}")
            logger.debug(f"预期: {expected_hash}, 实际: {actual_hash}")
        return result

    def calculate_file_hash(self, file_path: str, algorithm: str = 'sha256') -> str:
        """计算文件哈希值

        Args:
            file_path: 文件路径
            algorithm: 哈希算法

        Returns:
            哈希值字符串
        """
        hash_func = getattr(hashlib, algorithm.lower())()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_func.update(chunk)
        return hash_func.hexdigest()

    def generate_signature(self, file_path: str, private_key_path: str) -> str:
        """为文件生成签名

        Args:
            file_path: 文件路径
            private_key_path: 私钥路径

        Returns:
            Base64编码的签名
        """
        # 计算文件哈希
        file_hash = self.calculate_file_hash(file_path).encode()

        # 使用私钥签名
        with open(private_key_path, "rb") as key_file:
            private_key = load_pem_private_key(
                key_file.read(),
                password=None,
                backend=default_backend()
            )

        signature = private_key.sign(
            file_hash,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH
            ),
            hashes.SHA256()
        )

        return base64.b64encode(signature).decode()

    def verify_signature(self, file_path: str, signature: str, public_key_path: str) -> bool:
        """验证文件签名

        Args:
            file_path: 文件路径
            signature: Base64编码的签名
            public_key_path: 公钥路径

        Returns:
            如果签名验证通过则返回True
        """
        try:
            # 计算文件哈希
            file_hash = self.calculate_file_hash(file_path).encode()

            # 解码签名
            signature_bytes = base64.b64decode(signature)

            # 使用公钥验证
            with open(public_key_path, "rb") as key_file:
                public_key = load_pem_public_key(
                    key_file.read(),
                    backend=default_backend()
                )

            public_key.verify(
                signature_bytes,
                file_hash,
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.MAX_LENGTH
                ),
                hashes.SHA256()
            )

            return True
        except Exception as e:
            logger.warning(f"签名验证失败: {str(e)}")
            return False
