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


class SecurityEnhancer:
    def __init__(self):
        self.key = Fernet.generate_key()
        self.cipher_suite = Fernet(self.key)
        self.jwt_secret = os.urandom(32)
        self._init_asymmetric_keys()
        self.key_rotation_interval = 3600  # 1小时轮换一次密钥

    def _init_asymmetric_keys(self):
        """初始化非对称密钥对"""
        self.private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048
        )
        self.public_key = self.private_key.public_key()

    def create_ssl_context(self, cert_file: str, key_file: str) -> ssl.SSLContext:
        """创建SSL上下文"""
        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        context.load_cert_chain(certfile=cert_file, keyfile=key_file)
        return context

    def encrypt_file(self, file_path: str) -> str:
        """加密文件"""
        with open(file_path, 'rb') as file:
            file_data = file.read()
        encrypted_data = self.cipher_suite.encrypt(file_data)
        encrypted_path = f"{file_path}.encrypted"
        with open(encrypted_path, 'wb') as file:
            file.write(encrypted_data)
        return encrypted_path

    def decrypt_file(self, encrypted_file: str) -> str:
        """解密文件"""
        with open(encrypted_file, 'rb') as file:
            encrypted_data = file.read()
        decrypted_data = self.cipher_suite.decrypt(encrypted_data)
        decrypted_path = encrypted_file.replace('.encrypted', '')
        with open(decrypted_path, 'wb') as file:
            file.write(decrypted_data)
        return decrypted_path

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
