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
from typing import Tuple, Optional
from loguru import logger


class SecurityEnhancer:
    def __init__(self, key_dir: Optional[str] = None):
        """Initialize security enhancer

        Args:
            key_dir: Directory for key storage
        """
        self.key_dir = key_dir or os.path.join(
            os.path.expanduser("~"), ".ftp_keys")
        if not os.path.exists(self.key_dir):
            os.makedirs(self.key_dir, exist_ok=True)

        self.session_key = Fernet.generate_key()
        self.cipher = Fernet(self.session_key)
        self.jwt_secret = os.urandom(32)
        self._init_asymmetric_keys()
        self.key_rotation_interval = 3600  # Key rotation every hour
        self._key_cache = {}
        self._init_ciphers()

    def _init_asymmetric_keys(self):
        """Initialize asymmetric key pair"""
        self.private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048
        )
        self.public_key = self.private_key.public_key()

    def _init_ciphers(self):
        """Initialize encryption ciphers"""
        self.aes_gcm = AESGCM(AESGCM.generate_key(key_size=256))
        self._update_key_timestamp = time.time()

    @property
    def needs_key_rotation(self) -> bool:
        """Check if keys need rotation"""
        return time.time() - self._update_key_timestamp >= self.key_rotation_interval

    def create_ssl_context(self, cert_file: str, key_file: str) -> ssl.SSLContext:
        """Create SSL context for secure communications

        Args:
            cert_file: Certificate file path
            key_file: Key file path

        Returns:
            Configured SSL context
        """
        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        context.load_cert_chain(certfile=cert_file, keyfile=key_file)
        return context

    def encrypt_file(self, file_path: str, key_path: Optional[str] = None) -> str:
        """Encrypt a file

        Args:
            file_path: Path of file to encrypt
            key_path: Optional path to public key, uses session key if not provided

        Returns:
            Path to encrypted file

        Raises:
            Exception: If encryption fails
        """
        encrypted_path = f"{file_path}.enc"
        try:
            with open(file_path, "rb") as f:
                data = f.read()

            if key_path:
                # Use RSA public key encryption
                with open(key_path, "rb") as key_file:
                    public_key = load_pem_public_key(
                        key_file.read(),
                        backend=default_backend()
                    )

                # Ensure we have an RSA public key for encryption
                if not isinstance(public_key, rsa.RSAPublicKey):
                    raise ValueError("Only RSA public keys support encryption")

                # For large files, use hybrid encryption: random AES key + RSA
                file_key = Fernet.generate_key()
                file_cipher = Fernet(file_key)
                encrypted_data = file_cipher.encrypt(data)

                # Encrypt the file key with RSA
                encrypted_key = public_key.encrypt(
                    file_key,
                    OAEP(
                        mgf=MGF1(algorithm=hashes.SHA256()),
                        algorithm=hashes.SHA256(),
                        label=None
                    )
                )

                # Write encrypted key and data to file
                with open(encrypted_path, "wb") as f:
                    f.write(len(encrypted_key).to_bytes(4, byteorder='big'))
                    f.write(encrypted_key)
                    f.write(encrypted_data)
            else:
                # Use session key encryption
                encrypted_data = self.cipher.encrypt(data)
                with open(encrypted_path, "wb") as f:
                    f.write(encrypted_data)

            logger.debug(f"File encrypted: {file_path} -> {encrypted_path}")
            return encrypted_path
        except Exception as e:
            logger.error(f"Failed to encrypt file {file_path}: {str(e)}")
            if os.path.exists(encrypted_path):
                os.remove(encrypted_path)
            raise

    def decrypt_file(self, encrypted_path: str, output_path: Optional[str] = None,
                     private_key_path: Optional[str] = None) -> str:
        """Decrypt a file

        Args:
            encrypted_path: Path to encrypted file
            output_path: Output file path
            private_key_path: Path to private key

        Returns:
            Path to decrypted file

        Raises:
            Exception: If decryption fails
        """
        if not output_path:
            output_path = encrypted_path.replace('.enc', '.dec')

        try:
            with open(encrypted_path, "rb") as f:
                if private_key_path:
                    # RSA decryption
                    with open(private_key_path, "rb") as key_file:
                        private_key = load_pem_private_key(
                            key_file.read(),
                            password=None,
                            backend=default_backend()
                        )

                    # Ensure we have an RSA private key for decryption
                    if not isinstance(private_key, rsa.RSAPrivateKey):
                        raise ValueError(
                            "Only RSA private keys support decryption")

                    # Read encrypted key length
                    key_len_bytes = f.read(4)
                    key_len = int.from_bytes(key_len_bytes, byteorder='big')

                    # Read encrypted key
                    encrypted_key = f.read(key_len)

                    # Decrypt file key
                    file_key = private_key.decrypt(
                        encrypted_key,
                        OAEP(
                            mgf=MGF1(algorithm=hashes.SHA256()),
                            algorithm=hashes.SHA256(),
                            label=None
                        )
                    )

                    # Use file key to decrypt file data
                    file_cipher = Fernet(file_key)
                    encrypted_data = f.read()
                    data = file_cipher.decrypt(encrypted_data)
                else:
                    # Use session key to decrypt
                    encrypted_data = f.read()
                    data = self.cipher.decrypt(encrypted_data)

                with open(output_path, "wb") as f:
                    f.write(data)

                logger.debug(
                    f"File decrypted: {encrypted_path} -> {output_path}")
                return output_path
        except Exception as e:
            logger.error(f"Failed to decrypt file {encrypted_path}: {str(e)}")
            if os.path.exists(output_path):
                os.remove(output_path)
            raise

    def calculate_file_checksum(self, file_path: str) -> str:
        """Calculate file checksum

        Args:
            file_path: Path to file

        Returns:
            SHA-256 checksum as hex string
        """
        sha256_hash = hashlib.sha256()
        with open(file_path, 'rb') as file:
            for chunk in iter(lambda: file.read(4096), b''):
                sha256_hash.update(chunk)
        return sha256_hash.hexdigest()

    def generate_token(self, user_data: dict[str, str], expiry: int = 3600) -> str:
        """Generate JWT token

        Args:
            user_data: User data to include in token
            expiry: Token expiry time in seconds

        Returns:
            JWT token string
        """
        payload: dict[str, str | float] = {
            **user_data,
            'exp': time.time() + expiry
        }
        return jwt.encode(payload, self.jwt_secret, algorithm='HS256')

    def verify_token(self, token: str) -> dict[str, str]:
        """Verify JWT token

        Args:
            token: JWT token to verify

        Returns:
            Token payload if valid

        Raises:
            SecurityError: If token is invalid or expired
        """
        try:
            return jwt.decode(token, self.jwt_secret, algorithms=['HS256'])
        except jwt.ExpiredSignatureError:
            raise SecurityError("Token has expired")
        except jwt.InvalidTokenError:
            raise SecurityError("Invalid token")

    def derive_key(self, password: str, salt: Optional[bytes] = None) -> Tuple[bytes, bytes]:
        """Derive encryption key from password

        Args:
            password: Password to derive key from
            salt: Optional salt, random if not provided

        Returns:
            Tuple of (key, salt)
        """
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
        """Securely delete file by overwriting with random data

        Args:
            file_path: Path to file to delete
        """
        if os.path.exists(file_path):
            size = os.path.getsize(file_path)
            with open(file_path, 'wb') as f:
                f.write(os.urandom(size))
            os.remove(file_path)

    def encrypt_with_rsa(self, data: bytes) -> bytes:
        """Encrypt data using RSA

        Args:
            data: Data to encrypt

        Returns:
            Encrypted data
        """
        return self.public_key.encrypt(
            data,
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None
            )
        )

    def rotate_keys(self):
        """Rotate encryption keys"""
        self.session_key = Fernet.generate_key()
        self.cipher = Fernet(self.session_key)
        self.jwt_secret = os.urandom(32)
        self._init_asymmetric_keys()
        self._update_key_timestamp = time.time()
        logger.info("Security keys rotated")

    def encrypt_large_file(self, file_path: str) -> str:
        """Encrypt large file using AESGCM

        Args:
            file_path: Path to file

        Returns:
            Path to encrypted file
        """
        aes_key = AESGCM.generate_key(key_size=256)
        aesgcm = AESGCM(aes_key)
        nonce = os.urandom(12)

        output_path = f"{file_path}.aes"
        with open(file_path, 'rb') as infile, open(output_path, 'wb') as outfile:
            # Write encrypted AES key and nonce
            encrypted_key = self.encrypt_with_rsa(aes_key)
            outfile.write(len(encrypted_key).to_bytes(4, 'big'))
            outfile.write(encrypted_key)
            outfile.write(nonce)

            # Encrypt file content in chunks
            while chunk := infile.read(64 * 1024):  # 64KB chunks
                encrypted_chunk = aesgcm.encrypt(nonce, chunk, None)
                outfile.write(encrypted_chunk)

        logger.debug(f"Large file encrypted: {file_path} -> {output_path}")
        return output_path

    def generate_key_pair(self, key_name: str = "default") -> Tuple[str, str]:
        """Generate RSA key pair

        Args:
            key_name: Key name

        Returns:
            Tuple of (private_key_path, public_key_path)
        """
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
            backend=default_backend()
        )
        public_key = private_key.public_key()

        # Save private key
        private_key_path = os.path.join(self.key_dir, f"{key_name}.pem")
        with open(private_key_path, "wb") as f:
            f.write(private_key.private_bytes(
                encoding=Encoding.PEM,
                format=PrivateFormat.PKCS8,
                encryption_algorithm=NoEncryption()
            ))

        # Save public key
        public_key_path = os.path.join(self.key_dir, f"{key_name}.pub")
        with open(public_key_path, "wb") as f:
            f.write(public_key.public_bytes(
                encoding=Encoding.PEM,
                format=PublicFormat.SubjectPublicKeyInfo
            ))

        logger.info(f"Generated key pair: {key_name}")
        return private_key_path, public_key_path

    def calculate_file_hash(self, file_path: str, algorithm: str = 'sha256') -> str:
        """Calculate file hash using specified algorithm

        Args:
            file_path: Path to file
            algorithm: Hash algorithm to use

        Returns:
            Checksum as hex string
        """
        hash_func = getattr(hashlib, algorithm.lower())()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_func.update(chunk)
        return hash_func.hexdigest()

    def verify_file_integrity(self, file_path: str, expected_hash: str,
                              algorithm: str = 'sha256') -> bool:
        """Verify file integrity with hash comparison

        Args:
            file_path: Path to file
            expected_hash: Expected hash value
            algorithm: Hash algorithm

        Returns:
            True if integrity check passes
        """
        actual_hash = self.calculate_file_hash(file_path, algorithm)
        result = actual_hash.lower() == expected_hash.lower()
        if not result:
            logger.warning(f"File integrity verification failed: {file_path}")
            logger.debug(f"Expected: {expected_hash}, Actual: {actual_hash}")
        return result

    def generate_signature(self, file_path: str, private_key_path: str) -> str:
        """Generate file signature

        Args:
            file_path: Path to file
            private_key_path: Path to private key

        Returns:
            Base64 encoded signature
        """
        # Calculate file hash
        file_hash = self.calculate_file_hash(file_path).encode()

        # Sign using private key
        with open(private_key_path, "rb") as key_file:
            private_key = load_pem_private_key(
                key_file.read(),
                password=None,
                backend=default_backend()
            )

        # Ensure we have an RSA private key for signing
        if not isinstance(private_key, rsa.RSAPrivateKey):
            raise ValueError("Only RSA private keys support signing")

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
        """Verify file signature

        Args:
            file_path: Path to file
            signature: Base64 encoded signature
            public_key_path: Path to public key

        Returns:
            True if signature is valid
        """
        try:
            # Calculate file hash
            file_hash = self.calculate_file_hash(file_path).encode()

            # Decode signature
            signature_bytes = base64.b64decode(signature)

            # Verify using public key
            with open(public_key_path, "rb") as key_file:
                public_key = load_pem_public_key(
                    key_file.read(),
                    backend=default_backend()
                )

            # Ensure we have an RSA public key for verification
            if not isinstance(public_key, rsa.RSAPublicKey):
                raise ValueError(
                    "Only RSA public keys support signature verification")

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
            logger.warning(f"Signature verification failed: {str(e)}")
            return False
