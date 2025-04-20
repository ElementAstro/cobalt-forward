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
    """
    Handles encryption and decryption of configuration data.
    Provides key management and secure storage capabilities.
    """
    def __init__(self, key_file: Path):
        """
        Initialize the crypto service with a key file path.
        
        Args:
            key_file: Path to the encryption key file
        """
        self.key_file = key_file
        self._key = self._load_or_create_key()
        self._fernet = Fernet(self._key)

    def _load_or_create_key(self) -> bytes:
        """
        Load existing encryption key or create a new one.
        
        Returns:
            Encryption key bytes
        """
        try:
            if self.key_file.exists():
                logger.debug(f"Loading encryption key from {self.key_file}")
                key_data = self.key_file.read_bytes()
                return key_data
            else:
                logger.info(f"Creating new encryption key: {self.key_file}")
                # Ensure parent directory exists
                self.key_file.parent.mkdir(parents=True, exist_ok=True)

                # Generate strong key
                key = Fernet.generate_key()

                # Securely write key file
                temp_key_file = self.key_file.with_suffix('.tmp')
                temp_key_file.write_bytes(key)
                temp_key_file.chmod(0o600)  # Owner read/write only
                temp_key_file.replace(self.key_file)

                return key
        except Exception as e:
            logger.error(f"Error handling encryption key: {e}")
            # If error occurs, generate temporary key (session only)
            logger.warning("Using temporary session key")
            return Fernet.generate_key()

    def derive_key(self, password: str, salt: bytes = None) -> bytes:
        """
        Derive encryption key from password.
        
        Args:
            password: User password
            salt: Salt for key derivation, generated if None
            
        Returns:
            Tuple of (derived_key, salt)
        """
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
        """
        Encrypt string data.
        
        Args:
            data: String data to encrypt
            
        Returns:
            Base64-encoded encrypted string
            
        Raises:
            Exception: If encryption fails
        """
        try:
            return base64.urlsafe_b64encode(self._fernet.encrypt(data.encode())).decode()
        except Exception as e:
            logger.error(f"Encryption failed: {e}")
            raise

    def decrypt(self, encrypted_data: str) -> str:
        """
        Decrypt string data.
        
        Args:
            encrypted_data: Base64-encoded encrypted string
            
        Returns:
            Decrypted string
            
        Raises:
            InvalidToken: If data is corrupted or wrong key
            Exception: If decryption fails
        """
        try:
            return self._fernet.decrypt(base64.urlsafe_b64decode(encrypted_data)).decode()
        except InvalidToken:
            logger.error("Invalid encryption token, data may be corrupted or using wrong key")
            raise
        except Exception as e:
            logger.error(f"Decryption failed: {e}")
            raise

    def encrypt_bytes(self, data: bytes) -> bytes:
        """
        Encrypt binary data directly.
        
        Args:
            data: Bytes to encrypt
            
        Returns:
            Encrypted bytes
            
        Raises:
            Exception: If encryption fails
        """
        try:
            return self._fernet.encrypt(data)
        except Exception as e:
            logger.error(f"Byte encryption failed: {e}")
            raise

    def decrypt_bytes(self, encrypted_data: bytes) -> bytes:
        """
        Decrypt binary data directly.
        
        Args:
            encrypted_data: Encrypted bytes
            
        Returns:
            Decrypted bytes
            
        Raises:
            InvalidToken: If data is corrupted or wrong key
            Exception: If decryption fails
        """
        try:
            return self._fernet.decrypt(encrypted_data)
        except InvalidToken:
            logger.error("Invalid encryption token, data may be corrupted or using wrong key")
            raise
        except Exception as e:
            logger.error(f"Byte decryption failed: {e}")
            raise

    def rotate_key(self):
        """
        Rotate encryption key for enhanced security.
        
        Returns:
            Old key for re-encryption of existing data if needed
            
        Raises:
            Exception: If key rotation fails
        """
        try:
            old_key = self._key
            new_key = Fernet.generate_key()

            # Create new key's Fernet instance
            new_fernet = Fernet(new_key)

            # Backup old key
            backup_path = self.key_file.with_suffix('.bak')
            if backup_path.exists():
                backup_path.unlink()
            self.key_file.rename(backup_path)

            # Write new key
            self.key_file.write_bytes(new_key)
            self.key_file.chmod(0o600)  # Owner read/write only

            self._key = new_key
            self._fernet = new_fernet
            logger.info("Encryption key rotated successfully")

            return old_key  # Return old key for reencryption of existing data
        except Exception as e:
            logger.error(f"Key rotation failed: {e}")
            raise
