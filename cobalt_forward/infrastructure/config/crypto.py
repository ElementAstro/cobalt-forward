"""
Configuration encryption and decryption for the Cobalt Forward application.

This module provides secure encryption and decryption of configuration data
with key management and secure storage capabilities.
"""

from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from pathlib import Path
import base64
import logging
import os
import secrets
import json
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class ConfigCrypto:
    """
    Handles encryption and decryption of configuration data.
    
    Provides key management and secure storage capabilities for sensitive
    configuration information.
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
                
                # Atomic move to final location
                temp_key_file.replace(self.key_file)
                
                # Set restrictive permissions (Unix-like systems)
                try:
                    os.chmod(self.key_file, 0o600)
                except (OSError, AttributeError):
                    # Windows or permission error
                    pass
                
                logger.info("New encryption key created successfully")
                return key
                
        except Exception as e:
            logger.error(f"Failed to load or create encryption key: {e}")
            raise

    def encrypt_data(self, data: Dict[str, Any]) -> bytes:
        """
        Encrypt configuration data.
        
        Args:
            data: Configuration data to encrypt
            
        Returns:
            Encrypted data as bytes
            
        Raises:
            Exception: If encryption fails
        """
        try:
            # Convert to JSON string
            json_data = json.dumps(data, ensure_ascii=False, separators=(',', ':'))
            
            # Encrypt the JSON string
            encrypted_data = self._fernet.encrypt(json_data.encode('utf-8'))
            
            logger.debug("Configuration data encrypted successfully")
            return encrypted_data
            
        except Exception as e:
            logger.error(f"Failed to encrypt configuration data: {e}")
            raise

    def decrypt_data(self, encrypted_data: bytes) -> Dict[str, Any]:
        """
        Decrypt configuration data.
        
        Args:
            encrypted_data: Encrypted data bytes
            
        Returns:
            Decrypted configuration data
            
        Raises:
            InvalidToken: If decryption fails due to invalid data or key
            Exception: If other decryption errors occur
        """
        try:
            # Decrypt the data
            decrypted_bytes = self._fernet.decrypt(encrypted_data)
            
            # Convert back to dictionary
            json_data = decrypted_bytes.decode('utf-8')
            data = json.loads(json_data)
            
            logger.debug("Configuration data decrypted successfully")
            return data
            
        except InvalidToken:
            logger.error("Failed to decrypt configuration: invalid token or corrupted data")
            raise
        except Exception as e:
            logger.error(f"Failed to decrypt configuration data: {e}")
            raise

    def encrypt_file(self, input_file: Path, output_file: Path) -> None:
        """
        Encrypt a configuration file.
        
        Args:
            input_file: Path to the input configuration file
            output_file: Path to the encrypted output file
            
        Raises:
            FileNotFoundError: If input file doesn't exist
            Exception: If encryption fails
        """
        try:
            if not input_file.exists():
                raise FileNotFoundError(f"Input file not found: {input_file}")
            
            # Read and parse the configuration file
            with open(input_file, 'r', encoding='utf-8') as f:
                if input_file.suffix.lower() == '.json':
                    data = json.load(f)
                else:
                    # For other formats, read as text and try to parse as JSON
                    content = f.read()
                    try:
                        data = json.loads(content)
                    except json.JSONDecodeError:
                        # If not JSON, treat as plain text
                        data = {"content": content}
            
            # Encrypt the data
            encrypted_data = self.encrypt_data(data)
            
            # Write encrypted data to output file
            output_file.parent.mkdir(parents=True, exist_ok=True)
            output_file.write_bytes(encrypted_data)
            
            logger.info(f"Configuration file encrypted: {input_file} -> {output_file}")
            
        except Exception as e:
            logger.error(f"Failed to encrypt file {input_file}: {e}")
            raise

    def decrypt_file(self, input_file: Path, output_file: Path) -> None:
        """
        Decrypt a configuration file.
        
        Args:
            input_file: Path to the encrypted input file
            output_file: Path to the decrypted output file
            
        Raises:
            FileNotFoundError: If input file doesn't exist
            Exception: If decryption fails
        """
        try:
            if not input_file.exists():
                raise FileNotFoundError(f"Input file not found: {input_file}")
            
            # Read encrypted data
            encrypted_data = input_file.read_bytes()
            
            # Decrypt the data
            data = self.decrypt_data(encrypted_data)
            
            # Write decrypted data to output file
            output_file.parent.mkdir(parents=True, exist_ok=True)
            
            if "content" in data and len(data) == 1:
                # This was originally a plain text file
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(data["content"])
            else:
                # This was originally a JSON file
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Configuration file decrypted: {input_file} -> {output_file}")
            
        except Exception as e:
            logger.error(f"Failed to decrypt file {input_file}: {e}")
            raise

    def rotate_key(self, new_key_file: Optional[Path] = None) -> None:
        """
        Rotate the encryption key.
        
        Args:
            new_key_file: Optional path for the new key file
            
        Note:
            This will invalidate all previously encrypted data.
            Use with caution and ensure you have backups.
        """
        try:
            # Generate new key
            new_key = Fernet.generate_key()
            
            # Determine key file path
            if new_key_file is None:
                new_key_file = self.key_file
            
            # Backup old key if it exists
            if self.key_file.exists():
                backup_key_file = self.key_file.with_suffix('.bak')
                self.key_file.replace(backup_key_file)
                logger.info(f"Old key backed up to: {backup_key_file}")
            
            # Write new key
            new_key_file.parent.mkdir(parents=True, exist_ok=True)
            new_key_file.write_bytes(new_key)
            
            # Set restrictive permissions
            try:
                os.chmod(new_key_file, 0o600)
            except (OSError, AttributeError):
                pass
            
            # Update internal state
            self.key_file = new_key_file
            self._key = new_key
            self._fernet = Fernet(self._key)
            
            logger.info("Encryption key rotated successfully")
            
        except Exception as e:
            logger.error(f"Failed to rotate encryption key: {e}")
            raise

    def verify_key(self) -> bool:
        """
        Verify that the encryption key is valid.
        
        Returns:
            True if key is valid, False otherwise
        """
        try:
            # Test encryption/decryption with sample data
            test_data = {"test": "verification"}
            encrypted = self.encrypt_data(test_data)
            decrypted = self.decrypt_data(encrypted)
            
            return decrypted == test_data
            
        except Exception as e:
            logger.error(f"Key verification failed: {e}")
            return False

    def get_key_info(self) -> Dict[str, Any]:
        """
        Get information about the current encryption key.
        
        Returns:
            Dictionary with key information
        """
        try:
            key_info = {
                "key_file": str(self.key_file),
                "key_exists": self.key_file.exists(),
                "key_valid": False,
                "key_size": 0,
                "created_at": None
            }
            
            if self.key_file.exists():
                stat = self.key_file.stat()
                key_info.update({
                    "key_size": stat.st_size,
                    "created_at": stat.st_mtime,
                    "key_valid": self.verify_key()
                })
            
            return key_info
            
        except Exception as e:
            logger.error(f"Failed to get key info: {e}")
            return {"error": str(e)}

    @staticmethod
    def generate_password_key(password: str, salt: Optional[bytes] = None) -> bytes:
        """
        Generate an encryption key from a password using PBKDF2.
        
        Args:
            password: Password to derive key from
            salt: Optional salt bytes (generated if not provided)
            
        Returns:
            Derived encryption key
        """
        if salt is None:
            salt = os.urandom(16)
        
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
            backend=default_backend()
        )
        
        key = base64.urlsafe_b64encode(kdf.derive(password.encode('utf-8')))
        return key
