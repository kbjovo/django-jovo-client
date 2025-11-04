"""
Encryption utilities for sensitive data like database passwords.
Uses Fernet symmetric encryption from cryptography library.
"""
from cryptography.fernet import Fernet
from django.conf import settings
import base64
import hashlib


def get_fernet_key():
    """
    Get or create a Fernet key from the settings.
    Converts the settings key to a proper Fernet key format.
    """
    key = settings.DB_PASSWORD_ENCRYPTION_KEY
    # Ensure the key is 32 bytes and base64 encoded for Fernet
    hashed = hashlib.sha256(key.encode()).digest()
    return base64.urlsafe_b64encode(hashed)


def encrypt_password(plain_password):
    """
    Encrypt a plain text password.

    Args:
        plain_password (str): The password to encrypt

    Returns:
        str: Encrypted password as a string
    """
    if not plain_password:
        return ""

    fernet = Fernet(get_fernet_key())
    encrypted = fernet.encrypt(plain_password.encode())
    return encrypted.decode()


def decrypt_password(encrypted_password):
    """
    Decrypt an encrypted password.

    Args:
        encrypted_password (str): The encrypted password

    Returns:
        str: Decrypted plain text password
    """
    if not encrypted_password:
        return ""

    fernet = Fernet(get_fernet_key())
    try:
        decrypted = fernet.decrypt(encrypted_password.encode())
        return decrypted.decode()
    except Exception as e:
        # If decryption fails, might be an unencrypted password
        # This can happen during migration
        print(f"Decryption error: {e}")
        return encrypted_password