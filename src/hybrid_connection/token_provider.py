"""Token provider for Azure Relay SAS authentication."""

import base64
import hashlib
import hmac
import time
from typing import Optional
from urllib.parse import quote_plus


class SecurityToken:
    """Represents a SAS security token with expiration."""

    def __init__(self, token: str, expires_at_utc: float, audience: str):
        """Initialize a security token.
        
        Args:
            token: The SAS token string.
            expires_at_utc: UTC timestamp when the token expires.
            audience: The resource URI the token is valid for.
        """
        self._token = token
        self._expires_at_utc = expires_at_utc
        self._audience = audience

    @property
    def token(self) -> str:
        """Get the SAS token string."""
        return self._token

    @property
    def expires_at_utc(self) -> float:
        """Get the UTC timestamp when the token expires."""
        return self._expires_at_utc

    @property
    def audience(self) -> str:
        """Get the resource URI the token is valid for."""
        return self._audience

    @property
    def is_expired(self) -> bool:
        """Check if the token has expired."""
        return time.time() >= self._expires_at_utc

    def expires_in_seconds(self) -> float:
        """Get seconds until token expires (negative if already expired)."""
        return self._expires_at_utc - time.time()

    def __str__(self) -> str:
        """Return the token string."""
        return self._token


class TokenProvider:
    """Provides SAS tokens for Azure Relay authentication."""

    DEFAULT_TOKEN_VALIDITY_SECONDS = 3600  # 1 hour

    def __init__(
        self,
        key_name: str,
        shared_access_key: str,
        token_validity_seconds: int = DEFAULT_TOKEN_VALIDITY_SECONDS
    ):
        """Initialize the token provider.
        
        Args:
            key_name: The shared access key name.
            shared_access_key: The shared access key value.
            token_validity_seconds: How long tokens should be valid (default 1 hour).
        """
        if not key_name:
            raise ValueError("key_name cannot be empty")
        if not shared_access_key:
            raise ValueError("shared_access_key cannot be empty")
        if token_validity_seconds <= 0:
            raise ValueError("token_validity_seconds must be positive")

        self._key_name = key_name
        self._shared_access_key = shared_access_key
        self._token_validity_seconds = token_validity_seconds

    @property
    def key_name(self) -> str:
        """Get the shared access key name."""
        return self._key_name

    @property
    def token_validity_seconds(self) -> int:
        """Get the token validity duration in seconds."""
        return self._token_validity_seconds

    def get_token(self, audience: str) -> SecurityToken:
        """Generate a SAS token for the given audience.
        
        Args:
            audience: The resource URI to generate a token for.
            
        Returns:
            A SecurityToken instance.
            
        Raises:
            ValueError: If audience is empty.
        """
        if not audience:
            raise ValueError("audience cannot be empty")

        expires_at_utc = time.time() + self._token_validity_seconds
        token = self._generate_sas_token(audience, expires_at_utc)
        return SecurityToken(token, expires_at_utc, audience)

    def _generate_sas_token(self, audience: str, expires_at_utc: float) -> str:
        """Generate the SAS token string.
        
        The SAS token format is:
        SharedAccessSignature sr={URI}&sig={signature}&se={expiry}&skn={keyname}
        
        Args:
            audience: The resource URI.
            expires_at_utc: UTC timestamp when the token expires.
            
        Returns:
            The SAS token string.
        """
        # Azure Relay requires the audience URI to use http:// scheme (not sb://)
        # and to be lowercase
        if audience.startswith("sb://"):
            audience = "http://" + audience[5:]
        audience = audience.lower()
        
        # URL encode the audience (resource URI)
        encoded_audience = quote_plus(audience)

        # Expiry time as integer string
        expiry = str(int(expires_at_utc))

        # String to sign: {encoded_uri}\n{expiry}
        string_to_sign = f"{encoded_audience}\n{expiry}"

        # Compute HMAC-SHA256 signature
        signature = self._compute_signature(string_to_sign)

        # URL encode the signature
        encoded_signature = quote_plus(signature)

        # Build the token
        token = (
            f"SharedAccessSignature "
            f"sr={encoded_audience}&"
            f"sig={encoded_signature}&"
            f"se={expiry}&"
            f"skn={self._key_name}"
        )

        return token

    def _compute_signature(self, string_to_sign: str) -> str:
        """Compute HMAC-SHA256 signature and return as base64.
        
        Args:
            string_to_sign: The string to sign.
            
        Returns:
            Base64-encoded signature.
        """
        # Use the shared access key as UTF-8 bytes (per Azure documentation)
        key_bytes = self._shared_access_key.encode('utf-8')

        # Compute HMAC-SHA256
        message_bytes = string_to_sign.encode('utf-8')
        signature = hmac.new(key_bytes, message_bytes, hashlib.sha256).digest()

        # Return base64-encoded signature
        return base64.b64encode(signature).decode('utf-8')

    @classmethod
    def from_connection_string(
        cls,
        key_name: str,
        shared_access_key: str,
        token_validity_seconds: Optional[int] = None
    ) -> "TokenProvider":
        """Create a TokenProvider from connection string components.
        
        Args:
            key_name: The shared access key name from connection string.
            shared_access_key: The shared access key from connection string.
            token_validity_seconds: Optional token validity duration.
            
        Returns:
            A TokenProvider instance.
        """
        if token_validity_seconds is not None:
            return cls(key_name, shared_access_key, token_validity_seconds)
        return cls(key_name, shared_access_key)
