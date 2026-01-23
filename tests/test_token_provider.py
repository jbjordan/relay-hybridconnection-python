"""Unit tests for TokenProvider and SecurityToken."""

import base64
import hashlib
import hmac
import secrets
import string
import time
from unittest.mock import patch
from urllib.parse import quote_plus, unquote_plus

import pytest

from src.hybrid_connection.token_provider import SecurityToken, TokenProvider


def generate_random_sas_key(length: int = 44) -> str:
    """Generate a random SAS key similar to Azure's base64-encoded keys."""
    alphabet = string.ascii_letters + string.digits + "+/"
    key = "".join(secrets.choice(alphabet) for _ in range(length - 1))
    return key + "="


def generate_random_token_str() -> str:
    """Generate a random SAS token string for testing."""
    sig = generate_random_sas_key(32)
    se = str(int(time.time()) + 3600)
    return f"SharedAccessSignature sr=test&sig={sig}&se={se}&skn=key"


class TestSecurityToken:
    """Tests for the SecurityToken class."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures with randomly generated token string."""
        self.token_str = generate_random_token_str()

    def test_token_properties(self):
        """Test that token properties return correct values."""
        expires = time.time() + 3600
        audience = "https://test.servicebus.windows.net/hc"
        
        token = SecurityToken(self.token_str, expires, audience)
        
        assert token.token == self.token_str
        assert token.expires_at_utc == expires
        assert token.audience == audience

    def test_str_returns_token(self):
        """Test that str() returns the token string."""
        token = SecurityToken(self.token_str, time.time() + 3600, "audience")
        
        assert str(token) == self.token_str

    def test_is_expired_false_for_future_token(self):
        """Test is_expired returns False for non-expired token."""
        token = SecurityToken("token", time.time() + 3600, "audience")
        
        assert token.is_expired is False

    def test_is_expired_true_for_past_token(self):
        """Test is_expired returns True for expired token."""
        token = SecurityToken("token", time.time() - 1, "audience")
        
        assert token.is_expired is True

    def test_expires_in_seconds_positive(self):
        """Test expires_in_seconds returns positive value for future token."""
        expires = time.time() + 3600
        token = SecurityToken("token", expires, "audience")
        
        # Allow small delta for test execution time
        assert 3599 <= token.expires_in_seconds() <= 3600

    def test_expires_in_seconds_negative(self):
        """Test expires_in_seconds returns negative value for expired token."""
        expires = time.time() - 100
        token = SecurityToken("token", expires, "audience")
        
        assert token.expires_in_seconds() < 0


class TestTokenProvider:
    """Tests for the TokenProvider class."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures with randomly generated SAS key."""
        self.test_key = generate_random_sas_key()
        self.test_key_name = "RootManageSharedAccessKey"

    def test_constructor_with_valid_params(self):
        """Test constructor accepts valid parameters."""
        provider = TokenProvider(self.test_key_name, self.test_key)
        
        assert provider.key_name == self.test_key_name
        assert provider.token_validity_seconds == TokenProvider.DEFAULT_TOKEN_VALIDITY_SECONDS

    def test_constructor_with_custom_validity(self):
        """Test constructor accepts custom token validity."""
        provider = TokenProvider(self.test_key_name, self.test_key, 7200)
        
        assert provider.token_validity_seconds == 7200

    def test_constructor_raises_on_empty_key_name(self):
        """Test constructor raises ValueError for empty key name."""
        with pytest.raises(ValueError, match="key_name cannot be empty"):
            TokenProvider("", self.test_key)

    def test_constructor_raises_on_empty_shared_key(self):
        """Test constructor raises ValueError for empty shared key."""
        with pytest.raises(ValueError, match="shared_access_key cannot be empty"):
            TokenProvider(self.test_key_name, "")

    def test_constructor_raises_on_zero_validity(self):
        """Test constructor raises ValueError for zero validity."""
        with pytest.raises(ValueError, match="token_validity_seconds must be positive"):
            TokenProvider(self.test_key_name, self.test_key, 0)

    def test_constructor_raises_on_negative_validity(self):
        """Test constructor raises ValueError for negative validity."""
        with pytest.raises(ValueError, match="token_validity_seconds must be positive"):
            TokenProvider(self.test_key_name, self.test_key, -100)

    def test_get_token_returns_security_token(self):
        """Test get_token returns a SecurityToken instance."""
        provider = TokenProvider(self.test_key_name, self.test_key)
        audience = "https://test.servicebus.windows.net/hc"
        
        token = provider.get_token(audience)
        
        assert isinstance(token, SecurityToken)
        assert token.audience == audience

    def test_get_token_raises_on_empty_audience(self):
        """Test get_token raises ValueError for empty audience."""
        provider = TokenProvider(self.test_key_name, self.test_key)
        
        with pytest.raises(ValueError, match="audience cannot be empty"):
            provider.get_token("")

    def test_get_token_expiration_time(self):
        """Test get_token sets correct expiration time."""
        validity = 1800
        provider = TokenProvider(self.test_key_name, self.test_key, validity)
        
        before = time.time()
        token = provider.get_token("https://test.servicebus.windows.net/hc")
        after = time.time()
        
        assert before + validity <= token.expires_at_utc <= after + validity

    def test_token_format(self):
        """Test generated token has correct SAS format."""
        provider = TokenProvider(self.test_key_name, self.test_key)
        audience = "https://test.servicebus.windows.net/hc"
        
        token = provider.get_token(audience)
        token_str = str(token)
        
        # Token should start with SharedAccessSignature
        assert token_str.startswith("SharedAccessSignature ")
        
        # Token should contain required parts
        assert "sr=" in token_str
        assert "sig=" in token_str
        assert "se=" in token_str
        assert "skn=" in token_str

    def test_token_contains_correct_key_name(self):
        """Test token contains the correct key name."""
        provider = TokenProvider(self.test_key_name, self.test_key)
        token = provider.get_token("https://test.servicebus.windows.net/hc")
        
        assert f"skn={self.test_key_name}" in str(token)

    def test_token_contains_encoded_audience(self):
        """Test token contains URL-encoded audience."""
        provider = TokenProvider(self.test_key_name, self.test_key)
        audience = "https://test.servicebus.windows.net/hc"
        
        token = provider.get_token(audience)
        encoded_audience = quote_plus(audience)
        
        assert f"sr={encoded_audience}" in str(token)

    def test_token_signature_is_valid(self):
        """Test that the signature in the token is correctly computed."""
        provider = TokenProvider(self.test_key_name, self.test_key)
        audience = "https://test.servicebus.windows.net/hc"
        
        # Mock time to get predictable expiry
        fixed_time = 1700000000.0
        with patch('src.hybrid_connection.token_provider.time.time', return_value=fixed_time):
            token = provider.get_token(audience)
        
        # Parse the token to extract components
        token_str = str(token)
        parts = token_str.replace("SharedAccessSignature ", "").split("&")
        token_parts = {}
        for part in parts:
            key, value = part.split("=", 1)
            token_parts[key] = value
        
        # Verify the signature - implementation lowercases the audience
        encoded_audience = quote_plus(audience.lower())
        expiry = token_parts['se']
        string_to_sign = f"{encoded_audience}\n{expiry}"
        
        # Azure uses the key as UTF-8 bytes directly (not base64-decoded)
        key_bytes = self.test_key.encode('utf-8')
        expected_signature = hmac.new(
            key_bytes,
            string_to_sign.encode('utf-8'),
            hashlib.sha256
        ).digest()
        expected_sig_b64 = base64.b64encode(expected_signature).decode('utf-8')
        
        actual_sig = unquote_plus(token_parts['sig'])
        assert actual_sig == expected_sig_b64

    def test_from_connection_string_factory(self):
        """Test from_connection_string factory method."""
        provider = TokenProvider.from_connection_string(
            self.test_key_name,
            self.test_key
        )
        
        assert provider.key_name == self.test_key_name
        assert provider.token_validity_seconds == TokenProvider.DEFAULT_TOKEN_VALIDITY_SECONDS

    def test_from_connection_string_with_validity(self):
        """Test from_connection_string factory with custom validity."""
        provider = TokenProvider.from_connection_string(
            self.test_key_name,
            self.test_key,
            token_validity_seconds=7200
        )
        
        assert provider.token_validity_seconds == 7200

    def test_multiple_tokens_have_different_signatures_at_different_times(self):
        """Test that tokens generated at different times have different signatures."""
        provider = TokenProvider(self.test_key_name, self.test_key)
        audience = "https://test.servicebus.windows.net/hc"
        
        with patch('src.hybrid_connection.token_provider.time.time', return_value=1700000000.0):
            token1 = provider.get_token(audience)
        
        with patch('src.hybrid_connection.token_provider.time.time', return_value=1700001000.0):
            token2 = provider.get_token(audience)
        
        # Extract signatures
        sig1 = str(token1).split("sig=")[1].split("&")[0]
        sig2 = str(token2).split("sig=")[1].split("&")[0]
        
        assert sig1 != sig2

    def test_token_not_expired_immediately(self):
        """Test that newly generated token is not expired."""
        provider = TokenProvider(self.test_key_name, self.test_key)
        token = provider.get_token("https://test.servicebus.windows.net/hc")
        
        assert not token.is_expired
