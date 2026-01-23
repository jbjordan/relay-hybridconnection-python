"""Unit tests for connection string parser.

Based on Azure Relay .NET SDK tests:
https://github.com/Azure/azure-relay-dotnet/blob/dev/test/Microsoft.Azure.Relay.UnitTests/ConnectionStringBuilderTests.cs
"""

import secrets
import string

import pytest
from src.hybrid_connection.connection_string import RelayConnectionStringBuilder


def generate_random_sas_key(length: int = 44) -> str:
    """Generate a random SAS key similar to Azure's base64-encoded keys."""
    alphabet = string.ascii_letters + string.digits + "+/"
    key = "".join(secrets.choice(alphabet) for _ in range(length - 1))
    return key + "="


class TestConnectionStringBuilder:
    """Tests for RelayConnectionStringBuilder class.
    
    Test fixture setup mirrors the C# test class pattern with instance variables
    for endpoint, sasKeyName, entityPath, and sasKeyValue.
    """

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures similar to C# constructor initialization."""
        self.endpoint = "sb://contoso.servicebus.windows.net/"
        self.sas_key_name = "RootManageSharedAccessKey"
        self.entity_path = "hc1"
        self.sas_key_value = generate_random_sas_key()

    def test_connection_string_builder_operation_validation(self):
        """Test creating connection string using RelayConnectionStringBuilder properties."""
        # Create a new connection string using RelayConnectionStringBuilder properties
        builder = RelayConnectionStringBuilder()
        builder.endpoint = self.endpoint
        builder.entity_path = self.entity_path
        builder.shared_access_key_name = self.sas_key_name
        builder.shared_access_key = self.sas_key_value

        connection_string = str(builder)

        # Endpoint is expected to appear first in the connection string
        assert connection_string.startswith("Endpoint=")
        assert f"Endpoint={self.endpoint}" in connection_string
        assert f"EntityPath={self.entity_path}" in connection_string
        assert f"SharedAccessKeyName={self.sas_key_name}" in connection_string
        assert f"SharedAccessKey={self.sas_key_value}" in connection_string

    def test_create_connection_string_builder_from_connection_string(self):
        """Test parsing a connection string with the constructor."""
        # Build a connection string first
        original_builder = RelayConnectionStringBuilder()
        original_builder.endpoint = self.endpoint
        original_builder.entity_path = self.entity_path
        original_builder.shared_access_key_name = self.sas_key_name
        original_builder.shared_access_key = self.sas_key_value

        connection_string = str(original_builder)

        # Use constructor to parse the created connection string
        parsed_builder = RelayConnectionStringBuilder(connection_string)

        assert parsed_builder.endpoint == self.endpoint
        assert parsed_builder.entity_path == self.entity_path
        assert parsed_builder.shared_access_key_name == self.sas_key_name
        assert parsed_builder.shared_access_key == self.sas_key_value

    def test_to_string_with_sas_key_name_but_no_key_value_raises(self):
        """Test that ToString with SAS KeyName but no KeyValue raises exception."""
        builder = RelayConnectionStringBuilder()
        builder.endpoint = self.endpoint
        builder.shared_access_key_name = self.sas_key_name
        builder.shared_access_key = None

        with pytest.raises((ValueError, ArgumentError)):
            str(builder)

    def test_to_string_with_sas_key_value_but_no_key_name_raises(self):
        """Test that ToString with SAS KeyValue but no KeyName raises exception."""
        builder = RelayConnectionStringBuilder()
        builder.endpoint = self.endpoint
        builder.shared_access_key_name = None
        builder.shared_access_key = self.sas_key_value

        with pytest.raises((ValueError, ArgumentError)):
            str(builder)

    def test_to_string_with_no_properties_raises(self):
        """Test that ToString with no properties set raises exception."""
        builder = RelayConnectionStringBuilder()

        with pytest.raises((ValueError, ArgumentError)):
            str(builder)

    def test_to_string_with_only_endpoint(self):
        """Test that ToString with only Endpoint set works."""
        builder = RelayConnectionStringBuilder()
        builder.endpoint = self.endpoint

        connection_string = str(builder)

        assert f"Endpoint={self.endpoint}" in connection_string

    def test_host_name_property(self):
        """Test that host_name extracts the hostname from endpoint."""
        builder = RelayConnectionStringBuilder()
        builder.endpoint = self.endpoint
        builder.entity_path = self.entity_path
        builder.shared_access_key_name = self.sas_key_name
        builder.shared_access_key = self.sas_key_value

        assert builder.host_name == "contoso.servicebus.windows.net"

    def test_build_uri(self):
        """Test building the full hybrid connection URI."""
        builder = RelayConnectionStringBuilder()
        builder.endpoint = self.endpoint
        builder.entity_path = self.entity_path
        builder.shared_access_key_name = self.sas_key_name
        builder.shared_access_key = self.sas_key_value

        uri = builder.build_uri()

        assert uri == "sb://contoso.servicebus.windows.net/hc1"

    def test_build_uri_without_endpoint_raises(self):
        """Test that build_uri without endpoint raises ValueError."""
        builder = RelayConnectionStringBuilder()
        builder.entity_path = self.entity_path

        with pytest.raises(ValueError) as exc_info:
            builder.build_uri()

        assert "Endpoint" in str(exc_info.value)

    def test_build_uri_without_entity_path_raises(self):
        """Test that build_uri without entity_path raises ValueError."""
        builder = RelayConnectionStringBuilder()
        builder.endpoint = self.endpoint

        with pytest.raises(ValueError) as exc_info:
            builder.build_uri()

        assert "EntityPath" in str(exc_info.value)


class TestConnectionStringBuilderParsing:
    """Tests for connection string parsing edge cases."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        self.endpoint = "sb://contoso.servicebus.windows.net/"
        self.sas_key_name = "RootManageSharedAccessKey"
        self.entity_path = "hc1"
        self.sas_key_value = generate_random_sas_key()

    def test_parse_connection_string_without_entity_path(self):
        """Test parsing a connection string without EntityPath (optional)."""
        conn_str = (
            f"Endpoint={self.endpoint};"
            f"SharedAccessKeyName={self.sas_key_name};"
            f"SharedAccessKey={self.sas_key_value}"
        )

        builder = RelayConnectionStringBuilder(conn_str)

        assert builder.endpoint == self.endpoint
        assert builder.shared_access_key_name == self.sas_key_name
        assert builder.shared_access_key == self.sas_key_value
        assert builder.entity_path is None

    def test_parse_connection_string_missing_endpoint_raises(self):
        """Test that missing Endpoint raises ValueError."""
        conn_str = (
            f"SharedAccessKeyName={self.sas_key_name};"
            f"SharedAccessKey={self.sas_key_value}"
        )

        with pytest.raises(ValueError) as exc_info:
            RelayConnectionStringBuilder(conn_str)

        assert "Endpoint" in str(exc_info.value)

    def test_parse_connection_string_missing_shared_access_key_name_raises(self):
        """Test that missing SharedAccessKeyName raises ValueError."""
        conn_str = (
            f"Endpoint={self.endpoint};"
            f"SharedAccessKey={self.sas_key_value}"
        )

        with pytest.raises(ValueError) as exc_info:
            RelayConnectionStringBuilder(conn_str)

        assert "SharedAccessKeyName" in str(exc_info.value)

    def test_parse_connection_string_missing_shared_access_key_raises(self):
        """Test that missing SharedAccessKey raises ValueError."""
        conn_str = (
            f"Endpoint={self.endpoint};"
            f"SharedAccessKeyName={self.sas_key_name}"
        )

        with pytest.raises(ValueError) as exc_info:
            RelayConnectionStringBuilder(conn_str)

        assert "SharedAccessKey" in str(exc_info.value)

    def test_parse_empty_connection_string_raises(self):
        """Test that empty connection string raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            RelayConnectionStringBuilder("")

        assert "empty" in str(exc_info.value).lower() or "missing" in str(exc_info.value).lower()

    def test_parse_invalid_endpoint_url_raises(self):
        """Test that invalid endpoint URL raises ValueError."""
        conn_str = (
            f"Endpoint=not-a-valid-url;"
            f"SharedAccessKeyName={self.sas_key_name};"
            f"SharedAccessKey={self.sas_key_value}"
        )

        with pytest.raises(ValueError) as exc_info:
            RelayConnectionStringBuilder(conn_str)

        assert "Invalid endpoint" in str(exc_info.value)

    def test_parse_connection_string_with_special_characters_in_key(self):
        """Test parsing connection string with base64 special characters in key."""
        # Generate a key with guaranteed base64 special characters (+, /, =)
        special_key = generate_random_sas_key()[:20] + "+/=" + generate_random_sas_key()[:20] + "="
        conn_str = (
            f"Endpoint={self.endpoint};"
            f"SharedAccessKeyName={self.sas_key_name};"
            f"SharedAccessKey={special_key};"
            f"EntityPath={self.entity_path}"
        )

        builder = RelayConnectionStringBuilder(conn_str)

        assert builder.shared_access_key == special_key

    def test_parse_connection_string_with_extra_semicolons(self):
        """Test parsing connection string with extra semicolons."""
        conn_str = (
            f"Endpoint={self.endpoint};;"
            f"SharedAccessKeyName={self.sas_key_name};"
            f"SharedAccessKey={self.sas_key_value};"
        )

        builder = RelayConnectionStringBuilder(conn_str)

        assert builder.endpoint == self.endpoint
        assert builder.shared_access_key_name == self.sas_key_name
        assert builder.shared_access_key == self.sas_key_value


# Allow ArgumentError as an alias for tests (not used in current implementation)
ArgumentError = Exception
