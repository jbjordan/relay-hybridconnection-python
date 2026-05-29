"""Connection string parser for Azure Relay Hybrid Connections."""

from typing import Dict, Optional
from urllib.parse import urlparse


class RelayConnectionStringBuilder:
    """Parses and validates Azure Relay connection strings."""

    ENDPOINT_KEY = "Endpoint"
    SHARED_ACCESS_KEY_NAME_KEY = "SharedAccessKeyName"
    SHARED_ACCESS_KEY_KEY = "SharedAccessKey"
    ENTITY_PATH_KEY = "EntityPath"

    # Endpoint is the only field required to construct a valid connection
    # string. SharedAccessKeyName/SharedAccessKey are optional: anonymous
    # senders for Hybrid Connections may omit them.
    REQUIRED_KEYS = [ENDPOINT_KEY]

    def __init__(self, connection_string: Optional[str] = None):
        """Initialize the connection string builder.
        
        Args:
            connection_string: Optional connection string to parse.
        """
        self._endpoint: Optional[str] = None
        self._shared_access_key_name: Optional[str] = None
        self._shared_access_key: Optional[str] = None
        self._entity_path: Optional[str] = None

        if connection_string is not None:
            self._parse(connection_string)

    def _parse(self, connection_string: str) -> None:
        """Parse a connection string into its components.
        
        Args:
            connection_string: The connection string to parse.
            
        Raises:
            ValueError: If the connection string is invalid or missing required fields.
        """
        if not connection_string:
            raise ValueError("Connection string cannot be empty")

        parts = self._parse_connection_string(connection_string)

        # Validate required fields
        missing_keys = [key for key in self.REQUIRED_KEYS if key not in parts]
        if missing_keys:
            raise ValueError(f"Connection string is missing required fields: {', '.join(missing_keys)}")

        # SAS key name and value must either both be present or both be
        # absent. A name without a value (or vice versa) is invalid.
        has_key_name = self.SHARED_ACCESS_KEY_NAME_KEY in parts
        has_key_value = self.SHARED_ACCESS_KEY_KEY in parts
        if has_key_name != has_key_value:
            raise ValueError(
                "Connection string must specify both SharedAccessKeyName and "
                "SharedAccessKey, or neither (for anonymous senders)"
            )

        self._endpoint = parts.get(self.ENDPOINT_KEY)
        self._shared_access_key_name = parts.get(self.SHARED_ACCESS_KEY_NAME_KEY)
        self._shared_access_key = parts.get(self.SHARED_ACCESS_KEY_KEY)
        self._entity_path = parts.get(self.ENTITY_PATH_KEY)

        # Validate endpoint format
        if self._endpoint:
            self._validate_endpoint(self._endpoint)

    def _parse_connection_string(self, connection_string: str) -> Dict[str, str]:
        """Parse connection string into key-value pairs.
        
        Args:
            connection_string: The connection string to parse.
            
        Returns:
            Dictionary of key-value pairs.
        """
        result = {}
        parts = connection_string.split(";")

        for part in parts:
            part = part.strip()
            if not part:
                continue

            # Find the first '=' to split key and value
            eq_index = part.find("=")
            if eq_index == -1:
                continue

            key = part[:eq_index].strip()
            value = part[eq_index + 1:].strip()

            if key and value:
                result[key] = value

        return result

    def _validate_endpoint(self, endpoint: str) -> None:
        """Validate the endpoint URL format.
        
        Args:
            endpoint: The endpoint URL to validate.
            
        Raises:
            ValueError: If the endpoint is not a valid URL.
        """
        parsed = urlparse(endpoint)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError(f"Invalid endpoint URL: {endpoint}")

    @property
    def endpoint(self) -> Optional[str]:
        """Get the relay endpoint URL."""
        return self._endpoint

    @endpoint.setter
    def endpoint(self, value: str) -> None:
        """Set the relay endpoint URL."""
        if value:
            self._validate_endpoint(value)
        self._endpoint = value

    @property
    def shared_access_key_name(self) -> Optional[str]:
        """Get the shared access key name."""
        return self._shared_access_key_name

    @shared_access_key_name.setter
    def shared_access_key_name(self, value: str) -> None:
        """Set the shared access key name."""
        self._shared_access_key_name = value

    @property
    def shared_access_key(self) -> Optional[str]:
        """Get the shared access key."""
        return self._shared_access_key

    @shared_access_key.setter
    def shared_access_key(self, value: str) -> None:
        """Set the shared access key."""
        self._shared_access_key = value

    @property
    def entity_path(self) -> Optional[str]:
        """Get the entity path (hybrid connection name)."""
        return self._entity_path

    @entity_path.setter
    def entity_path(self, value: str) -> None:
        """Set the entity path (hybrid connection name)."""
        self._entity_path = value

    @property
    def host_name(self) -> Optional[str]:
        """Get the host name from the endpoint."""
        if self._endpoint:
            parsed = urlparse(self._endpoint)
            return parsed.netloc
        return None

    def build_uri(self) -> str:
        """Build the full hybrid connection URI.
        
        Returns:
            The full URI for the hybrid connection.
            
        Raises:
            ValueError: If endpoint or entity_path is not set.
        """
        if not self._endpoint:
            raise ValueError("Endpoint is required to build URI")
        if not self._entity_path:
            raise ValueError("EntityPath is required to build URI")

        # Remove trailing slash from endpoint if present
        endpoint = self._endpoint.rstrip("/")
        return f"{endpoint}/{self._entity_path}"

    def __str__(self) -> str:
        """Build the connection string from current values.
        
        Returns:
            The connection string.
            
        Raises:
            ValueError: If no properties are set, or if SAS key name/value mismatch.
        """
        # Validate that at least endpoint is set
        if not self._endpoint:
            raise ValueError("Endpoint is required to build connection string")

        # Validate SAS key name and value consistency
        if self._shared_access_key_name and not self._shared_access_key:
            raise ValueError("SharedAccessKey is required when SharedAccessKeyName is specified")
        if self._shared_access_key and not self._shared_access_key_name:
            raise ValueError("SharedAccessKeyName is required when SharedAccessKey is specified")

        parts = []

        if self._endpoint:
            parts.append(f"{self.ENDPOINT_KEY}={self._endpoint}")
        if self._shared_access_key_name:
            parts.append(f"{self.SHARED_ACCESS_KEY_NAME_KEY}={self._shared_access_key_name}")
        if self._shared_access_key:
            parts.append(f"{self.SHARED_ACCESS_KEY_KEY}={self._shared_access_key}")
        if self._entity_path:
            parts.append(f"{self.ENTITY_PATH_KEY}={self._entity_path}")

        return ";".join(parts)
