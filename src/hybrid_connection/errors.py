"""Stable Relay exceptions and credential-safe diagnostics."""

import re
from websockets.exceptions import ConnectionClosed, InvalidHandshake


def redact(value: object) -> str:
    """Remove credentials from exception text before exposing or logging it."""
    text = str(value)
    text = re.sub(r"(?i)(sb-hc-token=)[^&\s]+", r"\1<redacted>", text)
    return re.sub(
        r"(?i)(SharedAccessSignature\s+|Bearer\s+)[^\r\n]+",
        r"\1<redacted>", text,
    )


class RelayError(ConnectionError):
    def __init__(self, message, *, is_transient=True, status_code=None,
                 close_code=None, tracking_id=None):
        super().__init__(redact(message))
        self.is_transient = is_transient
        self.status_code = status_code
        self.close_code = close_code
        self.tracking_id = tracking_id


class AuthorizationError(RelayError):
    pass


class EndpointNotFoundError(RelayError):
    pass


class ServerBusyError(RelayError):
    pass


class RelayTimeoutError(RelayError, TimeoutError):
    pass


class ProtocolError(RelayError):
    def __init__(self, message, **kwargs):
        super().__init__(message, is_transient=False, **kwargs)


def relay_error(error, *, tracking_id=None, status_code=None, listener=False):
    if isinstance(error, RelayError):
        return error
    response = getattr(error, "response", None)
    status = status_code or getattr(response, "status_code", None)
    received = error.rcvd if isinstance(error, ConnectionClosed) else None
    close_code = received.code if received else None
    text = redact(error) or type(error).__name__
    reason = getattr(response, "reason_phrase", None)
    if isinstance(reason, str) and reason:
        text += ": " + redact(reason)
    match = re.search(r"(?i)TrackingId[:=]\s*([\w.-]+)", text)
    tracking_id = match.group(1) if match else tracking_id
    kind = RelayError
    transient = True
    if status in (401, 403):
        kind, transient = AuthorizationError, False
    elif status in (204, 404):
        kind, transient = EndpointNotFoundError, not listener
    elif status in (429, 503):
        kind = ServerBusyError
    elif status in (408, 504) or isinstance(error, TimeoutError):
        kind = RelayTimeoutError
    elif status is not None:
        transient = status >= 500
    elif isinstance(error, (ValueError, TypeError, InvalidHandshake)):
        transient = False
    # 1008 can mean an expired token: reconnect with a newly acquired token.
    return kind(text, is_transient=transient, status_code=status,
                close_code=close_code, tracking_id=tracking_id)
