"""Separate unsupported mockgun behavior from emulated server rejections."""


class MockgunError(Exception):
    """Report mockgun setup failures and behavior the fake site cannot emulate."""


class ServerFaultError(Exception):
    """Represent a server rejection encoded as a JSON-RPC fault.

    Attributes:
        error_code: Optional server error code accompanying the message.
    """

    def __init__(self, message: str, error_code: int | None = None) -> None:
        """Initialize a rejection with its wire message and optional code."""
        super().__init__(message)
        self.error_code = error_code
