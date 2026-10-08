"""Stable client-side administration errors."""


class AdminError(Exception):
    """Base error for failures reported by ``adxadmin``."""


class InvalidInput(AdminError):
    """A local argument, endpoint or credential is invalid."""


class ApiError(AdminError):
    """A structured non-success response returned by ADX."""

    def __init__(
        self,
        status: int,
        message: str,
        *,
        code: str = "UNKNOWN",
        retry: str = "UNKNOWN",
        outcome: str = "UNKNOWN",
        request_id: str = "unknown",
    ) -> None:
        self.status = status
        self.message = message
        self.code = code
        self.retry = retry
        self.outcome = outcome
        self.request_id = request_id
        super().__init__(
            f"ADX returned HTTP {status}: {message} "
            f"[code={code}, retry={retry}, outcome={outcome}, request_id={request_id}]"
        )
