from typing import Any, Dict


class APIError(Exception):
    def __init__(self, status: int, code: str, message: str, details: Any = None):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.details = details

    def body(self) -> Dict[str, Any]:
        error = {"code": self.code, "message": self.message}
        if self.details is not None:
            error["details"] = self.details
        return {"error": error}


class UpstreamError(Exception):
    def __init__(
        self,
        code: str,
        message: str,
        status: int = 502,
        retryable: bool = False,
        uncertain: bool = False,
        retry_after: float = 0,
    ):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status
        self.retryable = retryable
        self.uncertain = uncertain
        self.retry_after = retry_after

    def body(self) -> Dict[str, Any]:
        return {"code": self.code, "message": self.message}
