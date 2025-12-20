class AppError(Exception):
    """Base exception class for the application."""
    def __init__(self, message, status_code=500, payload=None):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['error'] = self.message
        return rv

class ValidationError(AppError):
    """Raised when input validation fails (400)."""
    def __init__(self, message, payload=None):
        super().__init__(message, status_code=400, payload=payload)

class ExternalServiceError(AppError):
    """Raised when an external service (e.g., Tradier) fails (503)."""
    def __init__(self, message, payload=None):
        super().__init__(message, status_code=503, payload=payload)

class ResourceNotFoundError(AppError):
    """Raised when a requested resource is not found (404)."""
    def __init__(self, message, payload=None):
        super().__init__(message, status_code=404, payload=payload)
