"""
aapi.exceptions
~~~~~~~~~~~~~~~
"""


class InternalError(RuntimeError):
    """Internal exception."""
    status_code = 500


class RequestError(RuntimeError):
    """Invalid request."""
    status_code = 400


class LoginError(RequestError):
    """Invalid login info."""
