class AuthResolutionError(RuntimeError):
    """Raised when GitHub credentials cannot be resolved or authenticated."""


class OrgTargetingError(RuntimeError):
    """Raised when the target organization is invalid or inaccessible."""
