class OrgpulseError(RuntimeError):
    """Base error for orgpulse-specific failures."""


class AuthResolutionError(OrgpulseError):
    """Raised when GitHub credentials cannot be resolved or authenticated."""


class OrgTargetingError(OrgpulseError):
    """Raised when the target organization is invalid or inaccessible."""
