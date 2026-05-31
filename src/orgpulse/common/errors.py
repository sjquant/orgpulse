class OrgpulseError(RuntimeError):
    """Base error for orgpulse-specific failures."""


class AuthResolutionError(OrgpulseError):
    """Raised when GitHub credentials cannot be resolved or authenticated."""


class GitHubApiError(OrgpulseError):
    """Raised when GitHub returns a non-auth API failure."""


class OrgTargetingError(OrgpulseError):
    """Raised when the target organization is invalid or inaccessible."""


class AnalysisInputError(OrgpulseError):
    """Raised when local analysis inputs are missing or incompatible."""
