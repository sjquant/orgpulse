class OrgpulseError(RuntimeError):
    """Base error for user-facing orgpulse failures."""


class ConfigError(OrgpulseError):
    """Raised when the orgpulse config file is missing or invalid."""


class GitHubError(OrgpulseError):
    """Raised when GitHub credentials or API calls fail."""
