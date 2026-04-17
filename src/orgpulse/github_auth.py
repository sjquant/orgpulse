from __future__ import annotations

import subprocess
from typing import Callable

from github import Auth, Github, GithubException

from orgpulse.errors import AuthResolutionError, OrgTargetingError
from orgpulse.models import AuthSource, GitHubTargetContext, ResolvedToken, RunConfig

AUTH_REQUIRED_MESSAGE = (
    "GitHub authentication is required. Set GH_TOKEN or authenticate with `gh auth login`."
)


class GitHubAuthService:
    """Resolve GitHub credentials and validate org access for a run configuration."""

    def __init__(
        self,
        github_client_factory: Callable[[str], Github] | None = None,
    ) -> None:
        self._github_client_factory = (
            self._build_github_client if github_client_factory is None else github_client_factory
        )

    def validate_access(self, config: RunConfig) -> GitHubTargetContext:
        """Validate the current GitHub credentials and configured target organization."""
        resolved_token = self._resolve_auth_token(config)
        client = self._github_client_factory(resolved_token.token)
        viewer_login = self._get_viewer_login(client)
        organization_login = self._get_organization_login(client, config.org)
        return GitHubTargetContext(
            auth_source=resolved_token.source,
            viewer_login=viewer_login,
            organization_login=organization_login,
        )

    def _resolve_auth_token(self, config: RunConfig) -> ResolvedToken:
        """Resolve GitHub auth from RunConfig first, then fall back to GitHub CLI auth."""
        if config.github_token is not None:
            github_token = config.github_token.get_secret_value().strip()
            if github_token:
                return ResolvedToken(source=AuthSource.GH_TOKEN, token=github_token)

        gh_cli_token = self._read_gh_auth_token().strip()
        if not gh_cli_token:
            raise AuthResolutionError(AUTH_REQUIRED_MESSAGE)
        return ResolvedToken(source=AuthSource.GH_CLI, token=gh_cli_token)

    def _build_github_client(self, token: str) -> Github:
        """Create a PyGithub client from a resolved token."""
        return Github(auth=Auth.Token(token))

    def _get_viewer_login(self, client: Github) -> str:
        """Read the authenticated user login and normalize auth failures."""
        try:
            return client.get_user().login
        except GithubException as exc:
            if exc.status == 401:
                raise AuthResolutionError(
                    "GitHub authentication failed. The resolved credentials were rejected by the GitHub API."
                ) from exc
            raise AuthResolutionError(f"GitHub authentication failed: {exc.data}") from exc

    def _get_organization_login(self, client: Github, org: str) -> str:
        """Validate that the target organization is reachable with the current credentials."""
        try:
            return client.get_organization(org).login
        except GithubException as exc:
            raise self._build_org_targeting_error(org, exc) from exc

    def _build_org_targeting_error(self, org: str, exc: GithubException) -> OrgTargetingError:
        """Normalize GitHub org lookup failures into user-facing targeting errors."""
        if exc.status == 404:
            return OrgTargetingError(
                f"Target organization '{org}' was not found or is not accessible with the current GitHub credentials."
            )
        if exc.status in {401, 403}:
            return OrgTargetingError(
                f"Target organization '{org}' is not accessible with the current GitHub credentials."
            )
        return OrgTargetingError(f"Failed to validate target organization '{org}': {exc.data}")

    def _read_gh_auth_token(self) -> str:
        """Read the active GitHub CLI token for the current host."""
        try:
            result = subprocess.run(
                ["gh", "auth", "token"],
                capture_output=True,
                text=True,
                check=False,
            )
        except FileNotFoundError as exc:
            raise AuthResolutionError(AUTH_REQUIRED_MESSAGE) from exc

        if result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip()
            message = AUTH_REQUIRED_MESSAGE
            if detail:
                message = f"{message}\n{detail}"
            raise AuthResolutionError(message)

        token = result.stdout.strip()
        if not token:
            raise AuthResolutionError(AUTH_REQUIRED_MESSAGE)
        return token
