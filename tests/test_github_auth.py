from __future__ import annotations

import subprocess
from typing import Callable

import pytest
from github import GithubException

from orgpulse.errors import AuthResolutionError, OrgTargetingError
from orgpulse.github_auth import AUTH_REQUIRED_MESSAGE, GitHubAuthService, read_gh_auth_token
from orgpulse.models import AuthSource, RunConfig, RunMode


class FakeNamedObject:
    def __init__(self, login: str) -> None:
        """Store a fake GitHub login for lightweight auth tests."""
        self.login = login


class FakeGithubClient:
    def __init__(
        self,
        *,
        viewer_login: str = "maintainer",
        user_error: GithubException | None = None,
        org_error: GithubException | None = None,
    ) -> None:
        """Provide a deterministic PyGithub test double."""
        self.viewer_login = viewer_login
        self.user_error = user_error
        self.org_error = org_error

    def get_user(self) -> FakeNamedObject:
        """Return a fake authenticated user or raise the configured error."""
        if self.user_error is not None:
            raise self.user_error
        return FakeNamedObject(self.viewer_login)

    def get_organization(self, org: str) -> FakeNamedObject:
        """Return a fake organization or raise the configured error."""
        if self.org_error is not None:
            raise self.org_error
        return FakeNamedObject(org)


class TestGitHubAuthService:
    def test_prefers_gh_token_from_environment(self) -> None:
        """Resolve GH_TOKEN before attempting any GitHub CLI fallback."""
        # Given
        service = GitHubAuthService(env={"GH_TOKEN": "env-token"}, command_runner=lambda: "gh-token")

        # When
        resolved = service.resolve_auth_token()

        # Then
        assert resolved.source == AuthSource.GH_TOKEN
        assert resolved.token == "env-token"

    def test_falls_back_to_github_cli_when_env_is_missing(self) -> None:
        """Resolve credentials from `gh auth token` when GH_TOKEN is absent."""
        # Given
        service = GitHubAuthService(env={}, command_runner=lambda: "gh-token")

        # When
        resolved = service.resolve_auth_token()

        # Then
        assert resolved.source == AuthSource.GH_CLI
        assert resolved.token == "gh-token"

    def test_rejects_missing_auth_sources(self) -> None:
        """Reject authentication when neither GH_TOKEN nor GitHub CLI auth is available."""
        # Given
        service = GitHubAuthService(env={}, command_runner=lambda: "")

        # When
        with pytest.raises(AuthResolutionError, match="GitHub authentication is required"):
            service.resolve_auth_token()

        # Then

    def test_validates_target_organization_access(self) -> None:
        """Validate an accessible target organization with the resolved GitHub credentials."""
        # Given
        service = GitHubAuthService(
            env={"GH_TOKEN": "env-token"},
            github_factory=self.build_github_factory(),
        )

        # When
        context = service.validate_access(self.build_run_config())

        # Then
        assert context.auth_source == AuthSource.GH_TOKEN
        assert context.viewer_login == "maintainer"
        assert context.organization_login == "acme"

    def test_rejects_inaccessible_target_organization(self) -> None:
        """Reject target organizations that are not accessible to the resolved credentials."""
        # Given
        service = GitHubAuthService(
            env={"GH_TOKEN": "env-token"},
            github_factory=lambda _: FakeGithubClient(
                org_error=GithubException(404, {"message": "Not Found"}, None)
            ),
        )

        # When
        with pytest.raises(OrgTargetingError, match="Target organization 'acme' was not found"):
            service.validate_access(self.build_run_config())

        # Then

    def test_rejects_invalid_resolved_token(self) -> None:
        """Reject resolved credentials when the GitHub API returns 401 for the viewer lookup."""
        # Given
        service = GitHubAuthService(
            env={"GH_TOKEN": "env-token"},
            github_factory=lambda _: FakeGithubClient(
                user_error=GithubException(401, {"message": "Bad credentials"}, None)
            ),
        )

        # When
        with pytest.raises(AuthResolutionError, match="resolved credentials were rejected"):
            service.validate_access(self.build_run_config())

        # Then

    def test_rejects_forbidden_target_organization(self) -> None:
        """Reject target organizations that return a forbidden access error."""
        # Given
        service = GitHubAuthService(
            env={"GH_TOKEN": "env-token"},
            github_factory=lambda _: FakeGithubClient(
                org_error=GithubException(403, {"message": "Forbidden"}, None)
            ),
        )

        # When
        with pytest.raises(OrgTargetingError, match="is not accessible"):
            service.validate_access(self.build_run_config())

        # Then

    def build_github_factory(self) -> Callable[[str], FakeGithubClient]:
        """Build a deterministic GitHub client factory for successful auth tests."""

        def github_factory(token: str) -> FakeGithubClient:
            assert token == "env-token"
            return FakeGithubClient()

        return github_factory

    def build_run_config(self) -> RunConfig:
        """Build the minimal run configuration needed for auth validation tests."""
        return RunConfig(org="acme", mode=RunMode.INCREMENTAL)


class TestReadGhAuthToken:
    def test_rejects_missing_gh_binary(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Reject GitHub CLI auth resolution when the gh binary is not installed."""
        # Given
        def raise_file_not_found(*args, **kwargs) -> subprocess.CompletedProcess[str]:
            raise FileNotFoundError()

        monkeypatch.setattr("orgpulse.github_auth.subprocess.run", raise_file_not_found)

        # When
        with pytest.raises(AuthResolutionError, match=AUTH_REQUIRED_MESSAGE):
            read_gh_auth_token()

        # Then

    def test_rejects_gh_cli_failures_with_stderr_detail(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Reject GitHub CLI auth resolution when `gh auth token` returns an error."""
        # Given
        monkeypatch.setattr(
            "orgpulse.github_auth.subprocess.run",
            lambda *args, **kwargs: subprocess.CompletedProcess(
                args=["gh", "auth", "token"],
                returncode=1,
                stdout="",
                stderr="authentication required",
            ),
        )

        # When
        with pytest.raises(AuthResolutionError, match="authentication required"):
            read_gh_auth_token()

        # Then
