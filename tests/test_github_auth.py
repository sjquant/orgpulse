from __future__ import annotations

import subprocess

import pytest
from github import GithubException

from orgpulse.errors import AuthResolutionError, OrgTargetingError
from orgpulse.github_auth import AUTH_REQUIRED_MESSAGE, GitHubAuthService
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
    def test_prefers_github_token_from_config(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Resolve GitHub credentials from RunConfig before attempting GitHub CLI fallback."""
        # Given
        service = GitHubAuthService()
        config = self.build_run_config(github_token="env-token")

        def fail_if_called(self) -> str:
            raise AssertionError("expected config.github_token to take precedence")

        monkeypatch.setattr(GitHubAuthService, "_read_gh_auth_token", fail_if_called)

        # When
        resolved = service._resolve_auth_token(config)

        # Then
        assert resolved.source == AuthSource.GH_TOKEN
        assert resolved.token == "env-token"

    def test_falls_back_to_github_cli_when_config_token_is_missing(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Resolve credentials from `gh auth token` when RunConfig does not provide a token."""
        # Given
        service = GitHubAuthService()
        config = self.build_run_config()
        monkeypatch.setattr(GitHubAuthService, "_read_gh_auth_token", lambda self: "gh-token")

        # When
        resolved = service._resolve_auth_token(config)

        # Then
        assert resolved.source == AuthSource.GH_CLI
        assert resolved.token == "gh-token"

    def test_rejects_missing_auth_sources(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Reject authentication when neither RunConfig nor GitHub CLI provides a token."""
        # Given
        service = GitHubAuthService()
        config = self.build_run_config()
        monkeypatch.setattr(GitHubAuthService, "_read_gh_auth_token", lambda self: "")

        # When
        with pytest.raises(AuthResolutionError, match="GitHub authentication is required"):
            service._resolve_auth_token(config)

        # Then

    def test_validates_target_organization_access(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Validate an accessible target organization with the resolved GitHub credentials."""
        # Given
        service = GitHubAuthService()
        config = self.build_run_config(github_token="env-token")
        monkeypatch.setattr(GitHubAuthService, "_create_github_client", self.build_github_client)

        # When
        context = service.validate_access(config)

        # Then
        assert context.auth_source == AuthSource.GH_TOKEN
        assert context.viewer_login == "maintainer"
        assert context.organization_login == "acme"

    def test_rejects_inaccessible_target_organization(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Reject target organizations that are not accessible to the resolved credentials."""
        # Given
        service = GitHubAuthService()
        config = self.build_run_config(github_token="env-token")
        monkeypatch.setattr(
            GitHubAuthService,
            "_create_github_client",
            lambda self, _: FakeGithubClient(org_error=GithubException(404, {"message": "Not Found"}, None)),
        )

        # When
        with pytest.raises(OrgTargetingError, match="Target organization 'acme' was not found"):
            service.validate_access(config)

        # Then

    def test_rejects_invalid_resolved_token(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Reject resolved credentials when the GitHub API returns 401 for the viewer lookup."""
        # Given
        service = GitHubAuthService()
        config = self.build_run_config(github_token="env-token")
        monkeypatch.setattr(
            GitHubAuthService,
            "_create_github_client",
            lambda self, _: FakeGithubClient(user_error=GithubException(401, {"message": "Bad credentials"}, None)),
        )

        # When
        with pytest.raises(AuthResolutionError, match="resolved credentials were rejected"):
            service.validate_access(config)

        # Then

    def test_rejects_forbidden_target_organization(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Reject target organizations that return a forbidden access error."""
        # Given
        service = GitHubAuthService()
        config = self.build_run_config(github_token="env-token")
        monkeypatch.setattr(
            GitHubAuthService,
            "_create_github_client",
            lambda self, _: FakeGithubClient(org_error=GithubException(403, {"message": "Forbidden"}, None)),
        )

        # When
        with pytest.raises(OrgTargetingError, match="is not accessible"):
            service.validate_access(config)

        # Then

    def build_github_client(self, token: str) -> FakeGithubClient:
        """Build a deterministic GitHub client for successful auth tests."""
        assert token == "env-token"
        return FakeGithubClient()

    def build_run_config(self, **overrides: object) -> RunConfig:
        """Build the minimal run configuration needed for auth validation tests."""
        return RunConfig(org="acme", mode=RunMode.INCREMENTAL, **overrides)


class TestReadGhAuthToken:
    def test_rejects_missing_gh_binary(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Reject GitHub CLI auth resolution when the gh binary is not installed."""
        # Given
        service = GitHubAuthService()

        def raise_file_not_found(*args, **kwargs) -> subprocess.CompletedProcess[str]:
            raise FileNotFoundError()

        monkeypatch.setattr("orgpulse.github_auth.subprocess.run", raise_file_not_found)

        # When
        with pytest.raises(AuthResolutionError, match=AUTH_REQUIRED_MESSAGE):
            service._read_gh_auth_token()

        # Then

    def test_rejects_gh_cli_failures_with_stderr_detail(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Reject GitHub CLI auth resolution when `gh auth token` returns an error."""
        # Given
        service = GitHubAuthService()
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
            service._read_gh_auth_token()

        # Then
