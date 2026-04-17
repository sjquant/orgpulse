from __future__ import annotations

import subprocess
from types import SimpleNamespace
from unittest.mock import create_autospec

import pytest
from github import Github, GithubException

from orgpulse.errors import AuthResolutionError, GitHubApiError, OrgTargetingError
from orgpulse.github_auth import AUTH_REQUIRED_MESSAGE, GitHubAuthService, resolve_auth_token
from orgpulse.models import AuthSource, RunConfig, RunMode


class TestGitHubAuthService:
    def test_prefers_github_token_from_config(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Resolve GitHub credentials from RunConfig before attempting GitHub CLI fallback."""
        # Given
        client = self.build_github_client()
        config = self.build_run_config(github_token="env-token")
        service = GitHubAuthService(client, AuthSource.GH_TOKEN)

        def fail_if_called(*args, **kwargs) -> subprocess.CompletedProcess[str]:
            raise AssertionError("expected config.github_token to take precedence")

        monkeypatch.setattr("orgpulse.github_auth.subprocess.run", fail_if_called)

        # When
        resolved = resolve_auth_token(config)
        context = service.validate_access(config)

        # Then
        assert resolved.source == AuthSource.GH_TOKEN
        assert context.auth_source == AuthSource.GH_TOKEN
        assert context.viewer_login == "maintainer"
        assert context.organization_login == "acme"

    def test_falls_back_to_github_cli_when_config_token_is_missing(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Resolve credentials from `gh auth token` when RunConfig does not provide a token."""
        # Given
        client = self.build_github_client()
        config = self.build_run_config()
        service = GitHubAuthService(client, AuthSource.GH_CLI)
        monkeypatch.setattr(
            "orgpulse.github_auth.subprocess.run",
            lambda *args, **kwargs: subprocess.CompletedProcess(
                args=["gh", "auth", "token"],
                returncode=0,
                stdout="gh-token\n",
                stderr="",
            ),
        )

        # When
        resolved = resolve_auth_token(config)
        context = service.validate_access(config)

        # Then
        assert resolved.source == AuthSource.GH_CLI
        assert context.auth_source == AuthSource.GH_CLI
        assert context.viewer_login == "maintainer"
        assert context.organization_login == "acme"

    def test_rejects_missing_auth_sources(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Reject authentication when neither RunConfig nor GitHub CLI provides a token."""
        # Given
        config = self.build_run_config()
        monkeypatch.setattr(
            "orgpulse.github_auth.subprocess.run",
            lambda *args, **kwargs: subprocess.CompletedProcess(
                args=["gh", "auth", "token"],
                returncode=0,
                stdout="",
                stderr="",
            ),
        )

        # When
        with pytest.raises(AuthResolutionError, match="GitHub authentication is required"):
            resolve_auth_token(config)

        # Then

    def test_validates_target_organization_access(self) -> None:
        """Validate an accessible target organization with the resolved GitHub credentials."""
        # Given
        client = self.build_github_client()
        config = self.build_run_config(github_token="env-token")
        service = GitHubAuthService(client, AuthSource.GH_TOKEN)

        # When
        context = service.validate_access(config)

        # Then
        assert context.auth_source == AuthSource.GH_TOKEN
        assert context.viewer_login == "maintainer"
        assert context.organization_login == "acme"

    def test_rejects_inaccessible_target_organization(self) -> None:
        """Reject target organizations that are not accessible to the resolved credentials."""
        # Given
        client = self.build_github_client()
        client.get_organization.side_effect = GithubException(404, {"message": "Not Found"}, None)
        config = self.build_run_config(github_token="env-token")
        service = GitHubAuthService(client, AuthSource.GH_TOKEN)

        # When
        with pytest.raises(OrgTargetingError, match="Target organization 'acme' was not found"):
            service.validate_access(config)

        # Then

    def test_rejects_invalid_resolved_token(self) -> None:
        """Reject resolved credentials when the GitHub API returns 401 for the viewer lookup."""
        # Given
        client = self.build_github_client()
        client.get_user.side_effect = GithubException(401, {"message": "Bad credentials"}, None)
        config = self.build_run_config(github_token="env-token")
        service = GitHubAuthService(client, AuthSource.GH_TOKEN)

        # When
        with pytest.raises(AuthResolutionError, match="resolved credentials were rejected"):
            service.validate_access(config)

        # Then

    def test_surfaces_non_auth_viewer_lookup_failures_as_github_api_errors(self) -> None:
        """Surface non-401 viewer lookup failures as generic GitHub API errors."""
        # Given
        client = self.build_github_client()
        client.get_user.side_effect = GithubException(500, {"message": "Server Error"}, None)
        config = self.build_run_config(github_token="env-token")
        service = GitHubAuthService(client, AuthSource.GH_TOKEN)

        # When
        with pytest.raises(GitHubApiError, match="GitHub API request failed"):
            service.validate_access(config)

        # Then

    def test_rejects_forbidden_target_organization(self) -> None:
        """Reject target organizations that return a forbidden access error."""
        # Given
        client = self.build_github_client()
        client.get_organization.side_effect = GithubException(403, {"message": "Forbidden"}, None)
        config = self.build_run_config(github_token="env-token")
        service = GitHubAuthService(client, AuthSource.GH_TOKEN)

        # When
        with pytest.raises(OrgTargetingError, match="is not accessible"):
            service.validate_access(config)

        # Then

    def test_rejects_missing_gh_binary(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Reject GitHub CLI auth resolution when the gh binary is not installed."""
        # Given
        config = self.build_run_config()

        def raise_file_not_found(*args, **kwargs) -> subprocess.CompletedProcess[str]:
            raise FileNotFoundError()

        monkeypatch.setattr("orgpulse.github_auth.subprocess.run", raise_file_not_found)

        # When
        with pytest.raises(AuthResolutionError, match=AUTH_REQUIRED_MESSAGE):
            resolve_auth_token(config)

        # Then

    def test_rejects_gh_cli_failures_with_stderr_detail(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Reject GitHub CLI auth resolution when `gh auth token` returns an error."""
        # Given
        config = self.build_run_config()
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
            resolve_auth_token(config)

        # Then

    def build_github_client(self) -> Github:
        """Build an autospecced GitHub client for auth service tests."""
        client = create_autospec(Github, instance=True, spec_set=True)
        client.get_user.return_value = SimpleNamespace(login="maintainer")
        client.get_organization.return_value = SimpleNamespace(login="acme")
        return client

    def build_run_config(self, **overrides: object) -> RunConfig:
        """Build the minimal run configuration needed for auth validation tests."""
        return RunConfig(org="acme", mode=RunMode.INCREMENTAL, **overrides)
