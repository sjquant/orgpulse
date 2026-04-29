from __future__ import annotations

from calendar import monthrange
from datetime import date, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Any, Callable

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    StringConstraints,
    ValidationInfo,
    computed_field,
    field_validator,
    model_validator,
)

ORG_PATTERN = r"^[A-Za-z0-9](?:[A-Za-z0-9-]{0,38}[A-Za-z0-9])?$"
REPO_PATTERN = r"^(?:[A-Za-z0-9_.-]+/)?[A-Za-z0-9_.-]+$"

OrgSlug = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1, pattern=ORG_PATTERN)
]
RepoSlug = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1, pattern=REPO_PATTERN)
]


class PeriodGrain(StrEnum):
    """Enumerate supported reporting grains."""

    WEEK = "week"
    MONTH = "month"

    def start_for(self, value: date) -> date:
        if self is PeriodGrain.MONTH:
            return value.replace(day=1)
        return value - timedelta(days=value.weekday())

    def end_for(self, value: date) -> date:
        start_date = self.start_for(value)
        if self is PeriodGrain.MONTH:
            return _month_end(start_date)
        return start_date + timedelta(days=6)

    def is_period_start(self, value: date) -> bool:
        return value == self.start_for(value)

    def is_period_end(self, value: date) -> bool:
        return value == self.end_for(value)

    def key_for(self, value: date) -> str:
        if self is PeriodGrain.MONTH:
            return _period_key_for_month(value)
        return _period_key_for_week(value)

    def start_for_key(self, key: str) -> date:
        if self is PeriodGrain.MONTH:
            return _month_start_for_period_key(key)
        return _week_start_for_period_key(key)

    def count_periods(self, start_date: date, end_date: date) -> int:
        if self is PeriodGrain.MONTH:
            return _count_periods(start_date, end_date, _next_month_start)
        return _count_periods(start_date, end_date, _next_week_start)


class RunMode(StrEnum):
    """Enumerate supported collection strategies."""

    FULL = "full"
    INCREMENTAL = "incremental"
    BACKFILL = "backfill"


class TimeAnchor(StrEnum):
    """Enumerate pull request timestamps that can anchor reporting."""

    CREATED_AT = "created_at"
    UPDATED_AT = "updated_at"
    MERGED_AT = "merged_at"

    def pull_request_datetime(
        self,
        pull_request: "PullRequestRecord",
    ) -> datetime | None:
        if self is TimeAnchor.CREATED_AT:
            return pull_request.created_at
        if self is TimeAnchor.UPDATED_AT:
            return pull_request.updated_at
        return pull_request.merged_at

    def github_rest_sort(self) -> str:
        if self is TimeAnchor.CREATED_AT:
            return "created"
        return "updated"

    def github_graphql_order_field(self) -> str:
        if self is TimeAnchor.CREATED_AT:
            return "CREATED_AT"
        return "UPDATED_AT"

    def supports_early_stop(self) -> bool:
        return self is not TimeAnchor.MERGED_AT


class RunScope(StrEnum):
    """Enumerate effective collection scopes derived from run mode."""

    FULL_HISTORY = "full_history"
    OPEN_PERIOD = "open_period"
    BOUNDED_BACKFILL = "bounded_backfill"


class AuthSource(StrEnum):
    """Enumerate supported GitHub authentication sources."""

    GH_TOKEN = "GH_TOKEN"
    GH_CLI = "gh"


class ReportingPeriod(BaseModel):
    """Describe one reporting period and its closure state."""

    model_config = ConfigDict(frozen=True)

    grain: PeriodGrain
    start_date: date
    end_date: date
    key: str
    closed: bool


class PeriodRange(BaseModel):
    """Describe a contiguous range of reporting periods."""

    model_config = ConfigDict(frozen=True)

    grain: PeriodGrain
    start_date: date
    end_date: date
    period_count: int


class CollectionWindow(BaseModel):
    """Describe the inclusive date window fetched by a run."""

    model_config = ConfigDict(frozen=True)

    scope: RunScope
    start_date: date | None
    end_date: date


class RepositoryInventoryItem(BaseModel):
    """Describe one repository discovered for collection."""

    model_config = ConfigDict(frozen=True)

    name: str
    full_name: str
    default_branch: str
    private: bool
    archived: bool
    disabled: bool


class RepositoryInventory(BaseModel):
    """Store the repository inventory for a target organization."""

    model_config = ConfigDict(frozen=True)

    organization_login: str
    repositories: tuple[RepositoryInventoryItem, ...]


class RepositoryCollectionFailure(BaseModel):
    """Describe one repository-level collection failure."""

    model_config = ConfigDict(frozen=True)

    repository_full_name: str
    operation: str
    status_code: int
    retriable: bool
    message: str


class PullRequestReviewRecord(BaseModel):
    """Store one normalized pull request review record."""

    model_config = ConfigDict(frozen=True)

    review_id: int
    state: str
    author_login: str | None
    submitted_at: datetime | None
    commit_id: str | None


class PullRequestTimelineEventRecord(BaseModel):
    """Store one normalized pull request timeline event record."""

    model_config = ConfigDict(frozen=True)

    event_id: int
    event: str
    actor_login: str | None
    created_at: datetime | None
    requested_reviewer_login: str | None
    requested_team_name: str | None


class PullRequestRecord(BaseModel):
    """Store one normalized pull request record."""

    model_config = ConfigDict(frozen=True)

    repository_full_name: str
    number: int
    title: str
    state: str
    draft: bool
    merged: bool
    author_login: str | None
    created_at: datetime
    updated_at: datetime
    closed_at: datetime | None
    merged_at: datetime | None
    additions: int
    deletions: int
    changed_files: int
    commits: int
    html_url: str
    reviews: tuple[PullRequestReviewRecord, ...] = ()
    timeline_events: tuple[PullRequestTimelineEventRecord, ...] = ()


class PullRequestCollection(BaseModel):
    """Store a pull request collection window and its results."""

    model_config = ConfigDict(frozen=True)

    window: CollectionWindow
    pull_requests: tuple[PullRequestRecord, ...]
    failures: tuple[RepositoryCollectionFailure, ...]


class RawSnapshotPeriod(BaseModel):
    """Describe one persisted raw snapshot period on disk."""

    model_config = ConfigDict(frozen=True)

    key: str
    start_date: date
    end_date: date
    closed: bool = False
    directory: Path
    pull_requests_path: Path
    pull_request_count: int
    reviews_path: Path
    review_count: int
    timeline_events_path: Path
    timeline_event_count: int


class RawSnapshotWriteResult(BaseModel):
    """Describe the filesystem layout produced by raw snapshot writing."""

    model_config = ConfigDict(frozen=True)

    root_dir: Path
    periods: tuple[RawSnapshotPeriod, ...]


class PullRequestMetricRecord(BaseModel):
    """Store one derived pull request metric record."""

    model_config = ConfigDict(frozen=True)

    period_key: str
    repository_full_name: str
    pull_request_number: int
    author_login: str | None
    merged: bool
    created_at: datetime
    updated_at: datetime
    review_ready_at: datetime | None
    review_requested_at: datetime | None
    review_started_at: datetime | None
    first_review_submitted_at: datetime | None
    time_to_first_review_seconds: int | None
    merged_at: datetime | None
    time_to_merge_seconds: int | None
    additions: int
    deletions: int
    changed_lines: int
    changed_files: int
    commits: int


class PullRequestMetricPeriod(BaseModel):
    """Store derived pull request metrics for one reporting period."""

    model_config = ConfigDict(frozen=True)

    key: str
    start_date: date
    end_date: date
    closed: bool
    pull_request_metrics: tuple[PullRequestMetricRecord, ...]


class PullRequestMetricCollection(BaseModel):
    """Store derived pull request metrics across periods."""

    model_config = ConfigDict(frozen=True)

    periods: tuple[PullRequestMetricPeriod, ...]


class MetricValueSummary(BaseModel):
    """Summarize a numeric metric distribution."""

    model_config = ConfigDict(frozen=True)

    count: int
    total: int
    average: float | None
    median: float | None


class RepositoryMetricRollup(BaseModel):
    """Store one repository-level metric rollup."""

    model_config = ConfigDict(frozen=True)

    repository_full_name: str
    pull_request_count: int
    merged_pull_request_count: int
    active_author_count: int
    merged_pull_requests_per_active_author: float | None
    time_to_merge_seconds: MetricValueSummary
    time_to_first_review_seconds: MetricValueSummary
    additions: MetricValueSummary
    deletions: MetricValueSummary
    changed_lines: MetricValueSummary
    changed_files: MetricValueSummary
    commits: MetricValueSummary


class RepositoryMetricPeriod(BaseModel):
    """Store repository rollups for one reporting period."""

    model_config = ConfigDict(frozen=True)

    key: str
    start_date: date
    end_date: date
    closed: bool
    repositories: tuple[RepositoryMetricRollup, ...]


class RepositoryMetricCollection(BaseModel):
    """Store repository rollups across periods for one organization."""

    model_config = ConfigDict(frozen=True)

    target_org: str
    periods: tuple[RepositoryMetricPeriod, ...]


class OrganizationMetricRollup(BaseModel):
    """Store one organization-level metric rollup."""

    model_config = ConfigDict(frozen=True)

    repository_count: int
    pull_request_count: int
    merged_pull_request_count: int
    active_author_count: int
    merged_pull_requests_per_active_author: float | None
    time_to_merge_seconds: MetricValueSummary
    time_to_first_review_seconds: MetricValueSummary
    additions: MetricValueSummary
    deletions: MetricValueSummary
    changed_lines: MetricValueSummary
    changed_files: MetricValueSummary
    commits: MetricValueSummary


class OrganizationMetricPeriod(BaseModel):
    """Store organization metrics for one reporting period."""

    model_config = ConfigDict(frozen=True)

    key: str
    start_date: date
    end_date: date
    closed: bool
    summary: OrganizationMetricRollup


class OrganizationMetricCollection(BaseModel):
    """Store organization metrics across periods."""

    model_config = ConfigDict(frozen=True)

    target_org: str
    periods: tuple[OrganizationMetricPeriod, ...]


class OrgSummaryPeriodWriteResult(BaseModel):
    """Describe one written organization summary period artifact set."""

    model_config = ConfigDict(frozen=True)

    key: str
    start_date: date
    end_date: date
    closed: bool
    directory: Path
    markdown_path: Path
    json_path: Path


class OrgSummaryWriteResult(BaseModel):
    """Describe all organization summary artifacts written for a run."""

    model_config = ConfigDict(frozen=True)

    target_org: str
    root_dir: Path
    contract_path: Path
    index_path: Path
    readme_path: Path
    latest_directory: Path | None
    latest_markdown_path: Path | None
    latest_json_path: Path | None
    periods: tuple[OrgSummaryPeriodWriteResult, ...]


class AnalysisReportPeriodWriteResult(BaseModel):
    """Describe one written analysis report period artifact set."""

    model_config = ConfigDict(frozen=True)

    key: str
    start_date: date
    end_date: date
    closed: bool
    directory: Path
    html_path: Path
    json_path: Path


class AnalysisReportWriteResult(BaseModel):
    """Describe all analysis report artifacts written for a run."""

    model_config = ConfigDict(frozen=True)

    target_org: str
    root_dir: Path
    contract_path: Path
    index_path: Path
    readme_path: Path
    latest_directory: Path | None
    latest_html_path: Path | None
    latest_json_path: Path | None
    periods: tuple[AnalysisReportPeriodWriteResult, ...]


class DashboardOverviewPayload(BaseModel):
    """Store high-level dashboard summary metrics."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    org: str
    generated_at: str
    since: str
    until: str
    time_anchor: str
    open_pull_requests: int | None = None
    repositories: int | None = None
    authors: int | None = None
    top_repository: str | None = None
    top_author: str | None = None
    unique_reviewers: int | None = None
    pull_requests: int | None = None
    merged_pull_requests: int | None = None
    review_submissions: int | None = None
    total_changed_lines: int | None = None
    total_commits: int | None = None
    median_first_review_hours: float | None = None
    median_merge_hours: float | None = None
    median_close_hours: float | None = None
    average_reviews_per_pr: float | None = None
    average_changed_lines_per_pr: float | None = None
    review_coverage_pct: float | None = None
    review_sla_24h_pct: float | None = None
    stale_open_pull_requests: int | None = None
    merge_rate_pct: float | None = None
    distribution_percentile: int | None = None
    average_active_authors_per_month: float | None = None
    latest_active_authors: int | None = None
    pull_requests_per_active_author: float | None = None
    changed_lines_per_active_author: float | None = None
    review_submissions_per_reviewer: float | None = None


class DashboardReviewerPayload(BaseModel):
    """Store reviewer leaderboard metrics for the dashboard."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    reviewer_login: str
    review_submissions: int
    pull_requests_reviewed: int
    approvals: int
    changes_requested: int
    comments: int
    authors_supported: int


class DashboardPullRequestPayload(BaseModel):
    """Store one dashboard-ready pull request row."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    repository_full_name: str
    pull_request_number: int
    title: str
    author_login: str
    state: str
    created_at: str
    updated_at: str
    closed_at: str | None = None
    merged_at: str | None = None
    html_url: str
    additions: int
    deletions: int
    changed_files: int
    changed_lines: int
    commits: int
    review_count: int
    approval_count: int
    changes_requested_count: int
    comment_review_count: int
    reviewer_count: int
    first_review_hours: float | None = None
    merge_hours: float | None = None
    close_hours: float | None = None
    review_rounds: int
    review_ready_at: str
    review_requested_at: str | None = None
    size_bucket: str


class DashboardInsightPayload(BaseModel):
    """Store one dashboard insight callout."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    title: str
    body: str


class DashboardTimeSeriesPointPayload(BaseModel):
    """Store one time-series point for dashboard charts."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    date: str
    count: int


class DashboardAuthorPayload(BaseModel):
    """Store author leaderboard metrics for the dashboard."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    author_login: str
    pull_requests: int
    merged_pull_requests: int
    open_pull_requests: int
    changed_lines: int
    commits: int
    review_submissions_received: int
    average_reviews_per_pr: float | None = None
    median_first_review_hours: float | None = None
    median_merge_hours: float | None = None
    median_changed_lines: float | None = None
    share_of_prs_pct: float | None = None


class DashboardRepositoryPayload(BaseModel):
    """Store repository leaderboard metrics for the dashboard."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    repository_full_name: str
    pull_requests: int
    merged_pull_requests: int
    open_pull_requests: int
    authors: int
    changed_lines: int
    review_submissions: int
    average_reviews_per_pr: float | None = None
    median_first_review_hours: float | None = None
    median_merge_hours: float | None = None
    share_of_prs_pct: float | None = None


class DashboardSizeBucketPayload(BaseModel):
    """Store pull request size bucket diagnostics for the dashboard."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    bucket: str
    pull_requests: int
    median_changed_lines: float | None = None
    median_first_review_hours: float | None = None
    median_merge_hours: float | None = None
    average_reviews_per_pr: float | None = None


class DashboardReviewStatePayload(BaseModel):
    """Store review state distribution metrics for the dashboard."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    state: str
    count: int
    share_pct: float | None = None


class DashboardAuthorThroughputPointPayload(BaseModel):
    """Store author throughput chart values for the dashboard."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    label: str
    pull_requests: int
    merged_pull_requests: int
    changed_lines: int


class DashboardReviewLatencyPointPayload(BaseModel):
    """Store author review latency chart values for the dashboard."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    label: str
    median_first_review_hours: float | None = None


class DashboardRepositoryThroughputPointPayload(BaseModel):
    """Store repository throughput chart values for the dashboard."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    label: str
    pull_requests: int
    merged_pull_requests: int


class DashboardChartsPayload(BaseModel):
    """Collect chart series used by the dashboard UI."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    created_series: list[DashboardTimeSeriesPointPayload] = Field(default_factory=list)
    merged_series: list[DashboardTimeSeriesPointPayload] = Field(default_factory=list)
    review_series: list[DashboardTimeSeriesPointPayload] = Field(default_factory=list)
    author_throughput: list[DashboardAuthorThroughputPointPayload] = Field(
        default_factory=list
    )
    review_latency_by_author: list[DashboardReviewLatencyPointPayload] = Field(
        default_factory=list
    )
    repository_throughput: list[
        DashboardRepositoryThroughputPointPayload
    ] = Field(default_factory=list)
    size_bucket_latency: list[DashboardSizeBucketPayload] = Field(default_factory=list)


class DashboardTrendRowPayload(BaseModel):
    """Store one period trend row used by the dashboard."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    period_key: str
    pull_requests: int
    merged_pull_requests: int
    open_pull_requests: int
    active_authors: int
    changed_lines: int
    review_submissions: int
    pull_requests_per_active_author: float | None = None
    changed_lines_per_active_author: float | None = None
    average_reviews_per_pr: float | None = None
    median_first_review_hours: float | None = None
    median_merge_hours: float | None = None
    pull_request_delta: int | None = None
    changed_lines_delta: int | None = None


class DashboardMethodologyPayload(BaseModel):
    """Store methodology metadata shown by the dashboard."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    window: str
    anchor: str
    distribution_percentile: int
    generated_at: str


class DashboardReferenceSummaryPayload(BaseModel):
    """Store reference-section coverage metadata for the dashboard."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    author_roster_coverage_pct: float | None = None
    reviewers_top_coverage_pct: float | None = None
    repositories_top_coverage_pct: float | None = None
    top3_author_share_pct: float | None = None
    top3_repository_share_pct: float | None = None
    weekly_hidden_count: int
    monthly_hidden_count: int
    author_reference_count: int


class DashboardSizeDiagnosticPayload(BaseModel):
    """Store explanatory size-diagnostic copy for the dashboard."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    headline: str
    supporting: str


class DashboardSourcePayload(BaseModel):
    """Store the typed source payload consumed by dashboard preparation."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    overview: DashboardOverviewPayload
    insights: list[DashboardInsightPayload] = Field(default_factory=list)
    charts: DashboardChartsPayload = Field(default_factory=DashboardChartsPayload)
    authors: list[DashboardAuthorPayload] = Field(default_factory=list)
    reviewers: list[DashboardReviewerPayload]
    repositories: list[DashboardRepositoryPayload] = Field(default_factory=list)
    size_buckets: list[DashboardSizeBucketPayload] = Field(default_factory=list)
    review_state_rows: list[DashboardReviewStatePayload] = Field(default_factory=list)
    pull_requests: list[DashboardPullRequestPayload]


class DashboardPreparedPayload(BaseModel):
    """Store template-ready dashboard presentation data."""

    model_config = ConfigDict(extra="allow")

    overview: dict[str, Any]
    authors: list[dict[str, Any]]
    authors_roster_top: list[dict[str, Any]]
    authors_roster_rest: list[dict[str, Any]]
    reviewers: list[dict[str, Any]]
    reviewers_top: list[dict[str, Any]]
    reviewers_rest: list[dict[str, Any]]
    repositories: list[dict[str, Any]]
    repositories_top: list[dict[str, Any]]
    repositories_rest: list[dict[str, Any]]
    size_buckets: list[dict[str, Any]]
    review_state_rows: list[dict[str, Any]]
    weekly_trends: list[dict[str, Any]]
    monthly_trends: list[dict[str, Any]]
    weekly_trends_recent: list[dict[str, Any]]
    weekly_trends_older: list[dict[str, Any]]
    monthly_trends_recent: list[dict[str, Any]]
    monthly_trends_older: list[dict[str, Any]]
    methodology: dict[str, Any]
    reference_summary: dict[str, Any]
    size_diagnostic: dict[str, Any]
    default_author: str | None
    author_details_json: str
    distribution_percentile: int
    pull_requests: list[DashboardPullRequestPayload]


class AnalysisReportMetricDefinition(BaseModel):
    """Describe one metric available in an analysis report view."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    key: str
    label: str
    format: str


class AnalysisReportPeriodDescriptor(BaseModel):
    """Describe one period label and boundary in an analysis report."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    key: str
    label: str
    start_date: str
    end_date: str
    closed: bool


class AnalysisReportPeriodValues(BaseModel):
    """Store one period's values for a specific analysis report view."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    key: str
    label: str
    start_date: str
    end_date: str
    closed: bool
    values: dict[str, int | float | None]


class AnalysisReportPeriodPayload(AnalysisReportPeriodDescriptor):
    """Store one period section in the analysis report."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    summary: dict[str, int | float | None]
    values: dict[str, int | float | None]
    diagnostics: dict[str, Any]


class AnalysisReportEntityPayload(BaseModel):
    """Store one repository or author section in the analysis report."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    key: str
    label: str
    period_values: list[AnalysisReportPeriodValues]
    totals: dict[str, int | float | None]


class AnalysisReportPeriodViewPayload(BaseModel):
    """Store the period-focused interactive analysis report view."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    default_metric: str
    metrics: list[AnalysisReportMetricDefinition]
    periods: list[AnalysisReportPeriodValues]


class AnalysisReportEntityViewPayload(BaseModel):
    """Store one entity-focused interactive analysis report view."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    default_metric: str
    metrics: list[AnalysisReportMetricDefinition]
    periods: list[AnalysisReportPeriodDescriptor]
    entities: list[AnalysisReportEntityPayload]


class AnalysisReportViewsPayload(BaseModel):
    """Collect all interactive views exposed by an analysis report."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    period: AnalysisReportPeriodViewPayload
    repository: AnalysisReportEntityViewPayload
    author: AnalysisReportEntityViewPayload


class AnalysisReportPayload(BaseModel):
    """Store the complete HTML analysis report payload."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    target_org: str
    grain: str
    time_anchor: str
    initial_view: str
    default_top_n: int
    since: str | None = None
    until: str | None = None
    distribution_percentile: int
    matched_pull_request_count: int
    default_period_key: str
    periods: list[AnalysisReportPeriodPayload]
    views: AnalysisReportViewsPayload


class TimeAnchorContextPayload(BaseModel):
    """Describe how a file's metrics are anchored in time."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    field: str
    scope: str
    description: str


class PeriodStatePayload(BaseModel):
    """Describe whether a persisted reporting period is open or closed."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    status: str
    label: str
    is_open: bool
    is_closed: bool
    is_partial: bool
    observed_through_date: str


class RepositorySummaryHistoryEntryPayload(BaseModel):
    """Store one repository summary history entry."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    key: str
    start_date: str
    end_date: str
    closed: bool
    status: str
    label: str
    is_open: bool
    is_closed: bool
    is_partial: bool
    observed_through_date: str
    path: str


class RepositorySummaryLatestPayload(RepositorySummaryHistoryEntryPayload):
    """Store the latest repository summary pointer."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    source_path: str


class RepositorySummaryContractPayload(BaseModel):
    """Store the repository summary contract JSON payload."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    target_org: str
    period_grain: str
    time_anchor: str
    time_anchor_context: TimeAnchorContextPayload
    period_state_fields: list[str]
    include_repos: list[str]
    exclude_repos: list[str]


class RepositorySummaryIndexPayload(BaseModel):
    """Store the repository summary index JSON payload."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    target_org: str
    period_grain: str
    time_anchor: str
    time_anchor_context: TimeAnchorContextPayload
    include_repos: list[str]
    exclude_repos: list[str]
    latest: RepositorySummaryLatestPayload | None
    history: list[RepositorySummaryHistoryEntryPayload]


class OrgSummaryPeriodPayload(BaseModel):
    """Store one organization summary period descriptor."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    key: str
    start_date: str
    end_date: str
    closed: bool
    status: str
    label: str
    is_open: bool
    is_closed: bool
    is_partial: bool
    observed_through_date: str


class OrgSummaryHistoryEntryPayload(OrgSummaryPeriodPayload):
    """Store one organization summary history entry."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    markdown_path: str
    json_path: str


class OrgSummaryLatestPayload(OrgSummaryPeriodPayload):
    """Store the latest organization summary pointer."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    markdown_path: str
    json_path: str
    source_markdown_path: str
    source_json_path: str


class OrgSummaryContractPayload(BaseModel):
    """Store the organization summary contract JSON payload."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    target_org: str
    period_grain: str
    time_anchor: str
    time_anchor_context: TimeAnchorContextPayload
    include_repos: list[str]
    exclude_repos: list[str]


class OrgSummaryJsonPayload(BaseModel):
    """Store the organization summary JSON document payload."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    target_org: str
    period_grain: str
    time_anchor: str
    time_anchor_context: TimeAnchorContextPayload
    include_repos: list[str]
    exclude_repos: list[str]
    period: OrgSummaryPeriodPayload
    summary_labels: dict[str, str]
    summary: dict[str, Any]


class OrgSummaryIndexPayload(BaseModel):
    """Store the organization summary index JSON payload."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    target_org: str
    period_grain: str
    time_anchor: str
    time_anchor_context: TimeAnchorContextPayload
    include_repos: list[str]
    exclude_repos: list[str]
    latest: OrgSummaryLatestPayload | None
    history: list[OrgSummaryHistoryEntryPayload]


class ManifestPeriodPayload(BaseModel):
    """Store one manifest period descriptor."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    key: str
    start_date: str
    end_date: str
    closed: bool
    status: str
    label: str
    is_open: bool
    is_closed: bool
    is_partial: bool
    observed_through_date: str


class ManifestIndexLatestPayload(BaseModel):
    """Store the latest manifest pointer in the manifest index."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    manifest_path: str
    completed_at: str
    as_of: str
    mode: str
    refresh_scope: str


class ManifestHistoryPayload(BaseModel):
    """Store manifest history pointers for refreshed and locked periods."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    refreshed_periods: list[ManifestPeriodPayload]
    locked_periods: list[ManifestPeriodPayload]


class ManifestIndexPayload(BaseModel):
    """Store the manifest index JSON payload."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    target_org: str
    period_grain: str
    time_anchor: str
    time_anchor_context: TimeAnchorContextPayload
    include_repos: list[str]
    exclude_repos: list[str]
    latest: ManifestIndexLatestPayload
    history: ManifestHistoryPayload
    watermarks: dict[str, Any]


class MetricValidationIssue(BaseModel):
    """Store one metric validation issue."""

    model_config = ConfigDict(frozen=True)

    code: str
    message: str
    repository_full_name: str | None = None
    pull_request_number: int | None = None


class RepositoryMetricValidationSummary(BaseModel):
    """Summarize metric validation counts for one repository."""

    model_config = ConfigDict(frozen=True)

    repository_full_name: str
    pull_request_count: int
    merged_pull_request_count: int
    time_to_merge_count: int
    time_to_first_review_count: int


class OrganizationMetricValidationSummary(BaseModel):
    """Summarize metric validation counts for one organization period."""

    model_config = ConfigDict(frozen=True)

    repository_count: int
    pull_request_count: int
    merged_pull_request_count: int
    time_to_merge_count: int
    time_to_first_review_count: int


class MetricValidationPeriod(BaseModel):
    """Store metric validation results for one reporting period."""

    model_config = ConfigDict(frozen=True)

    key: str
    start_date: date
    end_date: date
    closed: bool
    raw_pull_request_count: int
    raw_review_count: int
    raw_timeline_event_count: int
    repository_summaries: tuple[RepositoryMetricValidationSummary, ...]
    org_summary: OrganizationMetricValidationSummary
    valid: bool
    issues: tuple[MetricValidationIssue, ...]


class MetricValidationCollection(BaseModel):
    """Store metric validation results across periods."""

    model_config = ConfigDict(frozen=True)

    target_org: str
    periods: tuple[MetricValidationPeriod, ...]


class ManifestWatermarks(BaseModel):
    """Store watermark dates associated with a run manifest."""

    model_config = ConfigDict(frozen=True)

    collection_window_start_date: date | None
    collection_window_end_date: date
    latest_refreshed_period_end_date: date | None
    latest_locked_period_end_date: date | None


class LastSuccessfulRun(BaseModel):
    """Store metadata about the latest successful run."""

    model_config = ConfigDict(frozen=True)

    completed_at: datetime
    as_of: date
    mode: RunMode
    refresh_scope: RunScope
    repository_count: int
    pull_request_count: int


class RunManifest(BaseModel):
    """Store the canonical manifest for persisted raw outputs."""

    model_config = ConfigDict(frozen=True)

    target_org: str
    period_grain: PeriodGrain
    time_anchor: TimeAnchor
    include_repos: tuple[str, ...]
    exclude_repos: tuple[str, ...]
    raw_snapshot_root_dir: Path
    refreshed_periods: tuple[RawSnapshotPeriod, ...]
    locked_periods: tuple[ReportingPeriod, ...]
    watermarks: ManifestWatermarks
    last_successful_run: LastSuccessfulRun


class ManifestWriteResult(BaseModel):
    """Describe manifest artifacts written for a run."""

    model_config = ConfigDict(frozen=True)

    path: Path
    index_path: Path
    readme_path: Path
    manifest: RunManifest


class RepositorySummaryCsvPeriod(BaseModel):
    """Describe one written repository summary CSV period artifact."""

    model_config = ConfigDict(frozen=True)

    key: str
    start_date: date
    end_date: date
    closed: bool
    path: Path
    repository_count: int


class RepositorySummaryCsvWriteResult(BaseModel):
    """Describe repository summary CSV artifacts written for a run."""

    model_config = ConfigDict(frozen=True)

    root_dir: Path
    contract_path: Path
    index_path: Path
    readme_path: Path
    latest_path: Path | None
    periods: tuple[RepositorySummaryCsvPeriod, ...]


class CheckpointPolicy(BaseModel):
    """Describe checkpoint behavior for a run configuration."""

    model_config = ConfigDict(frozen=True)

    resume_from_checkpoint: bool
    persist_checkpoint: bool
    overwrite_checkpoint: bool


class LockPolicy(BaseModel):
    """Describe locked-period handling for a run configuration."""

    model_config = ConfigDict(frozen=True)

    skip_locked_periods: bool
    refresh_locked_periods: bool
    lock_closed_periods_on_success: bool


class RunConfig(BaseModel):
    """Capture validated settings for a collection run."""

    model_config = ConfigDict(frozen=True)

    org: OrgSlug
    github_token: SecretStr | None = Field(
        default=None,
        exclude=True,
        repr=False,
    )
    as_of: date = Field(default_factory=date.today)
    period: PeriodGrain = PeriodGrain.MONTH
    time_anchor: TimeAnchor = TimeAnchor.CREATED_AT
    mode: RunMode = RunMode.INCREMENTAL
    output_dir: Path = Field(default_factory=lambda: Path("output"))
    include_repos: tuple[RepoSlug, ...] = ()
    exclude_repos: tuple[RepoSlug, ...] = ()
    backfill_start: date | None = None
    backfill_end: date | None = None

    @field_validator("include_repos", "exclude_repos", mode="before")
    @classmethod
    def normalize_repo_filters(
        cls, value: Any, info: ValidationInfo
    ) -> tuple[str, ...]:
        if value is None:
            return ()
        if isinstance(value, str):
            items = [value]
        else:
            items = list(value)

        org = info.data.get("org")
        deduped: list[str] = []
        seen: set[str] = set()
        for item in items:
            cleaned = item.strip()
            canonical = canonicalize_repo_filter(cleaned, org=org)
            if cleaned and canonical not in seen:
                deduped.append(cleaned)
                seen.add(canonical)
        return tuple(deduped)

    @field_validator("output_dir", mode="before")
    @classmethod
    def normalize_output_dir(cls, value: Any) -> Path:
        if value is None:
            return Path("output")
        return Path(value).expanduser()

    @model_validator(mode="after")
    def validate_cross_field_constraints(self) -> "RunConfig":
        overlapping = _find_overlapping_repo_filters(
            org=self.org,
            include_repos=self.include_repos,
            exclude_repos=self.exclude_repos,
        )
        if overlapping:
            overlap = ", ".join(sorted(overlapping))
            raise ValueError(
                f"repo filters overlap across include and exclude lists: {overlap}"
            )

        for repo_filter in (*self.include_repos, *self.exclude_repos):
            if "/" not in repo_filter:
                continue
            owner, _ = repo_filter.split("/", 1)
            if owner.lower() != self.org.lower():
                raise ValueError(
                    f"repo filter owner must match target org '{self.org}': {repo_filter}"
                )

        has_backfill_bounds = (
            self.backfill_start is not None or self.backfill_end is not None
        )
        if self.mode is RunMode.BACKFILL:
            if self.backfill_start is None or self.backfill_end is None:
                raise ValueError(
                    "backfill mode requires both --backfill-start and --backfill-end"
                )
            if self.backfill_start > self.backfill_end:
                raise ValueError("--backfill-start must be on or before --backfill-end")
            if not self.period.is_period_start(self.backfill_start):
                raise ValueError(
                    "backfill start must align to the selected period boundary"
                )
            if not self.period.is_period_end(self.backfill_end):
                raise ValueError(
                    "backfill end must align to the selected period boundary"
                )
            if self.backfill_end >= self.active_period.start_date:
                raise ValueError(
                    "backfill range must end before the current open period begins"
                )
        elif has_backfill_bounds:
            raise ValueError(
                "backfill date bounds are only valid when --mode backfill is selected"
            )

        return self

    @computed_field(return_type=RunScope)
    @property
    def refresh_scope(self) -> RunScope:
        if self.mode is RunMode.FULL:
            return RunScope.FULL_HISTORY
        if self.mode is RunMode.BACKFILL:
            return RunScope.BOUNDED_BACKFILL
        return RunScope.OPEN_PERIOD

    @computed_field(return_type=ReportingPeriod)
    @property
    def active_period(self) -> ReportingPeriod:
        start_date = self.period.start_for(self.as_of)
        end_date = self.period.end_for(self.as_of)
        return ReportingPeriod(
            grain=self.period,
            start_date=start_date,
            end_date=end_date,
            key=self.period.key_for(start_date),
            closed=False,
        )

    @computed_field(return_type=PeriodRange | None)
    @property
    def requested_range(self) -> PeriodRange | None:
        if self.mode is not RunMode.BACKFILL:
            return None
        assert self.backfill_start is not None
        assert self.backfill_end is not None
        return PeriodRange(
            grain=self.period,
            start_date=self.backfill_start,
            end_date=self.backfill_end,
            period_count=self.period.count_periods(
                self.backfill_start, self.backfill_end
            ),
        )

    @computed_field(return_type=CollectionWindow)
    @property
    def collection_window(self) -> CollectionWindow:
        if self.mode is RunMode.FULL:
            return CollectionWindow(
                scope=self.refresh_scope,
                start_date=None,
                end_date=self.as_of,
            )
        if self.mode is RunMode.BACKFILL:
            assert self.backfill_start is not None
            assert self.backfill_end is not None
            return CollectionWindow(
                scope=self.refresh_scope,
                start_date=self.backfill_start,
                end_date=self.backfill_end,
            )
        return CollectionWindow(
            scope=self.refresh_scope,
            start_date=self.active_period.start_date,
            end_date=self.as_of,
        )

    @computed_field(return_type=CheckpointPolicy)
    @property
    def checkpoint_policy(self) -> CheckpointPolicy:
        if self.mode is RunMode.FULL:
            return CheckpointPolicy(
                resume_from_checkpoint=False,
                persist_checkpoint=True,
                overwrite_checkpoint=True,
            )
        if self.mode is RunMode.BACKFILL:
            return CheckpointPolicy(
                resume_from_checkpoint=False,
                persist_checkpoint=False,
                overwrite_checkpoint=False,
            )
        return CheckpointPolicy(
            resume_from_checkpoint=True,
            persist_checkpoint=True,
            overwrite_checkpoint=False,
        )

    @computed_field(return_type=LockPolicy)
    @property
    def lock_policy(self) -> LockPolicy:
        if self.mode is RunMode.INCREMENTAL:
            return LockPolicy(
                skip_locked_periods=True,
                refresh_locked_periods=False,
                lock_closed_periods_on_success=True,
            )
        return LockPolicy(
            skip_locked_periods=False,
            refresh_locked_periods=True,
            lock_closed_periods_on_success=True,
        )


def _period_key_for_month(value: date) -> str:
    return value.strftime("%Y-%m")


def _period_key_for_week(value: date) -> str:
    iso_year, iso_week, _ = value.isocalendar()
    return f"{iso_year}-W{iso_week:02d}"


def _month_start_for_period_key(key: str) -> date:
    return datetime.strptime(key, "%Y-%m").date()


def _week_start_for_period_key(key: str) -> date:
    iso_year, iso_week = key.split("-W", 1)
    return date.fromisocalendar(int(iso_year), int(iso_week), 1)


def _month_end(value: date) -> date:
    return value.replace(day=monthrange(value.year, value.month)[1])


def _next_month_start(value: date) -> date:
    year = value.year + (value.month // 12)
    month = 1 if value.month == 12 else value.month + 1
    return date(year, month, 1)


def _next_week_start(value: date) -> date:
    return value + timedelta(days=7)


def _count_periods(
    start_date: date, end_date: date, next_period_start: Callable[[date], date]
) -> int:
    count = 0
    current = start_date
    while current <= end_date:
        count += 1
        current = next_period_start(current)
    return count


def _find_overlapping_repo_filters(
    *,
    org: str,
    include_repos: tuple[str, ...],
    exclude_repos: tuple[str, ...],
) -> tuple[str, ...]:
    include_index = {
        canonicalize_repo_filter(repo_filter, org=org): repo_filter
        for repo_filter in include_repos
    }
    overlapping: list[str] = []
    seen: set[str] = set()
    for repo_filter in exclude_repos:
        canonical = canonicalize_repo_filter(repo_filter, org=org)
        if canonical not in include_index or canonical in seen:
            continue
        overlapping.append(include_index[canonical])
        seen.add(canonical)
    return tuple(overlapping)


def canonicalize_repo_filter(value: str, *, org: str | None = None) -> str:
    """Normalize a repo filter into a canonical comparison form.

    Args:
        value: Repository filter supplied by a user or persisted contract.
        org: Optional organization context for bare repository names.

    Returns:
        A lowercase normalized repository filter string.
    """

    normalized = value.strip().lower()
    if "/" in normalized or org is None:
        return normalized
    return f"{org.lower()}/{normalized}"


def repo_filter_matches(
    repo_filter: str, *, full_name: str, name: str, org: str
) -> bool:
    """Check whether a filter matches a repository identity.

    Args:
        repo_filter: Repository filter to evaluate.
        full_name: Full repository name including owner.
        name: Bare repository name.
        org: Organization used to resolve bare repository filters.

    Returns:
        Whether the filter matches the repository.
    """

    canonical_filter = canonicalize_repo_filter(repo_filter, org=org)
    return canonical_filter in {
        canonicalize_repo_filter(full_name),
        canonicalize_repo_filter(name, org=org),
    }


class ResolvedToken(BaseModel):
    """Store a resolved GitHub token and its provenance."""

    model_config = ConfigDict(frozen=True)

    source: AuthSource
    token: str


class GitHubTargetContext(BaseModel):
    """Store validated GitHub identity and organization targeting context."""

    model_config = ConfigDict(frozen=True)

    auth_source: AuthSource
    viewer_login: str
    organization_login: str
