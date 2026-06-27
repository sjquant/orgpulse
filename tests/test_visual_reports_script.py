from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path


def test_generates_korean_visual_testing_reports(
    tmp_path: Path,
) -> None:
    """Generate Korean visual-testing HTML reports for manual inspection."""
    # Given
    output_root = tmp_path / "visual-reports"

    # When
    result = subprocess.run(
        [
            sys.executable,
            "scripts/generate_visual_test_reports.py",
            "--output-root",
            str(output_root),
            "--locale",
            "ko",
        ],
        check=False,
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True,
        text=True,
    )

    # Then
    assert result.returncode == 0, result.stderr
    assert "analysis_html=" not in result.stdout
    assert "person_html=" in result.stdout
    assert "org_html=" in result.stdout
    report_dir = output_root / "reports"
    person_html = (report_dir / "alex-rivera-person-report.html").read_text(
        encoding="utf-8"
    )
    dashboard_html = (report_dir / "acme-cloud-org-dashboard.html").read_text(
        encoding="utf-8"
    )
    assert not (report_dir / "analysis-report.html").exists()
    assert '<html lang="ko">' in person_html
    assert "개인 기여 요약" in person_html
    assert "받은 리뷰 수" in person_html
    assert "열린 월" in person_html or "닫힌 월" in person_html
    assert 'href="#methodology"' not in person_html
    assert 'id="methodology"' not in person_html
    assert "report__metric-tooltip" in person_html
    assert "data-tooltip=" in person_html
    assert (
        ".person-report #repositories .report__table-wrap:has(.report__metric-tooltip:hover)"
        in (person_html)
    )
    assert (
        ".person-report #repositories .report__table-wrap:focus-within" in person_html
    )
    assert not re.search(
        r'class="report__tab-button[^"]*"[^>]*\sdata-tooltip=', person_html
    )
    assert ".report__metric-tooltip::before" not in person_html
    assert (
        "grid-template-columns: repeat(auto-fit, minmax(min(300px, 100%), 1fr));"
        in person_html
    )
    assert "--tooltip-bg: #e8edf7;" in person_html
    assert "--tooltip-text: #070b12;" in person_html
    assert "--tooltip-bg: #0f1b2d;" in person_html
    assert "--tooltip-text: #f8fbff;" in person_html
    assert "overflow-x: hidden;" in person_html
    assert "color: var(--bg);" not in person_html
    assert "@media (max-width: 420px)" in person_html
    assert ".report__hero-meta {\n        grid-template-columns: 1fr;\n      }" in person_html
    legacy_classes = (
        "tab-button",
        "tab-strip",
        "ghost-button",
        "chart-root",
        "metric-tooltip",
        "metric-label-with-tooltip",
        "table-wrap",
        "person-card",
        "author-button",
        "is-active",
        "is-hidden",
    )
    for legacy_class in legacy_classes:
        assert not re.search(
            rf'class="(?:[^"]*\s)?{re.escape(legacy_class)}(?:\s|")',
            person_html,
        )
    assert '<html lang="ko">' in dashboard_html
    assert "엔지니어링 생산성 현황" in dashboard_html
    assert "24시간 이내" in dashboard_html
    assert "저장소별 현황" in dashboard_html
    assert "첫 리뷰까지 가장 오래 걸린 구간은" in dashboard_html
    assert "크기 PR의 중앙값" in dashboard_html
    assert "waited the longest for first review" not in dashboard_html
    assert "Compared with" not in dashboard_html
    assert "<summary>작성자 원장</summary>" not in dashboard_html
    assert "<summary>방법론</summary>" not in dashboard_html
    assert "report__metric-tooltip" in dashboard_html
    assert "data-tooltip=" in dashboard_html
    assert (
        "grid-template-columns: repeat(auto-fit, minmax(min(300px, 100%), 1fr));"
        in dashboard_html
    )
    assert "overflow-x: hidden;" in dashboard_html
    assert "color: var(--bg);" not in dashboard_html
    assert not re.search(
        r'class="report__tab-button[^"]*"[^>]*\sdata-tooltip=', dashboard_html
    )
    assert ".report__metric-tooltip::before" not in dashboard_html
    for legacy_class in legacy_classes:
        assert not re.search(
            rf'class="(?:[^"]*\s)?{re.escape(legacy_class)}(?:\s|")',
            dashboard_html,
        )
