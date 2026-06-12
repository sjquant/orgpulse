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
    assert "analysis_html=" in result.stdout
    assert "person_html=" in result.stdout
    assert "org_html=" in result.stdout
    report_dir = output_root / "reports"
    analysis_html = (report_dir / "analysis-report.html").read_text(encoding="utf-8")
    person_html = (report_dir / "alex-rivera-person-report.html").read_text(
        encoding="utf-8"
    )
    dashboard_html = (report_dir / "acme-cloud-org-dashboard.html").read_text(
        encoding="utf-8"
    )
    assert '<html lang="ko">' in analysis_html
    assert "orgpulse 분석 보고서" in analysis_html
    assert "작성자" in analysis_html
    assert "선택한 보고 기간에 집계된 풀 리퀘스트 수입니다." in analysis_html
    assert "metric-tooltip" in analysis_html
    assert "data-tooltip=" in analysis_html
    assert not re.search(r'class="metric-tooltip"[^>]*\stitle=', analysis_html)
    assert ".metric-tooltip::before" not in analysis_html
    assert '<html lang="ko">' in person_html
    assert "개인 성과 추출" in person_html
    assert "받은 리뷰 제출" in person_html
    assert "열린 월" in person_html or "닫힌 월" in person_html
    assert "metric-tooltip" in person_html
    assert "data-tooltip=" in person_html
    assert not re.search(r'class="metric-tooltip"[^>]*\stitle=', person_html)
    assert ".metric-tooltip::before" not in person_html
    assert '<html lang="ko">' in dashboard_html
    assert "엔지니어링 생산성 리더보드" in dashboard_html
    assert "24시간 이내" in dashboard_html
    assert "저장소 리더보드" in dashboard_html
    assert "metric-tooltip" in dashboard_html
    assert "data-tooltip=" in dashboard_html
    assert not re.search(r'class="metric-tooltip"[^>]*\stitle=', dashboard_html)
    assert ".metric-tooltip::before" not in dashboard_html
