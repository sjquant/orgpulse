from __future__ import annotations

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
    assert '<html lang="ko">' in (report_dir / "analysis-report.html").read_text(
        encoding="utf-8"
    )
    assert '<html lang="ko">' in (
        report_dir / "alex-rivera-person-report.html"
    ).read_text(encoding="utf-8")
    assert '<html lang="ko">' in (
        report_dir / "acme-cloud-org-dashboard.html"
    ).read_text(encoding="utf-8")
