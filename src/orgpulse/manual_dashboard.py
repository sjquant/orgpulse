from orgpulse.reporting.dashboard_html import (
    prepare_manual_dashboard_payload,
    render_manual_dashboard_artifact,
    render_manual_dashboard_html,
)

__all__ = [
    "prepare_manual_dashboard_payload",
    "render_manual_dashboard_artifact",
    "render_manual_dashboard_html",
]


if __name__ == "__main__":
    raise SystemExit(
        "orgpulse.manual_dashboard is no longer executable. "
        "Use `orgpulse dashboard-render`."
    )
