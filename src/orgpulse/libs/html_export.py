from __future__ import annotations

import base64
import json
from html import escape
from typing import Any
from urllib.parse import urlsplit
from urllib.request import urlopen


def inline_github_avatar(url: str | None) -> str | None:
    """Fetch a profile photo for an offline report."""
    if (
        not url
        or urlsplit(url).scheme != "https"
        or urlsplit(url).hostname != "avatars.githubusercontent.com"
    ):
        return None
    try:
        with urlopen(url, timeout=5) as response:
            mime = response.headers.get_content_type()
            if mime not in {"image/png", "image/jpeg", "image/webp", "image/gif"}:
                return None
            data = response.read(512_001)
            if len(data) > 512_000:
                return None
            return f"data:{mime};base64,{base64.b64encode(data).decode()}"
    except OSError:
        return None


def render_report_html(
    snapshot: dict[str, Any], *, template: str, styles: str, script: str, icon: bytes
) -> str:
    """Render an interactive HTML report that opens offline."""
    payload = (
        json.dumps(snapshot, ensure_ascii=False)
        .replace("&", "\\u0026")
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
    )
    title = escape(
        f"{snapshot['meta']['snapshot'].get('name', snapshot['meta']['snapshot'].get('person', 'everyone'))} · {snapshot['meta']['org']} · orgpulse"
    )
    html = template.replace("<title>orgpulse</title>", f"<title>{title}</title>")
    html = html.replace(
        'href="/favicon.svg"',
        f'href="data:image/svg+xml;base64,{base64.b64encode(icon).decode()}"',
    )
    html = html.replace(
        '<link rel="stylesheet" href="/app.css">', f"<style>{styles}</style>"
    )
    html = html.replace(
        '<script src="/app.js"></script>',
        f'<script id="report-data" type="application/json">{payload}</script>\n<script>{script}</script>',
    )
    return html
