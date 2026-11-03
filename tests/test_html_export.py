import json
from html.parser import HTMLParser

from orgpulse.libs.html_export import inline_github_avatar, render_person_html


class SnapshotParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.in_data = False
        self.payload = ""
        self.external_assets = []

    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)
        if tag == "script" and attributes.get("id") == "report-data":
            self.in_data = True
        if tag in ("script", "img", "link"):
            url = attributes.get("src") or attributes.get("href")
            if url and not url.startswith("data:"):
                self.external_assets.append(url)

    def handle_endtag(self, tag):
        if tag == "script":
            self.in_data = False

    def handle_data(self, data):
        if self.in_data:
            self.payload += data


def parse_snapshot(html):
    parser = SnapshotParser()
    parser.feed(html)
    return json.loads(parser.payload), parser.external_assets


def test_snapshot_json_cannot_close_its_script_element():
    text = '</script><script>alert("unsafe")</script> & <img src=x>'
    snapshot = {"meta": {"snapshot": {"person": "alice"}, "org": "acme"}, "title": text}
    template = '<title>orgpulse</title><link rel="icon" href="/favicon.svg"><link rel="stylesheet" href="/app.css"><script src="/app.js"></script>'
    html = render_person_html(
        snapshot, template=template, styles="body {}", script="void 0;", icon=b"<svg/>"
    )
    result, external = parse_snapshot(html)
    assert result["title"] == text
    assert text not in html
    assert not external


def test_missing_or_non_github_avatar_does_not_need_network():
    assert inline_github_avatar(None) is None
    assert inline_github_avatar("http://avatars.githubusercontent.com/u/1") is None
    assert inline_github_avatar("https://unrelated.example/photo.png") is None
