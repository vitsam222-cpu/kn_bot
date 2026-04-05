import html
import re

_LINK_RE = re.compile(r"\[(.+?)\]\((https?://[^\s)]+)\)")


def markdown_to_html(text: str | None) -> str:
    raw = text or ""
    escaped = html.escape(raw)

    escaped = _LINK_RE.sub(r'<a href="\2">\1</a>', escaped)
    escaped = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", escaped)
    escaped = re.sub(r"__(.+?)__", r"<u>\1</u>", escaped)
    escaped = re.sub(r"\*(.+?)\*", r"<i>\1</i>", escaped)
    escaped = re.sub(r"~~(.+?)~~", r"<s>\1</s>", escaped)
    escaped = re.sub(r"`(.+?)`", r"<code>\1</code>", escaped)
    return escaped.replace("\n", "<br>")
