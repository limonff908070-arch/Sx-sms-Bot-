"""
message_preprocessor.py
Intercepts every outgoing Telegram message, converts Markdown to HTML,
and replaces unicode emoji characters with animated custom emoji stickers
from the three SXSponsor packs.
"""
from __future__ import annotations

import re
from typing import Any

from telegram import Bot
from telegram.request import BaseRequest

from custom_emojis import EMOJI_MAP, _SORTED_EMOJIS


# ── Markdown → HTML ────────────────────────────────────────────────────────────

def _md_to_html(text: str) -> str:
    """Convert Telegram legacy Markdown to HTML.

    Handles *bold*, `code`, ```pre```, [link](url).
    Does NOT convert _italic_ to avoid false-positives with snake_case.
    """
    if not text:
        return text

    # Triple-backtick code blocks (multi-line)
    text = re.sub(r'```([^`]*?)```', r'<pre>\1</pre>', text, flags=re.DOTALL)

    # Inline code `text`
    text = re.sub(r'`([^`\n]+?)`', r'<code>\1</code>', text)

    # Bold *text*  (single asterisk, Telegram Markdown)
    text = re.sub(r'\*([^\*\n]+?)\*', r'<b>\1</b>', text)

    # Hyperlinks [label](url)
    text = re.sub(
        r'\[([^\]]+?)\]\((https?://[^\)]+?)\)',
        r'<a href="\2">\1</a>',
        text,
    )

    # Remove Markdown backslash escapes (e.g. \* → *)
    # Only for Telegram Markdown special chars, NOT for \\ or \n etc.
    text = re.sub(r'\\([*_`\[\]()\!#])', r'\1', text)

    return text


# ── Custom emoji replacement ───────────────────────────────────────────────────

def _add_custom_emojis(text: str) -> str:
    """Replace unicode emoji characters with animated <tg-emoji> HTML tags.

    Skips emojis that are already inside an existing <tg-emoji> block so
    service-specific sticker tags injected by _get_service_sticker_html()
    are never double-processed.
    """
    # Split into alternating segments: plain text (even) / already-tagged (odd)
    parts = re.split(r'(<tg-emoji[^>]*>.*?</tg-emoji>)', text, flags=re.DOTALL)
    result = []
    for i, part in enumerate(parts):
        if i % 2 == 1:
            # Already a <tg-emoji> block — keep it untouched
            result.append(part)
        else:
            # Plain text — apply emoji replacement as normal
            for emoji in _SORTED_EMOJIS:
                if emoji in part:
                    eid = EMOJI_MAP[emoji]
                    part = part.replace(
                        emoji,
                        f'<tg-emoji emoji-id="{eid}">{emoji}</tg-emoji>',
                    )
            result.append(part)
    return ''.join(result)


# ── Full pre-processor ─────────────────────────────────────────────────────────

def _extract_emojis_from_code(html: str) -> str:
    """Move <tg-emoji> tags from the START of <code> blocks to just before them.

    Telegram does not animate custom emojis inside <code> blocks.
    This regex finds patterns like <code><tg-emoji ...>🏳</tg-emoji> text</code>
    and converts them to <tg-emoji ...>🏳</tg-emoji> <code>text</code> so the
    flag emoji animates while the rest stays monospace.
    """
    pattern = re.compile(
        r'<code>((?:<tg-emoji[^>]+>[^<]+</tg-emoji>\s*)+)(.*?)</code>',
        re.DOTALL,
    )

    def _replacer(m: re.Match) -> str:
        emoji_part = m.group(1).rstrip()
        content    = m.group(2).strip()
        if content:
            return f'{emoji_part} <code>{content}</code>'
        return emoji_part

    return pattern.sub(_replacer, html)


def _preprocess(text: str | None, parse_mode: str | None) -> tuple[str | None, str]:
    """Convert a message to HTML with custom emojis.

    Returns (new_text, new_parse_mode).
    """
    if not text:
        return text, 'HTML'

    # Convert existing Markdown to HTML
    html = _md_to_html(text)

    # Inject animated emoji stickers
    html = _add_custom_emojis(html)

    # Pull flag emojis out of <code> blocks so Telegram animates them
    html = _extract_emojis_from_code(html)

    return html, 'HTML'


# ── Custom Bot subclass ────────────────────────────────────────────────────────

class AnimatedEmojiBot(Bot):
    """Drop-in Bot replacement that automatically applies animated custom emojis
    and converts Markdown to HTML on all outgoing text messages."""

    # ── send_message ──────────────────────────────────────────────────────────
    async def send_message(  # type: ignore[override]
        self,
        chat_id: int | str,
        text: str,
        parse_mode: str | None = None,
        **kwargs: Any,
    ):
        text, parse_mode = _preprocess(text, parse_mode)
        return await super().send_message(
            chat_id=chat_id,
            text=text,
            parse_mode=parse_mode,
            **kwargs,
        )

    # ── edit_message_text ─────────────────────────────────────────────────────
    async def edit_message_text(  # type: ignore[override]
        self,
        text: str,
        parse_mode: str | None = None,
        **kwargs: Any,
    ):
        text, parse_mode = _preprocess(text, parse_mode)
        return await super().edit_message_text(
            text=text,
            parse_mode=parse_mode,
            **kwargs,
        )

    # ── copy_message has no text to process, skip ─────────────────────────────
    # ── answer_callback_query has optional text but it's a toast, skip ────────
