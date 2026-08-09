from __future__ import annotations

# Bot username — set dynamically at startup via set_bot_username()
_BOT_USERNAME: str = "UnofficialNumberBOT"


def set_bot_username(username: str) -> None:
    """Called once at startup with the bot's actual @username."""
    global _BOT_USERNAME
    _BOT_USERNAME = username.lstrip("@")


"""
OTP Monitor — three independent monitors run in parallel:

1. OTPMonitor  (SMS Hadi — sesskey-based auth)
   Logs into http://smshadi.net, fetches individual SMS records via
   /agent/SMSCDRStats DataTables AJAX (sesskey in URL).
   Column order (fg=0):
     [0] datetime  [1] range_name  [2] number  [3] client/website
     [4] cli       [5] sms_body    [6] currency [7] my_payout

2. ClientPanelMonitor  (generic — cookie-based auth, /client/ path)
   Used for both Konekta Premium and MSI SMS panels.
   Logs in, fetches /client/res/data_smscdr.php — no sesskey needed.
   Column order (fg=0):
     [0] datetime  [1] range_name  [2] number  [3] cli (sender)
     [4] sms_body  [5] currency    [6] my_payout
   Website is detected from SMS body text.

   Instances:
     • konekta_monitor  → https://konektapremium.net  (login at /sign-in)
     • msi_sms_monitor  → http://145.239.130.45/ints  (login at /login)
"""

import asyncio
import concurrent.futures
import hashlib
import html as _html
import logging
import random
import re
import time
from datetime import datetime, timedelta


def _bd_now() -> datetime:
    """Return UTC now + 1 day, used as the upper-bound for panel fdate2 filters.

    All 14 panels reset at UTC 0:00 (midnight UTC). After the reset, new messages
    carry the new UTC calendar date. By making fdate2 always point to *tomorrow*,
    we guarantee that freshly-arrived messages are always within the query window
    regardless of when in the UTC day the poll fires — including the critical
    window right after midnight when the date rolls over.

    d1 ends up 6 days ago  (7 days before tomorrow) — still plenty of history.
    d2 ends up tomorrow    — safely covers the new UTC date from the first poll.
    """
    return datetime.utcnow() + timedelta(days=1)


async def _midnight_relogin_jitter(label: str = '') -> None:
    """Sleep a random 0–60 s before re-logging in near UTC midnight.

    At UTC 0:00 all 14 panels reset simultaneously, often expiring all active
    sessions at once. Without a stagger, every monitor fires _login() at the
    same instant, flooding panel servers with 14 concurrent auth requests.
    A per-monitor random delay spreads them out naturally.

    Active window: UTC 23:50–23:59 and 00:00–00:10.
    """
    now = datetime.utcnow()
    minutes = now.hour * 60 + now.minute
    near_midnight = minutes >= 23 * 60 + 50 or minutes <= 10
    if near_midnight:
        delay = random.uniform(0, 60)
        logger.info("[MidnightJitter] %s — sleeping %.1fs before re-login", label or '?', delay)
        await asyncio.sleep(delay)
from urllib.parse import urlencode

import requests

from config import (
    POLL_JITTER_FRAC,
    BACKOFF_BASE_SECS,
    BACKOFF_MAX_SECS,
    BACKOFF_MULTIPLIER,
)

# ── Login retry config (Fresh Session Reload) ────────────────────────────────
# When a panel login fails, the bot will immediately drop the session,
# build a brand-new session, GET the login page again, solve the fresh captcha
# and POST credentials. This mimics a manual page-reload retry in a browser.
_LOGIN_FAST_RETRIES = 3   # number of fast in-call retries before giving up
_LOGIN_RETRY_DELAY  = 2   # seconds between fast retries

# ── Dedicated thread pool for OTP monitors ────────────────────────────────────
# Keeps all OTP HTTP I/O and DB calls off the main bot-handler thread pool
# so 20 000 concurrent users never compete with background panel polling.
_OTP_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
    max_workers=24,
    thread_name_prefix="otp-monitor",
)


async def _otp_thread(func, *args):
    """Run *func* in the dedicated OTP executor (not the shared bot pool)."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_OTP_EXECUTOR, func, *args)

from config import (
    SMS_HADI_BASE,
    SMS_HADI_LOGIN_URL,
    SMS_HADI_SIGNIN_URL,
    SMS_HADI_AJAX_URL,
    SMS_HADI_USERNAME,
    SMS_HADI_PASSWORD,
    KONEKTA_BASE,
    KONEKTA_LOGIN_URL,
    KONEKTA_SIGNIN_URL,
    KONEKTA_USERNAME,
    KONEKTA_PASSWORD,
    MSI_SMS_BASE,
    MSI_SMS_LOGIN_URL,
    MSI_SMS_SIGNIN_URL,
    MSI_SMS_STATS_URL,
    MSI_SMS_AJAX_URL,
    MSI_SMS_USERNAME,
    MSI_SMS_PASSWORD,
    SMS_MONITOR_INTERVAL,
    NUMBER_PANEL_BASE,
    NUMBER_PANEL_LOGIN_URL,
    NUMBER_PANEL_SIGNIN_URL,
    NUMBER_PANEL_STATS_URL,
    NUMBER_PANEL_AJAX_URL,
    NUMBER_PANEL_USERNAME,
    NUMBER_PANEL_PASSWORD,
    NUMBER_PANEL_INTERVAL,
    PURPLE_SMS_BASE,
    PURPLE_SMS_LOGIN_URL,
    PURPLE_SMS_SIGNIN_URL,
    PURPLE_SMS_STATS_URL,
    PURPLE_SMS_AJAX_URL,
    PURPLE_SMS_USERNAME,
    PURPLE_SMS_PASSWORD,
    PROOF_SMS_BASE,
    PROOF_SMS_LOGIN_URL,
    PROOF_SMS_SIGNIN_URL,
    PROOF_SMS_USERNAME,
    PROOF_SMS_PASSWORD,
    LAMIX_SMS_BASE,
    LAMIX_SMS_LOGIN_URL,
    LAMIX_SMS_SIGNIN_URL,
    LAMIX_SMS_USERNAME,
    LAMIX_SMS_PASSWORD,
    SEVEN1TEL_BASE,
    SEVEN1TEL_LOGIN_URL,
    SEVEN1TEL_SIGNIN_URL,
    SEVEN1TEL_STATS_URL,
    SEVEN1TEL_AJAX_URL,
    SEVEN1TEL_USERNAME,
    SEVEN1TEL_PASSWORD,
    SEVEN1TEL_INTERVAL,
    MAIT_SMS_BASE,
    MAIT_SMS_LOGIN_URL,
    MAIT_SMS_SIGNIN_URL,
    MAIT_SMS_USERNAME,
    MAIT_SMS_PASSWORD,
    ZENTO_SMS_BASE,
    ZENTO_SMS_LOGIN_URL,
    ZENTO_SMS_SIGNIN_URL,
    ZENTO_SMS_USERNAME,
    ZENTO_SMS_PASSWORD,
    WOLF_SMS_BASE,
    WOLF_SMS_LOGIN_URL,
    WOLF_SMS_SIGNIN_URL,
    WOLF_SMS_STATS_URL,
    WOLF_SMS_AJAX_URL,
    WOLF_SMS_USERNAME,
    WOLF_SMS_PASSWORD,
    WOLF_SMS_INTERVAL,
    SHARK_SMS_BASE,
    SHARK_SMS_LOGIN_URL,
    SHARK_SMS_SIGNIN_URL,
    SHARK_SMS_STATS_URL,
    SHARK_SMS_AJAX_URL,
    SHARK_SMS_USERNAME,
    SHARK_SMS_PASSWORD,
    SHARK_SMS_INTERVAL,
    SMS_HADI2_USERNAME,
    SMS_HADI2_PASSWORD,
    SMS_HADI2_LOGIN_URL,
    SMS_HADI2_SIGNIN_URL,
    SMS_HADI2_STATS_URL,
    SMS_HADI2_AJAX_URL,
    KM_CARRIER_SMS_LOGIN_URL,
    KM_CARRIER_SMS_SIGNIN_URL,
    KM_CARRIER_SMS_STATS_URL,
    KM_CARRIER_SMS_AJAX_URL,
    KM_CARRIER_SMS_USERNAME,
    KM_CARRIER_SMS_PASSWORD,
    KM_CARRIER_SMS_INTERVAL,
)

logger = logging.getLogger(__name__)


async def _notify_admins_login_fail(bot, panel_name: str):
    """Send a login-failure alert to all admin users that have a known user_id."""
    try:
        from database import _get_all_admins_with_details
        admins = _get_all_admins_with_details()
        for admin in admins:
            uid = admin.get("user_id")
            if uid:
                try:
                    await bot.send_message(
                        chat_id=uid,
                        text=(
                            f"⚠️ *Panel Login Failed*\n\n"
                            f"*{panel_name}* could not log in.\n"
                            "The bot will keep retrying automatically.\n\n"
                            "_Check credentials or panel status._"
                        ),
                        parse_mode="Markdown",
                    )
                except Exception:
                    pass
    except Exception:
        pass


async def _notify_admins_login_success(bot, panel_name: str):
    """Send a login-success alert to all admins (after a previous failure)."""
    try:
        from database import _get_all_admins_with_details
        admins = _get_all_admins_with_details()
        for admin in admins:
            uid = admin.get("user_id")
            if uid:
                try:
                    await bot.send_message(
                        chat_id=uid,
                        text=(
                            f"✅ *Panel Login Successful*\n\n"
                            f"*{panel_name}* has successfully logged in.\n"
                            "The bot has started receiving SMS/OTP from this panel."
                        ),
                        parse_mode="Markdown",
                    )
                except Exception:
                    pass
    except Exception:
        pass


# ── Constants ─────────────────────────────────────────────────────────────────

SMS_HADI_REPORTS_URL = f"{SMS_HADI_BASE}/agent/SMSCDRStats"
SMS_HADI_AJAX_BASE   = f"{SMS_HADI_BASE}/agent/res/data_smscdr.php"

_USER_AGENT = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
    'AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/124.0.0.0 Safari/537.36'
)

# ── Shared helpers ────────────────────────────────────────────────────────────

_SKIP_WORDS = {
    's', 'mix', 'smsc', 'route', 'pack', 'pool', 'num', 'number',
    'sms', 'gsm', 'virtual', 'did',
}

# Telecom operator / network suffix words — stop country extraction here
_OPERATOR_STOP_WORDS = {
    'mtn', 'grand', 'mobile', 'telecom', 'cellular', 'network', 'wireless',
    'communications', 'orange', 'airtel', 'vodafone', 'zain', 'ooredoo',
    'etisalat', 'tmobile', 't-mobile', 'telecel', 'tigo', 'digicel', 'glo',
    'claro', 'movistar', 'entel', 'nextel', 'turkcell', 'telefonica',
    'beeline', 'megafon', 'safaricom', 'grameenphone', 'robi', 'banglalink',
    'teletalk', 'unitel', 'africell', 'expresso', 'moov', 'libyana',
    'almadar', 'sudatel', 'sudani', 'jawwal', 'palestel', 'premium',
    'standard', 'national', 'international', 'local', 'direct', 'special',
    'geo', 'geo2', 'tier', 'basic', 'plus', 'pro', 'fixed', 'landline',
    'voip', 'tollfree', 'toll', 'free', 'shared', 'service',
    'turk', 'telekom', 'mts', 'vimpelcom', 'canal', 'nine', 'xl',
}

# Known multi-word countries (longest first so we match greedily)
_MULTI_WORD_COUNTRIES = [
    'United Arab Emirates', 'United States', 'United Kingdom',
    'South Africa', 'South Korea', 'South Sudan', 'North Korea',
    'Saudi Arabia', 'Sri Lanka', 'New Zealand', 'Costa Rica',
    'Puerto Rico', 'El Salvador', 'Papua New Guinea', 'Burkina Faso',
    'Sierra Leone', 'Ivory Coast', 'Trinidad And Tobago',
    'Bosnia And Herzegovina', 'Dominican Republic', 'Czech Republic',
]

_OTP_PATTERNS = [
    # "628114 is your verification code"  /  "1914 is your Facebook verification code"
    re.compile(r'\b(\d{4,8})\s+is your\b[^.]{0,40}?\bcode\b', re.I),
    # "code: 123456"  /  "code is 123456"  /  "Your code 123456"
    re.compile(r'\b(?:code|otp|password|pin|token)\b[^\d]{0,20}(\d{4,8})', re.I),
    # "Your Viber code 896141"  /  "verification code: 123456"
    re.compile(r'(?:verification|verify|one.time|login|access)[^\d]{0,30}(\d{4,8})', re.I),
    # "G-123456" (Google)  /  "# 83976" (Facebook Spanish — space after # allowed)
    re.compile(r'(?:G-|#)\s*(\d{4,8})', re.I),
    # Multilingual code keyword BEFORE number:
    # Spanish "código", Portuguese "senha/código", French "code", Italian "codice",
    # German "Code", Arabic "رمز", Turkish "kod", etc.
    re.compile(r'\b(?:c[oó]digo|clave|senha|codice|kod\b|kode|doğrulama)[^\d]{0,20}(\d{4,8})', re.I),
    # Multilingual: number then code keyword (e.g. "83976 es tu código de Facebook")
    re.compile(r'(\d{4,8})[^.\n]{0,60}?\b(?:c[oó]digo|clave|senha|codice|kod\b|kode)\b', re.I),
    # "Your code is 42237697"
    re.compile(r'\b(?:your|the)\s+(?:\w+\s+)?(?:code|otp|pin)\s+(?:is\s+)?(\d{4,8})\b', re.I),
    # "use 123456 to"  /  "enter 123456"
    re.compile(r'\b(?:use|enter|input)\s+(\d{4,8})\b', re.I),
    # Broad fallback: any standalone 4-8 digit number (covers 5/7-digit OTPs missed above)
    re.compile(r'\b(\d{4,8})\b'),
]

_WEBSITE_PATTERNS = [
    (re.compile(r'\b(facebook|fb)\b', re.I),              'Facebook'),
    (re.compile(r'\b(instagram|ig)\b', re.I),              'Instagram'),
    (re.compile(r'\b(whatsapp)\b', re.I),                  'WhatsApp'),
    (re.compile(r'\b(telegram)\b', re.I),                  'Telegram'),
    (re.compile(r'\b(google|gmail|youtube)\b', re.I),      'Google'),
    (re.compile(r'\b(twitter|x\.com)\b', re.I),            'Twitter/X'),
    (re.compile(r'\b(tiktok)\b', re.I),                    'TikTok'),
    (re.compile(r'\b(snapchat)\b', re.I),                  'Snapchat'),
    (re.compile(r'\b(amazon)\b', re.I),                    'Amazon'),
    (re.compile(r'\b(netflix)\b', re.I),                   'Netflix'),
    (re.compile(r'\b(microsoft|outlook|hotmail)\b', re.I), 'Microsoft'),
    (re.compile(r'\b(apple|icloud|itunes)\b', re.I),       'Apple'),
    (re.compile(r'\b(paypal)\b', re.I),                    'PayPal'),
    (re.compile(r'\b(uber)\b', re.I),                      'Uber'),
    (re.compile(r'\b(linkedin)\b', re.I),                  'LinkedIn'),
    (re.compile(r'\b(binance)\b', re.I),                   'Binance'),
    (re.compile(r'\b(coinbase)\b', re.I),                  'Coinbase'),
    (re.compile(r'\b(discord)\b', re.I),                   'Discord'),
    (re.compile(r'\b(spotify)\b', re.I),                   'Spotify'),
    (re.compile(r'\b(shopify)\b', re.I),                   'Shopify'),
    (re.compile(r'\b(alibaba|aliexpress)\b', re.I),        'Alibaba'),
    (re.compile(r'\b(lazada)\b', re.I),                    'Lazada'),
    (re.compile(r'\b(grab)\b', re.I),                      'Grab'),
    (re.compile(r'\b(airbnb)\b', re.I),                    'Airbnb'),
    (re.compile(r'\b(ebay)\b', re.I),                      'eBay'),
    # ── Additional services seen on panels ───────────────────────────────────
    (re.compile(r'\b(viber)\b', re.I),                     'Viber'),
    (re.compile(r'\b(wechat|weixin)\b', re.I),             'WeChat'),
    (re.compile(r'\b(line)\b', re.I),                      'LINE'),
    (re.compile(r'\b(signal)\b', re.I),                    'Signal'),
    (re.compile(r'\b(shopee)\b', re.I),                    'Shopee'),
    (re.compile(r'\b(tokopedia)\b', re.I),                 'Tokopedia'),
    (re.compile(r'\b(truecaller)\b', re.I),                'Truecaller'),
    (re.compile(r'\b(bumble)\b', re.I),                    'Bumble'),
    (re.compile(r'\b(badoo)\b', re.I),                     'Badoo'),
    (re.compile(r'\b(threads)\b', re.I),                   'Threads'),
    (re.compile(r'\b(pinterest)\b', re.I),                 'Pinterest'),
    (re.compile(r'\b(reddit)\b', re.I),                    'Reddit'),
    (re.compile(r'\b(zoom)\b', re.I),                      'Zoom'),
    (re.compile(r'\b(steam)\b', re.I),                     'Steam'),
    (re.compile(r'\b(roblox)\b', re.I),                    'Roblox'),
    (re.compile(r'\b(gojek|gofood)\b', re.I),              'Gojek'),
    (re.compile(r'\b(imo)\b', re.I),                       'IMO'),
    (re.compile(r'\b(helo)\b', re.I),                      'Helo'),
    (re.compile(r'\b(bigo)\b', re.I),                      'Bigo'),
    (re.compile(r'\b(likee)\b', re.I),                     'Likee'),
]

# ── App name → animated sticker ID (APPEmojiSXSponsor pack) ─────────────────
# Identified via color-analysis of sticker thumbnails from the pack.
# WhatsApp ID confirmed by user. Others identified by brand-color matching.
# If an app is not in this map, NO service emoji is shown (no default sticker).
_APP_STICKER_MAP: dict[str, str] = {
    # ── Social / Messaging ───────────────────────────────────────────────────
    'whatsapp':    '6298480844214379008',  # green  — user-confirmed ✓
    'facebook':    '6069027232947379692',  # blue   — dist 23.4 from #1877F2
    'instagram':   '6068863345585299844',  # pink   — dist 40.2 from #E1306C
    'telegram':    '6068867859595927448',  # lt-blue — dist 6.6 from #2AABEE ✓
    'tiktok':      '6298708640689824023',  # dark   — TikTok black theme
    'viber':       '6068997975630160222',  # purple — dist 41.2 from #7360F2
    'twitter/x':   '6298350676640538162',  # blue   — dist 27.8 from #1DA1F2
    'snapchat':    '6069107140813921396',  # yellow — dist 65.3 from #FFFC00
    'discord':     '6068761580630188159',  # blurple — dist 52.3 from #5865F2
    'linkedin':    '6298800982486685996',  # dk-blue — dist 33.3 from #0077B5
    'signal':      '6071133841391623449',  # green  — dist 37.2 from #3A8C5C
    'threads':     '6069027232947379692',  # same as Facebook (Meta)
    'wechat':      '6068867859595927448',  # green chat (using Telegram green)
    'line':        '6068867859595927448',  # green  — LINE uses similar teal
    'imo':         '6068867859595927448',  # blue-green
    # ── Financial ────────────────────────────────────────────────────────────
    'amazon':      '6068904302393433242',  # orange — dist 49.6 from #FF9900
    'paypal':      '6068717883632917743',  # dk-blue — dist 34.0 from #003087
    'binance':     '6068867507408609199',  # gold/yellow
    'coinbase':    '6298800982486685996',  # blue
    # ── Shopping / E-commerce ────────────────────────────────────────────────
    'shopee':      '6068982165855543196',  # orange-red
    'tokopedia':   '6068867859595927448',  # green
    'lazada':      '6068810882559778610',  # pink/red
    'alibaba':     '6068982165855543196',  # orange-red
    'ebay':        '6068982165855543196',  # red/orange
    'shopify':     '6071133841391623449',  # green
    # ── Tech / Entertainment ─────────────────────────────────────────────────
    'google':      '6069097687590903402',  # blue  — dist 17.3 from Google blue
    'youtube':     '6068663500757016577',  # red   — dist 37.8 from #FF0000
    'netflix':     '6068663500757016577',  # red   — Netflix red
    'spotify':     '6300761828330840482',  # bright green — dist 43.9 (green)
    'microsoft':   '6298800982486685996',  # blue
    'apple':       '6068965101950477168',  # dark/black
    # ── Transport / Travel ───────────────────────────────────────────────────
    'uber':        '6068965101950477168',  # black — Uber brand color
    'grab':        '6071133841391623449',  # green — Grab brand color
    'airbnb':      '6068810882559778610',  # pink/red — Airbnb color
    'gojek':       '6071133841391623449',  # green — GoJek brand color
    # ── Social / Niche ───────────────────────────────────────────────────────
    'reddit':      '6068982165855543196',  # orange-red — dist 26.7 from Reddit
    'pinterest':   '6068824403116826236',  # red   — dist 35.6 from #E60023
    'truecaller':  '6068867859595927448',  # blue-green
    'bumble':      '6068867507408609199',  # yellow
    'badoo':       '6068810882559778610',  # pink
    'bigo':        '6068810882559778610',  # pink/red
    'likee':       '6068810882559778610',  # pink
    'helo':        '6068867859595927448',  # blue
    # ── Gaming ───────────────────────────────────────────────────────────────
    'steam':       '6298800982486685996',  # dark blue
    'roblox':      '6068663500757016577',  # red — Roblox red
    'zoom':        '6298800982486685996',  # blue — Zoom blue
}


def _get_service_sticker_html(website: str) -> str:
    """Return a <tg-emoji> HTML tag for the detected app, or '' if unknown.

    Priority: DB service_emojis (set via ⚙️ Add Service) → _APP_STICKER_MAP.
    The preprocessor's _add_custom_emojis() correctly skips existing <tg-emoji>
    blocks, so these tags are never double-processed.
    """
    key = (website or '').strip().lower()
    # Check DB emoji overrides first (set via Add Service admin panel)
    try:
        from database import _get_all_service_emojis
        db_emojis = _get_all_service_emojis()
        db_id = db_emojis.get(website) or db_emojis.get(key)
        if db_id:
            return f'<tg-emoji emoji-id="{db_id}">📱</tg-emoji>'
    except Exception:
        pass
    sticker_id = _APP_STICKER_MAP.get(key)
    if sticker_id:
        return f'<tg-emoji emoji-id="{sticker_id}">📱</tg-emoji>'
    return ''


# ── Service 2-letter short names (plain text — no animated sticker) ────────────
_SERVICE_SHORT_NAME_MAP: dict[str, str] = {
    'whatsapp':    'WA',
    'facebook':    'FB',
    'instagram':   'IG',
    'telegram':    'TG',
    'tiktok':      'TT',
    'viber':       'VB',
    'twitter/x':   'TW',
    'snapchat':    'SC',
    'discord':     'DC',
    'linkedin':    'LI',
    'signal':      'SG',
    'threads':     'TH',
    'wechat':      'WC',
    'line':        'LN',
    'imo':         'IM',
    'amazon':      'AZ',
    'paypal':      'PP',
    'binance':     'BN',
    'coinbase':    'CB',
    'shopee':      'SP',
    'tokopedia':   'TP',
    'lazada':      'LZ',
    'alibaba':     'AB',
    'ebay':        'EB',
    'shopify':     'SH',
    'google':      'GG',
    'youtube':     'YT',
    'netflix':     'NF',
    'spotify':     'ST',
    'microsoft':   'MS',
    'apple':       'AP',
    'uber':        'UB',
    'grab':        'GB',
    'airbnb':      'AN',
    'gojek':       'GJ',
    'reddit':      'RD',
    'pinterest':   'PT',
    'truecaller':  'TC',
    'bumble':      'BM',
    'badoo':       'BD',
    'bigo':        'BG',
    'likee':       'LK',
    'helo':        'HL',
    'steam':       'SM',
    'roblox':      'RX',
    'zoom':        'ZM',
}


def _get_service_short_name(website: str) -> str:
    """Return 2-letter plain-text short name for the service.
    Falls back to first 2 uppercase letters of the website name,
    or '??' for truly unknown services.
    """
    key = (website or '').strip().lower()
    if key in _SERVICE_SHORT_NAME_MAP:
        return _SERVICE_SHORT_NAME_MAP[key]
    if key and key not in ('unknown', ''):
        return key[:2].upper()
    return '??'


# ── Unicode / HTML-entity digit normaliser ────────────────────────────────────

def _normalise_captcha_text(raw: str) -> str:
    """Decode HTML entities and normalise digit/operator variants so every
    captcha regex can work on plain ASCII text."""
    # Step 1 — HTML entity decode (&amp; → &, &#43; → +, etc.)
    text = _html.unescape(raw)

    # Step 2 — Unicode digit → ASCII digit (e.g. Arabic-Indic ٣ → 3)
    _UNICODE_DIGIT_TABLES = [
        ('\u0660', '\u0669'),  # Arabic-Indic
        ('\u06f0', '\u06f9'),  # Extended Arabic-Indic (Urdu/Farsi)
        ('\u0966', '\u096f'),  # Devanagari
        ('\u09e6', '\u09ef'),  # Bengali
        ('\uff10', '\uff19'),  # Fullwidth
    ]
    for start, end in _UNICODE_DIGIT_TABLES:
        for i, ch in enumerate(start):
            text = text.replace(ch, str(i))
        # enumerate gives (index, char) for first char only → do range properly
    for start, _end in _UNICODE_DIGIT_TABLES:
        for i in range(10):
            text = text.replace(chr(ord(start) + i), str(i))

    # Step 3 — word-form operators → symbols
    text = re.sub(r'\bplus\b',     '+', text, flags=re.I)
    text = re.sub(r'\bminus\b',    '-', text, flags=re.I)
    text = re.sub(r'\btimes\b',    '*', text, flags=re.I)
    text = re.sub(r'\bmultiplied\s+by\b', '*', text, flags=re.I)
    text = re.sub(r'\bdivided\s+by\b',    '/', text, flags=re.I)
    text = re.sub(r'\bmod\b',      '%', text, flags=re.I)

    # Step 4 — Unicode operator variants → ASCII
    text = text.replace('\u00d7', '*')  # ×
    text = text.replace('\u00f7', '/')  # ÷
    text = text.replace('\u2212', '-')  # −
    text = text.replace('\u2715', '*')  # ✕
    text = text.replace('\u2013', '-')  # en-dash used as minus

    return text


def _solve_captcha(html: str) -> str | None:
    """Solve a simple arithmetic captcha embedded in an HTML page.

    Handles:
      • Standard ASCII digits + operators
      • HTML-entity-encoded operators (&#43;, &amp;, etc.)
      • Word-form operators (plus, minus, times, divided by)
      • Unicode digit blocks (Arabic-Indic, Devanagari, Bengali, …)
      • Unicode operator characters (×, ÷, −)
      • Extra whitespace / HTML tags between tokens
      • Safe division — returns None instead of raising on division by zero

    Returns the string result of the arithmetic, or None if unsolvable.
    """
    try:
        text = _normalise_captcha_text(html)

        _OPS = {
            '+': lambda a, b: a + b,
            '-': lambda a, b: a - b,
            '*': lambda a, b: a * b,
            '/': lambda a, b: a // b if b != 0 else None,
            '%': lambda a, b: a % b  if b != 0 else None,
        }

        # ── Specific patterns tried first (high precision) ───────────────────
        _SPECIFIC = [
            # "What is X OP Y"
            re.compile(r'[Ww]hat\s+is\s+(\d+)\s*([+\-*/%])\s*(\d+)', re.I),
            # "Calculate X OP Y"
            re.compile(r'[Cc]alculate\s+(\d+)\s*([+\-*/%])\s*(\d+)', re.I),
            # "X OP Y = ?"
            re.compile(r'(\d+)\s*([+\-*/%])\s*(\d+)\s*=\s*\?'),
            # value="" attribute containing the expression
            re.compile(r'value\s*=\s*["\']?\s*(\d+)\s*([+\-*/%])\s*(\d+)', re.I),
        ]

        for pattern in _SPECIFIC:
            m = pattern.search(text)
            if m:
                a, op, b = int(m.group(1)), m.group(2), int(m.group(3))
                result = _OPS.get(op, lambda _a, _b: None)(a, b)
                if result is not None:
                    return str(result)

        # ── Broad fallback: only search within captcha-labelled context ──────
        # This prevents false matches against unrelated numbers in the page
        # (e.g. phone numbers, timestamps, version strings).
        _CAPTCHA_SCOPE = re.compile(
            r'(?:captcha|capt|math.?question|verify|security.?code'
            r'|type\s+the\s+result|enter\s+the\s+result|solve|answer\s+to'
            r'|[Ww]hat\s+is|compute|evaluate).{0,400}',
            re.I | re.DOTALL,
        )
        scoped_text = ' '.join(_CAPTCHA_SCOPE.findall(text))
        if scoped_text:
            m = re.search(r'(?<!\w)(\d{1,4})\s*([+\-*/%])\s*(\d{1,4})(?!\w)', scoped_text)
            if m:
                a, op, b = int(m.group(1)), m.group(2), int(m.group(3))
                result = _OPS.get(op, lambda _a, _b: None)(a, b)
                if result is not None:
                    return str(result)

        return None

    except Exception as exc:
        logger.warning("_solve_captcha: unexpected error — %s", exc)
        return None


async def _jittered_sleep(base_seconds: float) -> None:
    """Sleep for base_seconds ± POLL_JITTER_FRAC to de-synchronise panel polls.

    Example: base=16, POLL_JITTER_FRAC=0.25 → actual sleep drawn uniformly
    from [12.0, 20.0] seconds.
    """
    jitter = base_seconds * POLL_JITTER_FRAC
    actual = base_seconds + random.uniform(-jitter, jitter)
    await asyncio.sleep(max(1.0, actual))


# ── Country dial-code → country name (used as fallback when panel
#    does not provide a range_name from which to extract a country) ──
_COUNTRY_DIAL_CODES: dict[str, str] = {
    # 1-digit
    "1": "United States", "7": "Russia",
    # 2-digit
    "20": "Egypt", "27": "South Africa", "30": "Greece", "31": "Netherlands",
    "32": "Belgium", "33": "France", "34": "Spain", "36": "Hungary",
    "39": "Italy", "40": "Romania", "41": "Switzerland", "43": "Austria",
    "44": "United Kingdom", "45": "Denmark", "46": "Sweden", "47": "Norway",
    "48": "Poland", "49": "Germany", "51": "Peru", "52": "Mexico",
    "53": "Cuba", "54": "Argentina", "55": "Brazil", "56": "Chile",
    "57": "Colombia", "58": "Venezuela", "60": "Malaysia", "61": "Australia",
    "62": "Indonesia", "63": "Philippines", "64": "New Zealand",
    "65": "Singapore", "66": "Thailand", "81": "Japan", "82": "South Korea",
    "84": "Vietnam", "86": "China", "90": "Turkey", "91": "India",
    "92": "Pakistan", "93": "Afghanistan", "94": "Sri Lanka", "95": "Myanmar",
    "98": "Iran",
    # 3-digit
    "211": "South Sudan", "212": "Morocco", "213": "Algeria", "216": "Tunisia",
    "218": "Libya", "220": "Gambia", "221": "Senegal", "222": "Mauritania",
    "223": "Mali", "224": "Guinea", "225": "Côte d'Ivoire",
    "226": "Burkina Faso", "227": "Niger", "228": "Togo", "229": "Benin",
    "230": "Mauritius", "231": "Liberia", "232": "Sierra Leone", "233": "Ghana",
    "234": "Nigeria", "235": "Chad", "236": "Central African Republic",
    "237": "Cameroon", "238": "Cape Verde", "239": "São Tomé",
    "240": "Equatorial Guinea", "241": "Gabon", "242": "Congo",
    "243": "DR Congo", "244": "Angola", "245": "Guinea-Bissau",
    "248": "Seychelles", "249": "Sudan", "250": "Rwanda", "251": "Ethiopia",
    "252": "Somalia", "253": "Djibouti", "254": "Kenya", "255": "Tanzania",
    "256": "Uganda", "257": "Burundi", "258": "Mozambique", "260": "Zambia",
    "261": "Madagascar", "262": "Réunion", "263": "Zimbabwe", "264": "Namibia",
    "265": "Malawi", "266": "Lesotho", "267": "Botswana", "268": "Eswatini",
    "269": "Comoros", "291": "Eritrea", "297": "Aruba", "298": "Faroe Islands",
    "299": "Greenland", "350": "Gibraltar", "351": "Portugal",
    "352": "Luxembourg", "353": "Ireland", "354": "Iceland", "355": "Albania",
    "356": "Malta", "357": "Cyprus", "358": "Finland", "359": "Bulgaria",
    "370": "Lithuania", "371": "Latvia", "372": "Estonia", "373": "Moldova",
    "374": "Armenia", "375": "Belarus", "376": "Andorra", "377": "Monaco",
    "378": "San Marino", "380": "Ukraine", "381": "Serbia", "382": "Montenegro",
    "383": "Kosovo", "385": "Croatia", "386": "Slovenia",
    "387": "Bosnia and Herzegovina", "389": "North Macedonia",
    "420": "Czech Republic", "421": "Slovakia", "423": "Liechtenstein",
    "500": "Falkland Islands", "501": "Belize", "502": "Guatemala",
    "503": "El Salvador", "504": "Honduras", "505": "Nicaragua",
    "506": "Costa Rica", "507": "Panama", "509": "Haiti", "591": "Bolivia",
    "592": "Guyana", "593": "Ecuador", "594": "French Guiana",
    "595": "Paraguay", "596": "Martinique", "597": "Suriname", "598": "Uruguay",
    "599": "Curaçao", "670": "East Timor", "673": "Brunei", "674": "Nauru",
    "675": "Papua New Guinea", "676": "Tonga", "677": "Solomon Islands",
    "678": "Vanuatu", "679": "Fiji", "680": "Palau", "682": "Cook Islands",
    "685": "Samoa", "686": "Kiribati", "687": "New Caledonia", "688": "Tuvalu",
    "689": "French Polynesia", "691": "Micronesia", "692": "Marshall Islands",
    "850": "North Korea", "852": "Hong Kong", "853": "Macau", "855": "Cambodia",
    "856": "Laos", "880": "Bangladesh", "886": "Taiwan", "960": "Maldives",
    "961": "Lebanon", "962": "Jordan", "963": "Syria", "964": "Iraq",
    "965": "Kuwait", "966": "Saudi Arabia", "967": "Yemen", "968": "Oman",
    "970": "Palestine", "971": "United Arab Emirates", "972": "Israel",
    "973": "Bahrain", "974": "Qatar", "975": "Bhutan", "976": "Mongolia",
    "977": "Nepal", "992": "Tajikistan", "993": "Turkmenistan",
    "994": "Azerbaijan", "995": "Georgia", "996": "Kyrgyzstan",
    "998": "Uzbekistan",
}

# ── Dial-code → ISO 2-letter country code ─────────────────────────────────────
_DIAL_CODE_TO_ISO: dict[str, str] = {
    "1": "US", "7": "RU",
    "20": "EG", "27": "ZA", "30": "GR", "31": "NL",
    "32": "BE", "33": "FR", "34": "ES", "36": "HU",
    "39": "IT", "40": "RO", "41": "CH", "43": "AT",
    "44": "GB", "45": "DK", "46": "SE", "47": "NO",
    "48": "PL", "49": "DE", "51": "PE", "52": "MX",
    "53": "CU", "54": "AR", "55": "BR", "56": "CL",
    "57": "CO", "58": "VE", "60": "MY", "61": "AU",
    "62": "ID", "63": "PH", "64": "NZ",
    "65": "SG", "66": "TH", "81": "JP", "82": "KR",
    "84": "VN", "86": "CN", "90": "TR", "91": "IN",
    "92": "PK", "93": "AF", "94": "LK", "95": "MM",
    "98": "IR",
    "211": "SS", "212": "MA", "213": "DZ", "216": "TN",
    "218": "LY", "220": "GM", "221": "SN", "222": "MR",
    "223": "ML", "224": "GN", "225": "CI",
    "226": "BF", "227": "NE", "228": "TG", "229": "BJ",
    "230": "MU", "231": "LR", "232": "SL", "233": "GH",
    "234": "NG", "235": "TD", "236": "CF",
    "237": "CM", "238": "CV", "239": "ST",
    "240": "GQ", "241": "GA", "242": "CG",
    "243": "CD", "244": "AO", "245": "GW",
    "248": "SC", "249": "SD", "250": "RW", "251": "ET",
    "252": "SO", "253": "DJ", "254": "KE", "255": "TZ",
    "256": "UG", "257": "BI", "258": "MZ", "260": "ZM",
    "261": "MG", "262": "RE", "263": "ZW", "264": "NA",
    "265": "MW", "266": "LS", "267": "BW", "268": "SZ",
    "269": "KM", "291": "ER", "297": "AW", "298": "FO",
    "299": "GL", "350": "GI", "351": "PT",
    "352": "LU", "353": "IE", "354": "IS", "355": "AL",
    "356": "MT", "357": "CY", "358": "FI", "359": "BG",
    "370": "LT", "371": "LV", "372": "EE", "373": "MD",
    "374": "AM", "375": "BY", "376": "AD", "377": "MC",
    "378": "SM", "380": "UA", "381": "RS", "382": "ME",
    "383": "XK", "385": "HR", "386": "SI",
    "387": "BA", "389": "MK",
    "420": "CZ", "421": "SK", "423": "LI",
    "500": "FK", "501": "BZ", "502": "GT",
    "503": "SV", "504": "HN", "505": "NI",
    "506": "CR", "507": "PA", "509": "HT", "591": "BO",
    "592": "GY", "593": "EC", "594": "GF",
    "595": "PY", "596": "MQ", "597": "SR", "598": "UY",
    "599": "CW", "670": "TL", "673": "BN", "674": "NR",
    "675": "PG", "676": "TO", "677": "SB",
    "678": "VU", "679": "FJ", "680": "PW", "682": "CK",
    "685": "WS", "686": "KI", "687": "NC", "688": "TV",
    "689": "PF", "691": "FM", "692": "MH",
    "850": "KP", "852": "HK", "853": "MO", "855": "KH",
    "856": "LA", "880": "BD", "886": "TW", "960": "MV",
    "961": "LB", "962": "JO", "963": "SY", "964": "IQ",
    "965": "KW", "966": "SA", "967": "YE", "968": "OM",
    "970": "PS", "971": "AE", "972": "IL",
    "973": "BH", "974": "QA", "975": "BT", "976": "MN",
    "977": "NP", "992": "TJ", "993": "TM",
    "994": "AZ", "995": "GE", "996": "KG",
    "998": "UZ",
}


def country_code_to_flag(iso: str) -> str:
    """Convert ISO 2-letter country code to flag emoji using Unicode
    regional indicator symbols (e.g. 'MM' → '🇲🇲')."""
    if not iso or len(iso) != 2:
        return "🌐"
    return ''.join(chr(0x1F1E6 + ord(c) - ord('A')) for c in iso.upper())


def _detect_iso_from_number(number: str) -> str:
    """Return the ISO 2-letter country code guessed from a phone number's
    leading dial-code. Tries 3-digit, then 2-digit, then 1-digit prefix."""
    if not number:
        return ''
    digits = re.sub(r'\D', '', str(number))
    if not digits:
        return ''
    for prefix_len in (3, 2, 1):
        if len(digits) >= prefix_len:
            iso = _DIAL_CODE_TO_ISO.get(digits[:prefix_len])
            if iso:
                return iso
    return ''


def _detect_sms_language(text: str) -> str:
    """Detect the natural language of an SMS body using Unicode script ranges."""
    if not text:
        return 'English'
    if re.search(r'[\u1000-\u109F]', text):
        return 'Burmese'
    if re.search(r'[\u0600-\u06FF]', text):
        return 'Arabic'
    if re.search(r'[\u0980-\u09FF]', text):
        return 'Bengali'
    if re.search(r'[\u0900-\u097F]', text):
        return 'Hindi'
    if re.search(r'[\u0400-\u04FF]', text):
        return 'Russian'
    if re.search(r'[\uAC00-\uD7AF]', text):
        return 'Korean'
    if re.search(r'[\u3040-\u30FF]', text):
        return 'Japanese'
    if re.search(r'[\u4E00-\u9FFF]', text):
        return 'Chinese'
    if re.search(r'[\u0E00-\u0E7F]', text):
        return 'Thai'
    if re.search(r'[\u0370-\u03FF]', text):
        return 'Greek'
    if re.search(r'[\u0590-\u05FF]', text):
        return 'Hebrew'
    return 'English'


# ── Service name → short code mapping ─────────────────────────────────────────
_SERVICE_SHORT_MAP: dict[str, str] = {
    # Social media
    'facebook':        'FB',
    'instagram':       'IG',
    'whatsapp':        'WA',
    'telegram':        'TG',
    'google':          'GG',
    'twitter':         'TW',
    'twitter/x':       'TW',
    'x':               'TW',
    'tiktok':          'TK',
    'snapchat':        'SC',
    'linkedin':        'LI',
    'discord':         'DC',
    'pinterest':       'PI',
    'reddit':          'RD',
    'youtube':         'YT',
    'wechat':          'WC',
    'line':            'LN',
    'viber':           'VB',
    'signal':          'SG',
    'threads':         'TH',
    # E-commerce / tech
    'amazon':          'AZ',
    'netflix':         'NF',
    'microsoft':       'MS',
    'apple':           'AP',
    'paypal':          'PP',
    'uber':            'UB',
    'shopify':         'SH',
    'alibaba':         'AL',
    'lazada':          'LZ',
    'grab':            'GR',
    'airbnb':          'AB',
    'ebay':            'EB',
    'shopee':          'SE',
    'daraz':           'DZ',
    'noon':            'NN',
    'tokopedia':       'TP',
    'flipkart':        'FK',
    'jio':             'JO',
    'swiggy':          'SW',
    'zomato':          'ZM',
    # Crypto / finance
    'binance':         'BN',
    'coinbase':        'CO',
    'bybit':           'BB',
    'okx':             'OK',
    'kucoin':          'KC',
    'kraken':          'KR',
    'huobi':           'HB',
    'bitget':          'BG',
    'gate':            'GT',
    'mexc':            'MX',
    # Delivery / transport
    'pathao':          'PT',
    'shohoz':          'SZ',
    'bkash':           'BK',
    'nagad':           'NG',
    'rocket':          'RK',
    'upay':            'UP',
    'celcoin':         'CC',
    'paytm':           'PM',
    'phonepe':         'PE',
    'gpay':            'GP',
    # Other common services
    'netflix':         'NF',
    'spotify':         'SP',
    'hulu':            'HL',
    'zoom':            'ZM',
    'slack':           'SL',
    'notion':          'NT',
    'dropbox':         'DB',
    'adobe':           'AD',
    'steam':           'ST',
    'playstation':     'PS',
    'xbox':            'XB',
    'twitch':          'TC',
    'roblox':          'RL',
}


def _get_service_short(website: str) -> str:
    """Return a 2-letter short code for a detected service/website name.

    Lookup is case-insensitive. Unknown services fall back to first 2
    consonant-like letters from the name (always 2 chars, never 3+).
    """
    if not website or website in ('—', '-', 'Unknown', ''):
        return 'OT'
    key = website.strip().lower()
    short = _SERVICE_SHORT_MAP.get(key)
    if short:
        return short
    # Strip non-alpha chars and take first 2 uppercase letters
    clean = re.sub(r'[^A-Za-z]', '', website)
    return clean[:2].upper() if len(clean) >= 2 else (clean.upper() or 'OT')


def _detect_country_from_number(number: str) -> str:
    """Return the country name guessed from a phone number's leading
    dial-code. Tries the 3-digit, then 2-digit, then 1-digit prefix.
    Returns '' when no match is found.
    """
    if not number:
        return ''
    digits = re.sub(r'\D', '', str(number))
    if not digits:
        return ''
    for prefix_len in (3, 2, 1):
        if len(digits) >= prefix_len:
            name = _COUNTRY_DIAL_CODES.get(digits[:prefix_len])
            if name:
                return name
    return ''


def _extract_country(range_name: str) -> str:
    """Extract only the country name from a range_name.
    Handles space-separated ('Syria Mtn Grand') and hyphen-separated
    ('Madagascar-Sacel-Cn-01') formats.
    """
    if not range_name:
        return ''
    cleaned = range_name.strip()

    # Handle fully hyphenated range names (no spaces): "Madagascar-Sacel-Cn-01"
    # Split by hyphen and use only the first segment as the candidate country.
    if '-' in cleaned and ' ' not in cleaned:
        cleaned = cleaned.split('-')[0].strip()
        if cleaned:
            return cleaned.title()
        return ''

    # Try to match known multi-word countries first (greedy, longest first)
    upper = cleaned.title()
    for country in _MULTI_WORD_COUNTRIES:
        if upper.startswith(country):
            return country

    # Otherwise take words one by one, stopping at operator/network keywords
    parts = cleaned.split()
    country_parts = []
    for part in parts:
        clean = part.strip('.,;:-()')
        if not clean:
            continue
        # Stop if digit found (e.g. "Zone1")
        if re.search(r'\d', clean):
            break
        lower = clean.lower()
        # Stop at short noise words
        if len(clean) <= 1 or lower in _SKIP_WORDS:
            break
        # Stop at telecom operator / suffix words
        if lower in _OPERATOR_STOP_WORDS:
            break
        country_parts.append(clean)
        # Allow at most 2 words for country name unless it's a known multi-word
        if len(country_parts) >= 2:
            break

    return ' '.join(country_parts).title() if country_parts else cleaned.split()[0].title()


def _extract_otp(sms_body: str) -> str:
    if not sms_body:
        return ''
    # Normalize hyphenated codes: "123-456" → "123456" (e.g. WhatsApp format)
    body = re.sub(r'\b(\d{3,4})-(\d{3,4})\b', r'\1\2', sms_body)
    for pat in _OTP_PATTERNS:
        m = pat.search(body)
        if m:
            return m.group(1)
    return ''


def _parse_panel_dt(dt_str: str) -> float:
    """Parse a panel SMS datetime string to a Unix timestamp (float).
    Returns 0.0 if parsing fails."""
    if not dt_str:
        return 0.0
    try:
        return datetime.strptime(dt_str.strip(), "%Y-%m-%d %H:%M:%S").timestamp()
    except Exception:
        return 0.0


def _extract_all_otps(sms_body: str) -> str:
    """Extract ALL OTP-like numbers (4-8 digits) from the SMS body, deduplicated."""
    if not sms_body:
        return '—'
    matches = re.findall(r'\b(\d{4,8})\b', sms_body)
    if not matches:
        return '—'
    seen: set[str] = set()
    unique: list[str] = []
    for m in matches:
        if m not in seen:
            seen.add(m)
            unique.append(m)
    return ' | '.join(unique)


def _get_col_cfg(panel_name: str, default_num: int, default_body: int) -> tuple[int, int]:
    """
    Read admin-assigned column indexes from DB.
    Falls back to hardcoded defaults if not configured.
    No restart needed — reads fresh on every polling cycle.
    """
    try:
        from database import _get_panel_column_config
        cfg = _get_panel_column_config(panel_name)
        if cfg:
            num  = int(cfg['number_col']) if 'number_col' in cfg else default_num
            body = int(cfg['body_col'])   if 'body_col'   in cfg else default_body
            return num, body
    except Exception:
        pass
    return default_num, default_body


def _build_sms_notify_text(number: str, website: str, sms_body: str,
                            old_balance=None, new_balance=None,
                            bonus_amount=None) -> str:
    """Build unified HTML notification text for all panels.

    First line: animated service emoji + service name (detected from SMS body
    or the website parameter). Falls back to 📱 if service is unknown.
    """
    otp       = _extract_otp(sms_body) or '—'
    safe_num  = _html.escape(str(number or ''))
    safe_otp  = _html.escape(str(otp))
    safe_body = _html.escape(str(sms_body or '—'))

    # ── Resolve the best service name ─────────────────────────────────────────
    # Priority: detected from SMS body (most reliable) → website param fallback
    detected  = _detect_website_from_body(sms_body)
    svc_name  = (detected if detected and detected != 'Unknown'
                 else (website or '').strip()) or ''

    # ── Build the header line: animated sticker + service name ─────────────────
    sticker   = _get_service_sticker_html(svc_name)
    if svc_name:
        header = f"<b>Service: {_html.escape(svc_name)}</b>"
    else:
        header = ""

    _get_num_tag   = '<tg-emoji emoji-id="6068772120479932812">➕</tg-emoji>'
    _otp_tag       = '<tg-emoji emoji-id="6068876810307772648">🔑</tg-emoji>'
    _full_msg_tag  = '<tg-emoji emoji-id="6068727156467309816">💬</tg-emoji>'

    parts = []
    if header:
        parts.append(header)
        parts.append("")
    parts += [
        f"{_get_num_tag}GET NUMBER: +{safe_num}",
        "",
        f"{_otp_tag} OTP : <code>{safe_otp}</code>",
        "",
        f"{_full_msg_tag} Full Message :",
        f"<code>{safe_body}</code>",
    ]
    if bonus_amount is not None and new_balance is not None:
        parts += [
            "",
            f"💰 +{bonus_amount:.2f}৳ »»»–»»» {new_balance:.2f}৳ 💸",
        ]
    parts += [
        "",
        f"😅 Thanks For using @{_BOT_USERNAME}",
    ]
    return "\n".join(parts)


# ── Cross-panel group-broadcast deduplication ─────────────────────────────────
# Prevents the same SMS from being forwarded to the group more than once when
# multiple panels (e.g. Konekta + MSI SMS) see the same record simultaneously.
_GROUP_BROADCAST_SEEN: set[str] = set()
_GROUP_BROADCAST_LOCK = asyncio.Lock()   # makes check-and-claim atomic


def _group_broadcast_key(number: str, sms_body: str) -> str:
    """Stable cross-panel key for a single SMS event.

    Intentionally excludes panel_name and dt_str so that the same SMS
    detected by multiple panels (even with slightly different timestamps)
    is treated as one event and sent to the group only once.
    """
    return hashlib.sha256(
        f"grp:{number}:{sms_body}".encode()
    ).hexdigest()


async def _broadcast_to_groups(
    bot, panel_name: str, grp_text: str, grp_markup,
    dt_str: str = '', number: str = '', sms_body: str = '',
):
    """Send the OTP notification to the main group AND every extra group
    independently — failure of one does not block the others. Extra-group
    sends run concurrently for speed (important under 15k user load).

    Cross-panel deduplication: uses a module-level in-memory set PLUS the
    persistent DB so that only the FIRST panel to reach this call actually
    broadcasts — even across bot restarts."""

    # ── Cross-panel duplicate guard (atomic via asyncio.Lock) ────────────────
    if number and sms_body:
        from database import _is_otp_delivered, _mark_otp_delivered
        gkey = _group_broadcast_key(number, sms_body)
        # Fast in-memory check without lock (cheap early exit)
        if gkey in _GROUP_BROADCAST_SEEN:
            logger.info(
                f"{panel_name}: group broadcast skipped — "
                "already sent by another panel (in-memory)."
            )
            return
        # Acquire lock so only one panel can check-and-claim at a time
        async with _GROUP_BROADCAST_LOCK:
            # Re-check inside lock (another panel may have claimed while waiting)
            if gkey in _GROUP_BROADCAST_SEEN:
                logger.info(
                    f"{panel_name}: group broadcast skipped — "
                    "already sent by another panel (lock re-check)."
                )
                return
            # Persistent DB check (survives restarts)
            if await _otp_thread(_is_otp_delivered, gkey):
                _GROUP_BROADCAST_SEEN.add(gkey)
                logger.info(
                    f"{panel_name}: group broadcast skipped — "
                    "already sent by another panel (DB check)."
                )
                return
            # Claim atomically — mark BOTH before releasing lock
            _GROUP_BROADCAST_SEEN.add(gkey)
            await _otp_thread(_mark_otp_delivered, gkey)

    # ── Default group (always sends, no config needed) ───────────────────────
    from config import DEFAULT_GROUP_CHAT_ID
    all_chat_ids: list[int | str] = []
    if DEFAULT_GROUP_CHAT_ID:
        all_chat_ids.append(DEFAULT_GROUP_CHAT_ID)

    # ── Extra groups (admin panel থেকে add করা) ──────────────────────────────
    try:
        from database import _get_all_extra_groups
        extra_groups = await _otp_thread(_get_all_extra_groups)
        for eg in (extra_groups or []):
            cid = eg.get('chat_id')
            if cid and cid not in all_chat_ids:
                all_chat_ids.append(cid)
    except Exception as _ege2:
        logger.warning(f"{panel_name}: extra groups fetch failed — {_ege2}")

    if not all_chat_ids:
        logger.warning(f"{panel_name}: no groups configured — message not sent.")
        return

    async def _send_one(chat_id):
        try:
            await bot.send_message(chat_id=chat_id, text=grp_text,
                                   parse_mode='HTML', reply_markup=grp_markup)
        except Exception as _ege:
            logger.warning(f"{panel_name}: group {chat_id} send failed — {_ege}")

    await asyncio.gather(*(_send_one(cid) for cid in all_chat_ids),
                         return_exceptions=True)


def _build_group_notify_text(number: str, country: str, website: str, otp: str, sms_body: str) -> tuple:
    """Build the unified group-broadcast message used by all panels.

    Format (known app):
        {flag}#ISO <tg-emoji id=APP>📱</tg-emoji>📱{first_3}💯{last_4}

    Format (unknown app — no default sticker):
        {flag}#ISO 📱{first_3}💯{last_4}

    Service emoji: raw <tg-emoji> tags from _APP_STICKER_MAP (APPEmojiSXSponsor
    pack). The preprocessor skips existing <tg-emoji> blocks — no double-wrap.
    The old default 📱 animated sticker (6069122383652856477) is NOT used.

    Buttons:
        Row 1 — [OTP_CODE]  icon_custom_emoji_id=6068876810307772648  (copy)
        Row 2 — [NUMBER]  [CHANNEL]

    Returns (text, InlineKeyboardMarkup).
    """
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup, CopyTextButton

    # ── ISO code + flag char ─────────────────────────────────────────────────
    iso           = _detect_iso_from_number(number) or 'XX'
    flag_char     = country_code_to_flag(iso)
    country_short = iso

    # ── Number format: first-3 + 💯 + last-4 (whole line is wrapped in <b>) ──
    digits_only = re.sub(r'\D', '', str(number or ''))
    if len(digits_only) >= 7:
        num_display = f"{digits_only[:3]}💯{digits_only[-4:]}"
    elif len(digits_only) >= 4:
        num_display = f"{digits_only[:3]}💯{digits_only[3:]}"
    elif digits_only:
        num_display = digits_only
    else:
        num_display = '—'

    # ── OTP value ────────────────────────────────────────────────────────────
    # Primary: use the pattern-matched OTP passed in.
    # Fallback: if pattern matching missed (returns '' or None), scan the SMS
    # body for ANY 4-8 digit number using the broad _extract_all_otps regex.
    # This ensures the button always shows a code even for exotic SMS formats.
    if otp and otp.strip():
        otp_clean = otp.strip()
    else:
        fallback = _extract_all_otps(sms_body) if sms_body else '—'
        otp_clean = fallback if fallback != '—' else '—'

    # ── Service 2-letter short name (plain text — no animated sticker) ──────
    # _get_service_short_name returns e.g. 'FB', 'IG', 'WA', 'TG', '??' etc.
    service_short = _get_service_short_name(website or 'Unknown')

    # ── Message text ─────────────────────────────────────────────────────────
    # Full line is bold via <b> HTML tags (parse_mode=HTML set by preprocessor).
    # Using <b> directly avoids *...* nesting issues with the number segment.
    # flag_char and 💯 are animated by _add_custom_emojis inside the <b> tag.
    # Final look: 🇲🇳#MN #FB #976💯7308  (all bold)
    text = f"<b>{flag_char}#{country_short} #{service_short} #{num_display}</b>"

    # ── Buttons ──────────────────────────────────────────────────────────────
    # Animated emoji IDs (from SXEmojisSXSponsor pack):
    #   🔑 OTP   button icon — 6068876810307772648
    #   😉 NUMBER  button icon — 6068827654407070288
    #   🔝 CHANNEL button icon — 6068892242125266188
    from database import _get_setting
    lnk_number  = _get_setting("otp_btn_number",  "")
    lnk_channel = _get_setting("otp_btn_channel", "")

    # copy_text must be a plain code string — use only the first code if
    # _extract_all_otps returned multiple values separated by " | "
    copy_otp = otp_clean.split(' | ')[0] if ' | ' in otp_clean else otp_clean

    otp_btn = InlineKeyboardButton(
        f"{otp_clean}",
        copy_text=CopyTextButton(text=copy_otp),
        api_kwargs={"style": "success", "icon_custom_emoji_id": "6068876810307772648"},
    )

    first_row = [otp_btn]
    if sms_body and sms_body.strip():
        sms_text = sms_body.strip()
        first_row.append(InlineKeyboardButton(
            "SMS",
            copy_text=CopyTextButton(text=sms_text),
            api_kwargs={"style": "primary", "icon_custom_emoji_id": "6068974954605453642"},
        ))

    rows = [first_row]
    link_row = []
    if lnk_number:
        link_row.append(InlineKeyboardButton(
            "NUMBER",
            url=lnk_number,
            api_kwargs={"style": "primary", "icon_custom_emoji_id": "6068827654407070288"},
        ))
    if lnk_channel:
        link_row.append(InlineKeyboardButton(
            "CHANNEL",
            url=lnk_channel,
            api_kwargs={"style": "danger", "icon_custom_emoji_id": "6068892242125266188"},
        ))
    if link_row:
        rows.append(link_row)

    markup = InlineKeyboardMarkup(rows)
    return text, markup


def _detect_website_from_body(message: str) -> str:
    if not message:
        return 'Unknown'
    for pattern, name in _WEBSITE_PATTERNS:
        if pattern.search(message):
            return name
    url_match = re.search(
        r'(?:https?://)?(?:www\.)?([a-zA-Z0-9\-]+)\.[a-z]{2,}', message
    )
    if url_match:
        domain = url_match.group(1).capitalize()
        if len(domain) > 2:
            return domain
    return 'Unknown'


def _new_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({'User-Agent': _USER_AGENT})
    return s


# ── SMS Hadi AJAX URL builder ─────────────────────────────────────────────────

def _build_hadi_ajax_url(sesskey: str, days_back: int = 7) -> str:
    now = _bd_now()
    d1  = (now - timedelta(days=days_back)).strftime('%Y-%m-%d 00:00:00')
    d2  = now.strftime('%Y-%m-%d 23:59:59')
    params = urlencode({
        'fdate1': d1, 'fdate2': d2,
        'frange': '', 'fclient': '', 'fnum': '', 'fcli': '',
        'fgdate': '', 'fgmonth': '', 'fgrange': '', 'fgclient': '',
        'fgnumber': '', 'fgcli': '',
        'fg': '0',
        'sesskey': sesskey,
        'iDisplayStart': '0',
        'iDisplayLength': '999999',
        'iSortCol_0': '0',
        'sSortDir_0': 'desc',
    })
    return f"{SMS_HADI_AJAX_BASE}?{params}"


# ── Client panel AJAX URL builder (Konekta / MSI SMS) ────────────────────────

def _build_client_ajax_url(ajax_base: str, days_back: int = 7) -> str:
    now = _bd_now()
    d1  = (now - timedelta(days=days_back)).strftime('%Y-%m-%d 00:00:00')
    d2  = now.strftime('%Y-%m-%d 23:59:59')
    params = urlencode({
        'fdate1': d1, 'fdate2': d2,
        'frange': '', 'fnum': '', 'fcli': '',
        'fgdate': '', 'fgmonth': '', 'fgrange': '', 'fgnumber': '', 'fgcli': '',
        'fg': '0',
        'iDisplayStart': '0',
        'iDisplayLength': '999999',
        'iSortCol_0': '0',
        'sSortDir_0': 'desc',
    })
    return f"{ajax_base}?{params}"


# ══════════════════════════════════════════════════════════════════════════════
# SMS Hadi Monitor  (sesskey-based)
# ══════════════════════════════════════════════════════════════════════════════

class OTPMonitor:
    """Background monitor for SMS Hadi panel (sesskey-based auth)."""

    def __init__(self):
        self.panel_name     = 'SMS Hadi'
        self.interval       = SMS_MONITOR_INTERVAL
        self.retry_interval = 60
        self._running       = False
        self._task          = None
        self._seen_keys: set[str] = set()
        self._is_first_poll = True
        self.session: requests.Session | None = None
        self.logged_in      = False
        self._sesskey       = None
        self._manual_only   = False  # If True, _loop will not auto-login (set after Session Cleanup)
        self._username      = SMS_HADI_USERNAME
        self._password      = SMS_HADI_PASSWORD
        self._latest_record = None   # cached latest SMS for get_latest_today()

    def set_interval(self, seconds: int):
        """Update the polling interval live (no restart needed)."""
        self.interval = max(1, int(seconds))

    def set_retry_interval(self, seconds: int):
        """Update the login-retry interval live (no restart needed)."""
        self.retry_interval = max(1, int(seconds))

    def _refresh_credentials(self):
        """Read latest credentials from DB so admin-panel changes take effect."""
        try:
            from database import _get_panel_by_name
            p = _get_panel_by_name(self.panel_name)
            if p and p.get('username'):
                self._username = p['username']
                self._password = p['password']
        except Exception:
            pass

    def _login(self) -> bool:
        self._refresh_credentials()
        if not self._username or not self._password:
            logger.warning("OTPMonitor: credentials not set.")
            return False
        last_reason = "unknown"
        for attempt in range(1, _LOGIN_FAST_RETRIES + 1):
            try:
                # Fresh Session Reload — drop any old cookies and rebuild
                self.session = _new_session()
                r1 = self.session.get(SMS_HADI_LOGIN_URL, timeout=15)
                captcha = _solve_captcha(r1.text)
                if captcha is None:
                    last_reason = "captcha unsolvable"
                    logger.warning(
                        f"OTPMonitor: Attempt {attempt}/{_LOGIN_FAST_RETRIES} — {last_reason}."
                    )
                    if attempt < _LOGIN_FAST_RETRIES:
                        time.sleep(_LOGIN_RETRY_DELAY)
                    continue
                logger.info(
                    f"OTPMonitor: Attempt {attempt}/{_LOGIN_FAST_RETRIES} — Captcha solved → {captcha}"
                )
                r2 = self.session.post(
                    SMS_HADI_SIGNIN_URL,
                    data={'username': self._username, 'password': self._password, 'capt': captcha},
                    headers={'Referer': SMS_HADI_LOGIN_URL},
                    timeout=15, allow_redirects=True,
                )
                if 'login' in r2.url.lower():
                    last_reason = "login rejected"
                    logger.warning(
                        f"OTPMonitor: Attempt {attempt}/{_LOGIN_FAST_RETRIES} — {last_reason}."
                    )
                    if attempt < _LOGIN_FAST_RETRIES:
                        time.sleep(_LOGIN_RETRY_DELAY)
                    continue
                self.logged_in = True
                logger.info(
                    f"OTPMonitor: Logged in successfully (attempt {attempt}/{_LOGIN_FAST_RETRIES})."
                )
                return True
            except Exception as exc:
                last_reason = f"exception: {exc}"
                logger.warning(
                    f"OTPMonitor: Attempt {attempt}/{_LOGIN_FAST_RETRIES} — {last_reason}"
                )
                if attempt < _LOGIN_FAST_RETRIES:
                    time.sleep(_LOGIN_RETRY_DELAY)
        logger.error(
            f"OTPMonitor: All {_LOGIN_FAST_RETRIES} login attempts failed — {last_reason}"
        )
        self.logged_in = False
        return False

    def _extract_sesskey(self) -> bool:
        try:
            now = _bd_now()
            r = self.session.post(
                SMS_HADI_REPORTS_URL,
                data={
                    'fdate1': (now - timedelta(days=1)).strftime('%Y-%m-%d 00:00:00'),
                    'fdate2': now.strftime('%Y-%m-%d 23:59:59'),
                    'fnum': '', 'fcli': '', 'frange': '', 'fclient': '',
                },
                headers={'Referer': SMS_HADI_REPORTS_URL},
                timeout=20,
            )
            if 'login' in r.url.lower():
                self.logged_in = False
                return False
            m = re.search(
                r'"sAjaxSource"\s*:\s*"res/data_smscdr\.php[^"]*sesskey=([^"&]+)"',
                r.text
            )
            if not m:
                logger.error("OTPMonitor: sesskey not found.")
                return False
            self._sesskey = m.group(1)
            logger.info("OTPMonitor: sesskey extracted successfully.")
            return True
        except Exception as exc:
            logger.error(f"OTPMonitor: _extract_sesskey error — {exc}")
            return False

    def _fetch_individual_records(self) -> list[dict] | None:
        if not self._sesskey:
            return None
        try:
            url = _build_hadi_ajax_url(self._sesskey)
            r = self.session.get(
                url,
                headers={'Referer': SMS_HADI_REPORTS_URL, 'X-Requested-With': 'XMLHttpRequest'},
                timeout=25,
            )
            if 'login' in r.url.lower():
                self.logged_in = False
                return None
            data = r.json()
            rows = data.get('aaData', [])
            num_col, body_col = _get_col_cfg(self.panel_name, 2, 5)
            website_col = 4
            results = []
            for row in rows:
                if not isinstance(row, list) or len(row) < 3:
                    continue
                dt_str     = str(row[0]).strip() if row[0] else ''
                range_name = str(row[1]).strip() if row[1] else ''
                number     = re.sub(r'[^\d]', '', str(row[num_col])) if len(row) > num_col and row[num_col] else ''
                website    = (str(row[website_col]).strip() if len(row) > website_col and row[website_col] else '') or 'Unknown'
                sms_body   = str(row[body_col]).strip() if len(row) > body_col and row[body_col] else ''
                detected = _detect_website_from_body(sms_body)
                if detected and detected != 'Unknown':
                    website = detected
                if not number or not dt_str:
                    continue
                results.append({
                    'datetime': dt_str, 'range_name': range_name,
                    'number': number, 'website': website, 'sms_body': sms_body,
                })
            logger.info(f"OTPMonitor: Fetched {len(results)} SMS records.")
            return results
        except Exception as exc:
            logger.error(f"OTPMonitor: _fetch_individual_records error — {exc}")
            return None

    def fetch_24h_count(self, website_name: str) -> int:
        if not self._sesskey or not self.logged_in:
            return -1
        try:
            now = _bd_now()
            d1  = (now - timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S')
            d2  = now.strftime('%Y-%m-%d %H:%M:%S')
            url = (
                f"{SMS_HADI_BASE}/agent/res/data_smscdr.php?"
                + urlencode({
                    'fdate1': d1, 'fdate2': d2,
                    'frange': '', 'fclient': '', 'fnum': '', 'fcli': '',
                    'fgdate': '1', 'fgmonth': '1', 'fgrange': '1',
                    'fgclient': '1', 'fgnumber': '1', 'fgcli': '1',
                    'fg': '1', 'sesskey': self._sesskey,
                    'iDisplayStart': '0', 'iDisplayLength': '999999',
                })
            )
            r = self.session.get(
                url,
                headers={'Referer': f"{SMS_HADI_BASE}/agent/SMSCDRStats",
                         'X-Requested-With': 'XMLHttpRequest'},
                timeout=20,
            )
            if 'login' in r.url.lower():
                self.logged_in = False
                return -1
            data  = r.json()
            rows  = data.get('aaData', [])
            total = 0
            for row in rows:
                if not isinstance(row, list) or len(row) < 7:
                    continue
                # col[4]=Client (website name), col[6]=payout count
                client = str(row[4]).strip() if len(row) > 4 and row[4] else ''
                if client.lower() == website_name.lower():
                    try:
                        total += int(str(row[6]).strip())
                    except (ValueError, TypeError):
                        pass
            return total
        except Exception as exc:
            logger.error(f"OTPMonitor: fetch_24h_count error — {exc}")
            return -1

    def get_latest_today(self) -> 'dict | None':
        """Always fetch fresh live data from SMS Hadi. Returns cached only if session is down."""
        if not self.logged_in or not self.session or not self._sesskey:
            return None
        try:
            now = _bd_now()
            params = urlencode({
                'fdate1': (now - timedelta(days=7)).strftime('%Y-%m-%d 00:00:00'),
                'fdate2': now.strftime('%Y-%m-%d 23:59:59'),
                'frange': '', 'fclient': '', 'fnum': '', 'fcli': '',
                'fgdate': '', 'fgmonth': '', 'fgrange': '', 'fgclient': '',
                'fgnumber': '', 'fgcli': '', 'fg': '0',
                'sesskey': self._sesskey,
                'iDisplayStart': '0', 'iDisplayLength': '999999',
                'iSortCol_0': '0', 'sSortDir_0': 'desc',
            })
            r = self.session.get(
                f"{SMS_HADI_AJAX_BASE}?{params}",
                headers={'Referer': SMS_HADI_REPORTS_URL, 'X-Requested-With': 'XMLHttpRequest'},
                timeout=20,
            )
            if 'login' in r.url.lower():
                self.logged_in = False
                logger.warning("OTPMonitor: get_latest_today — session expired, falling back to cache.")
                return None
            rows = r.json().get('aaData', [])
            valid = []
            for row in rows:
                if not isinstance(row, list) or len(row) < 6:
                    continue
                dt_str = str(row[0]).strip() if row[0] else ''
                if not re.match(r'\d{4}-\d{2}-\d{2}', dt_str):
                    continue
                number = re.sub(r'[^\d]', '', str(row[2])) if row[2] else ''
                if not number:
                    continue
                sms_body = str(row[5]).strip() if len(row) > 5 and row[5] else ''
                # col[3]=CLI (sender), col[4]=Client (website name)
                valid.append({
                    'dt': dt_str, 'number': number,
                    'website': str(row[4]).strip() if len(row) > 4 and row[4] else '',
                    'range_name': str(row[1]).strip() if row[1] else '',
                    'sms_body': sms_body,
                })
            if not valid:
                logger.info("OTPMonitor: get_latest_today — no valid rows in live fetch, falling back to cache.")
                return None
            valid.sort(key=lambda x: x['dt'], reverse=True)
            rec = valid[0]
            uid = hashlib.md5(f"{rec['dt']}:{rec['number']}:{rec['sms_body']}".encode()).hexdigest()
            result = {
                'id': uid, 'datetime': rec['dt'], 'number': rec['number'],
                'website': rec['website'] or _detect_website_from_body(rec['sms_body']),
                'country': _extract_country(rec['range_name']),
                'otp': _extract_otp(rec['sms_body']), 'message': rec['sms_body'],
                'received_at': rec['dt'], 'panel_name': self.panel_name,
            }
            return result
        except Exception as exc:
            logger.error(f"OTPMonitor: get_latest_today error — {exc}. Falling back to cache.")
            return None

    async def _notify_user(self, bot, number: str, website: str, otp: str, sms_body: str, delivery_key: str = '', sms_dt_str: str = ''):
        try:
            from database import (
                _get_recent_user_by_number as _gub,
                _get_otp_bonus_settings,
                _has_otp_bonus_received, _record_otp_bonus,
                _get_effective_otp_bonus, _get_user_balance,
                _get_notify_window,
            )
            sms_ts = _parse_panel_dt(sms_dt_str)
            _win_sec = _get_notify_window() * 60
            _gub_ts = lambda n: _gub(n, max_age_seconds=_win_sec, sms_ts=sms_ts)
            uid = await _otp_thread(_gub_ts, number)
            if not uid:
                uid = await _otp_thread(_gub_ts, '+' + number)
            if uid and bot:
                bonus_amount_credited = None
                new_balance = None
                if delivery_key:
                    bonus_cfg = await _otp_thread(_get_otp_bonus_settings)
                    if bonus_cfg['enabled']:
                        already = await _otp_thread(_has_otp_bonus_received, delivery_key)
                        if not already:
                            effective_amount = await _otp_thread(_get_effective_otp_bonus, number, bonus_cfg['amount'])
                            credited = await _otp_thread(
                                _record_otp_bonus, uid, delivery_key, effective_amount
                            )
                            if credited:
                                new_balance = await _otp_thread(_get_user_balance, uid)
                                bonus_amount_credited = effective_amount
                                logger.info(f"OTPMonitor: OTP bonus BDT {effective_amount:.2f} credited to user {uid}")
                notify_text = _build_sms_notify_text(
                    number, website, sms_body,
                    bonus_amount=bonus_amount_credited,
                    new_balance=new_balance,
                )
                await bot.send_message(chat_id=uid, text=notify_text, parse_mode='HTML')
                logger.info(f"OTPMonitor: Notified user {uid} about SMS for +{number}")
        except Exception as notify_exc:
            logger.warning(f"OTPMonitor: Could not notify user — {notify_exc}")

    async def _loop(self, bot):
        from database import (_is_otp_delivered, _mark_otp_delivered,
                              _update_panel_status, _is_panel_enabled)
        pname = self.panel_name
        logger.info("OTPMonitor: Starting.")
        # ── Wait until panel is enabled before attempting login
        while self._running and (
            not _is_panel_enabled(pname)
            or getattr(self, '_manual_only', False)
        ):
            await asyncio.sleep(5)
        if not self._running:
            return
        ok = await _otp_thread(self._login)
        _login_fail_notified = False
        while not ok and self._running:
            logger.warning(f"OTPMonitor: Login failed — retrying in {self.retry_interval}s…")
            await _otp_thread(_update_panel_status, pname, False, None, 'Login failed — retrying')
            if not _login_fail_notified:
                await _notify_admins_login_fail(bot, pname)
                _login_fail_notified = True
            await asyncio.sleep(self.retry_interval)
            while self._running and (
                not _is_panel_enabled(pname)
                or getattr(self, '_manual_only', False)
            ):
                await asyncio.sleep(5)
            if not self._running:
                return
            ok = await _otp_thread(self._login)
        if not self._running:
            return
        await _otp_thread(_update_panel_status, pname, True)
        if _login_fail_notified:
            await _notify_admins_login_success(bot, pname)
            _login_fail_notified = False
        ok = await _otp_thread(self._extract_sesskey)
        if not ok:
            logger.error("OTPMonitor: Could not extract sesskey.")
            return

        while self._running:
            if not _is_panel_enabled(pname) or getattr(self, '_manual_only', False):
                await asyncio.sleep(5)
                continue
            try:
                records = await _otp_thread(self._fetch_individual_records)

                if records is None:
                    logger.info("OTPMonitor: Session expired — re-logging in …")
                    _cf = getattr(self, '_consec_failures', 0) + 1
                    setattr(self, '_consec_failures', _cf)
                    _back = min(
                        BACKOFF_BASE_SECS * (BACKOFF_MULTIPLIER ** min(_cf - 1, 6)),
                        BACKOFF_MAX_SECS,
                    )
                    if _cf > 1:
                        logger.warning(
                            "OTPMonitor: consecutive failure #%d — extra back-off %.0fs", _cf, _back
                        )
                    # Wait if in manual-only mode (set after Session Cleanup)
                    while self._running and getattr(self, '_manual_only', False):
                        await asyncio.sleep(5)
                    if not self._running:
                        return
                    await _midnight_relogin_jitter(pname)
                    ok = await _otp_thread(self._login)
                    if ok:
                        await _otp_thread(_update_panel_status, pname, True)
                        await _otp_thread(self._extract_sesskey)
                    await _jittered_sleep(self.interval + _back)
                    continue
                setattr(self, '_consec_failures', 0)
                await _otp_thread(_update_panel_status, pname, True, len(records))

                # Always update _latest_record with the most recent SMS from each poll
                if records:
                    _r0 = records[0]
                    uid0 = hashlib.md5(f"{_r0['datetime']}:{_r0['number']}:{_r0['sms_body']}".encode()).hexdigest()
                    self._latest_record = {
                        'id': uid0, 'datetime': _r0['datetime'], 'number': _r0['number'],
                        'website': _r0['website'] or _detect_website_from_body(_r0['sms_body']),
                        'country': _extract_country(_r0['range_name']),
                        'otp': _extract_otp(_r0['sms_body']), 'message': _r0['sms_body'],
                        'received_at': _r0['datetime'], 'panel_name': pname,
                    }

                for rec in records:
                    dt_str     = rec['datetime']
                    range_name = rec['range_name']
                    number     = rec['number']
                    website    = rec['website']
                    sms_body   = rec['sms_body']

                    delivery_key = hashlib.sha256(
                        f"sms:{number}:{sms_body}".encode()
                    ).hexdigest()

                    if delivery_key in self._seen_keys:
                        continue
                    already = await _otp_thread(_is_otp_delivered, delivery_key)
                    if already:
                        self._seen_keys.add(delivery_key)
                        if self._is_first_poll and getattr(self, '_grp_backfill_n', 0) < 5:
                            self._grp_backfill_n = getattr(self, '_grp_backfill_n', 0) + 1
                            try:
                                _bc = _extract_country(range_name)
                                _bo = _extract_otp(sms_body)
                                _bg, _bm = _build_group_notify_text(number, _bc, website, _bo, sms_body)
                                await _broadcast_to_groups(bot, pname, _bg, _bm,
                                                           dt_str=dt_str, number=number, sms_body=sms_body)
                            except Exception as _ge:
                                logger.warning(f"OTPMonitor: group backfill failed — {_ge}")
                        continue

                    country = _extract_country(range_name)
                    otp     = _extract_otp(sms_body)

                    await _otp_thread(_mark_otp_delivered, delivery_key)
                    self._seen_keys.add(delivery_key)

                    logger.info(
                        f"OTPMonitor: NEW SMS — website={website}, "
                        f"number=+{number}, otp={otp or '—'}"
                    )
                    if not self._is_first_poll:
                        await self._notify_user(bot, number, website, otp, sms_body, delivery_key, sms_dt_str=dt_str)
                    try:
                        grp_text, grp_markup = _build_group_notify_text(number, country, website, otp, sms_body)
                        await _broadcast_to_groups(bot, pname, grp_text, grp_markup,
                                                   dt_str=dt_str, number=number, sms_body=sms_body)
                    except Exception as _ge:
                        logger.warning(f"OTPMonitor: group notify failed — {_ge}")

            except Exception as exc:
                logger.error(f"OTPMonitor: Unexpected error — {exc}")

            self._is_first_poll = False
            await _jittered_sleep(self.interval)

    def start(self, bot):
        self._running = True
        self._task    = asyncio.create_task(self._loop(bot))
        logger.info("OTPMonitor: Task created.")

    def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
        logger.info("OTPMonitor: Stopped.")


# ══════════════════════════════════════════════════════════════════════════════
# OTPMonitor2 — Second SMS Hadi account (same server, different credentials)
# ══════════════════════════════════════════════════════════════════════════════

class OTPMonitor2(OTPMonitor):
    """Second SMS Hadi account monitor — same smshadi.net server, different
    credentials. Inherits all session/sesskey/fetch logic from OTPMonitor;
    only _login is overridden to use SMS_HADI2_* credentials."""

    def __init__(self):
        super().__init__()
        self.panel_name = 'SMS Hadi 2'
        self._username  = SMS_HADI2_USERNAME
        self._password  = SMS_HADI2_PASSWORD

    def _refresh_credentials(self):
        """Read latest credentials from DB so admin-panel changes take effect."""
        try:
            from database import _get_panel_by_name
            p = _get_panel_by_name('SMS Hadi 2')
            if p and p.get('username'):
                self._username = p['username']
                self._password = p['password']
        except Exception:
            pass

    def _login(self) -> bool:
        self._refresh_credentials()
        if not self._username or not self._password:
            logger.warning("OTPMonitor2: credentials not set — set via Admin Panel → Panel List → SMS Hadi 2 → Edit Credentials")
            return False
        last_reason = "unknown"
        for attempt in range(1, _LOGIN_FAST_RETRIES + 1):
            try:
                self.session = _new_session()
                r1 = self.session.get(SMS_HADI2_LOGIN_URL, timeout=15)
                captcha = _solve_captcha(r1.text)
                if captcha is None:
                    last_reason = "captcha unsolvable"
                    logger.warning(f"OTPMonitor2: Attempt {attempt}/{_LOGIN_FAST_RETRIES} — {last_reason}.")
                    if attempt < _LOGIN_FAST_RETRIES:
                        time.sleep(_LOGIN_RETRY_DELAY)
                    continue
                logger.info(f"OTPMonitor2: Attempt {attempt}/{_LOGIN_FAST_RETRIES} — Captcha solved → {captcha}")
                r2 = self.session.post(
                    SMS_HADI2_SIGNIN_URL,
                    data={'username': self._username, 'password': self._password, 'capt': captcha},
                    headers={'Referer': SMS_HADI2_LOGIN_URL},
                    timeout=15, allow_redirects=True,
                )
                if 'login' in r2.url.lower():
                    last_reason = "login rejected"
                    logger.warning(f"OTPMonitor2: Attempt {attempt}/{_LOGIN_FAST_RETRIES} — {last_reason}.")
                    if attempt < _LOGIN_FAST_RETRIES:
                        time.sleep(_LOGIN_RETRY_DELAY)
                    continue
                self.logged_in = True
                logger.info(f"OTPMonitor2: Logged in successfully (attempt {attempt}/{_LOGIN_FAST_RETRIES}).")
                return True
            except Exception as exc:
                last_reason = f"exception: {exc}"
                logger.warning(f"OTPMonitor2: Attempt {attempt}/{_LOGIN_FAST_RETRIES} — {last_reason}")
                if attempt < _LOGIN_FAST_RETRIES:
                    time.sleep(_LOGIN_RETRY_DELAY)
        logger.error(f"OTPMonitor2: All {_LOGIN_FAST_RETRIES} login attempts failed — {last_reason}")
        self.logged_in = False
        return False


# ══════════════════════════════════════════════════════════════════════════════
# Generic Client Panel Monitor  (cookie-based, /client/ path)
# Used for: Konekta Premium, MSI SMS (and any future similar panel)
# ══════════════════════════════════════════════════════════════════════════════

class ClientPanelMonitor:
    """
    Generic background monitor for panels that use:
      - Cookie-based session (no sesskey)
      - /client/SMSCDRStats stats page
      - /client/res/data_smscdr.php AJAX endpoint
      - Columns: [0]datetime [1]range [2]number [3]cli [4]sms_body

    Website name is detected automatically from SMS body text.
    """

    def __init__(
        self,
        panel_name: str,
        base_url: str,
        login_page_url: str,
        signin_url: str,
        username: str,
        password: str,
        path_prefix: str = "client",
    ):
        self.panel_name    = panel_name
        self.base_url      = base_url.rstrip('/')
        self.login_page    = login_page_url
        self.signin_url    = signin_url
        self.username      = username
        self.password      = password
        self.ajax_url      = f"{self.base_url}/{path_prefix}/res/data_smscdr.php"
        self.referer_url   = f"{self.base_url}/{path_prefix}/SMSCDRStats"
        self._path_prefix  = path_prefix
        self._log          = logging.getLogger(f"otp_monitor.{panel_name}")

        self.interval       = SMS_MONITOR_INTERVAL
        self.retry_interval = 60
        self._running       = False
        self._task          = None
        self._seen_keys: set[str] = set()
        self._is_first_poll = True
        self.session: requests.Session | None = None
        self.logged_in      = False
        self._manual_only   = False
        self._latest_record = None   # cached latest SMS for get_latest_today()

    def set_interval(self, seconds: int):
        """Update the polling interval live (no restart needed)."""
        self.interval = max(1, int(seconds))

    def set_retry_interval(self, seconds: int):
        """Update the login-retry interval live (no restart needed)."""
        self.retry_interval = max(1, int(seconds))

    def _refresh_credentials(self):
        """Read latest credentials from DB so admin-panel changes take effect."""
        try:
            from database import _get_panel_by_name
            p = _get_panel_by_name(self.panel_name)
            if p and p.get('username'):
                self.username = p['username']
                self.password = p['password']
        except Exception:
            pass

    def _login(self) -> bool:
        self._refresh_credentials()
        if not self.username or not self.password:
            self._log.warning(f"{self.panel_name}: credentials not set.")
            return False
        last_reason = "unknown"
        for attempt in range(1, _LOGIN_FAST_RETRIES + 1):
            try:
                # Fresh Session Reload — drop any old cookies and rebuild
                self.session = _new_session()
                r1 = self.session.get(self.login_page, timeout=15)
                captcha = _solve_captcha(r1.text)
                if captcha is None:
                    last_reason = "captcha unsolvable"
                    self._log.warning(
                        f"{self.panel_name}: Attempt {attempt}/{_LOGIN_FAST_RETRIES} — {last_reason}."
                    )
                    if attempt < _LOGIN_FAST_RETRIES:
                        time.sleep(_LOGIN_RETRY_DELAY)
                    continue
                self._log.info(
                    f"{self.panel_name}: Attempt {attempt}/{_LOGIN_FAST_RETRIES} — Captcha solved → {captcha}"
                )
                r2 = self.session.post(
                    self.signin_url,
                    data={'username': self.username, 'password': self.password, 'capt': captcha},
                    headers={'Referer': self.login_page},
                    timeout=15, allow_redirects=True,
                )
                # Detect failed login by checking if we landed back on a login page
                final_path = r2.url.lower()
                if 'login' in final_path or 'sign-in' in final_path or 'signin' in final_path.split('/')[-1]:
                    last_reason = "login rejected"
                    self._log.warning(
                        f"{self.panel_name}: Attempt {attempt}/{_LOGIN_FAST_RETRIES} — {last_reason}."
                    )
                    if attempt < _LOGIN_FAST_RETRIES:
                        time.sleep(_LOGIN_RETRY_DELAY)
                    continue
                self.logged_in = True
                self._log.info(
                    f"{self.panel_name}: Logged in successfully (attempt {attempt}/{_LOGIN_FAST_RETRIES})."
                )
                return True
            except Exception as exc:
                last_reason = f"exception: {exc}"
                self._log.warning(
                    f"{self.panel_name}: Attempt {attempt}/{_LOGIN_FAST_RETRIES} — {last_reason}"
                )
                if attempt < _LOGIN_FAST_RETRIES:
                    time.sleep(_LOGIN_RETRY_DELAY)
        self._log.error(
            f"{self.panel_name}: All {_LOGIN_FAST_RETRIES} login attempts failed — {last_reason}"
        )
        self.logged_in = False
        return False

    def _fetch_records(self) -> list[dict] | None:
        try:
            url = _build_client_ajax_url(self.ajax_url, days_back=7)
            r = self.session.get(
                url,
                headers={
                    'Referer': self.referer_url,
                    'X-Requested-With': 'XMLHttpRequest',
                },
                timeout=25,
            )
            final_path = r.url.lower()
            if 'login' in final_path or 'sign-in' in final_path:
                self.logged_in = False
                return None

            data = r.json()
            rows = data.get('aaData', [])

            # Dynamic column config — reads from DB on every poll, no restart needed.
            # Agent default: num=2, body=5  |  Client default: num=2, body=4
            _def_body = 5 if self._path_prefix == 'agent' else 4
            num_col, body_col = _get_col_cfg(self.panel_name, 2, _def_body)

            results = []
            for row in rows:
                if not isinstance(row, list):
                    continue
                dt_str     = str(row[0]).strip() if row[0] else ''
                range_name = str(row[1]).strip() if row[1] else ''
                number     = re.sub(r'[^\d]', '', str(row[num_col])) if len(row) > num_col and row[num_col] else ''
                # Skip totals/summary rows — they don't have a valid datetime
                if not re.match(r'\d{4}-\d{2}-\d{2}', dt_str):
                    continue

                sms_body = str(row[body_col]).strip() if len(row) > body_col and row[body_col] else ''

                if self._path_prefix == 'agent':
                    website  = str(row[3]).strip() if len(row) > 3 and row[3] else ''
                    detected = _detect_website_from_body(sms_body)
                    if detected and detected != 'Unknown':
                        website = detected
                    elif not website or website.lower() in ('unknown', '', 'none', '-'):
                        website = 'Unknown'
                else:
                    website = _detect_website_from_body(sms_body)

                if not number or not dt_str:
                    continue

                results.append({
                    'datetime':   dt_str,
                    'range_name': range_name,
                    'number':     number,
                    'website':    website,
                    'sms_body':   sms_body,
                })

            self._log.info(f"{self.panel_name}: Fetched {len(results)} SMS records.")
            return results

        except Exception as exc:
            self._log.error(f"{self.panel_name}: _fetch_records error — {exc}")
            return None


    def get_latest_today(self) -> 'dict | None':
        """Always fetch fresh live data. Falls back to cache only if session is down or on error."""
        if not self.logged_in or not self.session:
            return None
        try:
            now = _bd_now()
            params = urlencode({
                'fdate1': (now - timedelta(days=7)).strftime('%Y-%m-%d 00:00:00'),
                'fdate2': now.strftime('%Y-%m-%d 23:59:59'),
                'frange': '', 'fnum': '', 'fcli': '',
                'fgdate': '', 'fgmonth': '', 'fgrange': '', 'fgnumber': '', 'fgcli': '',
                'fg': '0', 'iDisplayStart': '0', 'iDisplayLength': '999999',
                'iSortCol_0': '0', 'sSortDir_0': 'desc',
            })
            r = self.session.get(
                f"{self.ajax_url}?{params}",
                headers={'Referer': self.referer_url, 'X-Requested-With': 'XMLHttpRequest'},
                timeout=20,
            )
            if 'login' in r.url.lower() or 'sign-in' in r.url.lower():
                self.logged_in = False
                self._log.warning(f"{self.panel_name}: get_latest_today — session expired, falling back to cache.")
                return None
            rows = r.json().get('aaData', [])
            valid = []
            for row in rows:
                if not isinstance(row, list):
                    continue
                dt_str = str(row[0]).strip() if row[0] else ''
                if not re.match(r'\d{4}-\d{2}-\d{2}', dt_str):
                    continue
                number = re.sub(r'[^\d]', '', str(row[2])) if row[2] else ''
                if not number:
                    continue
                if self._path_prefix == 'agent':
                    if len(row) < 6:
                        continue
                    panel_website = str(row[3]).strip() if row[3] else ''
                    sms_body      = str(row[5]).strip() if row[5] else ''
                    detected = _detect_website_from_body(sms_body)
                    website = detected if (detected and detected != 'Unknown') else panel_website
                else:
                    if len(row) < 5:
                        continue
                    sms_body = str(row[4]).strip() if row[4] else ''
                    website  = _detect_website_from_body(sms_body)
                valid.append({
                    'dt': dt_str, 'number': number,
                    'range_name': str(row[1]).strip() if row[1] else '',
                    'sms_body': sms_body,
                    'website': website,
                })
            if not valid:
                self._log.info(f"{self.panel_name}: get_latest_today — no valid rows, falling back to cache.")
                return None
            valid.sort(key=lambda x: x['dt'], reverse=True)
            rec = valid[0]
            uid = hashlib.md5(f"{rec['dt']}:{rec['number']}:{rec['sms_body']}".encode()).hexdigest()
            result = {
                'id': uid, 'datetime': rec['dt'], 'number': rec['number'],
                'website': rec['website'],
                'country': _extract_country(rec['range_name']),
                'otp': _extract_otp(rec['sms_body']), 'message': rec['sms_body'],
                'received_at': rec['dt'], 'panel_name': self.panel_name,
            }
            return result
        except Exception as exc:
            self._log.error(f"{self.panel_name}: get_latest_today error — {exc}. Falling back to cache.")
            return None

    async def _notify_user(self, bot, number: str, website: str, otp: str, sms_body: str, delivery_key: str = '', sms_dt_str: str = ''):
        try:
            from database import (
                _get_recent_user_by_number as _gub,
                _get_otp_bonus_settings,
                _has_otp_bonus_received, _record_otp_bonus,
                _get_effective_otp_bonus, _get_user_balance,
                _get_notify_window,
            )
            sms_ts = _parse_panel_dt(sms_dt_str)
            _win_sec = _get_notify_window() * 60
            _gub_ts = lambda n: _gub(n, max_age_seconds=_win_sec, sms_ts=sms_ts)
            uid = await _otp_thread(_gub_ts, number)
            if not uid:
                uid = await _otp_thread(_gub_ts, '+' + number)
            if uid and bot:
                bonus_amount_credited = None
                new_balance = None
                if delivery_key:
                    bonus_cfg = await _otp_thread(_get_otp_bonus_settings)
                    if bonus_cfg['enabled']:
                        already = await _otp_thread(_has_otp_bonus_received, delivery_key)
                        if not already:
                            effective_amount = await _otp_thread(_get_effective_otp_bonus, number, bonus_cfg['amount'])
                            credited = await _otp_thread(
                                _record_otp_bonus, uid, delivery_key, effective_amount
                            )
                            if credited:
                                new_balance = await _otp_thread(_get_user_balance, uid)
                                bonus_amount_credited = effective_amount
                                self._log.info(f"{self.panel_name}: OTP bonus BDT {effective_amount:.2f} credited to user {uid}")
                notify_text = _build_sms_notify_text(
                    number, website, sms_body,
                    bonus_amount=bonus_amount_credited,
                    new_balance=new_balance,
                )
                await bot.send_message(chat_id=uid, text=notify_text, parse_mode='HTML')
                self._log.info(f"{self.panel_name}: Notified user {uid} for +{number}")
        except Exception as notify_exc:
            self._log.warning(f"{self.panel_name}: Could not notify user — {notify_exc}")

    async def _loop(self, bot):
        from database import (_is_otp_delivered, _mark_otp_delivered,
                              _update_panel_status, _is_panel_enabled)
        self._log.info(f"{self.panel_name}: Starting.")

        # ── Wait until panel is enabled before attempting login
        while self._running and (
            not _is_panel_enabled(self.panel_name)
            or getattr(self, '_manual_only', False)
        ):
            await asyncio.sleep(5)
        if not self._running:
            return

        ok = await _otp_thread(self._login)
        _login_fail_notified = False
        while not ok and self._running:
            self._log.warning(f"{self.panel_name}: Login failed — retrying in {self.retry_interval}s…")
            await _otp_thread(_update_panel_status, self.panel_name, False, None, 'Login failed — retrying')
            if not _login_fail_notified:
                await _notify_admins_login_fail(bot, self.panel_name)
                _login_fail_notified = True
            await asyncio.sleep(self.retry_interval)
            while self._running and (
                not _is_panel_enabled(self.panel_name)
                or getattr(self, '_manual_only', False)
            ):
                await asyncio.sleep(5)
            if not self._running:
                return
            ok = await _otp_thread(self._login)
        if not self._running:
            return
        await _otp_thread(_update_panel_status, self.panel_name, True)
        if _login_fail_notified:
            await _notify_admins_login_success(bot, self.panel_name)
            _login_fail_notified = False

        while self._running:
            if not _is_panel_enabled(self.panel_name) or getattr(self, '_manual_only', False):
                await asyncio.sleep(5)
                continue
            try:
                records = await _otp_thread(self._fetch_records)

                if records is None:
                    self._log.info(f"{self.panel_name}: Session expired — re-logging in …")
                    _cf = getattr(self, '_consec_failures', 0) + 1
                    setattr(self, '_consec_failures', _cf)
                    _back = min(
                        BACKOFF_BASE_SECS * (BACKOFF_MULTIPLIER ** min(_cf - 1, 6)),
                        BACKOFF_MAX_SECS,
                    )
                    if _cf > 1:
                        self._log.warning(
                            "%s: consecutive failure #%d — extra back-off %.0fs",
                            self.panel_name, _cf, _back,
                        )
                    # Wait if in manual-only mode (set after Session Cleanup)
                    while self._running and getattr(self, '_manual_only', False):
                        await asyncio.sleep(5)
                    if not self._running:
                        return
                    await _midnight_relogin_jitter(self.panel_name)
                    ok = await _otp_thread(self._login)
                    if ok:
                        await _otp_thread(_update_panel_status, self.panel_name, True)
                    await _jittered_sleep(self.interval + _back)
                    continue
                setattr(self, '_consec_failures', 0)
                await _otp_thread(_update_panel_status, self.panel_name, True, len(records))

                # Always update _latest_record with the most recent SMS from each poll
                if records:
                    _r0 = records[0]
                    uid0 = hashlib.md5(f"{_r0['datetime']}:{_r0['number']}:{_r0['sms_body']}".encode()).hexdigest()
                    self._latest_record = {
                        'id': uid0, 'datetime': _r0['datetime'], 'number': _r0['number'],
                        'website': _r0['website'] or _detect_website_from_body(_r0['sms_body']),
                        'country': _extract_country(_r0['range_name']),
                        'otp': _extract_otp(_r0['sms_body']), 'message': _r0['sms_body'],
                        'received_at': _r0['datetime'], 'panel_name': self.panel_name,
                    }

                for rec in records:
                    dt_str     = rec['datetime']
                    range_name = rec['range_name']
                    number     = rec['number']
                    website    = rec['website']
                    sms_body   = rec['sms_body']

                    delivery_key = hashlib.sha256(
                        f"sms:{number}:{sms_body}".encode()
                    ).hexdigest()

                    if delivery_key in self._seen_keys:
                        continue
                    already = await _otp_thread(_is_otp_delivered, delivery_key)
                    if already:
                        self._seen_keys.add(delivery_key)
                        if self._is_first_poll and getattr(self, '_grp_backfill_n', 0) < 5:
                            self._grp_backfill_n = getattr(self, '_grp_backfill_n', 0) + 1
                            try:
                                _bc = _extract_country(range_name)
                                _bo = _extract_otp(sms_body)
                                _bg, _bm = _build_group_notify_text(number, _bc, website, _bo, sms_body)
                                await _broadcast_to_groups(bot, self.panel_name, _bg, _bm,
                                                           dt_str=dt_str, number=number, sms_body=sms_body)
                            except Exception as _ge:
                                self._log.warning(f"{self.panel_name}: group backfill failed — {_ge}")
                        continue

                    country = _extract_country(range_name)
                    otp     = _extract_otp(sms_body)

                    self._log.info(
                        f"{self.panel_name}: NEW SMS — website={website}, "
                        f"number=+{number}, otp={otp or '—'}"
                    )
                    await _otp_thread(_mark_otp_delivered, delivery_key)
                    self._seen_keys.add(delivery_key)

                    if not self._is_first_poll:
                        await self._notify_user(bot, number, website, otp, sms_body, delivery_key, sms_dt_str=dt_str)
                    try:
                        grp_text, grp_markup = _build_group_notify_text(number, country, website, otp, sms_body)
                        await _broadcast_to_groups(bot, self.panel_name, grp_text, grp_markup,
                                                   dt_str=dt_str, number=number, sms_body=sms_body)
                    except Exception as _ge:
                        self._log.warning(f"{self.panel_name}: group notify failed — {_ge}")

                self._is_first_poll = False

            except Exception as exc:
                self._log.error(f"{self.panel_name}: Unexpected error — {exc}")

            await _jittered_sleep(self.interval)

    def start(self, bot):
        self._running = True
        self._task    = asyncio.create_task(self._loop(bot))
        self._log.info(f"{self.panel_name}: Task created.")

    def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
        self._log.info(f"{self.panel_name}: Stopped.")


# ══════════════════════════════════════════════════════════════════════════════
# Number Panel Monitor  (sesskey-based, /client/ path, 17-second interval)
# ══════════════════════════════════════════════════════════════════════════════

class NumberPanelMonitor:
    """
    Background monitor for Number Panel (http://51.89.99.105/NumberPanel).
    - Login: captcha + username/password  (same form as SMS Hadi)
    - Sesskey extracted from /client/SMSCDRStats page directly
    - AJAX: /client/res/data_smscdr.php?...&sesskey=...
    - Columns: [0]datetime [1]range [2]number [3]CLI [4]sms_body [5]currency [6]payout
    - Website detected from sms_body
    - Polls every NUMBER_PANEL_INTERVAL (17) seconds
    """

    def __init__(self):
        self.panel_name     = 'Number Panel'
        self.interval       = NUMBER_PANEL_INTERVAL
        self.retry_interval = 60
        self._running       = False
        self._task          = None
        self._seen_keys: set[str] = set()
        self._is_first_poll = True
        self.session: requests.Session | None = None
        self.logged_in      = False
        self._sesskey       = None
        self._manual_only   = False
        self._latest_record = None   # cached latest SMS for get_latest_today()
        self._log           = logging.getLogger('otp_monitor.Number Panel')
        # read credentials from DB at runtime (allows admin panel changes)
        self._username      = NUMBER_PANEL_USERNAME
        self._password      = NUMBER_PANEL_PASSWORD

    def set_interval(self, seconds: int):
        """Update the polling interval live (no restart needed)."""
        self.interval = max(1, int(seconds))

    def set_retry_interval(self, seconds: int):
        """Update the login-retry interval live (no restart needed)."""
        self.retry_interval = max(1, int(seconds))

    def _refresh_credentials(self):
        try:
            from database import _get_panel_by_name
            p = _get_panel_by_name('Number Panel')
            if p:
                self._username = p['username']
                self._password = p['password']
        except Exception:
            pass

    def _login(self) -> bool:
        self._refresh_credentials()
        if not self._username or not self._password:
            self._log.warning("Number Panel: credentials not set.")
            return False
        last_reason = "unknown"
        for attempt in range(1, _LOGIN_FAST_RETRIES + 1):
            try:
                # Fresh Session Reload — drop any old cookies and rebuild
                self.session = _new_session()
                r1 = self.session.get(NUMBER_PANEL_LOGIN_URL, timeout=15)
                captcha = _solve_captcha(r1.text)
                if captcha is None:
                    last_reason = "captcha unsolvable"
                    self._log.warning(
                        f"Number Panel: Attempt {attempt}/{_LOGIN_FAST_RETRIES} — {last_reason}."
                    )
                    if attempt < _LOGIN_FAST_RETRIES:
                        time.sleep(_LOGIN_RETRY_DELAY)
                    continue
                self._log.info(
                    f"Number Panel: Attempt {attempt}/{_LOGIN_FAST_RETRIES} — Captcha solved → {captcha}"
                )
                r2 = self.session.post(
                    NUMBER_PANEL_SIGNIN_URL,
                    data={'username': self._username, 'password': self._password, 'capt': captcha},
                    headers={'Referer': NUMBER_PANEL_LOGIN_URL},
                    timeout=15, allow_redirects=True,
                )
                if 'login' in r2.url.lower():
                    last_reason = "login rejected"
                    self._log.warning(
                        f"Number Panel: Attempt {attempt}/{_LOGIN_FAST_RETRIES} — {last_reason}."
                    )
                    if attempt < _LOGIN_FAST_RETRIES:
                        time.sleep(_LOGIN_RETRY_DELAY)
                    continue
                self.logged_in = True
                self._log.info(
                    f"Number Panel: Logged in successfully (attempt {attempt}/{_LOGIN_FAST_RETRIES})."
                )
                return True
            except Exception as exc:
                last_reason = f"exception: {exc}"
                self._log.warning(
                    f"Number Panel: Attempt {attempt}/{_LOGIN_FAST_RETRIES} — {last_reason}"
                )
                if attempt < _LOGIN_FAST_RETRIES:
                    time.sleep(_LOGIN_RETRY_DELAY)
        self._log.error(
            f"Number Panel: All {_LOGIN_FAST_RETRIES} login attempts failed — {last_reason}"
        )
        self.logged_in = False
        return False

    def _extract_sesskey(self) -> bool:
        try:
            r = self.session.get(NUMBER_PANEL_STATS_URL, timeout=20)
            if 'login' in r.url.lower():
                self.logged_in = False
                return False
            m = re.search(
                r'"sAjaxSource"\s*:\s*"res/data_smscdr\.php[^"]*sesskey=([^"&]+)"',
                r.text
            )
            if not m:
                self._log.error("Number Panel: sesskey not found in stats page.")
                return False
            self._sesskey = m.group(1)
            self._log.info("Number Panel: sesskey extracted successfully.")
            return True
        except Exception as exc:
            self._log.error(f"Number Panel: _extract_sesskey error — {exc}")
            return False

    def _fetch_records(self) -> list[dict] | None:
        if not self._sesskey:
            return None
        try:
            now = _bd_now()
            d1  = (now - timedelta(days=7)).strftime('%Y-%m-%d 00:00:00')
            d2  = now.strftime('%Y-%m-%d 23:59:59')
            params = urlencode({
                'fdate1': d1, 'fdate2': d2,
                'frange': '', 'fnum': '', 'fcli': '',
                'fgdate': '', 'fgmonth': '', 'fgrange': '',
                'fgnumber': '', 'fgcli': '',
                'fg': '0',
                'sesskey': self._sesskey,
                'iDisplayStart': '0',
                'iDisplayLength': '999999',
                'iSortCol_0': '0',
                'sSortDir_0': 'desc',
            })
            url = f"{NUMBER_PANEL_AJAX_URL}?{params}"
            r = self.session.get(
                url,
                headers={
                    'Referer': NUMBER_PANEL_STATS_URL,
                    'X-Requested-With': 'XMLHttpRequest',
                },
                timeout=25,
            )
            if 'login' in r.url.lower():
                self.logged_in = False
                return None
            data = r.json()
            rows = data.get('aaData', [])
            num_col, body_col = _get_col_cfg(self.panel_name, 2, 4)
            results = []
            for row in rows:
                if not isinstance(row, list) or len(row) < 3:
                    continue
                dt_str     = str(row[0]).strip() if row[0] else ''
                range_name = str(row[1]).strip() if row[1] else ''
                number     = re.sub(r'[^\d]', '', str(row[num_col])) if len(row) > num_col and row[num_col] else ''
                # Skip totals/summary rows — they don't have a valid datetime
                if not re.match(r'\d{4}-\d{2}-\d{2}', dt_str):
                    continue
                sms_body   = str(row[body_col]).strip() if len(row) > body_col and row[body_col] else ''
                if not number or not dt_str or number == '0':
                    continue
                website = _detect_website_from_body(sms_body)
                results.append({
                    'datetime':   dt_str,
                    'range_name': range_name,
                    'number':     number,
                    'website':    website,
                    'sms_body':   sms_body,
                })
            self._log.info(f"Number Panel: Fetched {len(results)} SMS records.")
            return results
        except Exception as exc:
            self._log.error(f"Number Panel: _fetch_records error — {exc}")
            return None


    def get_latest_today(self) -> 'dict | None':
        """Always fetch fresh live data. Falls back to cache only if session is down or on error."""
        if not self.logged_in or not self.session or not self._sesskey:
            return None
        try:
            now = _bd_now()
            params = urlencode({
                'fdate1': (now - timedelta(days=7)).strftime('%Y-%m-%d 00:00:00'),
                'fdate2': now.strftime('%Y-%m-%d 23:59:59'),
                'frange': '', 'fnum': '', 'fcli': '',
                'fgdate': '', 'fgmonth': '', 'fgrange': '',
                'fgnumber': '', 'fgcli': '', 'fg': '0',
                'sesskey': self._sesskey,
                'iDisplayStart': '0', 'iDisplayLength': '999999',
                'iSortCol_0': '0', 'sSortDir_0': 'desc',
            })
            r = self.session.get(
                f"{NUMBER_PANEL_AJAX_URL}?{params}",
                headers={'Referer': NUMBER_PANEL_STATS_URL, 'X-Requested-With': 'XMLHttpRequest'},
                timeout=20,
            )
            if 'login' in r.url.lower():
                self.logged_in = False
                self._log.warning("Number Panel: get_latest_today — session expired, falling back to cache.")
                return None
            rows = r.json().get('aaData', [])
            valid = []
            for row in rows:
                if not isinstance(row, list) or len(row) < 5:
                    continue
                dt_str = str(row[0]).strip() if row[0] else ''
                if not re.match(r'\d{4}-\d{2}-\d{2}', dt_str):
                    continue
                number = re.sub(r'[^\d]', '', str(row[2])) if row[2] else ''
                if not number or number == '0':
                    continue
                sms_body = str(row[4]).strip() if len(row) > 4 and row[4] else ''
                valid.append({
                    'dt': dt_str, 'number': number,
                    'range_name': str(row[1]).strip() if row[1] else '',
                    'sms_body': sms_body,
                })
            if not valid:
                self._log.info("Number Panel: get_latest_today — no valid rows, falling back to cache.")
                return None
            valid.sort(key=lambda x: x['dt'], reverse=True)
            rec = valid[0]
            uid = hashlib.md5(f"{rec['dt']}:{rec['number']}:{rec['sms_body']}".encode()).hexdigest()
            result = {
                'id': uid, 'datetime': rec['dt'], 'number': rec['number'],
                'website': _detect_website_from_body(rec['sms_body']),
                'country': _extract_country(rec['range_name']),
                'otp': _extract_otp(rec['sms_body']), 'message': rec['sms_body'],
                'received_at': rec['dt'], 'panel_name': 'Number Panel',
            }
            return result
        except Exception as exc:
            self._log.error(f"Number Panel: get_latest_today error — {exc}. Falling back to cache.")
            return None

    async def _notify_user(self, bot, number: str, website: str, otp: str, sms_body: str, delivery_key: str = '', sms_dt_str: str = ''):
        try:
            from database import (
                _get_recent_user_by_number as _gub,
                _get_otp_bonus_settings,
                _has_otp_bonus_received, _record_otp_bonus,
                _get_effective_otp_bonus, _get_user_balance,
                _get_notify_window,
            )
            sms_ts = _parse_panel_dt(sms_dt_str)
            _win_sec = _get_notify_window() * 60
            _gub_ts = lambda n: _gub(n, max_age_seconds=_win_sec, sms_ts=sms_ts)
            uid = await _otp_thread(_gub_ts, number)
            if not uid:
                uid = await _otp_thread(_gub_ts, '+' + number)
            if uid and bot:
                bonus_amount_credited = None
                new_balance = None
                if delivery_key:
                    bonus_cfg = await _otp_thread(_get_otp_bonus_settings)
                    if bonus_cfg['enabled']:
                        already = await _otp_thread(_has_otp_bonus_received, delivery_key)
                        if not already:
                            effective_amount = await _otp_thread(_get_effective_otp_bonus, number, bonus_cfg['amount'])
                            credited = await _otp_thread(
                                _record_otp_bonus, uid, delivery_key, effective_amount
                            )
                            if credited:
                                new_balance = await _otp_thread(_get_user_balance, uid)
                                bonus_amount_credited = effective_amount
                                self._log.info(f"Number Panel: OTP bonus BDT {effective_amount:.2f} credited to user {uid}")
                notify_text = _build_sms_notify_text(
                    number, website, sms_body,
                    bonus_amount=bonus_amount_credited,
                    new_balance=new_balance,
                )
                await bot.send_message(chat_id=uid, text=notify_text, parse_mode='HTML')
                self._log.info(f"Number Panel: Notified user {uid} for +{number}")
        except Exception as notify_exc:
            self._log.warning(f"Number Panel: Could not notify user — {notify_exc}")


    async def _loop(self, bot):
        from database import (_is_otp_delivered, _mark_otp_delivered,
                              _update_panel_status, _is_panel_enabled)
        self._log.info("Number Panel: Starting.")
        # ── Wait until panel is enabled before attempting login
        while self._running and (
            not _is_panel_enabled('Number Panel')
            or getattr(self, '_manual_only', False)
        ):
            await asyncio.sleep(5)
        if not self._running:
            return
        ok = await _otp_thread(self._login)
        _login_fail_notified = False
        while not ok and self._running:
            self._log.warning(f"Number Panel: Login failed — retrying in {self.retry_interval}s…")
            await _otp_thread(_update_panel_status, 'Number Panel', False, None, 'Login failed — retrying')
            if not _login_fail_notified:
                await _notify_admins_login_fail(bot, 'Number Panel')
                _login_fail_notified = True
            await asyncio.sleep(self.retry_interval)
            while self._running and not _is_panel_enabled('Number Panel'):
                await asyncio.sleep(5)
            if not self._running:
                return
            ok = await _otp_thread(self._login)
        if not self._running:
            return
        await _otp_thread(_update_panel_status, 'Number Panel', True)
        if _login_fail_notified:
            await _notify_admins_login_success(bot, 'Number Panel')
            _login_fail_notified = False
        ok = await _otp_thread(self._extract_sesskey)
        if not ok:
            self._log.error("Number Panel: Could not extract sesskey.")
            return

        while self._running:
            if not _is_panel_enabled('Number Panel') or getattr(self, '_manual_only', False):
                await asyncio.sleep(5)
                continue
            try:
                records = await _otp_thread(self._fetch_records)

                if records is None:
                    self._log.info("Number Panel: Session expired — re-logging in …")
                    _cf = getattr(self, '_consec_failures', 0) + 1
                    setattr(self, '_consec_failures', _cf)
                    _back = min(
                        BACKOFF_BASE_SECS * (BACKOFF_MULTIPLIER ** min(_cf - 1, 6)),
                        BACKOFF_MAX_SECS,
                    )
                    if _cf > 1:
                        self._log.warning(
                            "Number Panel: consecutive failure #%d — extra back-off %.0fs", _cf, _back
                        )
                    # Wait if in manual-only mode (set after Session Cleanup)
                    while self._running and getattr(self, '_manual_only', False):
                        await asyncio.sleep(5)
                    if not self._running:
                        return
                    await _midnight_relogin_jitter('Number Panel')
                    ok = await _otp_thread(self._login)
                    if ok:
                        await _otp_thread(_update_panel_status, 'Number Panel', True)
                        await _otp_thread(self._extract_sesskey)
                    await _jittered_sleep(self.interval + _back)
                    continue
                setattr(self, '_consec_failures', 0)
                await _otp_thread(_update_panel_status, 'Number Panel', True, len(records))

                # Always update _latest_record with the most recent SMS from each poll
                if records:
                    _r0 = records[0]
                    uid0 = hashlib.md5(f"{_r0['datetime']}:{_r0['number']}:{_r0['sms_body']}".encode()).hexdigest()
                    self._latest_record = {
                        'id': uid0, 'datetime': _r0['datetime'], 'number': _r0['number'],
                        'website': _r0['website'] or _detect_website_from_body(_r0['sms_body']),
                        'country': _extract_country(_r0['range_name']),
                        'otp': _extract_otp(_r0['sms_body']), 'message': _r0['sms_body'],
                        'received_at': _r0['datetime'], 'panel_name': 'Number Panel',
                    }

                for rec in records:
                    dt_str     = rec['datetime']
                    range_name = rec['range_name']
                    number     = rec['number']
                    website    = rec['website']
                    sms_body   = rec['sms_body']

                    delivery_key = hashlib.sha256(
                        f"sms:{number}:{sms_body}".encode()
                    ).hexdigest()

                    if delivery_key in self._seen_keys:
                        continue
                    already = await _otp_thread(_is_otp_delivered, delivery_key)
                    if already:
                        self._seen_keys.add(delivery_key)
                        if self._is_first_poll and getattr(self, '_grp_backfill_n', 0) < 5:
                            self._grp_backfill_n = getattr(self, '_grp_backfill_n', 0) + 1
                            try:
                                _bc = _extract_country(range_name)
                                _bo = _extract_otp(sms_body)
                                _bg, _bm = _build_group_notify_text(number, _bc, website, _bo, sms_body)
                                await _broadcast_to_groups(bot, "Number Panel", _bg, _bm,
                                                           dt_str=dt_str, number=number, sms_body=sms_body)
                            except Exception as _ge:
                                self._log.warning(f"Number Panel: group backfill failed — {_ge}")
                        continue

                    country = _extract_country(range_name)
                    otp     = _extract_otp(sms_body)

                    self._log.info(
                        f"Number Panel: NEW SMS — website={website}, "
                        f"number=+{number}, otp={otp or '—'}"
                    )
                    await _otp_thread(_mark_otp_delivered, delivery_key)
                    self._seen_keys.add(delivery_key)

                    if not self._is_first_poll:
                        await self._notify_user(bot, number, website, otp, sms_body, delivery_key, sms_dt_str=dt_str)
                    try:
                        grp_text, grp_markup = _build_group_notify_text(number, country, website, otp, sms_body)
                        await _broadcast_to_groups(bot, "Number Panel", grp_text, grp_markup,
                                                   dt_str=dt_str, number=number, sms_body=sms_body)
                    except Exception as _ge:
                        self._log.warning(f"Number Panel: group notify failed — {_ge}")

                self._is_first_poll = False

            except Exception as exc:
                self._log.error(f"Number Panel: Unexpected error — {exc}")

            await _jittered_sleep(self.interval)

    def start(self, bot):
        self._running = True
        self._task    = asyncio.create_task(self._loop(bot))
        self._log.info("Number Panel: Task created.")

    def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
        self._log.info("Number Panel: Stopped.")


# ══════════════════════════════════════════════════════════════════════════════
# Purple SMS Panel Monitor  (cookie-based, /sms/dialer/ path, no sesskey)
# Columns: [0]Date [1]Termination [2]Number [3]CLI [4]Currency
#          [5]Payterm [6]Payout [7]Message
# ══════════════════════════════════════════════════════════════════════════════

class PurpleSmsMonitor:
    """
    Cookie-based monitor for Purple SMS panel.
    Login → poll ajax/dt_reports.php every interval seconds.
    No sesskey needed — session cookie is sufficient.
    Columns: [0]Date [1]Termination [2]Number [3]CLI [4]Currency
             [5]Payterm [6]Payout [7]Message
    """

    def __init__(self, interval: int = SMS_MONITOR_INTERVAL):
        self.panel_name   = 'Purple sms'
        self.base_url     = PURPLE_SMS_BASE
        self.login_url    = PURPLE_SMS_LOGIN_URL
        self.signin_url   = PURPLE_SMS_SIGNIN_URL
        self.ajax_url     = f"{PURPLE_SMS_BASE}/dialer/ajax/dt_reports.php"
        self.stats_url    = PURPLE_SMS_STATS_URL
        self.username     = PURPLE_SMS_USERNAME
        self.password     = PURPLE_SMS_PASSWORD
        self.interval       = interval
        self.retry_interval = 60
        self._log           = logging.getLogger('otp_monitor.Purple sms')
        self._running       = False
        self._task          = None
        self._seen_keys: set[str] = set()
        self._is_first_poll = True
        self.session: requests.Session | None = None
        self.logged_in      = False
        self._manual_only   = False
        self._latest_record = None   # cached latest SMS for get_latest_today()

    def set_interval(self, seconds: int):
        """Update the polling interval live (no restart needed)."""
        self.interval = max(1, int(seconds))

    def set_retry_interval(self, seconds: int):
        """Update the login-retry interval live (no restart needed)."""
        self.retry_interval = max(1, int(seconds))

    def _refresh_credentials(self):
        """Read latest credentials from DB so admin-panel changes take effect."""
        try:
            from database import _get_panel_by_name
            p = _get_panel_by_name(self.panel_name)
            if p and p.get('username'):
                self.username = p['username']
                self.password = p['password']
        except Exception:
            pass

    def _login(self) -> bool:
        self._refresh_credentials()
        last_reason = "unknown"
        for attempt in range(1, _LOGIN_FAST_RETRIES + 1):
            try:
                # Fresh Session Reload — drop any old cookies and rebuild
                self.session = _new_session()
                r1 = self.session.get(self.login_url, timeout=15)
                captcha = _solve_captcha(r1.text)
                if captcha is None:
                    last_reason = "captcha unsolvable"
                    self._log.warning(
                        f"Purple sms: Attempt {attempt}/{_LOGIN_FAST_RETRIES} — {last_reason}."
                    )
                    if attempt < _LOGIN_FAST_RETRIES:
                        time.sleep(_LOGIN_RETRY_DELAY)
                    continue
                self._log.info(
                    f"Purple sms: Attempt {attempt}/{_LOGIN_FAST_RETRIES} — Captcha solved → {captcha}"
                )
                r2 = self.session.post(
                    self.signin_url,
                    data={'username': self.username, 'password': self.password, 'capt': captcha},
                    headers={'Referer': self.login_url},
                    timeout=15, allow_redirects=True,
                )
                final_lower = r2.url.lower().rstrip('/')
                final_last  = final_lower.split('/')[-1]
                if final_last in ('login', 'signin', 'sign-in') or \
                   final_last.endswith('login') or final_last.endswith('signin'):
                    last_reason = "login rejected"
                    self._log.warning(
                        f"Purple sms: Attempt {attempt}/{_LOGIN_FAST_RETRIES} — {last_reason}."
                    )
                    if attempt < _LOGIN_FAST_RETRIES:
                        time.sleep(_LOGIN_RETRY_DELAY)
                    continue
                self.logged_in = True
                self._log.info(
                    f"Purple sms: Logged in successfully (attempt {attempt}/{_LOGIN_FAST_RETRIES})."
                )
                return True
            except Exception as exc:
                last_reason = f"exception: {exc}"
                self._log.warning(
                    f"Purple sms: Attempt {attempt}/{_LOGIN_FAST_RETRIES} — {last_reason}"
                )
                if attempt < _LOGIN_FAST_RETRIES:
                    time.sleep(_LOGIN_RETRY_DELAY)
        self._log.error(
            f"Purple sms: All {_LOGIN_FAST_RETRIES} login attempts failed — {last_reason}"
        )
        self.logged_in = False
        return False

    def _fetch_records(self) -> list[dict] | None:
        try:
            now = _bd_now()
            d1  = (now - timedelta(days=7)).strftime('%Y-%m-%d 00:00:00')
            d2  = now.strftime('%Y-%m-%d 23:59:59')
            params = {
                'fdate1': d1, 'fdate2': d2,
                'ftermination': '', 'fnum': '', 'fcli': '',
                'fgdate': '0', 'fgtermination': '0',
                'fgnumber': '0', 'fgcli': '0', 'fg': '0',
                'iDisplayStart': '0', 'iDisplayLength': '999999',
                'iSortCol_0': '0', 'sSortDir_0': 'desc',
            }
            r = self.session.get(
                self.ajax_url,
                params=params,
                headers={
                    'Referer': self.stats_url,
                    'X-Requested-With': 'XMLHttpRequest',
                },
                timeout=25,
            )
            if 'login' in r.url.lower() or 'signin' in r.url.lower():
                self.logged_in = False
                return None
            data = r.json()
            rows = data.get('aaData', [])
            num_col, body_col = _get_col_cfg(self.panel_name, 2, 7)
            results = []
            for row in rows:
                if not isinstance(row, list) or len(row) < 3:
                    continue
                dt_str     = str(row[0]).strip() if row[0] else ''
                range_name = str(row[1]).strip() if row[1] else ''
                number     = re.sub(r'[^\d]', '', str(row[num_col])) if len(row) > num_col and row[num_col] else ''
                # Skip totals/summary rows — they don't have a valid datetime
                if not re.match(r'\d{4}-\d{2}-\d{2}', dt_str):
                    continue
                sms_body   = str(row[body_col]).strip() if len(row) > body_col and row[body_col] else ''
                if not number or not dt_str:
                    continue
                website = _detect_website_from_body(sms_body)
                results.append({
                    'datetime':   dt_str,
                    'range_name': range_name,
                    'number':     number,
                    'website':    website,
                    'sms_body':   sms_body,
                })
            self._log.info(f"Purple sms: Fetched {len(results)} SMS records.")
            return results
        except Exception as exc:
            self._log.error(f"Purple sms: _fetch_records error — {exc}")
            return None


    def get_latest_today(self) -> 'dict | None':
        """Always fetch fresh live data. Falls back to cache only if session is down or on error."""
        if not self.logged_in or not self.session:
            return None
        try:
            now = _bd_now()
            params = {
                'fdate1': (now - timedelta(days=7)).strftime('%Y-%m-%d 00:00:00'),
                'fdate2': now.strftime('%Y-%m-%d 23:59:59'),
                'ftermination': '', 'fnum': '', 'fcli': '',
                'fgdate': '0', 'fgtermination': '0',
                'fgnumber': '0', 'fgcli': '0', 'fg': '0',
                'iDisplayStart': '0', 'iDisplayLength': '999999',
                'iSortCol_0': '0', 'sSortDir_0': 'desc',
            }
            r = self.session.get(
                self.ajax_url,
                params=params,
                headers={'Referer': self.stats_url, 'X-Requested-With': 'XMLHttpRequest'},
                timeout=20,
            )
            if 'login' in r.url.lower() or 'signin' in r.url.lower():
                self.logged_in = False
                self._log.warning("Purple sms: get_latest_today — session expired, falling back to cache.")
                return None
            rows = r.json().get('aaData', [])
            valid = []
            for row in rows:
                if not isinstance(row, list) or len(row) < 8:
                    continue
                dt_str = str(row[0]).strip() if row[0] else ''
                if not re.match(r'\d{4}-\d{2}-\d{2}', dt_str):
                    continue
                number = re.sub(r'[^\d]', '', str(row[2])) if row[2] else ''
                if not number:
                    continue
                sms_body = str(row[7]).strip() if row[7] else ''
                valid.append({
                    'dt': dt_str, 'number': number,
                    'range_name': str(row[1]).strip() if row[1] else '',
                    'sms_body': sms_body,
                })
            if not valid:
                self._log.info("Purple sms: get_latest_today — no valid rows, falling back to cache.")
                return None
            valid.sort(key=lambda x: x['dt'], reverse=True)
            rec = valid[0]
            uid = hashlib.md5(f"{rec['dt']}:{rec['number']}:{rec['sms_body']}".encode()).hexdigest()
            result = {
                'id': uid, 'datetime': rec['dt'], 'number': rec['number'],
                'website': _detect_website_from_body(rec['sms_body']),
                'country': _extract_country(rec['range_name']),
                'otp': _extract_otp(rec['sms_body']), 'message': rec['sms_body'],
                'received_at': rec['dt'], 'panel_name': 'Purple sms',
            }
            return result
        except Exception as exc:
            self._log.error(f"Purple sms: get_latest_today error — {exc}. Falling back to cache.")
            return None

    async def _notify_user(self, bot, number: str, website: str, otp: str, sms_body: str, delivery_key: str = '', sms_dt_str: str = ''):
        try:
            from database import (
                _get_recent_user_by_number as _gub,
                _get_otp_bonus_settings,
                _has_otp_bonus_received, _record_otp_bonus,
                _get_effective_otp_bonus, _get_user_balance,
                _get_notify_window,
            )
            sms_ts = _parse_panel_dt(sms_dt_str)
            _win_sec = _get_notify_window() * 60
            _gub_ts = lambda n: _gub(n, max_age_seconds=_win_sec, sms_ts=sms_ts)
            uid = await _otp_thread(_gub_ts, number)
            if not uid:
                uid = await _otp_thread(_gub_ts, '+' + number)
            if uid and bot:
                bonus_amount_credited = None
                new_balance = None
                if delivery_key:
                    bonus_cfg = await _otp_thread(_get_otp_bonus_settings)
                    if bonus_cfg['enabled']:
                        already = await _otp_thread(_has_otp_bonus_received, delivery_key)
                        if not already:
                            effective_amount = await _otp_thread(_get_effective_otp_bonus, number, bonus_cfg['amount'])
                            credited = await _otp_thread(
                                _record_otp_bonus, uid, delivery_key, effective_amount
                            )
                            if credited:
                                new_balance = await _otp_thread(_get_user_balance, uid)
                                bonus_amount_credited = effective_amount
                notify_text = _build_sms_notify_text(
                    number, website, sms_body,
                    bonus_amount=bonus_amount_credited,
                    new_balance=new_balance,
                )
                await bot.send_message(chat_id=uid, text=notify_text, parse_mode='HTML')
                self._log.info(f"Purple sms: Notified user {uid} for +{number}")
        except Exception as notify_exc:
            self._log.warning(f"Purple sms: Could not notify user — {notify_exc}")

    async def _loop(self, bot):
        from database import (_is_otp_delivered, _mark_otp_delivered,
                              _update_panel_status, _is_panel_enabled)
        self._log.info("Purple sms: Starting.")
        # ── Wait until panel is enabled before attempting login
        while self._running and (
            not _is_panel_enabled('Purple sms')
            or getattr(self, '_manual_only', False)
        ):
            await asyncio.sleep(5)
        if not self._running:
            return
        ok = await _otp_thread(self._login)
        _login_fail_notified = False
        while not ok and self._running:
            self._log.warning(f"Purple sms: Login failed — retrying in {self.retry_interval}s…")
            await _otp_thread(_update_panel_status, 'Purple sms', False, None, 'Login failed — retrying')
            if not _login_fail_notified:
                await _notify_admins_login_fail(bot, 'Purple sms')
                _login_fail_notified = True
            await asyncio.sleep(self.retry_interval)
            while self._running and not _is_panel_enabled('Purple sms'):
                await asyncio.sleep(5)
            if not self._running:
                return
            ok = await _otp_thread(self._login)
        if not self._running:
            return
        await _otp_thread(_update_panel_status, 'Purple sms', True)
        if _login_fail_notified:
            await _notify_admins_login_success(bot, 'Purple sms')
            _login_fail_notified = False

        while self._running:
            if not _is_panel_enabled('Purple sms') or getattr(self, '_manual_only', False):
                await asyncio.sleep(5)
                continue
            try:
                records = await _otp_thread(self._fetch_records)

                if records is None:
                    self._log.info("Purple sms: Session expired — re-logging in …")
                    _cf = getattr(self, '_consec_failures', 0) + 1
                    setattr(self, '_consec_failures', _cf)
                    _back = min(
                        BACKOFF_BASE_SECS * (BACKOFF_MULTIPLIER ** min(_cf - 1, 6)),
                        BACKOFF_MAX_SECS,
                    )
                    if _cf > 1:
                        self._log.warning(
                            "Purple sms: consecutive failure #%d — extra back-off %.0fs", _cf, _back
                        )
                    # Wait if in manual-only mode (set after Session Cleanup)
                    while self._running and getattr(self, '_manual_only', False):
                        await asyncio.sleep(5)
                    if not self._running:
                        return
                    await _midnight_relogin_jitter('Purple sms')
                    ok = await _otp_thread(self._login)
                    if ok:
                        await _otp_thread(_update_panel_status, 'Purple sms', True)
                    await _jittered_sleep(self.interval + _back)
                    continue
                setattr(self, '_consec_failures', 0)
                await _otp_thread(_update_panel_status, 'Purple sms', True, len(records))

                # Always update _latest_record with the most recent SMS from each poll
                if records:
                    _r0 = records[0]
                    uid0 = hashlib.md5(f"{_r0['datetime']}:{_r0['number']}:{_r0['sms_body']}".encode()).hexdigest()
                    self._latest_record = {
                        'id': uid0, 'datetime': _r0['datetime'], 'number': _r0['number'],
                        'website': _r0['website'] or _detect_website_from_body(_r0['sms_body']),
                        'country': _extract_country(_r0['range_name']),
                        'otp': _extract_otp(_r0['sms_body']), 'message': _r0['sms_body'],
                        'received_at': _r0['datetime'], 'panel_name': 'Purple sms',
                    }

                for rec in records:
                    dt_str     = rec['datetime']
                    range_name = rec['range_name']
                    number     = rec['number']
                    website    = rec['website']
                    sms_body   = rec['sms_body']

                    delivery_key = hashlib.sha256(
                        f"sms:{number}:{sms_body}".encode()
                    ).hexdigest()

                    if delivery_key in self._seen_keys:
                        continue
                    already = await _otp_thread(_is_otp_delivered, delivery_key)
                    if already:
                        self._seen_keys.add(delivery_key)
                        if self._is_first_poll and getattr(self, '_grp_backfill_n', 0) < 5:
                            self._grp_backfill_n = getattr(self, '_grp_backfill_n', 0) + 1
                            try:
                                _bc = _extract_country(range_name)
                                _bo = _extract_otp(sms_body)
                                _bg, _bm = _build_group_notify_text(number, _bc, website, _bo, sms_body)
                                await _broadcast_to_groups(bot, "Purple sms", _bg, _bm,
                                                           dt_str=dt_str, number=number, sms_body=sms_body)
                            except Exception as _ge:
                                self._log.warning(f"Purple sms: group backfill failed — {_ge}")
                        continue

                    country = _extract_country(range_name)
                    otp     = _extract_otp(sms_body)

                    self._log.info(
                        f"Purple sms: NEW SMS — website={website}, "
                        f"number=+{number}, otp={otp or '—'}"
                    )
                    await _otp_thread(_mark_otp_delivered, delivery_key)
                    self._seen_keys.add(delivery_key)

                    if not self._is_first_poll:
                        await self._notify_user(bot, number, website, otp, sms_body, delivery_key, sms_dt_str=dt_str)
                    try:
                        grp_text, grp_markup = _build_group_notify_text(number, country, website, otp, sms_body)
                        await _broadcast_to_groups(bot, "Purple sms", grp_text, grp_markup,
                                                   dt_str=dt_str, number=number, sms_body=sms_body)
                    except Exception as _ge:
                        self._log.warning(f"Purple sms: group notify failed — {_ge}")

                self._is_first_poll = False

            except Exception as exc:
                self._log.error(f"Purple sms: Unexpected error — {exc}")

            await _jittered_sleep(self.interval)

    def start(self, bot):
        self._running = True
        self._task    = asyncio.create_task(self._loop(bot))
        self._log.info("Purple sms: Task created.")

    def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
        self._log.info("Purple sms: Stopped.")


# ══════════════════════════════════════════════════════════════════════════════
# Generic Sesskey-based Panel Monitor  (configurable, for future use)
# ══════════════════════════════════════════════════════════════════════════════

class GenericSessKeyMonitor:   # reserved for future sesskey-based panels
    """
    Generic background monitor for sesskey-based panels with configurable URLs.
    Login → extract sesskey from stats page → poll AJAX every interval seconds.
    Column order: [0]datetime [1]range [2]number [3]CLI/website [4]sms_body ...
    Website detected from col[3] if present, otherwise from SMS body text.
    """

    def __init__(
        self,
        panel_name: str,
        login_url: str,
        signin_url: str,
        stats_url: str,
        ajax_url: str,
        username: str,
        password: str,
        interval: int = 16,
    ):
        self.panel_name  = panel_name
        self.login_url   = login_url
        self.signin_url  = signin_url
        self.stats_url   = stats_url
        self.ajax_url    = ajax_url
        self.username    = username
        self.password    = password
        self.interval       = interval
        self.retry_interval = 60
        self._log           = logging.getLogger(f"otp_monitor.{panel_name}")
        self._running       = False
        self._task          = None
        self._seen_keys: set[str] = set()
        self._is_first_poll = True
        self.session: requests.Session | None = None
        self.logged_in      = False
        self._sesskey       = None
        self._latest_record = None   # cached latest SMS for get_latest_today()

    def set_interval(self, seconds: int):
        """Update the polling interval live (no restart needed)."""
        self.interval = max(1, int(seconds))

    def set_retry_interval(self, seconds: int):
        """Update the login-retry interval live (no restart needed)."""
        self.retry_interval = max(1, int(seconds))

    def _refresh_credentials(self):
        """Read latest credentials from DB so admin-panel changes take effect."""
        try:
            from database import _get_panel_by_name
            p = _get_panel_by_name(self.panel_name)
            if p and p.get('username'):
                self.username = p['username']
                self.password = p['password']
        except Exception:
            pass

    def _login(self) -> bool:
        self._refresh_credentials()
        if not self.username or not self.password:
            self._log.warning(f"{self.panel_name}: credentials not set.")
            return False
        last_reason = "unknown"
        for attempt in range(1, _LOGIN_FAST_RETRIES + 1):
            try:
                # Fresh Session Reload — drop any old cookies and rebuild
                self.session = _new_session()
                r1 = self.session.get(self.login_url, timeout=15)
                captcha = _solve_captcha(r1.text)
                if captcha is None:
                    last_reason = "captcha unsolvable"
                    self._log.warning(
                        f"{self.panel_name}: Attempt {attempt}/{_LOGIN_FAST_RETRIES} — {last_reason}."
                    )
                    if attempt < _LOGIN_FAST_RETRIES:
                        time.sleep(_LOGIN_RETRY_DELAY)
                    continue
                self._log.info(
                    f"{self.panel_name}: Attempt {attempt}/{_LOGIN_FAST_RETRIES} — Captcha solved → {captcha}"
                )
                r2 = self.session.post(
                    self.signin_url,
                    data={'username': self.username, 'password': self.password, 'capt': captcha},
                    headers={'Referer': self.login_url},
                    timeout=15, allow_redirects=True,
                )
                final_lower = r2.url.lower().rstrip('/')
                final_last  = final_lower.split('/')[-1]
                if final_last in ('login', 'signin', 'sign-in') or \
                   final_last.endswith('login') or final_last.endswith('signin'):
                    last_reason = "login rejected"
                    self._log.warning(
                        f"{self.panel_name}: Attempt {attempt}/{_LOGIN_FAST_RETRIES} — {last_reason}."
                    )
                    if attempt < _LOGIN_FAST_RETRIES:
                        time.sleep(_LOGIN_RETRY_DELAY)
                    continue
                self.logged_in = True
                self._log.info(
                    f"{self.panel_name}: Logged in successfully (attempt {attempt}/{_LOGIN_FAST_RETRIES})."
                )
                return True
            except Exception as exc:
                last_reason = f"exception: {exc}"
                self._log.warning(
                    f"{self.panel_name}: Attempt {attempt}/{_LOGIN_FAST_RETRIES} — {last_reason}"
                )
                if attempt < _LOGIN_FAST_RETRIES:
                    time.sleep(_LOGIN_RETRY_DELAY)
        self._log.error(
            f"{self.panel_name}: All {_LOGIN_FAST_RETRIES} login attempts failed — {last_reason}"
        )
        self.logged_in = False
        return False

    def _extract_sesskey(self) -> bool:
        try:
            r = self.session.get(self.stats_url, timeout=20)
            if 'login' in r.url.lower():
                self.logged_in = False
                return False
            m = re.search(
                r'"sAjaxSource"\s*:\s*"res/data_smscdr\.php[^"]*sesskey=([^"&]+)"',
                r.text
            )
            if not m:
                self._log.error(f"{self.panel_name}: sesskey not found in stats page.")
                return False
            self._sesskey = m.group(1)
            self._log.info(f"{self.panel_name}: sesskey extracted successfully.")
            return True
        except Exception as exc:
            self._log.error(f"{self.panel_name}: _extract_sesskey error — {exc}")
            return False

    def _fetch_records(self) -> list[dict] | None:
        if not self._sesskey:
            return None
        try:
            now = _bd_now()
            d1  = (now - timedelta(days=7)).strftime('%Y-%m-%d 00:00:00')
            d2  = now.strftime('%Y-%m-%d 23:59:59')
            params = urlencode({
                'fdate1': d1, 'fdate2': d2,
                'frange': '', 'fnum': '', 'fcli': '',
                'fgdate': '', 'fgmonth': '', 'fgrange': '',
                'fgnumber': '', 'fgcli': '',
                'fg': '0',
                'sesskey': self._sesskey,
                'iDisplayStart': '0',
                'iDisplayLength': '999999',
            })
            url = f"{self.ajax_url}?{params}"
            r = self.session.get(
                url,
                headers={
                    'Referer': self.stats_url,
                    'X-Requested-With': 'XMLHttpRequest',
                },
                timeout=25,
            )
            if 'login' in r.url.lower():
                self.logged_in = False
                return None
            data = r.json()
            rows = data.get('aaData', [])
            num_col, body_col = _get_col_cfg(self.panel_name, 2, 5)
            results = []
            for row in rows:
                if not isinstance(row, list) or len(row) < 3:
                    continue
                dt_str     = str(row[0]).strip() if row[0] else ''
                range_name = str(row[1]).strip() if row[1] else ''
                number     = re.sub(r'[^\d]', '', str(row[num_col])) if len(row) > num_col and row[num_col] else ''
                # Skip totals/summary rows — they don't have a valid datetime
                if not re.match(r'\d{4}-\d{2}-\d{2}', dt_str):
                    continue
                sms_body = str(row[body_col]).strip() if len(row) > body_col and row[body_col] else ''
                # Fall back gracefully if configured body_col out of range
                if not sms_body and len(row) > 4:
                    sms_body = str(row[4]).strip() if row[4] else ''
                website = str(row[3]).strip() if len(row) > 3 and row[3] else ''
                detected = _detect_website_from_body(sms_body)
                if detected and detected != 'Unknown':
                    website = detected
                elif not website or website.lower() in ('unknown', '', 'none'):
                    website = detected
                if not number or not dt_str:
                    continue
                results.append({
                    'datetime':   dt_str,
                    'range_name': range_name,
                    'number':     number,
                    'website':    website,
                    'sms_body':   sms_body,
                })
            self._log.info(f"{self.panel_name}: Fetched {len(results)} SMS records.")
            return results
        except Exception as exc:
            self._log.error(f"{self.panel_name}: _fetch_records error — {exc}")
            return None


    def get_latest_today(self) -> 'dict | None':
        """Always fetch fresh live data. Falls back to cache only if session is down or on error."""
        if not self.logged_in or not self.session or not self._sesskey:
            return None
        try:
            now = _bd_now()
            params = urlencode({
                'fdate1': (now - timedelta(days=7)).strftime('%Y-%m-%d 00:00:00'),
                'fdate2': now.strftime('%Y-%m-%d 23:59:59'),
                'frange': '', 'fnum': '', 'fcli': '',
                'fgdate': '', 'fgmonth': '', 'fgrange': '',
                'fgnumber': '', 'fgcli': '', 'fg': '0',
                'sesskey': self._sesskey,
                'iDisplayStart': '0', 'iDisplayLength': '999999',
            })
            r = self.session.get(
                f"{self.ajax_url}?{params}",
                headers={'Referer': self.stats_url, 'X-Requested-With': 'XMLHttpRequest'},
                timeout=20,
            )
            if 'login' in r.url.lower():
                self.logged_in = False
                self._log.warning(f"{self.panel_name}: get_latest_today — session expired, falling back to cache.")
                return None
            rows = r.json().get('aaData', [])
            valid = []
            for row in rows:
                if not isinstance(row, list) or len(row) < 3:
                    continue
                dt_str = str(row[0]).strip() if row[0] else ''
                if not re.match(r'\d{4}-\d{2}-\d{2}', dt_str):
                    continue
                number = re.sub(r'[^\d]', '', str(row[2])) if row[2] else ''
                if not number:
                    continue
                if len(row) > 5:
                    sms_body = str(row[5]).strip() if row[5] else ''
                elif len(row) > 4:
                    sms_body = str(row[4]).strip() if row[4] else ''
                else:
                    sms_body = ''
                valid.append({
                    'dt': dt_str, 'number': number,
                    'range_name': str(row[1]).strip() if row[1] else '',
                    'sms_body': sms_body,
                })
            if not valid:
                self._log.info(f"{self.panel_name}: get_latest_today — no valid rows, falling back to cache.")
                return None
            valid.sort(key=lambda x: x['dt'], reverse=True)
            rec = valid[0]
            uid = hashlib.md5(f"{rec['dt']}:{rec['number']}:{rec['sms_body']}".encode()).hexdigest()
            result = {
                'id': uid, 'datetime': rec['dt'], 'number': rec['number'],
                'website': _detect_website_from_body(rec['sms_body']),
                'country': _extract_country(rec['range_name']),
                'otp': _extract_otp(rec['sms_body']), 'message': rec['sms_body'],
                'received_at': rec['dt'], 'panel_name': self.panel_name,
            }
            return result
        except Exception as exc:
            self._log.error(f"{self.panel_name}: get_latest_today error — {exc}. Falling back to cache.")
            return None

    async def _notify_user(self, bot, number: str, website: str, otp: str, sms_body: str, delivery_key: str = '', sms_dt_str: str = ''):
        try:
            from database import (
                _get_recent_user_by_number as _gub,
                _get_otp_bonus_settings,
                _has_otp_bonus_received, _record_otp_bonus,
                _get_effective_otp_bonus, _get_user_balance,
                _get_notify_window,
            )
            sms_ts = _parse_panel_dt(sms_dt_str)
            _win_sec = _get_notify_window() * 60
            _gub_ts = lambda n: _gub(n, max_age_seconds=_win_sec, sms_ts=sms_ts)
            uid = await _otp_thread(_gub_ts, number)
            if not uid:
                uid = await _otp_thread(_gub_ts, '+' + number)
            if uid and bot:
                bonus_amount_credited = None
                new_balance = None
                if delivery_key:
                    bonus_cfg = await _otp_thread(_get_otp_bonus_settings)
                    if bonus_cfg['enabled']:
                        already = await _otp_thread(_has_otp_bonus_received, delivery_key)
                        if not already:
                            effective_amount = await _otp_thread(_get_effective_otp_bonus, number, bonus_cfg['amount'])
                            credited = await _otp_thread(
                                _record_otp_bonus, uid, delivery_key, effective_amount
                            )
                            if credited:
                                new_balance = await _otp_thread(_get_user_balance, uid)
                                bonus_amount_credited = effective_amount
                notify_text = _build_sms_notify_text(
                    number, website, sms_body,
                    bonus_amount=bonus_amount_credited,
                    new_balance=new_balance,
                )
                await bot.send_message(chat_id=uid, text=notify_text, parse_mode='HTML')
                self._log.info(f"{self.panel_name}: Notified user {uid} for +{number}")
        except Exception as notify_exc:
            self._log.warning(f"{self.panel_name}: Could not notify user — {notify_exc}")

    async def _loop(self, bot):
        from database import (_is_otp_delivered, _mark_otp_delivered,
                              _update_panel_status, _is_panel_enabled)
        self._log.info(f"{self.panel_name}: Starting.")
        # ── Wait until panel is enabled before attempting login
        while self._running and (
            not _is_panel_enabled(self.panel_name)
            or getattr(self, '_manual_only', False)
        ):
            await asyncio.sleep(5)
        if not self._running:
            return
        ok = await _otp_thread(self._login)
        _login_fail_notified = False
        while not ok and self._running:
            self._log.warning(f"{self.panel_name}: Login failed — retrying in {self.retry_interval}s…")
            await _otp_thread(_update_panel_status, self.panel_name, False, None, 'Login failed — retrying')
            if not _login_fail_notified:
                await _notify_admins_login_fail(bot, self.panel_name)
                _login_fail_notified = True
            await asyncio.sleep(self.retry_interval)
            while self._running and (
                not _is_panel_enabled(self.panel_name)
                or getattr(self, '_manual_only', False)
            ):
                await asyncio.sleep(5)
            if not self._running:
                return
            ok = await _otp_thread(self._login)
        if not self._running:
            return
        await _otp_thread(_update_panel_status, self.panel_name, True)
        if _login_fail_notified:
            await _notify_admins_login_success(bot, self.panel_name)
            _login_fail_notified = False
        ok = await _otp_thread(self._extract_sesskey)
        if not ok:
            self._log.error(f"{self.panel_name}: Could not extract sesskey.")
            return

        while self._running:
            if not _is_panel_enabled(self.panel_name) or getattr(self, '_manual_only', False):
                await asyncio.sleep(5)
                continue
            try:
                records = await _otp_thread(self._fetch_records)

                if records is None:
                    self._log.info(f"{self.panel_name}: Session expired — re-logging in …")
                    _cf = getattr(self, '_consec_failures', 0) + 1
                    setattr(self, '_consec_failures', _cf)
                    _back = min(
                        BACKOFF_BASE_SECS * (BACKOFF_MULTIPLIER ** min(_cf - 1, 6)),
                        BACKOFF_MAX_SECS,
                    )
                    if _cf > 1:
                        self._log.warning(
                            "%s: consecutive failure #%d — extra back-off %.0fs",
                            self.panel_name, _cf, _back,
                        )
                    # Wait if in manual-only mode (set after Session Cleanup)
                    while self._running and getattr(self, '_manual_only', False):
                        await asyncio.sleep(5)
                    if not self._running:
                        return
                    await _midnight_relogin_jitter(self.panel_name)
                    ok = await _otp_thread(self._login)
                    if ok:
                        await _otp_thread(_update_panel_status, self.panel_name, True)
                        await _otp_thread(self._extract_sesskey)
                    await _jittered_sleep(self.interval + _back)
                    continue
                setattr(self, '_consec_failures', 0)
                await _otp_thread(_update_panel_status, self.panel_name, True, len(records))

                # Always update _latest_record with the most recent SMS from each poll
                if records:
                    _r0 = records[0]
                    uid0 = hashlib.md5(f"{_r0['datetime']}:{_r0['number']}:{_r0['sms_body']}".encode()).hexdigest()
                    self._latest_record = {
                        'id': uid0, 'datetime': _r0['datetime'], 'number': _r0['number'],
                        'website': _r0['website'] or _detect_website_from_body(_r0['sms_body']),
                        'country': _extract_country(_r0['range_name']),
                        'otp': _extract_otp(_r0['sms_body']), 'message': _r0['sms_body'],
                        'received_at': _r0['datetime'], 'panel_name': self.panel_name,
                    }

                for rec in records:
                    dt_str     = rec['datetime']
                    range_name = rec['range_name']
                    number     = rec['number']
                    website    = rec['website']
                    sms_body   = rec['sms_body']

                    delivery_key = hashlib.sha256(
                        f"sms:{number}:{sms_body}".encode()
                    ).hexdigest()

                    if delivery_key in self._seen_keys:
                        continue
                    already = await _otp_thread(_is_otp_delivered, delivery_key)
                    if already:
                        self._seen_keys.add(delivery_key)
                        if self._is_first_poll and getattr(self, '_grp_backfill_n', 0) < 5:
                            self._grp_backfill_n = getattr(self, '_grp_backfill_n', 0) + 1
                            try:
                                _bc = _extract_country(range_name)
                                _bo = _extract_otp(sms_body)
                                _bg, _bm = _build_group_notify_text(number, _bc, website, _bo, sms_body)
                                await _broadcast_to_groups(bot, self.panel_name, _bg, _bm,
                                                           dt_str=dt_str, number=number, sms_body=sms_body)
                            except Exception as _ge:
                                self._log.warning(f"{self.panel_name}: group backfill failed — {_ge}")
                        continue

                    country = _extract_country(range_name)
                    otp     = _extract_otp(sms_body)

                    self._log.info(
                        f"{self.panel_name}: NEW SMS — website={website}, "
                        f"number=+{number}, otp={otp or '—'}"
                    )
                    await _otp_thread(_mark_otp_delivered, delivery_key)
                    self._seen_keys.add(delivery_key)

                    if not self._is_first_poll:
                        await self._notify_user(bot, number, website, otp, sms_body, delivery_key, sms_dt_str=dt_str)
                    try:
                        grp_text, grp_markup = _build_group_notify_text(number, country, website, otp, sms_body)
                        await _broadcast_to_groups(bot, self.panel_name, grp_text, grp_markup,
                                                   dt_str=dt_str, number=number, sms_body=sms_body)
                    except Exception as _ge:
                        self._log.warning(f"{self.panel_name}: group notify failed — {_ge}")

                self._is_first_poll = False

            except Exception as exc:
                self._log.error(f"{self.panel_name}: Unexpected error — {exc}")

            await _jittered_sleep(self.interval)

    def start(self, bot):
        self._running = True
        self._task    = asyncio.create_task(self._loop(bot))
        self._log.info(f"{self.panel_name}: Task created.")

    def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
        self._log.info(f"{self.panel_name}: Stopped.")


# ══════════════════════════════════════════════════════════════════════════════
# Wolf SMS — ClientPanelMonitor subclass (no captcha, /agent/ path)
# ══════════════════════════════════════════════════════════════════════════════

class WolfSmsMonitor(ClientPanelMonitor):
    """Wolf SMS panel: reCAPTCHA present on login page but NOT enforced server-
    side.  We POST username/password directly — no captcha solving needed."""

    def _login(self) -> bool:
        if not self.username or not self.password:
            self._log.warning(f"{self.panel_name}: credentials not set.")
            return False
        last_reason = "unknown"
        for attempt in range(1, _LOGIN_FAST_RETRIES + 1):
            try:
                self.session = _new_session()
                # Skip captcha — server accepts bare username/password POST
                r2 = self.session.post(
                    self.signin_url,
                    data={'username': self.username, 'password': self.password},
                    headers={'Referer': self.login_page},
                    timeout=15, allow_redirects=True,
                )
                final_path = r2.url.lower()
                final_last = final_path.rstrip('/').split('/')[-1]
                if final_last in ('login', 'signin', 'sign-in') or \
                   final_last.endswith('login') or final_last.endswith('signin'):
                    last_reason = "login rejected"
                    self._log.warning(
                        f"{self.panel_name}: Attempt {attempt}/{_LOGIN_FAST_RETRIES} — {last_reason}."
                    )
                    if attempt < _LOGIN_FAST_RETRIES:
                        time.sleep(_LOGIN_RETRY_DELAY)
                    continue
                self.logged_in = True
                self._log.info(
                    f"{self.panel_name}: Logged in successfully (attempt {attempt}/{_LOGIN_FAST_RETRIES})."
                )
                return True
            except Exception as exc:
                last_reason = f"exception: {exc}"
                self._log.warning(
                    f"{self.panel_name}: Attempt {attempt}/{_LOGIN_FAST_RETRIES} — {last_reason}"
                )
                if attempt < _LOGIN_FAST_RETRIES:
                    time.sleep(_LOGIN_RETRY_DELAY)
        self._log.error(
            f"{self.panel_name}: All {_LOGIN_FAST_RETRIES} login attempts failed — {last_reason}"
        )
        self.logged_in = False
        return False


# ══════════════════════════════════════════════════════════════════════════════
# Shark SMS — GenericSessKeyMonitor subclass (math captcha + crlf hidden field)
# ══════════════════════════════════════════════════════════════════════════════

class SharkSmsMonitor(GenericSessKeyMonitor):
    """Shark SMS panel: math captcha (name=capt) + hidden crlf='' field on login,
    then sesskey extracted from the stats page AJAX source."""

    def _login(self) -> bool:
        if not self.username or not self.password:
            self._log.warning(f"{self.panel_name}: credentials not set.")
            return False
        last_reason = "unknown"
        for attempt in range(1, _LOGIN_FAST_RETRIES + 1):
            try:
                self.session = _new_session()
                r1 = self.session.get(self.login_url, timeout=15)
                captcha = _solve_captcha(r1.text)
                if captcha is None:
                    last_reason = "captcha unsolvable"
                    self._log.warning(
                        f"{self.panel_name}: Attempt {attempt}/{_LOGIN_FAST_RETRIES} — {last_reason}."
                    )
                    if attempt < _LOGIN_FAST_RETRIES:
                        time.sleep(_LOGIN_RETRY_DELAY)
                    continue
                self._log.info(
                    f"{self.panel_name}: Attempt {attempt}/{_LOGIN_FAST_RETRIES} — Captcha solved → {captcha}"
                )
                r2 = self.session.post(
                    self.signin_url,
                    data={
                        'username': self.username,
                        'password': self.password,
                        'capt': captcha,
                        'crlf': '',
                    },
                    headers={'Referer': self.login_url},
                    timeout=15, allow_redirects=True,
                )
                final_lower = r2.url.lower().rstrip('/')
                final_last  = final_lower.split('/')[-1]
                if final_last in ('login', 'signin', 'sign-in') or \
                   final_last.endswith('login') or final_last.endswith('signin'):
                    last_reason = "login rejected"
                    self._log.warning(
                        f"{self.panel_name}: Attempt {attempt}/{_LOGIN_FAST_RETRIES} — {last_reason}."
                    )
                    if attempt < _LOGIN_FAST_RETRIES:
                        time.sleep(_LOGIN_RETRY_DELAY)
                    continue
                self.logged_in = True
                self._log.info(
                    f"{self.panel_name}: Logged in successfully (attempt {attempt}/{_LOGIN_FAST_RETRIES})."
                )
                return True
            except Exception as exc:
                last_reason = f"exception: {exc}"
                self._log.warning(
                    f"{self.panel_name}: Attempt {attempt}/{_LOGIN_FAST_RETRIES} — {last_reason}"
                )
                if attempt < _LOGIN_FAST_RETRIES:
                    time.sleep(_LOGIN_RETRY_DELAY)
        self._log.error(
            f"{self.panel_name}: All {_LOGIN_FAST_RETRIES} login attempts failed — {last_reason}"
        )
        self.logged_in = False
        return False


# ══════════════════════════════════════════════════════════════════════════════
# KM Carrier SMS Monitor  (sesskey-based, /agent/ path, math captcha + crlf)
# Same login mechanism as SharkSmsMonitor; different server & credentials.
# ══════════════════════════════════════════════════════════════════════════════

class KMCarrierSmsMonitor(SharkSmsMonitor):
    """KM Carrier SMS panel monitor.
    Server:   http://54.36.173.235/ints
    Login:    math captcha (capt) + crlf='' hidden field  — same as Shark SMS
    Path:     /agent/ (sesskey-based, same sAjaxSource extraction as OTPMonitor)
    Interval: 17 s minimum (panel enforces 15 s rate-limit)
    """

    def __init__(self):
        super().__init__(
            panel_name = 'KM Carrier sms',
            login_url  = KM_CARRIER_SMS_LOGIN_URL,
            signin_url = KM_CARRIER_SMS_SIGNIN_URL,
            stats_url  = KM_CARRIER_SMS_STATS_URL,
            ajax_url   = KM_CARRIER_SMS_AJAX_URL,
            username   = KM_CARRIER_SMS_USERNAME,
            password   = KM_CARRIER_SMS_PASSWORD,
            interval   = KM_CARRIER_SMS_INTERVAL,
        )


# ══════════════════════════════════════════════════════════════════════════════
# MSI SMS Monitor  (sesskey-based, /agent/ path — same panel type as SMS Hadi)
# ══════════════════════════════════════════════════════════════════════════════

class MsiSmsMonitor(OTPMonitor):
    """
    Background monitor for MSI SMS panel.
    Sesskey-based auth on /agent/ path — same panel software as SMS Hadi.
    Columns: [0]datetime [1]range [2]number [3]website/client [4]cli [5]sms_body
    """

    _STATS_URL = MSI_SMS_STATS_URL
    _AJAX_BASE = MSI_SMS_AJAX_URL

    def __init__(self):
        super().__init__()
        self.panel_name = 'Msi sms'
        self.interval   = SMS_MONITOR_INTERVAL
        self._username  = MSI_SMS_USERNAME
        self._password  = MSI_SMS_PASSWORD

    def _refresh_credentials(self):
        try:
            from database import _get_panel_by_name
            p = _get_panel_by_name(self.panel_name)
            if p and p.get('username'):
                self._username = p['username']
                self._password = p['password']
        except Exception:
            pass

    def _login(self) -> bool:
        self._refresh_credentials()
        if not self._username or not self._password:
            logger.warning("MsiSms: credentials not set.")
            return False
        last_reason = "unknown"
        for attempt in range(1, _LOGIN_FAST_RETRIES + 1):
            try:
                self.session = _new_session()
                r1 = self.session.get(MSI_SMS_LOGIN_URL, timeout=15)
                captcha = _solve_captcha(r1.text)
                if captcha is None:
                    last_reason = "captcha unsolvable"
                    logger.warning(f"MsiSms: Attempt {attempt}/{_LOGIN_FAST_RETRIES} — {last_reason}.")
                    if attempt < _LOGIN_FAST_RETRIES:
                        time.sleep(_LOGIN_RETRY_DELAY)
                    continue
                logger.info(f"MsiSms: Attempt {attempt}/{_LOGIN_FAST_RETRIES} — Captcha solved → {captcha}")
                r2 = self.session.post(
                    MSI_SMS_SIGNIN_URL,
                    data={'username': self._username, 'password': self._password, 'capt': captcha},
                    headers={'Referer': MSI_SMS_LOGIN_URL},
                    timeout=15, allow_redirects=True,
                )
                if 'login' in r2.url.lower():
                    last_reason = "login rejected"
                    logger.warning(f"MsiSms: Attempt {attempt}/{_LOGIN_FAST_RETRIES} — {last_reason}.")
                    if attempt < _LOGIN_FAST_RETRIES:
                        time.sleep(_LOGIN_RETRY_DELAY)
                    continue
                self.logged_in = True
                logger.info(f"MsiSms: Logged in successfully (attempt {attempt}/{_LOGIN_FAST_RETRIES}).")
                return True
            except Exception as exc:
                last_reason = f"exception: {exc}"
                logger.warning(f"MsiSms: Attempt {attempt}/{_LOGIN_FAST_RETRIES} — {last_reason}")
                if attempt < _LOGIN_FAST_RETRIES:
                    time.sleep(_LOGIN_RETRY_DELAY)
        logger.error(f"MsiSms: All {_LOGIN_FAST_RETRIES} login attempts failed — {last_reason}")
        self.logged_in = False
        return False

    def _extract_sesskey(self) -> bool:
        try:
            r = self.session.get(self._STATS_URL, timeout=20)
            if 'login' in r.url.lower():
                self.logged_in = False
                return False
            m = re.search(
                r'"sAjaxSource"\s*:\s*"res/data_smscdr\.php[^"]*sesskey=([^"&]+)"',
                r.text
            )
            if not m:
                logger.error("MsiSms: sesskey not found in stats page.")
                return False
            self._sesskey = m.group(1)
            logger.info(f"MsiSms: sesskey extracted → {self._sesskey[:12]}…")
            return True
        except Exception as exc:
            logger.error(f"MsiSms: _extract_sesskey error — {exc}")
            return False

    def _build_ajax_url(self, days_back: int = 7) -> str:
        now = _bd_now()
        params = {
            'fdate1': (now - timedelta(days=days_back)).strftime('%Y-%m-%d 00:00:00'),
            'fdate2': now.strftime('%Y-%m-%d 23:59:59'),
            'frange': '', 'fclient': '', 'fnum': '', 'fcli': '',
            'fgdate': '', 'fgmonth': '', 'fgrange': '', 'fgclient': '',
            'fgnumber': '', 'fgcli': '', 'fg': '0',
            'sesskey': self._sesskey,
            'iDisplayStart': '0', 'iDisplayLength': '999999',
            'iSortCol_0': '0', 'sSortDir_0': 'desc',
        }
        return f"{self._AJAX_BASE}?{urlencode(params)}"

    def _fetch_individual_records(self) -> list[dict] | None:
        if not self._sesskey:
            return None
        try:
            url = self._build_ajax_url()
            r = self.session.get(
                url,
                headers={'Referer': self._STATS_URL, 'X-Requested-With': 'XMLHttpRequest'},
                timeout=25,
            )
            if 'login' in r.url.lower():
                self.logged_in = False
                return None
            data = r.json()
            rows = data.get('aaData', [])
            results = []
            for row in rows:
                if not isinstance(row, list) or len(row) < 6:
                    continue
                dt_str     = str(row[0]).strip() if row[0] else ''
                range_name = str(row[1]).strip() if row[1] else ''
                number     = re.sub(r'[^\d]', '', str(row[2])) if row[2] else ''
                website    = (str(row[3]).strip() if row[3] else '') or 'Unknown'
                sms_body   = str(row[5]).strip() if len(row) > 5 and row[5] else ''
                detected = _detect_website_from_body(sms_body)
                if detected and detected != 'Unknown':
                    website = detected
                if not number or not dt_str:
                    continue
                results.append({
                    'datetime': dt_str, 'range_name': range_name,
                    'number': number, 'website': website, 'sms_body': sms_body,
                })
            logger.info(f"MsiSms: Fetched {len(results)} SMS records.")
            return results
        except Exception as exc:
            logger.error(f"MsiSms: _fetch_individual_records error — {exc}")
            return None

    def get_latest_today(self) -> 'dict | None':
        if not self.logged_in or not self.session or not self._sesskey:
            return None
        try:
            url = self._build_ajax_url(days_back=7)
            r = self.session.get(
                url,
                headers={'Referer': self._STATS_URL, 'X-Requested-With': 'XMLHttpRequest'},
                timeout=20,
            )
            if 'login' in r.url.lower():
                self.logged_in = False
                return None
            rows = r.json().get('aaData', [])
            valid = []
            for row in rows:
                if not isinstance(row, list) or len(row) < 6:
                    continue
                dt_str = str(row[0]).strip() if row[0] else ''
                if not re.match(r'\d{4}-\d{2}-\d{2}', dt_str):
                    continue
                number = re.sub(r'[^\d]', '', str(row[2])) if row[2] else ''
                if not number:
                    continue
                sms_body = str(row[5]).strip() if len(row) > 5 and row[5] else ''
                valid.append({
                    'dt': dt_str, 'number': number,
                    'website': str(row[3]).strip() if row[3] else '',
                    'range_name': str(row[1]).strip() if row[1] else '',
                    'sms_body': sms_body,
                })
            if not valid:
                return None
            valid.sort(key=lambda x: x['dt'], reverse=True)
            rec = valid[0]
            uid = hashlib.md5(f"{rec['dt']}:{rec['number']}:{rec['sms_body']}".encode()).hexdigest()
            return {
                'id': uid, 'datetime': rec['dt'], 'number': rec['number'],
                'website': rec['website'] or _detect_website_from_body(rec['sms_body']),
                'country': _extract_country(rec['range_name']),
                'otp': _extract_otp(rec['sms_body']), 'message': rec['sms_body'],
                'received_at': rec['dt'], 'panel_name': self.panel_name,
            }
        except Exception as exc:
            logger.error(f"MsiSms: get_latest_today error — {exc}")
            return None


# ══════════════════════════════════════════════════════════════════════════════
# Module-level singletons
# ══════════════════════════════════════════════════════════════════════════════

# SMS Hadi monitor (sesskey-based, /agent/ path — credentials are agent-type)
monitor = OTPMonitor()

# SMS Hadi 2 monitor (second account — same server, different credentials)
sms_hadi2_monitor = OTPMonitor2()

# Konekta Premium monitor (cookie-based, login at /sign-in)
konekta_monitor = ClientPanelMonitor(
    panel_name    = 'Konekta Premium',
    base_url      = KONEKTA_BASE,
    login_page_url= KONEKTA_LOGIN_URL,
    signin_url    = KONEKTA_SIGNIN_URL,
    username      = KONEKTA_USERNAME,
    password      = KONEKTA_PASSWORD,
)

# MSI SMS monitor (sesskey-based, /agent/ path — same panel type as SMS Hadi)
msi_sms_monitor = MsiSmsMonitor()

# Number Panel monitor (sesskey-based, /client/ path, 17s interval)
number_panel_monitor = NumberPanelMonitor()

# Purple SMS monitor (cookie-based, /sms/dialer/ajax/ path, no sesskey)
purple_sms_monitor = PurpleSmsMonitor()

# Proof SMS monitor (cookie-based, /ints/ path, 3s interval)
proof_sms_monitor = ClientPanelMonitor(
    panel_name    = 'Proof sms',
    base_url      = PROOF_SMS_BASE,
    login_page_url= PROOF_SMS_LOGIN_URL,
    signin_url    = PROOF_SMS_SIGNIN_URL,
    username      = PROOF_SMS_USERNAME,
    password      = PROOF_SMS_PASSWORD,
)

# Lamix SMS monitor (cookie-based, /ints/ path, /agent/ sub-path, 3s interval)
lamix_sms_monitor = ClientPanelMonitor(
    panel_name    = 'Lamix sms',
    base_url      = LAMIX_SMS_BASE,
    login_page_url= LAMIX_SMS_LOGIN_URL,
    signin_url    = LAMIX_SMS_SIGNIN_URL,
    username      = LAMIX_SMS_USERNAME,
    password      = LAMIX_SMS_PASSWORD,
    path_prefix   = 'agent',
)

# ══════════════════════════════════════════════════════════════════════════════
# Seven 1 Tel Monitor  (sesskey-based, /agent/ path — same panel type as SMS Hadi)
# ══════════════════════════════════════════════════════════════════════════════

class Seven1TelMonitor(OTPMonitor):
    """
    Background monitor for Seven 1 Tel panel.
    Sesskey-based auth on /agent/ path — same panel software as SMS Hadi.
    Overrides login/sesskey/fetch to use Seven 1 Tel URLs; inherits _loop,
    _notify_user, start, stop from OTPMonitor.
    Columns: [0]datetime [1]range [2]number [3]website/client [4]cli [5]sms_body
    """

    _STATS_URL = SEVEN1TEL_STATS_URL                        # /agent/SMSCDRStats
    _AJAX_BASE = SEVEN1TEL_AJAX_URL                         # /agent/res/data_smscdr.php

    def __init__(self):
        super().__init__()
        self.panel_name     = 'Seven 1 Tel'
        self.interval       = SEVEN1TEL_INTERVAL
        self._username      = SEVEN1TEL_USERNAME
        self._password      = SEVEN1TEL_PASSWORD

    def _refresh_credentials(self):
        try:
            from database import _get_panel_by_name
            p = _get_panel_by_name(self.panel_name)
            if p and p.get('username'):
                self._username = p['username']
                self._password = p['password']
        except Exception:
            pass

    def _login(self) -> bool:
        self._refresh_credentials()
        if not self._username or not self._password:
            logger.warning("Seven1Tel: credentials not set.")
            return False
        last_reason = "unknown"
        for attempt in range(1, _LOGIN_FAST_RETRIES + 1):
            try:
                self.session = _new_session()
                r1 = self.session.get(SEVEN1TEL_LOGIN_URL, timeout=15)
                captcha = _solve_captcha(r1.text)
                if captcha is None:
                    last_reason = "captcha unsolvable"
                    logger.warning(f"Seven1Tel: Attempt {attempt}/{_LOGIN_FAST_RETRIES} — {last_reason}.")
                    if attempt < _LOGIN_FAST_RETRIES:
                        time.sleep(_LOGIN_RETRY_DELAY)
                    continue
                logger.info(f"Seven1Tel: Attempt {attempt}/{_LOGIN_FAST_RETRIES} — Captcha solved → {captcha}")
                r2 = self.session.post(
                    SEVEN1TEL_SIGNIN_URL,
                    data={'username': self._username, 'password': self._password, 'capt': captcha},
                    headers={'Referer': SEVEN1TEL_LOGIN_URL},
                    timeout=15, allow_redirects=True,
                )
                if 'login' in r2.url.lower():
                    last_reason = "login rejected"
                    logger.warning(f"Seven1Tel: Attempt {attempt}/{_LOGIN_FAST_RETRIES} — {last_reason}.")
                    if attempt < _LOGIN_FAST_RETRIES:
                        time.sleep(_LOGIN_RETRY_DELAY)
                    continue
                self.logged_in = True
                logger.info(f"Seven1Tel: Logged in successfully (attempt {attempt}/{_LOGIN_FAST_RETRIES}).")
                return True
            except Exception as exc:
                last_reason = f"exception: {exc}"
                logger.warning(f"Seven1Tel: Attempt {attempt}/{_LOGIN_FAST_RETRIES} — {last_reason}")
                if attempt < _LOGIN_FAST_RETRIES:
                    time.sleep(_LOGIN_RETRY_DELAY)
        logger.error(f"Seven1Tel: All {_LOGIN_FAST_RETRIES} login attempts failed — {last_reason}")
        self.logged_in = False
        return False

    def _extract_sesskey(self) -> bool:
        try:
            r = self.session.get(self._STATS_URL, timeout=20)
            if 'login' in r.url.lower():
                self.logged_in = False
                return False
            m = re.search(
                r'"sAjaxSource"\s*:\s*"res/data_smscdr\.php[^"]*sesskey=([^"&]+)"',
                r.text
            )
            if not m:
                logger.error("Seven1Tel: sesskey not found in stats page.")
                return False
            self._sesskey = m.group(1)
            logger.info(f"Seven1Tel: sesskey extracted → {self._sesskey[:12]}…")
            return True
        except Exception as exc:
            logger.error(f"Seven1Tel: _extract_sesskey error — {exc}")
            return False

    def _build_ajax_url(self, days_back: int = 7) -> str:
        now = _bd_now()
        params = {
            'fdate1': (now - timedelta(days=days_back)).strftime('%Y-%m-%d 00:00:00'),
            'fdate2': now.strftime('%Y-%m-%d 23:59:59'),
            'frange': '', 'fclient': '', 'fnum': '', 'fcli': '',
            'fgdate': '', 'fgmonth': '', 'fgrange': '', 'fgclient': '',
            'fgnumber': '', 'fgcli': '', 'fg': '0',
            'sesskey': self._sesskey,
            'iDisplayStart': '0', 'iDisplayLength': '999999',
            'iSortCol_0': '0', 'sSortDir_0': 'desc',
        }
        return f"{self._AJAX_BASE}?{urlencode(params)}"

    def _fetch_individual_records(self) -> list[dict] | None:
        if not self._sesskey:
            return None
        try:
            url = self._build_ajax_url()
            r = self.session.get(
                url,
                headers={'Referer': self._STATS_URL, 'X-Requested-With': 'XMLHttpRequest'},
                timeout=25,
            )
            if 'login' in r.url.lower():
                self.logged_in = False
                return None
            data = r.json()
            rows = data.get('aaData', [])
            results = []
            for row in rows:
                if not isinstance(row, list) or len(row) < 6:
                    continue
                dt_str     = str(row[0]).strip() if row[0] else ''
                range_name = str(row[1]).strip() if row[1] else ''
                number     = re.sub(r'[^\d]', '', str(row[2])) if row[2] else ''
                website    = (str(row[3]).strip() if row[3] else '') or 'Unknown'
                sms_body   = str(row[5]).strip() if len(row) > 5 and row[5] else ''
                detected = _detect_website_from_body(sms_body)
                if detected and detected != 'Unknown':
                    website = detected
                if not number or not dt_str:
                    continue
                results.append({
                    'datetime': dt_str, 'range_name': range_name,
                    'number': number, 'website': website, 'sms_body': sms_body,
                })
            logger.info(f"Seven1Tel: Fetched {len(results)} SMS records.")
            return results
        except Exception as exc:
            logger.error(f"Seven1Tel: _fetch_individual_records error — {exc}")
            return None

    def get_latest_today(self) -> 'dict | None':
        if not self.logged_in or not self.session or not self._sesskey:
            return None
        try:
            url = self._build_ajax_url(days_back=7)
            r = self.session.get(
                url,
                headers={'Referer': self._STATS_URL, 'X-Requested-With': 'XMLHttpRequest'},
                timeout=20,
            )
            if 'login' in r.url.lower():
                self.logged_in = False
                return None
            rows = r.json().get('aaData', [])
            valid = []
            for row in rows:
                if not isinstance(row, list) or len(row) < 6:
                    continue
                dt_str = str(row[0]).strip() if row[0] else ''
                if not re.match(r'\d{4}-\d{2}-\d{2}', dt_str):
                    continue
                number = re.sub(r'[^\d]', '', str(row[2])) if row[2] else ''
                if not number:
                    continue
                sms_body = str(row[5]).strip() if len(row) > 5 and row[5] else ''
                valid.append({
                    'dt': dt_str, 'number': number,
                    'website': str(row[3]).strip() if row[3] else '',
                    'range_name': str(row[1]).strip() if row[1] else '',
                    'sms_body': sms_body,
                })
            if not valid:
                return None
            valid.sort(key=lambda x: x['dt'], reverse=True)
            rec = valid[0]
            uid = hashlib.md5(f"{rec['dt']}:{rec['number']}:{rec['sms_body']}".encode()).hexdigest()
            return {
                'id': uid, 'datetime': rec['dt'], 'number': rec['number'],
                'website': rec['website'] or _detect_website_from_body(rec['sms_body']),
                'country': _extract_country(rec['range_name']),
                'otp': _extract_otp(rec['sms_body']), 'message': rec['sms_body'],
                'received_at': rec['dt'], 'panel_name': self.panel_name,
            }
        except Exception as exc:
            logger.error(f"Seven1Tel: get_latest_today error — {exc}")
            return None


# Seven 1 Tel monitor (sesskey-based, /agent/ path — same panel software as SMS Hadi)
seven1tel_monitor = Seven1TelMonitor()

# Flex SMS monitor (cookie-based, /ints/agent/ path, 3s interval)
mait_sms_monitor = ClientPanelMonitor(
    panel_name    = 'Flex sms',
    base_url      = MAIT_SMS_BASE,
    login_page_url= MAIT_SMS_LOGIN_URL,
    signin_url    = MAIT_SMS_SIGNIN_URL,
    username      = MAIT_SMS_USERNAME,
    password      = MAIT_SMS_PASSWORD,
    path_prefix   = 'agent',
)

# Zento SMS monitor (cookie-based, /ints/ path, 3s interval)
zento_sms_monitor = ClientPanelMonitor(
    panel_name    = 'Zento sms',
    base_url      = ZENTO_SMS_BASE,
    login_page_url= ZENTO_SMS_LOGIN_URL,
    signin_url    = ZENTO_SMS_SIGNIN_URL,
    username      = ZENTO_SMS_USERNAME,
    password      = ZENTO_SMS_PASSWORD,
)

# Wolf SMS monitor (cookie-based, /ints/ agent path, no captcha, 3s interval)
wolf_sms_monitor = WolfSmsMonitor(
    panel_name    = 'Wolf sms',
    base_url      = WOLF_SMS_BASE,
    login_page_url= WOLF_SMS_LOGIN_URL,
    signin_url    = WOLF_SMS_SIGNIN_URL,
    username      = WOLF_SMS_USERNAME,
    password      = WOLF_SMS_PASSWORD,
    path_prefix   = 'agent',
)

# Shark SMS monitor (sesskey-based, /ints/ agent path, math captcha + crlf, 3s interval)
shark_sms_monitor = SharkSmsMonitor(
    panel_name  = 'Shark sms',
    login_url   = SHARK_SMS_LOGIN_URL,
    signin_url  = SHARK_SMS_SIGNIN_URL,
    stats_url   = SHARK_SMS_STATS_URL,
    ajax_url    = SHARK_SMS_AJAX_URL,
    username    = SHARK_SMS_USERNAME,
    password    = SHARK_SMS_PASSWORD,
    interval    = SHARK_SMS_INTERVAL,
)

# KM Carrier SMS monitor (sesskey-based, /agent/ path, math captcha + crlf, 17s interval)
km_carrier_sms_monitor = KMCarrierSmsMonitor()


# ══════════════════════════════════════════════════════════════════════════════
# Live "Latest Message" fetcher — routes to the correct monitor
# ══════════════════════════════════════════════════════════════════════════════

_PANEL_MONITOR_MAP = {
    'SMS Hadi':        lambda: monitor,
    'SMS Hadi 2':      lambda: sms_hadi2_monitor,
    'Konekta Premium': lambda: konekta_monitor,
    'Msi sms':         lambda: msi_sms_monitor,
    'Number Panel':    lambda: number_panel_monitor,
    'Purple sms':      lambda: purple_sms_monitor,
    'Proof sms':       lambda: proof_sms_monitor,
    'Lamix sms':       lambda: lamix_sms_monitor,
    'Seven 1 Tel':     lambda: seven1tel_monitor,
    'Flex sms':        lambda: mait_sms_monitor,
    'Zento sms':       lambda: zento_sms_monitor,
    'Wolf sms':        lambda: wolf_sms_monitor,
    'Shark sms':       lambda: shark_sms_monitor,
    'KM Carrier sms':  lambda: km_carrier_sms_monitor,
}


def get_panel_latest_today(panel_name: str) -> 'dict | None':
    """Always fetch fresh live data from the panel page.
    Checks both static _PANEL_MONITOR_MAP and dynamic DYNAMIC_PANEL_REGISTRY.
    Returns None if session is down or no messages found.
    """
    # Static panels
    getter = _PANEL_MONITOR_MAP.get(panel_name)
    if getter is not None:
        try:
            return getter().get_latest_today()
        except Exception as exc:
            logger.error(f"get_panel_latest_today({panel_name}): {exc}")
            return None
    # Dynamic panels added via wizard
    mon = DYNAMIC_PANEL_REGISTRY.get(panel_name)
    if mon is not None:
        try:
            return mon.get_latest_today()
        except Exception as exc:
            logger.error(f"get_panel_latest_today({panel_name}) [dynamic]: {exc}")
            return None
    return None


def get_panel_latest_cached(panel_name: str) -> 'dict | None':
    """Return the most recent SMS from in-memory cache (_latest_record).
    Used by 'Group এ পাঠাও' so it can still send even when live fetch fails.
    Falls back to live fetch if cache is empty.
    """
    # Static panels
    getter = _PANEL_MONITOR_MAP.get(panel_name)
    if getter is not None:
        try:
            m = getter()
            cached = getattr(m, '_latest_record', None)
            if cached:
                return cached
            return m.get_latest_today()
        except Exception as exc:
            logger.error(f"get_panel_latest_cached({panel_name}): {exc}")
            return None
    # Dynamic panels
    mon = DYNAMIC_PANEL_REGISTRY.get(panel_name)
    if mon is not None:
        try:
            cached = getattr(mon, '_latest_record', None)
            if cached:
                return cached
            return mon.get_latest_today()
        except Exception as exc:
            logger.error(f"get_panel_latest_cached({panel_name}) [dynamic]: {exc}")
            return None
    return None


# ══════════════════════════════════════════════════════════════════════════════
# Standalone panel data fetcher  (Admin → Panel List → Login & View Stats)
# ══════════════════════════════════════════════════════════════════════════════

def fetch_panel_data(base_url: str, username: str, password: str) -> dict | None:
    """
    Login to any panel with given credentials and return SMS stats.
    Automatically detects which panel type to use based on the base URL.
    Returns dict {'total': int, 'records': list} or None on failure.
    """
    base_url = base_url.rstrip('/')
    # SMS Hadi can be slow to accept a fresh login while the background
    # monitor is already polling. Try the panel credentials first, then
    # fall back to the live monitor session to avoid false "connection failed"
    # messages during temporary panel timeouts.
    if 'smshadi.net' in base_url or '2.59.169.96' in base_url:
        result = _fetch_client_panel_data(base_url, username, password)
        if result is not None:
            return result
        return _fetch_running_monitor_data('SMS Hadi')
    # Number Panel — requires sesskey extraction from stats page
    if '51.89.99.105' in base_url:
        return _fetch_number_panel_data(base_url, username, password)
    # Purple SMS — special /dialer/ path with different column layout
    if '85.195.94.50' in base_url:
        return _fetch_purple_panel_data(base_url, username, password)
    # Wolf SMS — agent path, cookie-based auth, no captcha
    if '213.32.24.208' in base_url:
        result = _fetch_wolf_panel_data(base_url, username, password)
        if result is not None:
            return result
        return _fetch_running_monitor_data('Wolf sms')
    # Shark SMS — agent path, sesskey-based, math captcha + crlf
    if '65.109.111.158' in base_url:
        result = _fetch_shark_panel_data(base_url, username, password)
        if result is not None:
            return result
        return _fetch_running_monitor_data('Shark sms')
    # KM Carrier SMS — agent path, sesskey-based, math captcha + crlf
    if '54.36.173.235' in base_url:
        result = _fetch_shark_panel_data(base_url, username, password)
        if result is not None:
            return result
        return _fetch_running_monitor_data('KM Carrier sms')
    # All other panels (SMS Hadi, Konekta, MSI, Proof, Lamix, Seven 1 Tel, Flex, Zento)
    # use /client/res/data_smscdr.php cookie-based auth — no sesskey needed
    return _fetch_client_panel_data(base_url, username, password)


def _fetch_running_monitor_data(panel_name: str) -> dict | None:
    _monitor_map = {
        'SMS Hadi':        monitor,
        'SMS Hadi 2':      sms_hadi2_monitor,
        'Konekta Premium': konekta_monitor,
        'Msi sms':         msi_sms_monitor,
        'Number Panel':    number_panel_monitor,
        'Purple sms':      purple_sms_monitor,
        'Proof sms':       proof_sms_monitor,
        'Lamix sms':       lamix_sms_monitor,
        'Seven 1 Tel':     seven1tel_monitor,
        'Flex sms':        mait_sms_monitor,
        'Zento sms':       zento_sms_monitor,
        'Wolf sms':        wolf_sms_monitor,
        'Shark sms':       shark_sms_monitor,
        'KM Carrier sms':  km_carrier_sms_monitor,
    }
    m = _monitor_map.get(panel_name)
    if m is None or not getattr(m, 'logged_in', False) or not getattr(m, 'session', None):
        return None
    try:
        raw_records = m._fetch_records()
        if raw_records is None:
            return None
        records = []
        for rec in raw_records:
            sms_body = rec.get('sms_body') or rec.get('message') or ''
            range_name = rec.get('range_name') or rec.get('country') or ''
            records.append({
                'datetime': rec.get('datetime') or rec.get('dt') or rec.get('received_at') or '',
                'country': _extract_country(range_name),
                'number': rec.get('number') or '',
                'otp': _extract_otp(sms_body),
                'website': rec.get('website') or _detect_website_from_body(sms_body),
                'message': sms_body,
            })
        return {'total': len(records), 'records': records}
    except Exception as exc:
        logger.error(f"fetch_running_monitor_data({panel_name}): Exception — {exc}")
        return None


def _fetch_client_panel_data(base_url: str, username: str, password: str) -> dict | None:
    """Fetch stats from a /client/SMSCDRStats panel (cookie-based auth)."""
    base_url   = base_url.rstrip('/')
    # Determine login page: Konekta uses /sign-in, others use /login
    if 'konektapremium' in base_url.lower():
        login_page = f"{base_url}/sign-in"
    else:
        login_page = f"{base_url}/login"
    signin_url = f"{base_url}/signin"
    ajax_url   = f"{base_url}/client/res/data_smscdr.php"
    referer    = f"{base_url}/client/SMSCDRStats"

    try:
        session = _new_session()

        r1 = session.get(login_page, timeout=15)
        captcha = _solve_captcha(r1.text)
        if not captcha:
            logger.error(f"fetch_client_panel_data ({base_url}): Could not solve captcha")
            return None

        r2 = session.post(
            signin_url,
            data={'username': username, 'password': password, 'capt': captcha},
            headers={'Referer': login_page},
            timeout=15, allow_redirects=True,
        )
        final_path = r2.url.lower()
        if 'login' in final_path or 'sign-in' in final_path:
            logger.error(f"fetch_client_panel_data ({base_url}): Login failed")
            return None

        url = _build_client_ajax_url(ajax_url, days_back=7)
        r3  = session.get(url, headers={
            'Referer': referer,
            'X-Requested-With': 'XMLHttpRequest',
        }, timeout=25)

        if 'login' in r3.url.lower() or 'sign-in' in r3.url.lower():
            logger.error(f"fetch_client_panel_data ({base_url}): Session expired during fetch")
            return None

        data  = r3.json()
        rows  = data.get('aaData', [])
        total = data.get('iTotalRecords', len(rows))

        records = []
        for row in rows:
            if not isinstance(row, list) or len(row) < 5:
                continue
            dt_str     = str(row[0]).strip() if row[0] else ''
            range_name = str(row[1]).strip() if row[1] else ''
            number     = re.sub(r'[^\d]', '', str(row[2])) if row[2] else ''
            sms_body   = str(row[4]).strip() if len(row) > 4 and row[4] else ''

            country = _extract_country(range_name)
            otp     = _extract_otp(sms_body)
            website = _detect_website_from_body(sms_body)

            records.append({
                'datetime': dt_str, 'country': country,
                'number': number, 'otp': otp,
                'website': website, 'message': sms_body,
            })

        return {'total': total, 'records': records}

    except Exception as exc:
        logger.error(f"fetch_client_panel_data ({base_url}): Exception — {exc}")
        return None


def _fetch_wolf_panel_data(base_url: str, username: str, password: str) -> dict | None:
    """Fetch stats from Wolf SMS panel (cookie-based, /agent/ path, no captcha)."""
    base_url   = base_url.rstrip('/')
    login_url  = f"{base_url}/login"
    signin_url = f"{base_url}/signin"
    ajax_url   = f"{base_url}/agent/res/data_smscdr.php"
    referer    = f"{base_url}/agent/SMSCDRStats"

    try:
        session = _new_session()
        # reCAPTCHA present on page but NOT enforced server-side — POST directly
        r2 = session.post(
            signin_url,
            data={'username': username, 'password': password},
            headers={'Referer': login_url},
            timeout=15, allow_redirects=True,
        )
        final_path = r2.url.lower()
        if 'login' in final_path or 'sign-in' in final_path:
            logger.error(f"_fetch_wolf_panel_data ({base_url}): Login failed")
            return None

        url = _build_client_ajax_url(ajax_url, days_back=7)
        r3  = session.get(url, headers={
            'Referer': referer,
            'X-Requested-With': 'XMLHttpRequest',
        }, timeout=25)
        if 'login' in r3.url.lower():
            logger.error(f"_fetch_wolf_panel_data ({base_url}): Session expired during fetch")
            return None

        data  = r3.json()
        rows  = data.get('aaData', [])
        total = data.get('iTotalRecords', len(rows))

        records = []
        for row in rows:
            if not isinstance(row, list) or len(row) < 5:
                continue
            dt_str     = str(row[0]).strip() if row[0] else ''
            range_name = str(row[1]).strip() if row[1] else ''
            number     = re.sub(r'[^\d]', '', str(row[2])) if row[2] else ''
            # Skip totals/summary rows — they don't have a valid datetime
            if not re.match(r'\d{4}-\d{2}-\d{2}', dt_str):
                continue
            sms_body   = str(row[4]).strip() if len(row) > 4 and row[4] else ''
            if not number or not dt_str:
                continue
            records.append({
                'datetime': dt_str,
                'country':  _extract_country(range_name),
                'number':   number,
                'otp':      _extract_otp(sms_body),
                'website':  _detect_website_from_body(sms_body),
                'message':  sms_body,
            })
        return {'total': total, 'records': records}

    except Exception as exc:
        logger.error(f"_fetch_wolf_panel_data ({base_url}): Exception — {exc}")
        return None


def _fetch_shark_panel_data(base_url: str, username: str, password: str) -> dict | None:
    """Fetch stats from Shark SMS panel (sesskey-based, /agent/ path, math captcha + crlf)."""
    base_url   = base_url.rstrip('/')
    login_url  = f"{base_url}/login"
    signin_url = f"{base_url}/signin"
    stats_url  = f"{base_url}/agent/SMSCDRStats"
    ajax_url   = f"{base_url}/agent/res/data_smscdr.php"

    try:
        session = _new_session()

        r1 = session.get(login_url, timeout=15)
        captcha = _solve_captcha(r1.text)
        if not captcha:
            logger.error(f"_fetch_shark_panel_data ({base_url}): Could not solve captcha")
            return None

        r2 = session.post(
            signin_url,
            data={'username': username, 'password': password, 'capt': captcha, 'crlf': ''},
            headers={'Referer': login_url},
            timeout=15, allow_redirects=True,
        )
        if 'login' in r2.url.lower():
            logger.error(f"_fetch_shark_panel_data ({base_url}): Login failed")
            return None

        r3 = session.get(stats_url, timeout=20)
        if 'login' in r3.url.lower():
            return None
        m = re.search(
            r'"sAjaxSource"\s*:\s*"res/data_smscdr\.php[^"]*sesskey=([^"&]+)"',
            r3.text
        )
        if not m:
            logger.error(f"_fetch_shark_panel_data ({base_url}): sesskey not found")
            return None
        sesskey = m.group(1)

        now = _bd_now()
        d1  = (now - timedelta(days=7)).strftime('%Y-%m-%d 00:00:00')
        d2  = now.strftime('%Y-%m-%d 23:59:59')
        params = urlencode({
            'fdate1': d1, 'fdate2': d2,
            'frange': '', 'fnum': '', 'fcli': '',
            'fgdate': '', 'fgmonth': '', 'fgrange': '',
            'fgnumber': '', 'fgcli': '',
            'fg': '0',
            'sesskey': sesskey,
            'iDisplayStart': '0', 'iDisplayLength': '999999',
        })
        r4 = session.get(
            f"{ajax_url}?{params}",
            headers={'Referer': stats_url, 'X-Requested-With': 'XMLHttpRequest'},
            timeout=25,
        )
        if 'login' in r4.url.lower():
            return None

        data  = r4.json()
        rows  = data.get('aaData', [])
        total = data.get('iTotalRecords', len(rows))

        records = []
        for row in rows:
            if not isinstance(row, list) or len(row) < 3:
                continue
            dt_str     = str(row[0]).strip() if row[0] else ''
            range_name = str(row[1]).strip() if row[1] else ''
            number     = re.sub(r'[^\d]', '', str(row[2])) if row[2] else ''
            # Skip totals/summary rows — they don't have a valid datetime
            if not re.match(r'\d{4}-\d{2}-\d{2}', dt_str):
                continue
            sms_body   = ''
            if len(row) > 5:
                sms_body = str(row[5]).strip() if row[5] else ''
            elif len(row) > 4:
                sms_body = str(row[4]).strip() if row[4] else ''
            if not number or not dt_str:
                continue
            records.append({
                'datetime': dt_str,
                'country':  _extract_country(range_name),
                'number':   number,
                'otp':      _extract_otp(sms_body),
                'website':  _detect_website_from_body(sms_body),
                'message':  sms_body,
            })
        return {'total': total, 'records': records}

    except Exception as exc:
        logger.error(f"_fetch_shark_panel_data ({base_url}): Exception — {exc}")
        return None


def _fetch_number_panel_data(base_url: str, username: str, password: str) -> dict | None:
    """Fetch stats from Number Panel (sesskey-based, /client/ path)."""
    base_url    = base_url.rstrip('/')
    login_url   = f"{base_url}/login"
    signin_url  = f"{base_url}/signin"
    stats_url   = f"{base_url}/client/SMSCDRStats"
    ajax_url    = f"{base_url}/client/res/data_smscdr.php"

    try:
        session = _new_session()

        r1 = session.get(login_url, timeout=15)
        captcha = _solve_captcha(r1.text)
        if not captcha:
            logger.error("fetch_number_panel_data: Could not solve captcha")
            return None

        r2 = session.post(
            signin_url,
            data={'username': username, 'password': password, 'capt': captcha},
            headers={'Referer': login_url},
            timeout=15, allow_redirects=True,
        )
        if 'login' in r2.url.lower():
            logger.error("fetch_number_panel_data: Login failed")
            return None

        r3 = session.get(stats_url, timeout=20)
        if 'login' in r3.url.lower():
            return None
        m = re.search(
            r'"sAjaxSource"\s*:\s*"res/data_smscdr\.php[^"]*sesskey=([^"&]+)"',
            r3.text
        )
        if not m:
            logger.error("fetch_number_panel_data: sesskey not found")
            return None
        sesskey = m.group(1)

        now = _bd_now()
        d1  = (now - timedelta(days=7)).strftime('%Y-%m-%d 00:00:00')
        d2  = now.strftime('%Y-%m-%d 23:59:59')
        params = urlencode({
            'fdate1': d1, 'fdate2': d2,
            'frange': '', 'fnum': '', 'fcli': '',
            'fgdate': '', 'fgmonth': '', 'fgrange': '',
            'fgnumber': '', 'fgcli': '',
            'fg': '0',
            'sesskey': sesskey,
            'iDisplayStart': '0', 'iDisplayLength': '999999',
            'iSortCol_0': '0', 'sSortDir_0': 'desc',
        })
        r4 = session.get(
            f"{ajax_url}?{params}",
            headers={'Referer': stats_url, 'X-Requested-With': 'XMLHttpRequest'},
            timeout=25,
        )
        if 'login' in r4.url.lower():
            return None

        data  = r4.json()
        rows  = data.get('aaData', [])
        total = data.get('iTotalRecords', len(rows))

        records = []
        for row in rows:
            if not isinstance(row, list) or len(row) < 5:
                continue
            dt_str     = str(row[0]).strip() if row[0] else ''
            range_name = str(row[1]).strip() if row[1] else ''
            number     = re.sub(r'[^\d]', '', str(row[2])) if row[2] else ''
            sms_body   = str(row[4]).strip() if len(row) > 4 and row[4] else ''
            if not number or number == '0':
                continue
            country = _extract_country(range_name)
            otp     = _extract_otp(sms_body)
            website = _detect_website_from_body(sms_body)
            records.append({
                'datetime': dt_str, 'country': country,
                'number': number, 'otp': otp,
                'website': website, 'message': sms_body,
            })

        return {'total': total, 'records': records}

    except Exception as exc:
        logger.error(f"fetch_number_panel_data: Exception — {exc}")
        return None


def _fetch_hadi_panel_data(base_url: str, username: str, password: str) -> dict | None:
    """Fetch stats from SMS Hadi panel (sesskey-based auth)."""
    login_url   = f"{base_url}/login"
    signin_url  = f"{base_url}/signin"
    reports_url = f"{base_url}/agent/SMSCDRReports"
    ajax_url    = f"{base_url}/agent/res/data_smscdr.php"

    try:
        session = _new_session()

        r1 = session.get(login_url, timeout=15)
        captcha = _solve_captcha(r1.text)
        if not captcha:
            logger.error("fetch_hadi_panel_data: Could not solve captcha")
            return None

        r2 = session.post(
            signin_url,
            data={'username': username, 'password': password, 'capt': captcha},
            headers={'Referer': login_url},
            timeout=15, allow_redirects=True,
        )
        if 'login' in r2.url.lower():
            logger.error("fetch_hadi_panel_data: Login failed")
            return None

        now       = _bd_now()
        yesterday = now - timedelta(days=1)
        d1        = yesterday.strftime('%Y-%m-%d 00:00:00')
        d2        = yesterday.strftime('%Y-%m-%d 23:59:59')

        r3 = session.post(
            reports_url,
            data={
                'fdate1': d1, 'fdate2': d2,
                'fnum': '', 'fcli': '', 'frange': '', 'fclient': '',
            },
            headers={'Referer': reports_url},
            timeout=20,
        )
        if 'login' in r3.url.lower():
            return None

        m = re.search(
            r'"sAjaxSource"\s*:\s*"res/data_smscdr\.php[^"]*sesskey=([^"&]+)"',
            r3.text
        )
        if not m:
            logger.error("fetch_hadi_panel_data: sesskey not found")
            return None
        sesskey = m.group(1)

        url = (ajax_url + '?' + urlencode({
            'fdate1': d1, 'fdate2': d2,
            'frange': '', 'fclient': '', 'fnum': '', 'fcli': '',
            'fgdate': '', 'fgmonth': '', 'fgrange': '', 'fgclient': '',
            'fgnumber': '', 'fgcli': '',
            'fg': '0', 'sesskey': sesskey,
            'iDisplayStart': '0', 'iDisplayLength': '999999',
            'iSortCol_0': '0', 'sSortDir_0': 'desc',
        }))

        r4 = session.get(url, headers={
            'Referer': reports_url,
            'X-Requested-With': 'XMLHttpRequest',
        }, timeout=25)

        if 'login' in r4.url.lower():
            return None

        data  = r4.json()
        rows  = data.get('aaData', [])
        total = data.get('iTotalRecords', len(rows))

        records = []
        for row in rows:
            if not isinstance(row, list) or len(row) < 6:
                continue
            dt_str     = str(row[0]).strip() if row[0] else ''
            range_name = str(row[1]).strip() if row[1] else ''
            number     = re.sub(r'[^\d]', '', str(row[2])) if row[2] else ''
            website    = str(row[3]).strip() if row[3] else 'Unknown'
            sms_body   = str(row[5]).strip() if len(row) > 5 and row[5] else ''
            detected = _detect_website_from_body(sms_body)
            if detected and detected != 'Unknown':
                website = detected

            country = _extract_country(range_name)
            otp     = _extract_otp(sms_body)

            records.append({
                'datetime': dt_str, 'country': country,
                'number': number, 'otp': otp,
                'website': website, 'message': sms_body,
            })

        return {'total': total, 'records': records}

    except Exception as exc:
        logger.error(f"fetch_hadi_panel_data: Exception — {exc}")
        return None


def _fetch_purple_panel_data(base_url: str, username: str, password: str) -> dict | None:
    """
    Fetch stats from Purple SMS panel.
    Cookie-based auth, AJAX at /dialer/ajax/dt_reports.php (no sesskey).
    Columns: [0]Date [1]Termination [2]Number [3]CLI [4]Currency
             [5]Payterm [6]Payout [7]Message
    """
    login_url  = f"{base_url}/SignIn"
    signin_url = f"{base_url}/signmein"
    stats_url  = f"{base_url}/dialer/SMSReports"
    ajax_url   = f"{base_url}/dialer/ajax/dt_reports.php"

    try:
        session = _new_session()

        r1 = session.get(login_url, timeout=15)
        captcha = _solve_captcha(r1.text)
        if not captcha:
            logger.error("fetch_purple_panel_data: Could not solve captcha")
            return None

        r2 = session.post(
            signin_url,
            data={'username': username, 'password': password, 'capt': captcha},
            headers={'Referer': login_url},
            timeout=15, allow_redirects=True,
        )
        final_lower = r2.url.lower().rstrip('/')
        final_last  = final_lower.split('/')[-1]
        if final_last in ('login', 'signin', 'sign-in') or final_last.endswith('signin'):
            logger.error("fetch_purple_panel_data: Login failed")
            return None

        now = _bd_now()
        d1  = (now - timedelta(days=7)).strftime('%Y-%m-%d 00:00:00')
        d2  = now.strftime('%Y-%m-%d 23:59:59')
        params = {
            'fdate1': d1, 'fdate2': d2,
            'ftermination': '', 'fnum': '', 'fcli': '',
            'fgdate': '0', 'fgtermination': '0',
            'fgnumber': '0', 'fgcli': '0', 'fg': '0',
            'iDisplayStart': '0', 'iDisplayLength': '999999',
            'iSortCol_0': '0', 'sSortDir_0': 'desc',
        }
        r4 = session.get(
            ajax_url,
            params=params,
            headers={'Referer': stats_url, 'X-Requested-With': 'XMLHttpRequest'},
            timeout=25,
        )
        if 'login' in r4.url.lower() or 'signin' in r4.url.lower():
            return None

        data  = r4.json()
        rows  = data.get('aaData', [])
        total = data.get('iTotalRecords', len(rows))

        records = []
        for row in rows:
            if not isinstance(row, list) or len(row) < 8:
                continue
            dt_str     = str(row[0]).strip() if row[0] else ''
            range_name = str(row[1]).strip() if row[1] else ''
            number     = re.sub(r'[^\d]', '', str(row[2])) if row[2] else ''
            # Skip totals/summary rows — they don't have a valid datetime
            if not re.match(r'\d{4}-\d{2}-\d{2}', dt_str):
                continue
            sms_body   = str(row[7]).strip() if row[7] else ''
            if not number or not dt_str:
                continue
            country = _extract_country(range_name)
            otp     = _extract_otp(sms_body)
            website = _detect_website_from_body(sms_body)
            records.append({
                'datetime': dt_str, 'country': country,
                'number': number, 'otp': otp,
                'website': website, 'message': sms_body,
            })

        return {'total': total, 'records': records}

    except Exception as exc:
        logger.error(f"fetch_purple_panel_data: Exception — {exc}")
        return None


# ══════════════════════════════════════════════════════════════════════════════
# DynamicPanelMonitor — configurable monitor for user-added panels
# ══════════════════════════════════════════════════════════════════════════════

class DynamicPanelMonitor(ClientPanelMonitor):
    """
    Configurable panel monitor for panels added via the ➕ Add Panel wizard.
    Supports both agent-type panels (math captcha + sesskey) and
    client-type panels (no captcha, cookie-only).

    config keys:
        name         – panel display name
        login_url    – login page URL (GET to show form)
        signin_url   – form POST URL (action= attribute)
        username     – login username
        password     – login password
        ajax_url     – DataTables AJAX endpoint URL
        stats_url    – referer URL for AJAX requests (optional, defaults to ajax_url)
        path_prefix  – 'client' or 'agent' (determines login/fetch strategy)
        captcha_type – 'math', 'none', 'recaptcha', 'hcaptcha', 'unknown'
        col_map      – dict with keys: datetime, range, number, sms_body, website (indices)
    """

    def __init__(self, config: dict):
        from urllib.parse import urlparse
        name        = config['name']
        login_url   = config['login_url']
        signin_url  = config.get('signin_url', login_url)
        username    = config.get('username', '')
        password    = config.get('password', '')
        path_prefix = config.get('path_prefix', 'client')

        parsed   = urlparse(login_url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"

        super().__init__(
            panel_name  = name,
            base_url    = base_url,
            login_page_url = login_url,
            signin_url  = signin_url,
            username    = username,
            password    = password,
            path_prefix = path_prefix,
        )

        ajax_url_override = config.get('ajax_url', '')
        if ajax_url_override:
            self.ajax_url    = ajax_url_override
            self.referer_url = config.get('stats_url', ajax_url_override)

        self._col_map = config.get('col_map', {
            'datetime': 0, 'range': 1, 'number': 2, 'sms_body': 4,
        })
        # Apply explicit index overrides set by the ➕ Add Panel wizard.
        # These take priority over the auto-detected col_map values.
        if 'phone_idx' in config:
            self._col_map['number'] = int(config['phone_idx'])
        if 'service_idx' in config:
            self._col_map['service_idx'] = int(config['service_idx'])
        if 'otp_idx' in config:
            self._col_map['sms_body'] = int(config['otp_idx'])
        self._config       = config
        self._captcha_type = config.get('captcha_type', 'none')
        self._is_agent     = (path_prefix == 'agent')
        self._sesskey: str = ''

    def _refresh_credentials(self):
        try:
            from database import _get_dynamic_panel
            cfg = _get_dynamic_panel(self.panel_name)
            if cfg:
                self.username = cfg.get('username', self.username)
                self.password = cfg.get('password', self.password)
        except Exception:
            pass

    # ── Login ──────────────────────────────────────────────────────────────────

    def _login(self) -> bool:
        self._refresh_credentials()
        if not self.username or not self.password:
            self._log.warning(f"{self.panel_name}: credentials not set.")
            return False
        last_reason = "unknown"
        for attempt in range(1, _LOGIN_FAST_RETRIES + 1):
            try:
                self.session = _new_session()

                if self._captcha_type == 'math':
                    # Agent-style: fetch login page and auto-solve math captcha
                    r1 = self.session.get(self.login_page, timeout=15)
                    captcha = _solve_captcha(r1.text)
                    if captcha is None:
                        last_reason = "captcha unsolvable"
                        self._log.warning(
                            f"{self.panel_name}: Attempt {attempt}/{_LOGIN_FAST_RETRIES} — {last_reason}."
                        )
                        if attempt < _LOGIN_FAST_RETRIES:
                            time.sleep(_LOGIN_RETRY_DELAY)
                        continue
                    self._log.info(
                        f"{self.panel_name}: Attempt {attempt}/{_LOGIN_FAST_RETRIES} — Captcha solved → {captcha}"
                    )
                    post_data = {
                        'username': self.username,
                        'password': self.password,
                        'capt':     captcha,
                    }
                else:
                    # Client-style: no captcha, direct POST
                    post_data = {
                        'username': self.username,
                        'password': self.password,
                    }

                r2 = self.session.post(
                    self.signin_url,
                    data=post_data,
                    headers={'Referer': self.login_page},
                    timeout=15, allow_redirects=True,
                )
                final_lower = r2.url.lower().rstrip('/')
                final_last  = final_lower.split('/')[-1]
                if final_last in ('login', 'signin', 'sign-in') or \
                   final_last.endswith('login') or final_last.endswith('signin'):
                    last_reason = "login rejected"
                    self._log.warning(
                        f"{self.panel_name}: Attempt {attempt}/{_LOGIN_FAST_RETRIES} — {last_reason}."
                    )
                    if attempt < _LOGIN_FAST_RETRIES:
                        time.sleep(_LOGIN_RETRY_DELAY)
                    continue

                self.logged_in = True
                self._log.info(
                    f"{self.panel_name}: Logged in successfully (attempt {attempt}/{_LOGIN_FAST_RETRIES})."
                )
                # Agent panels need sesskey after login
                if self._is_agent:
                    self._extract_sesskey()
                return True

            except Exception as exc:
                last_reason = f"exception: {exc}"
                self._log.warning(
                    f"{self.panel_name}: Attempt {attempt}/{_LOGIN_FAST_RETRIES} — {last_reason}"
                )
                if attempt < _LOGIN_FAST_RETRIES:
                    time.sleep(_LOGIN_RETRY_DELAY)

        self._log.error(
            f"{self.panel_name}: All {_LOGIN_FAST_RETRIES} login attempts failed — {last_reason}"
        )
        self.logged_in = False
        return False

    # ── Sesskey (agent panels only) ────────────────────────────────────────────

    def _extract_sesskey(self) -> bool:
        try:
            r = self.session.get(self.referer_url, timeout=20)
            if 'login' in r.url.lower():
                self.logged_in = False
                return False
            m = re.search(
                r'"sAjaxSource"\s*:\s*"res/data_smscdr\.php[^"]*sesskey=([^"&]+)"',
                r.text
            )
            if not m:
                self._log.error(f"{self.panel_name}: sesskey not found in stats page.")
                return False
            self._sesskey = m.group(1)
            self._log.info(f"{self.panel_name}: sesskey extracted → {self._sesskey[:12]}…")
            return True
        except Exception as exc:
            self._log.error(f"{self.panel_name}: _extract_sesskey error — {exc}")
            return False

    # ── get_latest_today override — uses _col_map, not hardcoded indices ──────

    def get_latest_today(self) -> 'dict | None':
        """Fetch fresh live data using this panel's configured column map.
        Overrides the parent method which uses hardcoded column indices."""
        if not self.logged_in or not self.session:
            if not self._login():
                return None
        try:
            records = self._fetch_records()
            if not records:
                return None
            records.sort(key=lambda x: x.get('datetime', ''), reverse=True)
            rec      = records[0]
            sms_body = rec.get('sms_body', '')
            otp      = _extract_all_otps(sms_body)
            uid      = hashlib.md5(
                f"{rec.get('datetime','')}:{rec.get('number','')}:{sms_body}".encode()
            ).hexdigest()
            result = {
                'id':          uid,
                'datetime':    rec.get('datetime', ''),
                'number':      rec.get('number', ''),
                'country':     _extract_country(rec.get('range_name', '')),
                'website':     rec.get('website', ''),
                'otp':         otp,
                'message':     sms_body,
                'received_at': rec.get('datetime', ''),
                'panel_name':  self.panel_name,
            }
            self._latest_record = result
            return result
        except Exception as exc:
            self._log.error(f"{self.panel_name}: get_latest_today error — {exc}")
            return None

    # ── Fetch records — route by panel type ───────────────────────────────────

    def _fetch_records(self) -> list[dict] | None:
        if self._is_agent:
            return self._fetch_records_agent()
        return self._fetch_records_client()

    def _parse_row(self, row: list) -> 'dict | None':
        """
        Parse a single AJAX data row into a record dict using self._col_map.
        Returns None if row is invalid or missing required fields.
        Falls back gracefully when optional columns are out of bounds.
        """
        if not isinstance(row, list) or len(row) < 2:
            return None
        cm = self._col_map

        # DateTime — always column 0 on known panel types
        dt_col = cm.get('datetime', 0)
        dt_str = str(row[dt_col]).strip() if dt_col < len(row) else ''
        if not re.match(r'\d{4}-\d{2}-\d{2}', dt_str):
            return None

        # Phone number — saved as phone_idx / 'number'
        num_col = cm.get('number', 2)
        number  = ''
        if num_col < len(row) and row[num_col] is not None:
            number = re.sub(r'[^\d]', '', str(row[num_col]))

        # SMS body / OTP text — saved as otp_idx / 'sms_body'
        sms_col  = cm.get('sms_body', 4)
        sms_body = ''
        if sms_col < len(row) and row[sms_col] is not None:
            sms_body = str(row[sms_col]).strip()

        # Service / country name — saved as service_idx / 'service_idx'
        # Fallback chain: service_idx → range → index 1 → empty
        rng_col    = cm.get('service_idx', cm.get('range', 1))
        range_name = ''
        if isinstance(rng_col, int) and rng_col < len(row) and row[rng_col] is not None:
            range_name = str(row[rng_col]).strip()

        # Website / app name — try to detect from SMS body first, then fall back
        web_col = cm.get('website', -1)
        website = ''
        if isinstance(web_col, int) and 0 <= web_col < len(row) and row[web_col]:
            website = str(row[web_col]).strip()
        detected = _detect_website_from_body(sms_body)
        if detected and detected != 'Unknown':
            website = detected
        elif not website or website.lower() in ('unknown', '', 'none', '-'):
            website = range_name if range_name else 'Unknown'

        # Require at least a phone number; datetime already validated above
        if not number:
            return None

        return {
            'datetime':   dt_str,
            'range_name': range_name,
            'number':     number,
            'website':    website,
            'sms_body':   sms_body,
        }

    def _fetch_records_agent(self) -> list[dict] | None:
        if not self._sesskey:
            self._log.warning(f"{self.panel_name}: sesskey missing — skipping agent fetch.")
            return None
        try:
            now = _bd_now()
            d1  = (now - timedelta(days=7)).strftime('%Y-%m-%d 00:00:00')
            d2  = now.strftime('%Y-%m-%d 23:59:59')
            params = urlencode({
                'fdate1': d1, 'fdate2': d2,
                'frange': '', 'fnum': '', 'fcli': '',
                'fgdate': '', 'fgmonth': '', 'fgrange': '',
                'fgnumber': '', 'fgcli': '',
                'fg': '0',
                'sesskey': self._sesskey,
                'iDisplayStart': '0',
                'iDisplayLength': '999999',
            })
            url = f"{self.ajax_url}?{params}"
            r = self.session.get(
                url,
                headers={'Referer': self.referer_url, 'X-Requested-With': 'XMLHttpRequest'},
                timeout=25,
            )
            if 'login' in r.url.lower():
                self.logged_in = False
                return None
            rows = r.json().get('aaData', [])
            results = [rec for row in rows if (rec := self._parse_row(row)) is not None]
            self._log.info(f"{self.panel_name}: Fetched {len(results)} SMS records (agent).")
            return results
        except Exception as exc:
            self._log.error(f"{self.panel_name}: _fetch_records_agent error — {exc}")
            return None

    def _fetch_records_client(self) -> list[dict] | None:
        try:
            url = _build_client_ajax_url(self.ajax_url, days_back=7)
            r = self.session.get(
                url,
                headers={'Referer': self.referer_url, 'X-Requested-With': 'XMLHttpRequest'},
                timeout=25,
            )
            final_path = r.url.lower()
            if 'login' in final_path or 'sign-in' in final_path:
                self.logged_in = False
                return None
            rows = r.json().get('aaData', [])
            results = [rec for row in rows if (rec := self._parse_row(row)) is not None]
            self._log.info(f"{self.panel_name}: Fetched {len(results)} SMS records (client).")
            return results
        except Exception as exc:
            self._log.error(f"{self.panel_name}: _fetch_records_client error — {exc}")
            return None


# ── Dynamic panel registry ────────────────────────────────────────────────────
DYNAMIC_PANEL_REGISTRY: dict[str, DynamicPanelMonitor] = {}


def load_dynamic_panels_from_db() -> list[tuple[str, 'DynamicPanelMonitor']]:
    """Load all dynamic panels from DB and return list of (name, monitor) tuples."""
    try:
        from database import _get_dynamic_panels
        configs = _get_dynamic_panels()
        result: list[tuple[str, DynamicPanelMonitor]] = []
        for cfg in configs:
            name = cfg.get('name', '')
            if not name or name in DYNAMIC_PANEL_REGISTRY:
                continue
            mon = DynamicPanelMonitor(cfg)
            DYNAMIC_PANEL_REGISTRY[name] = mon
            result.append((name, mon))
            logger.info(f"[DynamicPanel] Loaded '{name}' from DB.")
        return result
    except Exception as exc:
        logger.error(f"[DynamicPanel] load_dynamic_panels_from_db error — {exc}")
        return []


def create_and_register_dynamic_monitor(config: dict) -> DynamicPanelMonitor:
    """Create a new DynamicPanelMonitor, register it, and return it."""
    name = config['name']
    mon  = DynamicPanelMonitor(config)
    DYNAMIC_PANEL_REGISTRY[name] = mon
    return mon


# ══════════════════════════════════════════════════════════════════════════════
# Live Column Discovery
# ══════════════════════════════════════════════════════════════════════════════

def discover_panel_columns(panel_name: str, all_panels: list) -> dict:
    """
    Log in to the panel (if needed) and scrape the DataTables stats page to
    detect the real column headers (<th> tags).

    Returns a dict:
      {'status': 'ok',           'columns': [...]}
      {'status': '<error_code>', 'error':   '<human readable message>'}

    Error codes: no_monitor | no_url | login_failed | login_error |
                 redirected | rate_limited | no_columns | timeout | fetch_error
    """
    mon = next((m for n, m in all_panels if n == panel_name), None)
    if mon is None:
        logger.warning(f"discover_panel_columns: monitor '{panel_name}' not found.")
        return {'status': 'no_monitor', 'error': 'Monitor not found in panel list.'}

    # Determine the stats/referer page URL for this monitor
    stats_url: str = ''
    for attr in ('referer_url', 'stats_url', '_stats_url', '_referer_url'):
        v = getattr(mon, attr, None)
        if v:
            stats_url = v
            break

    if not stats_url:
        logger.warning(f"discover_panel_columns: no stats URL found for '{panel_name}'.")
        return {'status': 'no_url', 'error': 'No stats/referer URL configured for this panel type.'}

    # Use existing authenticated session, or attempt fresh login
    if not mon.logged_in or not mon.session:
        logger.info(f"discover_panel_columns: '{panel_name}' not logged in — attempting login.")
        try:
            ok = mon._login()
            if not ok:
                logger.warning(f"discover_panel_columns: login failed for '{panel_name}'.")
                return {'status': 'login_failed', 'error': 'Auto re-login attempt failed. Check credentials.'}
        except Exception as exc:
            logger.error(f"discover_panel_columns: login error for '{panel_name}' — {exc}")
            return {'status': 'login_error', 'error': f'Login exception: {exc}'}

    try:
        r = mon.session.get(stats_url, timeout=20, allow_redirects=True)

        # Rate-limited or Cloudflare blocked
        if r.status_code == 429:
            logger.warning(f"discover_panel_columns: '{panel_name}' rate limited (HTTP 429).")
            return {'status': 'rate_limited', 'error': 'HTTP 429 — Too Many Requests.'}
        if r.status_code in (403, 503) and 'cloudflare' in r.text.lower():
            logger.warning(f"discover_panel_columns: '{panel_name}' blocked by Cloudflare ({r.status_code}).")
            return {'status': 'rate_limited', 'error': f'Cloudflare protection active (HTTP {r.status_code}).'}

        # Session expired — redirected to login
        if 'login' in r.url.lower() or 'signin' in r.url.lower():
            logger.warning(f"discover_panel_columns: '{panel_name}' redirected to login page.")
            return {'status': 'redirected', 'error': 'Panel redirected to login — session expired.'}

        html = r.text

        # Try to extract <th> headers from <thead> first, then full page
        thead_match = re.search(r'<thead[^>]*>(.*?)</thead>', html, re.DOTALL | re.IGNORECASE)
        search_region = thead_match.group(1) if thead_match else html

        th_tags = re.findall(r'<th[^>]*>(.*?)</th>', search_region, re.DOTALL | re.IGNORECASE)
        cols: list[str] = []
        for th in th_tags:
            clean = re.sub(r'<[^>]+>', '', th).strip()
            if clean:
                cols.append(clean)

        if cols:
            logger.info(f"discover_panel_columns: '{panel_name}' → {len(cols)} columns: {cols}")
            return {'status': 'ok', 'columns': cols}
        else:
            logger.warning(f"discover_panel_columns: '{panel_name}' — no <th> found on stats page.")
            return {
                'status': 'no_columns',
                'error': 'Connected successfully but no HTML <th> headers found. '
                         'This panel may use AJAX/JSON data loading.',
            }

    except requests.exceptions.Timeout:
        logger.error(f"discover_panel_columns: timeout (20s) for '{panel_name}'.")
        return {'status': 'timeout', 'error': 'Connection timed out after 20s — server may be down or very slow.'}
    except Exception as exc:
        logger.error(f"discover_panel_columns: error fetching stats page for '{panel_name}' — {exc}")
        return {'status': 'fetch_error', 'error': f'Unexpected error: {exc}'}
