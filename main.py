from __future__ import annotations

import asyncio
import collections
import concurrent.futures
import logging
import re
import time

from telegram.request import HTTPXRequest
from message_preprocessor import AnimatedEmojiBot

from telegram import (Update, InlineKeyboardButton, InlineKeyboardMarkup,
                       ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove)
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes,
)
from datetime import datetime, timedelta

from config import (
    BOT_TOKEN, OTP_GROUP_LINK,
    SMS_HADI_USERNAME, KONEKTA_USERNAME, MSI_SMS_USERNAME,
    NUMBER_PANEL_USERNAME, PURPLE_SMS_USERNAME, PROOF_SMS_USERNAME,
    LAMIX_SMS_USERNAME, SEVEN1TEL_USERNAME, MAIT_SMS_USERNAME,
    ZENTO_SMS_USERNAME, WOLF_SMS_USERNAME, SHARK_SMS_USERNAME,
    SMS_HADI2_USERNAME, KM_CARRIER_SMS_USERNAME,
)
from database import (
    _init_db,
    _get_countries, _get_available_number_by_country,
    _assign_number_to_user,
    _get_numbers_count_by_country, _get_all_country_counts,
    _add_country, _add_numbers_to_country,
    _delete_number, _delete_all_numbers_from_country, _delete_country,
    _get_country_stats, _get_country_id_by_name,
    _reset_country_numbers, _reset_all_numbers, _delete_country_numbers,
    _get_notify_window, _set_notify_window,
    _add_admin, _add_admin_by_uid, _remove_admin, _get_all_admins,
    _get_all_admins_with_details, _is_admin,
    _add_user, _get_all_users, _get_all_users_with_info, _get_user_count,
    _get_user_stats_summary, _get_top_users_detailed,
    generate_users_excel, generate_user_stats_excel, generate_user_stats_zip,
    _get_panels, _get_panel_by_name, _update_panel_credentials,
    _get_user_by_number,
    _get_referral_settings, _set_referral_bonus, _toggle_referral,
    _get_user_balance, _update_user_balance, _set_user_balance,
    _credit_referral, _get_referral_count, _get_referral_total_earned,
    _get_user_referral_code, _get_user_by_ref_code, _get_user_info_by_id,
    _get_top_referrers,
    _get_min_withdraw, _set_min_withdraw,
    _create_withdraw_request, _get_pending_withdraws, _update_withdraw_status,
    _get_withdraw_request_by_id,
    _get_otp_bonus_settings, _toggle_otp_bonus, _set_otp_bonus_amount,
    _set_otp_daily_limit, _get_user_otp_bonus_stats,
    _reset_all_user_data, export_all_data_as_zip,
    _get_number_limit, _set_number_limit, _get_available_numbers_by_country,
    _get_all_panel_statuses, _update_panel_status,
    _set_panel_enabled, _is_panel_enabled,
    _get_country_otp_bonus, _set_country_otp_bonus,
    _reset_country_otp_bonus, _get_all_country_otp_bonuses,
    _add_extra_group, _remove_extra_group, _get_all_extra_groups,
    _get_panel_interval, _set_panel_interval,
    _get_panel_retry_interval, _set_panel_retry_interval,
    _get_setting, _set_setting,
    _get_bot_overview_stats,
    _get_services, _add_service, _delete_service,
    _get_service_map, _map_service_country, _unmap_service_country,
    _get_countries_for_service,
    _set_service_emoji, _get_all_service_emojis,
    _get_required_channels, _add_required_channel, _delete_required_channel,
    _save_dynamic_panel, _delete_dynamic_panel, _get_dynamic_panels, _get_dynamic_panel,
    _get_panel_column_config, _set_panel_column_config, _save_panel_discovered_columns,
)
from keyboards import (
    get_admin_keyboard, get_admin_tools_keyboard, get_manage_numbers_keyboard,
    get_otp_bonus_keyboard, get_referral_keyboard,
    get_manage_admins_keyboard, get_user_keyboard, get_users_keyboard,
    get_settings_keyboard, get_edit_bot_links_keyboard,
    get_extra_groups_keyboard,
    get_required_channels_keyboard,
    country_number_keyboard, countries_inline_keyboard,
    services_inline_keyboard,
    get_panel_action_keyboard,
    get_dynamic_panel_action_keyboard,
    get_panel_management_keyboard,
)


from otp_monitor import (
    monitor,
    konekta_monitor,
    msi_sms_monitor,
    zento_sms_monitor,
    number_panel_monitor,
    purple_sms_monitor,
    proof_sms_monitor,
    lamix_sms_monitor,
    seven1tel_monitor,
    mait_sms_monitor,
    wolf_sms_monitor,
    shark_sms_monitor,
    sms_hadi2_monitor,
    km_carrier_sms_monitor,
    fetch_panel_data,
    get_panel_latest_today, get_panel_latest_cached,
    _extract_all_otps,
    _notify_admins_login_success,
    _build_group_notify_text,
    _broadcast_to_groups,
    load_dynamic_panels_from_db,
    create_and_register_dynamic_monitor,
    DYNAMIC_PANEL_REGISTRY,
    discover_panel_columns,
)

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)

# ── Global update-id deduplication ────────────────────────────────────────────
# Prevents the same Telegram update from being processed twice when the bot
# restarts mid-flight and both the dying and new instance see the same update.
#
# Design: a deque(maxlen=10_000) acts as a FIFO rolling window — when full,
# appending automatically evicts the oldest ID in O(1).  A companion set gives
# O(1) membership tests.  The old sorted()-eviction approach was O(N log N) and
# could block the event loop for tens of milliseconds under load.
_PROCESSED_UPDATE_IDS_DEQUE: collections.deque = collections.deque(maxlen=10_000)
_PROCESSED_UPDATE_IDS_SET:   set[int]          = set()

# ── Add Panel wizard sessions ─────────────────────────────────────────────────
# Stores in-progress panel setup state keyed by admin user_id.
# We store it here (not context.user_data) to avoid pickling a requests.Session.
_PANEL_SETUP_SESSIONS: dict[int, dict] = {}


def _ap_analyze_login_page(url: str, existing_session=None) -> dict:
    """
    Sync helper: fetch and parse a login page.
    Returns comprehensive analysis including all form fields and captcha info.
    Runs inside asyncio.to_thread() so blocking I/O is safe.
    """
    import re as _re
    from urllib.parse import urljoin
    import requests as _rq

    try:
        if existing_session:
            sess = existing_session
        else:
            sess = _rq.Session()
            sess.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})
        r = sess.get(url, timeout=15, allow_redirects=True)
        html = r.text

        title_m = _re.search(r'<title[^>]*>([^<]+)</title>', html, _re.I)
        title = title_m.group(1).strip() if title_m else 'Unknown'

        form_m = _re.search(r'<form[^>]+action=["\']([^"\']+)["\']', html, _re.I)
        form_action = form_m.group(1).strip() if form_m else ''
        signin_url = urljoin(url, form_action) if form_action else url

        # ── Collect all input fields ──────────────────────────────────────────
        all_inputs: list[dict] = []
        for m in _re.finditer(r'<input([^>]+)>', html, _re.I):
            attrs = m.group(1)
            name_m  = _re.search(r'name=["\']([^"\']+)["\']',        attrs, _re.I)
            type_m  = _re.search(r'type=["\']([^"\']+)["\']',        attrs, _re.I)
            val_m   = _re.search(r'value=["\']([^"\']*)["\']',       attrs, _re.I)
            ph_m    = _re.search(r'placeholder=["\']([^"\']*)["\']', attrs, _re.I)
            if name_m:
                all_inputs.append({
                    'name':        name_m.group(1),
                    'type':        type_m.group(1).lower() if type_m else 'text',
                    'value':       val_m.group(1) if val_m else '',
                    'placeholder': ph_m.group(1) if ph_m else '',
                })

        # ── Captcha detection ─────────────────────────────────────────────────
        # IMPORTANT: Math captcha is checked FIRST — it takes priority over
        # reCAPTCHA/hCaptcha. Some panels include g-recaptcha scripts in their
        # HTML as dead code or comments even though the actual login uses a
        # simple math question. Detecting math captcha first avoids false
        # positives that would force the user into manual cookie entry.
        captcha_type   = 'none'
        captcha_detail = ''
        captcha_field  = ''
        captcha_question = ''
        recaptcha_sitekey = ''

        # 1️⃣ Math captcha — highest priority (auto-solvable)
        _math_patterns = [
            r'What is\s+(\d+)\s*\+\s*(\d+)',
            r'What is\s+(\d+)\s*-\s*(\d+)',
            r'What is\s+(\d+)\s*\*\s*(\d+)',
            r'What is\s+(\d+)\s*/\s*(\d+)',
            r'(\d+)\s*\+\s*(\d+)\s*=\s*\?',
            r'(\d+)\s*-\s*(\d+)\s*=\s*\?',
            r'(\d+)\s*\*\s*(\d+)\s*=\s*\?',
            r'(\d+)\s*/\s*(\d+)\s*=\s*\?',
        ]
        for pattern in _math_patterns:
            mm = _re.search(pattern, html, _re.I)
            if mm:
                captcha_type     = 'math'
                captcha_question = mm.group(0).strip()
                captcha_detail   = f'✅ Math captcha (auto-solvable): `{captcha_question}`'
                cap_f = _re.search(
                    r'<input[^>]+name=["\']([^"\']*(?:capt|verify|answer|code)[^"\']*)["\']',
                    html, _re.I
                )
                captcha_field = cap_f.group(1) if cap_f else 'capt'
                break

        if captcha_type == 'none':
            # 2️⃣ Google reCAPTCHA — only if no math captcha found
            grc_m = _re.search(r'data-sitekey=["\']([^"\']+)["\']', html, _re.I)
            if _re.search(r'google\.com/recaptcha|g-recaptcha', html, _re.I):
                captcha_type      = 'recaptcha'
                recaptcha_sitekey = grc_m.group(1) if grc_m else ''
                captcha_field     = 'g-recaptcha-response'
                captcha_detail    = '⚠️ Google reCAPTCHA'
            # 3️⃣ hCaptcha
            elif _re.search(r'hcaptcha\.com', html, _re.I):
                captcha_type  = 'hcaptcha'
                captcha_field = 'h-captcha-response'
                captcha_detail = '⚠️ hCaptcha'
            else:
                # 4️⃣ Unknown captcha field
                cap_f2 = _re.search(
                    r'<input[^>]+name=["\']([^"\']*(?:capt|captcha|verify|answer|code)[^"\']*)["\']',
                    html, _re.I
                )
                if cap_f2:
                    captcha_type   = 'unknown'
                    captcha_field  = cap_f2.group(1)
                    captcha_detail = f'❓ Unknown captcha — field: `{captcha_field}`'

        # ── Error messages on page ────────────────────────────────────────────
        err_m = _re.search(
            r'<[^>]*(?:class|id)=["\'][^"\']*(?:error|alert|danger|invalid)[^"\']*["\'][^>]*>([^<]{5,120})<',
            html, _re.I
        )
        page_error = err_m.group(1).strip() if err_m else ''

        return {
            'ok': True,
            'title': title,
            'signin_url': signin_url,
            'captcha_type': captcha_type,
            'captcha_detail': captcha_detail,
            'captcha_field': captcha_field,
            'captcha_question': captcha_question,
            'recaptcha_sitekey': recaptcha_sitekey,
            'all_inputs': all_inputs,
            'page_error': page_error,
            'html': html,
            'session': sess,
            'final_url': str(r.url),
        }
    except Exception as exc:
        return {'ok': False, 'error': str(exc)}


def _ap_attempt_login(setup: dict) -> dict:
    """
    Sync helper: attempt login — exactly like existing panel monitors.
    1. Always fetch a fresh login page (so captcha is fresh).
    2. Use _solve_captcha() from otp_monitor (math auto-solve).
    3. For reCAPTCHA/hCaptcha: try bypass first (no captcha field).
    4. If manual_captcha_value set: inject it into the captcha field.
    Returns dict with ok, html, session, url, captcha_auto_solved.
    """
    import re as _re
    from otp_monitor import _solve_captcha as _otp_solve

    login_url  = setup.get('login_url', setup.get('signin_url', ''))
    signin_url = setup['signin_url']
    username   = setup['username']
    password   = setup['password']
    captcha_type  = setup.get('captcha_type', 'none')
    captcha_field = setup.get('captcha_field', '')
    manual_cap_val = setup.get('manual_captcha_value', '')

    try:
        sess = setup['session']

        # ── Always re-fetch a fresh login page (matches existing panel behaviour)
        r_fresh = sess.get(login_url, timeout=15, allow_redirects=True)
        fresh_html = r_fresh.text

        # ── Parse hidden inputs from fresh page ────────────────────────────────
        payload: dict = {}
        for m in _re.finditer(
            r'<input[^>]+type=["\']hidden["\'][^>]*name=["\']([^"\']+)["\'][^>]*value=["\']([^"\']*)["\']'
            r'|<input[^>]+name=["\']([^"\']+)["\'][^>]*type=["\']hidden["\'][^>]*value=["\']([^"\']*)["\']',
            fresh_html, _re.I
        ):
            k = m.group(1) or m.group(3)
            v = m.group(2) or m.group(4)
            if k:
                payload[k] = v

        # ── Detect username/password field names ──────────────────────────────
        ufield_m = _re.search(
            r'<input[^>]+name=["\']([^"\']*(?:user|login|email|usr)[^"\']*)["\']',
            fresh_html, _re.I
        )
        pfield_m = _re.search(
            r'<input[^>]+type=["\']password["\'][^>]*name=["\']([^"\']+)["\']'
            r'|<input[^>]+name=["\']([^"\']+)["\'][^>]*type=["\']password["\']',
            fresh_html, _re.I
        )
        ufield = ufield_m.group(1) if ufield_m else 'username'
        pfield = (pfield_m.group(1) or pfield_m.group(2)) if pfield_m else 'password'

        payload[ufield] = username
        payload[pfield] = password

        captcha_auto_solved = False

        if captcha_type == 'math':
            # ── Same as existing panels: _solve_captcha() on fresh page ──────
            answer = _otp_solve(fresh_html)
            if answer is not None:
                cap_f = _re.search(
                    r'<input[^>]+name=["\']([^"\']*(?:capt|verify|answer|code)[^"\']*)["\']',
                    fresh_html, _re.I
                )
                cf = cap_f.group(1) if cap_f else (captcha_field or 'capt')
                payload[cf] = answer
                captcha_auto_solved = True
            elif manual_cap_val:
                payload[captcha_field or 'capt'] = manual_cap_val
        elif captcha_type in ('recaptcha', 'hcaptcha', 'unknown'):
            # ── Bypass attempt: submit without captcha (server may not enforce)
            # If manual value provided (admin gave it), inject it
            if manual_cap_val:
                payload[captcha_field] = manual_cap_val
                if captcha_type == 'recaptcha':
                    payload['g-recaptcha-response'] = manual_cap_val
                elif captcha_type == 'hcaptcha':
                    payload['h-captcha-response'] = manual_cap_val
            # If no manual value — try bypass (no captcha field in payload)

        r = sess.post(
            signin_url, data=payload,
            headers={'Referer': str(r_fresh.url)},
            timeout=20, allow_redirects=True
        )

        final     = r.url.lower()
        logged_in = ('login' not in final and 'signin' not in final
                     and 'sign-in' not in final)

        body_lower = r.text.lower()
        if 'invalid' in body_lower or 'incorrect' in body_lower or 'wrong password' in body_lower:
            if 'dashboard' not in body_lower and 'logout' not in body_lower:
                logged_in = False

        return {
            'ok': logged_in,
            'html': r.text,
            'session': sess,
            'url': str(r.url),
            'captcha_auto_solved': captcha_auto_solved,
        }
    except Exception as exc:
        return {'ok': False, 'error': str(exc)}


def _ap_check_cookie_session(setup: dict, cookie_str: str) -> dict:
    """
    Sync helper: inject a browser session cookie into the requests session
    and verify the session is valid by hitting the stats/dashboard page.
    cookie_str format: 'NAME=VALUE' or just 'VALUE' (assumed PHPSESSID).
    Returns dict with ok, session, error.
    """
    import re as _re
    from urllib.parse import urlparse

    try:
        import requests as _rq
        login_url = setup.get('login_url', '')
        parsed    = urlparse(login_url)
        domain    = parsed.hostname or ''

        sess = _rq.Session()
        sess.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})

        # Parse cookie string (supports "name=value" or "name1=v1; name2=v2")
        cookie_str = cookie_str.strip()
        for part in cookie_str.split(';'):
            part = part.strip()
            if '=' in part:
                cname, cval = part.split('=', 1)
                sess.cookies.set(cname.strip(), cval.strip(), domain=domain)
            elif part:
                sess.cookies.set('PHPSESSID', part, domain=domain)

        # Test the session: try to load the login page — if we're NOT redirected
        # to /login it means the cookie session is valid
        r = sess.get(login_url, timeout=15, allow_redirects=True)
        final_lower = r.url.lower()
        # Also try dashboard path
        base = f"{parsed.scheme}://{parsed.netloc}"
        r2 = sess.get(base + '/client/', timeout=10, allow_redirects=True)
        final2_lower = r2.url.lower()

        ok = ('login' not in final2_lower and 'signin' not in final2_lower) or \
             ('dashboard' in r2.text.lower() or 'logout' in r2.text.lower() or
              'sms' in r2.text.lower())

        return {'ok': ok, 'session': sess, 'html': r2.text}
    except Exception as exc:
        return {'ok': False, 'error': str(exc)}


def _ap_analyze_ajax(setup: dict, stats_url: str) -> dict:
    """
    Sync helper: fetch the SMS stats page and figure out the AJAX endpoint + column map.
    Returns dict with ok, ajax_url, path_prefix, col_map, sample_rows, error.
    """
    import re as _re
    from urllib.parse import urljoin, urlencode
    from datetime import datetime as _dt, timedelta as _td

    sess = setup['session']
    try:
        now = _dt.now()
        r   = sess.get(stats_url, timeout=20, allow_redirects=True)
        if 'login' in r.url.lower():
            return {'ok': False, 'error': 'Session expired — redirected to login page'}

        html = r.text

        # ── BeautifulSoup: extract column headers (multi-strategy) ──────────
        col_headers: list[str] = []
        try:
            from bs4 import BeautifulSoup as _BS
            _soup = _BS(html, 'html.parser')

            # Strategy 1: <thead> → <th>  (most reliable for DataTables panels)
            _thead = _soup.find('thead')
            if _thead:
                _ths = _thead.find_all('th')
                if _ths:
                    col_headers = [_th.get_text(separator=' ', strip=True) for _th in _ths]

            # Strategy 2: first <table> → <th> anywhere inside it
            if not col_headers:
                _table = _soup.find('table')
                if _table:
                    _ths = _table.find_all('th')
                    if _ths:
                        # Use only the first occurrence of each unique header to avoid
                        # thead+tfoot duplication
                        _seen: set = set()
                        for _th in _ths:
                            _h = _th.get_text(separator=' ', strip=True)
                            if _h and _h not in _seen:
                                col_headers.append(_h)
                                _seen.add(_h)

            # Strategy 3: DataTables JS aoColumns / columns → title (handles JS-defined headers)
            if not col_headers:
                _js_title_m = _re.findall(
                    r'"title"\s*:\s*"([^"]{1,80})"', html, _re.I
                )
                if _js_title_m:
                    col_headers = list(dict.fromkeys(_js_title_m))  # preserve order, deduplicate

            # Strategy 4: aoColumnDefs sTitle
            if not col_headers:
                _js_stitle_m = _re.findall(
                    r'"sTitle"\s*:\s*"([^"]{1,80})"', html, _re.I
                )
                if _js_stitle_m:
                    col_headers = list(dict.fromkeys(_js_stitle_m))

            # Strategy 5: first <tr> → <td> as last resort
            if not col_headers:
                _table2 = _soup.find('table')
                if _table2:
                    _first_tr = _table2.find('tr')
                    if _first_tr:
                        _tds = _first_tr.find_all('td')
                        col_headers = [_td.get_text(separator=' ', strip=True) for _td in _tds]

            # Clean: remove blanks / pure-icon entries (length < 1 char)
            col_headers = [h for h in col_headers if h and len(h.strip()) >= 1]
        except Exception:
            col_headers = []

        # Look for sAjaxSource (sesskey-based panels)
        sajax_m = _re.search(r'"sAjaxSource"\s*:\s*"([^"]+)"', html, _re.I)
        if sajax_m:
            ajax_url  = urljoin(stats_url, sajax_m.group(1))
            auth_type = 'sesskey'
        else:
            # Cookie-based — derive AJAX URL from stats URL
            ajax_url = _re.sub(r'/SMSCDRStats(\b.*)?$', '/client/res/data_smscdr.php', stats_url)
            if ajax_url == stats_url:
                ajax_url = stats_url.rstrip('/') + '/../res/data_smscdr.php'
            auth_type = 'cookie'

        # Detect path prefix
        if '/agent/' in stats_url.lower():
            path_prefix = 'agent'
        else:
            path_prefix = 'client'

        # Fetch sample rows from the AJAX endpoint
        sample_rows: list[list] = []
        col_count = 0
        try:
            params = {
                'fdate1': (now - _td(days=7)).strftime('%Y-%m-%d 00:00:00'),
                'fdate2': now.strftime('%Y-%m-%d 23:59:59'),
                'frange': '', 'fclient': '', 'fnum': '', 'fcli': '',
                'fgdate': '', 'fgmonth': '', 'fgrange': '', 'fgnumber': '', 'fgcli': '',
                'fg': '0', 'iDisplayStart': '0', 'iDisplayLength': '5',
                'iSortCol_0': '0', 'sSortDir_0': 'desc',
            }
            ra = sess.get(
                ajax_url,
                params=params,
                headers={'Referer': stats_url, 'X-Requested-With': 'XMLHttpRequest'},
                timeout=20,
            )
            data = ra.json()
            rows = data.get('aaData', [])
            for row in rows:
                if isinstance(row, list) and len(row) > 2:
                    if _re.match(r'\d{4}-\d{2}-\d{2}', str(row[0])):
                        sample_rows.append(row)
                        col_count = max(col_count, len(row))
                        if len(sample_rows) >= 3:
                            break
        except Exception:
            pass

        # Auto col_map based on column count and path
        if path_prefix == 'agent' or col_count >= 6:
            col_map = {'datetime': 0, 'range': 1, 'number': 2, 'website': 3, 'sms_body': 5}
        else:
            col_map = {'datetime': 0, 'range': 1, 'number': 2, 'sms_body': 4}

        return {
            'ok': True,
            'ajax_url': ajax_url,
            'auth_type': auth_type,
            'path_prefix': path_prefix,
            'col_map': col_map,
            'col_headers': col_headers,
            'sample_rows': sample_rows,
            'col_count': col_count,
        }
    except Exception as exc:
        return {'ok': False, 'error': str(exc)}

def _is_duplicate_update(update_id: int) -> bool:
    """Return True (and mark) if this update_id was already processed.

    Uses a deque(maxlen=10_000) + companion set for O(1) insert, O(1) lookup,
    and automatic FIFO eviction — no manual sorted() purge needed.
    """
    if update_id in _PROCESSED_UPDATE_IDS_SET:
        return True
    # When the deque is full, appending evicts _PROCESSED_UPDATE_IDS_DEQUE[0];
    # mirror that eviction in the set before the append happens.
    if len(_PROCESSED_UPDATE_IDS_DEQUE) == _PROCESSED_UPDATE_IDS_DEQUE.maxlen:
        evicted = _PROCESSED_UPDATE_IDS_DEQUE[0]
        _PROCESSED_UPDATE_IDS_SET.discard(evicted)
    _PROCESSED_UPDATE_IDS_DEQUE.append(update_id)
    _PROCESSED_UPDATE_IDS_SET.add(update_id)
    return False

# ── Country flag helpers ───────────────────────────────────────────────────────
from flags import _flag_from_code, COUNTRY_FLAGS  # noqa: E402

_COUNTRY_FLAGS_COMPAT: dict[str, str] = {
    'afghanistan': _flag_from_code('AF'), 'albania': _flag_from_code('AL'),
    'algeria': _flag_from_code('DZ'), 'andorra': _flag_from_code('AD'),
    'angola': _flag_from_code('AO'), 'antigua and barbuda': _flag_from_code('AG'),
    'argentina': _flag_from_code('AR'), 'armenia': _flag_from_code('AM'),
    'australia': _flag_from_code('AU'), 'austria': _flag_from_code('AT'),
    'azerbaijan': _flag_from_code('AZ'), 'bahamas': _flag_from_code('BS'),
    'bahrain': _flag_from_code('BH'), 'bangladesh': _flag_from_code('BD'),
    'barbados': _flag_from_code('BB'), 'belarus': _flag_from_code('BY'),
    'belgium': _flag_from_code('BE'), 'belize': _flag_from_code('BZ'),
    'benin': _flag_from_code('BJ'), 'bhutan': _flag_from_code('BT'),
    'bolivia': _flag_from_code('BO'), 'bosnia': _flag_from_code('BA'),
    'bosnia and herzegovina': _flag_from_code('BA'), 'botswana': _flag_from_code('BW'),
    'brazil': _flag_from_code('BR'), 'brasil': _flag_from_code('BR'),
    'brunei': _flag_from_code('BN'), 'bulgaria': _flag_from_code('BG'),
    'burkina faso': _flag_from_code('BF'), 'burundi': _flag_from_code('BI'),
    'cambodia': _flag_from_code('KH'), 'cameroon': _flag_from_code('CM'),
    'canada': _flag_from_code('CA'), 'cape verde': _flag_from_code('CV'),
    'cabo verde': _flag_from_code('CV'), 'central african republic': _flag_from_code('CF'),
    'chad': _flag_from_code('TD'), 'chile': _flag_from_code('CL'),
    'china': _flag_from_code('CN'), 'colombia': _flag_from_code('CO'),
    'comoros': _flag_from_code('KM'), 'congo': _flag_from_code('CG'),
    'dr congo': _flag_from_code('CD'), 'democratic republic of congo': _flag_from_code('CD'),
    'costa rica': _flag_from_code('CR'), 'croatia': _flag_from_code('HR'),
    'cuba': _flag_from_code('CU'), 'cyprus': _flag_from_code('CY'),
    'czech republic': _flag_from_code('CZ'), 'czechia': _flag_from_code('CZ'),
    'denmark': _flag_from_code('DK'), 'djibouti': _flag_from_code('DJ'),
    'dominica': _flag_from_code('DM'), 'dominican republic': _flag_from_code('DO'),
    'ecuador': _flag_from_code('EC'), 'egypt': _flag_from_code('EG'),
    'el salvador': _flag_from_code('SV'), 'equatorial guinea': _flag_from_code('GQ'),
    'eritrea': _flag_from_code('ER'), 'estonia': _flag_from_code('EE'),
    'eswatini': _flag_from_code('SZ'), 'swaziland': _flag_from_code('SZ'),
    'ethiopia': _flag_from_code('ET'), 'fiji': _flag_from_code('FJ'),
    'finland': _flag_from_code('FI'), 'france': _flag_from_code('FR'),
    'gabon': _flag_from_code('GA'), 'gambia': _flag_from_code('GM'),
    'georgia': _flag_from_code('GE'), 'germany': _flag_from_code('DE'),
    'ghana': _flag_from_code('GH'), 'greece': _flag_from_code('GR'),
    'grenada': _flag_from_code('GD'), 'guatemala': _flag_from_code('GT'),
    'guinea': _flag_from_code('GN'), 'guinea-bissau': _flag_from_code('GW'),
    'guyana': _flag_from_code('GY'), 'haiti': _flag_from_code('HT'),
    'honduras': _flag_from_code('HN'), 'hungary': _flag_from_code('HU'),
    'iceland': _flag_from_code('IS'), 'india': _flag_from_code('IN'),
    'indonesia': _flag_from_code('ID'), 'iran': _flag_from_code('IR'),
    'iraq': _flag_from_code('IQ'), 'ireland': _flag_from_code('IE'),
    'israel': _flag_from_code('IL'), 'italy': _flag_from_code('IT'),
    'ivory coast': _flag_from_code('CI'), "cote d'ivoire": _flag_from_code('CI'),
    'jamaica': _flag_from_code('JM'), 'japan': _flag_from_code('JP'),
    'jordan': _flag_from_code('JO'), 'kazakhstan': _flag_from_code('KZ'),
    'kenya': _flag_from_code('KE'), 'kiribati': _flag_from_code('KI'),
    'north korea': _flag_from_code('KP'), 'south korea': _flag_from_code('KR'),
    'korea': _flag_from_code('KR'), 'kosovo': _flag_from_code('XK'),
    'kuwait': _flag_from_code('KW'), 'kyrgyzstan': _flag_from_code('KG'),
    'laos': _flag_from_code('LA'), 'latvia': _flag_from_code('LV'),
    'lebanon': _flag_from_code('LB'), 'lesotho': _flag_from_code('LS'),
    'liberia': _flag_from_code('LR'), 'libya': _flag_from_code('LY'),
    'liechtenstein': _flag_from_code('LI'), 'lithuania': _flag_from_code('LT'),
    'luxembourg': _flag_from_code('LU'), 'madagascar': _flag_from_code('MG'),
    'malawi': _flag_from_code('MW'), 'malaysia': _flag_from_code('MY'),
    'maldives': _flag_from_code('MV'), 'mali': _flag_from_code('ML'),
    'malta': _flag_from_code('MT'), 'marshall islands': _flag_from_code('MH'),
    'mauritania': _flag_from_code('MR'), 'mauritius': _flag_from_code('MU'),
    'mexico': _flag_from_code('MX'), 'micronesia': _flag_from_code('FM'),
    'moldova': _flag_from_code('MD'), 'monaco': _flag_from_code('MC'),
    'mongolia': _flag_from_code('MN'), 'montenegro': _flag_from_code('ME'),
    'morocco': _flag_from_code('MA'), 'mozambique': _flag_from_code('MZ'),
    'myanmar': _flag_from_code('MM'), 'burma': _flag_from_code('MM'),
    'namibia': _flag_from_code('NA'), 'nauru': _flag_from_code('NR'),
    'nepal': _flag_from_code('NP'), 'netherlands': _flag_from_code('NL'),
    'holland': _flag_from_code('NL'), 'new zealand': _flag_from_code('NZ'),
    'nicaragua': _flag_from_code('NI'), 'niger': _flag_from_code('NE'),
    'nigeria': _flag_from_code('NG'), 'north macedonia': _flag_from_code('MK'),
    'macedonia': _flag_from_code('MK'), 'norway': _flag_from_code('NO'),
    'oman': _flag_from_code('OM'), 'pakistan': _flag_from_code('PK'),
    'palau': _flag_from_code('PW'), 'palestine': _flag_from_code('PS'),
    'panama': _flag_from_code('PA'), 'papua new guinea': _flag_from_code('PG'),
    'paraguay': _flag_from_code('PY'), 'peru': _flag_from_code('PE'),
    'philippines': _flag_from_code('PH'), 'poland': _flag_from_code('PL'),
    'portugal': _flag_from_code('PT'), 'qatar': _flag_from_code('QA'),
    'romania': _flag_from_code('RO'), 'russia': _flag_from_code('RU'),
    'russian federation': _flag_from_code('RU'), 'rwanda': _flag_from_code('RW'),
    'saint kitts and nevis': _flag_from_code('KN'), 'saint lucia': _flag_from_code('LC'),
    'saint vincent': _flag_from_code('VC'), 'samoa': _flag_from_code('WS'),
    'san marino': _flag_from_code('SM'), 'sao tome and principe': _flag_from_code('ST'),
    'saudi arabia': _flag_from_code('SA'), 'senegal': _flag_from_code('SN'),
    'serbia': _flag_from_code('RS'), 'seychelles': _flag_from_code('SC'),
    'sierra leone': _flag_from_code('SL'), 'singapore': _flag_from_code('SG'),
    'slovakia': _flag_from_code('SK'), 'slovenia': _flag_from_code('SI'),
    'solomon islands': _flag_from_code('SB'), 'somalia': _flag_from_code('SO'),
    'south africa': _flag_from_code('ZA'), 'south sudan': _flag_from_code('SS'),
    'spain': _flag_from_code('ES'), 'sri lanka': _flag_from_code('LK'),
    'sudan': _flag_from_code('SD'), 'suriname': _flag_from_code('SR'),
    'sweden': _flag_from_code('SE'), 'switzerland': _flag_from_code('CH'),
    'syria': _flag_from_code('SY'), 'taiwan': _flag_from_code('TW'),
    'tajikistan': _flag_from_code('TJ'), 'tanzania': _flag_from_code('TZ'),
    'thailand': _flag_from_code('TH'), 'timor-leste': _flag_from_code('TL'),
    'east timor': _flag_from_code('TL'), 'togo': _flag_from_code('TG'),
    'tonga': _flag_from_code('TO'), 'trinidad and tobago': _flag_from_code('TT'),
    'tunisia': _flag_from_code('TN'), 'turkey': _flag_from_code('TR'),
    'turkiye': _flag_from_code('TR'), 'turkmenistan': _flag_from_code('TM'),
    'tuvalu': _flag_from_code('TV'), 'uganda': _flag_from_code('UG'),
    'ukraine': _flag_from_code('UA'), 'united arab emirates': _flag_from_code('AE'),
    'uae': _flag_from_code('AE'), 'united kingdom': _flag_from_code('GB'),
    'uk': _flag_from_code('GB'), 'great britain': _flag_from_code('GB'),
    'england': _flag_from_code('GB'), 'united states': _flag_from_code('US'),
    'united states of america': _flag_from_code('US'), 'usa': _flag_from_code('US'),
    'us': _flag_from_code('US'), 'america': _flag_from_code('US'),
    'uruguay': _flag_from_code('UY'), 'uzbekistan': _flag_from_code('UZ'),
    'vanuatu': _flag_from_code('VU'), 'vatican': _flag_from_code('VA'),
    'venezuela': _flag_from_code('VE'), 'vietnam': _flag_from_code('VN'),
    'viet nam': _flag_from_code('VN'), 'yemen': _flag_from_code('YE'),
    'zambia': _flag_from_code('ZM'), 'zimbabwe': _flag_from_code('ZW'),
}


def _get_flag(name: str) -> str:
    """Prepend flag emoji to country name. Returns name as-is if unknown."""
    stripped = name.strip()
    flag = COUNTRY_FLAGS.get(stripped.lower())
    if flag:
        return f"{flag} {stripped}"
    return stripped


def _strip_flag_prefix(name: str) -> str:
    """Strip a leading flag emoji (and trailing space) from a country name."""
    s = name.strip()
    if not s:
        return s
    cp = ord(s[0])
    if 0x1F1E6 <= cp <= 0x1F1FF:
        return s[2:].lstrip()
    if cp > 0x1F000:
        return s[1:].lstrip()
    return s


# ── Service name → emoji mapping ──────────────────────────────────────────────

SERVICE_EMOJIS: dict[str, str] = {
    # Messaging / Social
    "whatsapp":    "💬", "telegram":    "✉️", "facebook":    "✉️",
    "fb":          "✉️", "instagram":   "", "insta":       "",
    "twitter":     "📱", "x":           "📱", "tiktok":      "📣",
    "tik tok":     "📣", "snapchat":    "", "discord":     "📱",
    "viber":       "📞", "line":        "", "wechat":      "",
    "signal":      "", "imo":         "📱", "skype":       "",
    "messenger":   "💬", "linkedin":    "", "pinterest":   "📌",
    "reddit":      "🤖", "tumblr":      "✏️", "threads":     "",
    "kwai":        "📱", "likee":       "", "bigo":        "📱",
    "zalo":        "", "kakao":       "", "kakaotalk":   "",
    "hike":        "🟢", "kik":         "💬", "bbm":         "🔴",
    "clubhouse":   "📣", "mastodon":    "📱", "bluesky":     "📱",
    # Streaming / Entertainment
    "youtube":     "📱", "netflix":     "📱", "spotify":     "📣",
    "amazon":      "🛍️", "prime":       "🛍️", "disneyplus":  "📱",
    "disney":      "📱", "hulu":        "📱", "hbo":         "📱",
    "apple":       "📱", "appletv":     "📱", "peacock":     "📱",
    "paramount":   "📱", "crunchyroll": "📱", "twitch":      "📱",
    "deezer":      "📣", "tidal":       "📣", "soundcloud":  "📱",
    "shazam":      "📣", "vimeo":       "📱",
    # E-commerce / Finance
    "paypal":      "💰", "binance":     "💰", "coinbase":    "💰",
    "bybit":       "💰", "okx":         "💰", "kraken":      "💰",
    "kucoin":      "💰", "huobi":       "💰", "gate":        "💰",
    "ebay":        "🛍️", "aliexpress":  "🛍️", "alibaba":     "🛍️",
    "shopee":      "🛍️", "lazada":      "🛍️", "daraz":       "🛍️",
    "flipkart":    "🛍️", "meesho":      "🛍️", "etsy":        "✏️",
    "wish":        "🌟", "shein":       "🛍️", "stripe":      "💰",
    "cashapp":     "💵", "venmo":       "💵", "revolut":     "💰",
    "wise":        "💸", "skrill":      "💰", "neteller":    "💰",
    # Ride / Food / Delivery
    "uber":        "📱", "ubereats":    "📱", "lyft":        "📱",
    "grab":        "📱", "gojek":       "📱", "rappi":       "📱",
    "zomato":      "📱", "swiggy":      "📱", "doordash":    "📱",
    "grubhub":     "📱", "foodpanda":   "📱", "pathao":      "📱",
    "shohoz":      "📱",
    # Tech / Cloud
    "google":      "🔍", "gmail":       "✉️", "microsoft":   "📱",
    "outlook":     "✉️", "yahoo":       "✉️", "dropbox":     "🛍️",
    "github":      "📱", "gitlab":      "📱", "slack":       "💬",
    "zoom":        "📱", "teams":       "📱", "notion":      "✏️",
    "trello":      "📌", "jira":        "🔵", "figma":       "✏️",
    "adobe":       "✏️", "canva":       "", "chatgpt":     "🤖",
    "openai":      "🤖", "gemini":      "🔵", "claude":      "🤖",
    # Dating
    "tinder":      "", "bumble":      "📱", "badoo":       "",
    "okcupid":     "", "hinge":       "⭐", "match":       "",
    "grindr":      "🟡",
    # Gaming
    "steam":       "📱", "epic":        "📱", "roblox":      "🔴",
    "minecraft":   "⭐️", "pubg":        "⭐️", "freefire":    "🌟",
    "mobile legends": "⭐️", "codm":     "⭐️", "valorant":    "🔴",
    # Travel / Maps
    "airbnb":      "", "booking":     "", "agoda":       "",
    "tripadvisor": "📌", "maps":        "🌍", "waze":        "🌍",
    # Other
    "truecaller":  "📱", "olx":         "💰", "craigslist":  "📌",
    "quora":       "❓", "medium":      "✏️", "substack":    "📌",
}

_SORTED_SVCS = sorted(SERVICE_EMOJIS, key=len, reverse=True)

# ── Service-specific animated sticker IDs ──────────────────────────────────────
# Maps lowercase service-name keywords → Telegram custom emoji sticker ID.
# Used in MESSAGE text only (not in keyboard button labels).
# Add more entries as new sticker IDs become available.
SERVICE_STICKERS: dict[str, str] = {
    "facebook":   "6298648828975259524",
    "fb":         "6298648828975259524",
    "messenger":  "6298648828975259524",
}
_SORTED_STICKER_SVCS = sorted(SERVICE_STICKERS, key=len, reverse=True)


def _get_service_emoji(name: str) -> str:
    """Return a plain unicode emoji for a known service (used in button labels)."""
    key = name.strip().lower()
    for svc in _SORTED_SVCS:
        if key == svc or key.startswith(svc) or svc in key:
            return SERVICE_EMOJIS[svc]
    return "📱"


def _get_service_sticker_html(name: str) -> str:
    """Return an animated <tg-emoji> HTML tag for a service (for message text).

    • If the service has a branded sticker ID in SERVICE_STICKERS, returns that.
    • Otherwise falls back to the plain emoji from SERVICE_EMOJIS, which the
      message preprocessor will animate via EMOJI_MAP automatically.
    """
    key = name.strip().lower()
    # Branded sticker lookup (longest key first to avoid partial mis-matches)
    for svc_key in _SORTED_STICKER_SVCS:
        if key == svc_key or key.startswith(svc_key) or svc_key in key:
            fallback = _get_service_emoji(name) or "📱"
            return f'<tg-emoji emoji-id="{SERVICE_STICKERS[svc_key]}">{fallback}</tg-emoji>'
    # No branded sticker — return plain emoji; preprocessor animates it
    return _get_service_emoji(name)


# ── Phone code / country name → flag resolution ───────────────────────────────

_PHONE_CODE_MAP: dict[str, tuple[str, str]] = {
    '+880': ('Bangladesh',        '🇧🇩'),
    '+91':  ('India',             '🇮🇳'),
    '+92':  ('Pakistan',          '🇵🇰'),
    '+1':   ('USA',               '🇺🇸'),
    '+44':  ('United Kingdom',    '🇬🇧'),
    '+966': ('Saudi Arabia',      '🇸🇦'),
    '+971': ('UAE',               '🇦🇪'),
    '+968': ('Oman',              '🇴🇲'),
    '+974': ('Qatar',             '🇶🇦'),
    '+973': ('Bahrain',           '🇧🇭'),
    '+965': ('Kuwait',            '🇰🇼'),
    '+962': ('Jordan',            '🇯🇴'),
    '+961': ('Lebanon',           '🇱🇧'),
    '+964': ('Iraq',              '🇮🇶'),
    '+963': ('Syria',             '🇸🇾'),
    '+20':  ('Egypt',             '🇪🇬'),
    '+212': ('Morocco',           '🇲🇦'),
    '+213': ('Algeria',           '🇩🇿'),
    '+216': ('Tunisia',           '🇹🇳'),
    '+218': ('Libya',             '🇱🇾'),
    '+249': ('Sudan',             '🇸🇩'),
    '+251': ('Ethiopia',          '🇪🇹'),
    '+234': ('Nigeria',           '🇳🇬'),
    '+233': ('Ghana',             '🇬🇭'),
    '+254': ('Kenya',             '🇰🇪'),
    '+255': ('Tanzania',          '🇹🇿'),
    '+256': ('Uganda',            '🇺🇬'),
    '+27':  ('South Africa',      '🇿🇦'),
    '+94':  ('Sri Lanka',         '🇱🇰'),
    '+95':  ('Myanmar',           '🇲🇲'),
    '+66':  ('Thailand',          '🇹🇭'),
    '+84':  ('Vietnam',           '🇻🇳'),
    '+60':  ('Malaysia',          '🇲🇾'),
    '+65':  ('Singapore',         '🇸🇬'),
    '+62':  ('Indonesia',         '🇮🇩'),
    '+63':  ('Philippines',       '🇵🇭'),
    '+82':  ('South Korea',       '🇰🇷'),
    '+81':  ('Japan',             '🇯🇵'),
    '+86':  ('China',             '🇨🇳'),
    '+852': ('Hong Kong',         '🇭🇰'),
    '+886': ('Taiwan',            '🇹🇼'),
    '+90':  ('Turkey',            '🇹🇷'),
    '+98':  ('Iran',              '🇮🇷'),
    '+93':  ('Afghanistan',       '🇦🇫'),
    '+977': ('Nepal',             '🇳🇵'),
    '+975': ('Bhutan',            '🇧🇹'),
    '+960': ('Maldives',          '🇲🇻'),
    '+94':  ('Sri Lanka',         '🇱🇰'),
    '+7':   ('Russia',            '🇷🇺'),
    '+380': ('Ukraine',           '🇺🇦'),
    '+48':  ('Poland',            '🇵🇱'),
    '+49':  ('Germany',           '🇩🇪'),
    '+33':  ('France',            '🇫🇷'),
    '+34':  ('Spain',             '🇪🇸'),
    '+39':  ('Italy',             '🇮🇹'),
    '+31':  ('Netherlands',       '🇳🇱'),
    '+32':  ('Belgium',           '🇧🇪'),
    '+41':  ('Switzerland',       '🇨🇭'),
    '+43':  ('Austria',           '🇦🇹'),
    '+46':  ('Sweden',            '🇸🇪'),
    '+47':  ('Norway',            '🇳🇴'),
    '+45':  ('Denmark',           '🇩🇰'),
    '+358': ('Finland',           '🇫🇮'),
    '+30':  ('Greece',            '🇬🇷'),
    '+351': ('Portugal',          '🇵🇹'),
    '+55':  ('Brazil',            '🇧🇷'),
    '+52':  ('Mexico',            '🇲🇽'),
    '+54':  ('Argentina',         '🇦🇷'),
    '+56':  ('Chile',             '🇨🇱'),
    '+57':  ('Colombia',          '🇨🇴'),
    '+51':  ('Peru',              '🇵🇪'),
    '+58':  ('Venezuela',         '🇻🇪'),
    '+61':  ('Australia',         '🇦🇺'),
    '+64':  ('New Zealand',       '🇳🇿'),
    '+380': ('Ukraine',           '🇺🇦'),
    '+375': ('Belarus',           '🇧🇾'),
    '+420': ('Czech Republic',    '🇨🇿'),
    '+421': ('Slovakia',          '🇸🇰'),
    '+36':  ('Hungary',           '🇭🇺'),
    '+40':  ('Romania',           '🇷🇴'),
    '+359': ('Bulgaria',          '🇧🇬'),
    '+381': ('Serbia',            '🇷🇸'),
    '+385': ('Croatia',           '🇭🇷'),
    '+387': ('Bosnia',            '🇧🇦'),
    '+370': ('Lithuania',         '🇱🇹'),
    '+371': ('Latvia',            '🇱🇻'),
    '+372': ('Estonia',           '🇪🇪'),
    '+995': ('Georgia',           '🇬🇪'),
    '+994': ('Azerbaijan',        '🇦🇿'),
    '+374': ('Armenia',           '🇦🇲'),
    '+996': ('Kyrgyzstan',        '🇰🇬'),
    '+992': ('Tajikistan',        '🇹🇯'),
    '+993': ('Turkmenistan',      '🇹🇲'),
    '+998': ('Uzbekistan',        '🇺🇿'),
    '+7':   ('Kazakhstan',        '🇰🇿'),
}

_COUNTRY_NAME_FLAG_MAP: dict[str, str] = {
    'bangladesh':      '🇧🇩',
    'india':           '🇮🇳',
    'pakistan':        '🇵🇰',
    'usa':             '🇺🇸',
    'united states':   '🇺🇸',
    'america':         '🇺🇸',
    'uk':              '🇬🇧',
    'united kingdom':  '🇬🇧',
    'england':         '🇬🇧',
    'britain':         '🇬🇧',
    'saudi arabia':    '🇸🇦',
    'saudi':           '🇸🇦',
    'ksa':             '🇸🇦',
    'uae':             '🇦🇪',
    'emirates':        '🇦🇪',
    'dubai':           '🇦🇪',
    'oman':            '🇴🇲',
    'qatar':           '🇶🇦',
    'bahrain':         '🇧🇭',
    'kuwait':          '🇰🇼',
    'jordan':          '🇯🇴',
    'lebanon':         '🇱🇧',
    'iraq':            '🇮🇶',
    'syria':           '🇸🇾',
    'egypt':           '🇪🇬',
    'morocco':         '🇲🇦',
    'algeria':         '🇩🇿',
    'tunisia':         '🇹🇳',
    'libya':           '🇱🇾',
    'sudan':           '🇸🇩',
    'ethiopia':        '🇪🇹',
    'nigeria':         '🇳🇬',
    'ghana':           '🇬🇭',
    'kenya':           '🇰🇪',
    'tanzania':        '🇹🇿',
    'uganda':          '🇺🇬',
    'south africa':    '🇿🇦',
    'sri lanka':       '🇱🇰',
    'myanmar':         '🇲🇲',
    'burma':           '🇲🇲',
    'thailand':        '🇹🇭',
    'vietnam':         '🇻🇳',
    'malaysia':        '🇲🇾',
    'singapore':       '🇸🇬',
    'indonesia':       '🇮🇩',
    'philippines':     '🇵🇭',
    'south korea':     '🇰🇷',
    'korea':           '🇰🇷',
    'japan':           '🇯🇵',
    'china':           '🇨🇳',
    'hong kong':       '🇭🇰',
    'taiwan':          '🇹🇼',
    'turkey':          '🇹🇷',
    'iran':            '🇮🇷',
    'afghanistan':     '🇦🇫',
    'nepal':           '🇳🇵',
    'bhutan':          '🇧🇹',
    'maldives':        '🇲🇻',
    'russia':          '🇷🇺',
    'ukraine':         '🇺🇦',
    'poland':          '🇵🇱',
    'germany':         '🇩🇪',
    'france':          '🇫🇷',
    'spain':           '🇪🇸',
    'italy':           '🇮🇹',
    'netherlands':     '🇳🇱',
    'belgium':         '🇧🇪',
    'switzerland':     '🇨🇭',
    'austria':         '🇦🇹',
    'sweden':          '🇸🇪',
    'norway':          '🇳🇴',
    'denmark':         '🇩🇰',
    'finland':         '🇫🇮',
    'greece':          '🇬🇷',
    'portugal':        '🇵🇹',
    'brazil':          '🇧🇷',
    'mexico':          '🇲🇽',
    'argentina':       '🇦🇷',
    'chile':           '🇨🇱',
    'colombia':        '🇨🇴',
    'peru':            '🇵🇪',
    'venezuela':       '🇻🇪',
    'australia':       '🇦🇺',
    'new zealand':     '🇳🇿',
    'czech republic':  '🇨🇿',
    'czechia':         '🇨🇿',
    'slovakia':        '🇸🇰',
    'hungary':         '🇭🇺',
    'romania':         '🇷🇴',
    'bulgaria':        '🇧🇬',
    'serbia':          '🇷🇸',
    'croatia':         '🇭🇷',
    'bosnia':          '🇧🇦',
    'lithuania':       '🇱🇹',
    'latvia':          '🇱🇻',
    'estonia':         '🇪🇪',
    'georgia':         '🇬🇪',
    'azerbaijan':      '🇦🇿',
    'armenia':         '🇦🇲',
    'kyrgyzstan':      '🇰🇬',
    'tajikistan':      '🇹🇯',
    'turkmenistan':    '🇹🇲',
    'uzbekistan':      '🇺🇿',
    'kazakhstan':      '🇰🇿',
    'belarus':         '🇧🇾',
    'canada':          '🇨🇦',
    'israel':          '🇮🇱',
    'palestine':       '🇵🇸',
    'yemen':           '🇾🇪',
    'libya':           '🇱🇾',
}


_ISO_CODE_MAP: dict[str, tuple[str, str]] = {
    'AF': ('Afghanistan',              _flag_from_code('AF')),
    'AL': ('Albania',                  _flag_from_code('AL')),
    'DZ': ('Algeria',                  _flag_from_code('DZ')),
    'AD': ('Andorra',                  _flag_from_code('AD')),
    'AO': ('Angola',                   _flag_from_code('AO')),
    'AG': ('Antigua and Barbuda',      _flag_from_code('AG')),
    'AR': ('Argentina',                _flag_from_code('AR')),
    'AM': ('Armenia',                  _flag_from_code('AM')),
    'AU': ('Australia',                _flag_from_code('AU')),
    'AT': ('Austria',                  _flag_from_code('AT')),
    'AZ': ('Azerbaijan',               _flag_from_code('AZ')),
    'BS': ('Bahamas',                  _flag_from_code('BS')),
    'BH': ('Bahrain',                  _flag_from_code('BH')),
    'BD': ('Bangladesh',               _flag_from_code('BD')),
    'BB': ('Barbados',                 _flag_from_code('BB')),
    'BY': ('Belarus',                  _flag_from_code('BY')),
    'BE': ('Belgium',                  _flag_from_code('BE')),
    'BZ': ('Belize',                   _flag_from_code('BZ')),
    'BJ': ('Benin',                    _flag_from_code('BJ')),
    'BT': ('Bhutan',                   _flag_from_code('BT')),
    'BO': ('Bolivia',                  _flag_from_code('BO')),
    'BA': ('Bosnia',                   _flag_from_code('BA')),
    'BW': ('Botswana',                 _flag_from_code('BW')),
    'BR': ('Brazil',                   _flag_from_code('BR')),
    'BN': ('Brunei',                   _flag_from_code('BN')),
    'BG': ('Bulgaria',                 _flag_from_code('BG')),
    'BF': ('Burkina Faso',             _flag_from_code('BF')),
    'BI': ('Burundi',                  _flag_from_code('BI')),
    'KH': ('Cambodia',                 _flag_from_code('KH')),
    'CM': ('Cameroon',                 _flag_from_code('CM')),
    'CA': ('Canada',                   _flag_from_code('CA')),
    'CV': ('Cape Verde',               _flag_from_code('CV')),
    'CF': ('Central African Republic', _flag_from_code('CF')),
    'TD': ('Chad',                     _flag_from_code('TD')),
    'CL': ('Chile',                    _flag_from_code('CL')),
    'CN': ('China',                    _flag_from_code('CN')),
    'CO': ('Colombia',                 _flag_from_code('CO')),
    'KM': ('Comoros',                  _flag_from_code('KM')),
    'CG': ('Congo',                    _flag_from_code('CG')),
    'CD': ('DR Congo',                 _flag_from_code('CD')),
    'CR': ('Costa Rica',               _flag_from_code('CR')),
    'HR': ('Croatia',                  _flag_from_code('HR')),
    'CU': ('Cuba',                     _flag_from_code('CU')),
    'CY': ('Cyprus',                   _flag_from_code('CY')),
    'CZ': ('Czech Republic',           _flag_from_code('CZ')),
    'DK': ('Denmark',                  _flag_from_code('DK')),
    'DJ': ('Djibouti',                 _flag_from_code('DJ')),
    'DM': ('Dominica',                 _flag_from_code('DM')),
    'DO': ('Dominican Republic',       _flag_from_code('DO')),
    'EC': ('Ecuador',                  _flag_from_code('EC')),
    'EG': ('Egypt',                    _flag_from_code('EG')),
    'SV': ('El Salvador',              _flag_from_code('SV')),
    'GQ': ('Equatorial Guinea',        _flag_from_code('GQ')),
    'ER': ('Eritrea',                  _flag_from_code('ER')),
    'EE': ('Estonia',                  _flag_from_code('EE')),
    'SZ': ('Eswatini',                 _flag_from_code('SZ')),
    'ET': ('Ethiopia',                 _flag_from_code('ET')),
    'FJ': ('Fiji',                     _flag_from_code('FJ')),
    'FI': ('Finland',                  _flag_from_code('FI')),
    'FR': ('France',                   _flag_from_code('FR')),
    'GA': ('Gabon',                    _flag_from_code('GA')),
    'GM': ('Gambia',                   _flag_from_code('GM')),
    'GE': ('Georgia',                  _flag_from_code('GE')),
    'DE': ('Germany',                  _flag_from_code('DE')),
    'GH': ('Ghana',                    _flag_from_code('GH')),
    'GR': ('Greece',                   _flag_from_code('GR')),
    'GD': ('Grenada',                  _flag_from_code('GD')),
    'GT': ('Guatemala',                _flag_from_code('GT')),
    'GN': ('Guinea',                   _flag_from_code('GN')),
    'GW': ('Guinea-Bissau',            _flag_from_code('GW')),
    'GY': ('Guyana',                   _flag_from_code('GY')),
    'HT': ('Haiti',                    _flag_from_code('HT')),
    'HN': ('Honduras',                 _flag_from_code('HN')),
    'HK': ('Hong Kong',                _flag_from_code('HK')),
    'HU': ('Hungary',                  _flag_from_code('HU')),
    'IS': ('Iceland',                  _flag_from_code('IS')),
    'IN': ('India',                    _flag_from_code('IN')),
    'ID': ('Indonesia',                _flag_from_code('ID')),
    'IR': ('Iran',                     _flag_from_code('IR')),
    'IQ': ('Iraq',                     _flag_from_code('IQ')),
    'IE': ('Ireland',                  _flag_from_code('IE')),
    'IL': ('Israel',                   _flag_from_code('IL')),
    'IT': ('Italy',                    _flag_from_code('IT')),
    'CI': ('Ivory Coast',              _flag_from_code('CI')),
    'JM': ('Jamaica',                  _flag_from_code('JM')),
    'JP': ('Japan',                    _flag_from_code('JP')),
    'JO': ('Jordan',                   _flag_from_code('JO')),
    'KZ': ('Kazakhstan',               _flag_from_code('KZ')),
    'KE': ('Kenya',                    _flag_from_code('KE')),
    'KI': ('Kiribati',                 _flag_from_code('KI')),
    'KP': ('North Korea',              _flag_from_code('KP')),
    'KR': ('South Korea',              _flag_from_code('KR')),
    'XK': ('Kosovo',                   _flag_from_code('XK')),
    'KW': ('Kuwait',                   _flag_from_code('KW')),
    'KG': ('Kyrgyzstan',               _flag_from_code('KG')),
    'LA': ('Laos',                     _flag_from_code('LA')),
    'LV': ('Latvia',                   _flag_from_code('LV')),
    'LB': ('Lebanon',                  _flag_from_code('LB')),
    'LS': ('Lesotho',                  _flag_from_code('LS')),
    'LR': ('Liberia',                  _flag_from_code('LR')),
    'LY': ('Libya',                    _flag_from_code('LY')),
    'LI': ('Liechtenstein',            _flag_from_code('LI')),
    'LT': ('Lithuania',                _flag_from_code('LT')),
    'LU': ('Luxembourg',               _flag_from_code('LU')),
    'MG': ('Madagascar',               _flag_from_code('MG')),
    'MW': ('Malawi',                   _flag_from_code('MW')),
    'MY': ('Malaysia',                 _flag_from_code('MY')),
    'MV': ('Maldives',                 _flag_from_code('MV')),
    'ML': ('Mali',                     _flag_from_code('ML')),
    'MT': ('Malta',                    _flag_from_code('MT')),
    'MH': ('Marshall Islands',         _flag_from_code('MH')),
    'MR': ('Mauritania',               _flag_from_code('MR')),
    'MU': ('Mauritius',                _flag_from_code('MU')),
    'MX': ('Mexico',                   _flag_from_code('MX')),
    'FM': ('Micronesia',               _flag_from_code('FM')),
    'MD': ('Moldova',                  _flag_from_code('MD')),
    'MC': ('Monaco',                   _flag_from_code('MC')),
    'MN': ('Mongolia',                 _flag_from_code('MN')),
    'ME': ('Montenegro',               _flag_from_code('ME')),
    'MA': ('Morocco',                  _flag_from_code('MA')),
    'MZ': ('Mozambique',               _flag_from_code('MZ')),
    'MM': ('Myanmar',                  _flag_from_code('MM')),
    'NA': ('Namibia',                  _flag_from_code('NA')),
    'NR': ('Nauru',                    _flag_from_code('NR')),
    'NP': ('Nepal',                    _flag_from_code('NP')),
    'NL': ('Netherlands',              _flag_from_code('NL')),
    'NZ': ('New Zealand',              _flag_from_code('NZ')),
    'NI': ('Nicaragua',                _flag_from_code('NI')),
    'NE': ('Niger',                    _flag_from_code('NE')),
    'NG': ('Nigeria',                  _flag_from_code('NG')),
    'MK': ('North Macedonia',          _flag_from_code('MK')),
    'NO': ('Norway',                   _flag_from_code('NO')),
    'OM': ('Oman',                     _flag_from_code('OM')),
    'PK': ('Pakistan',                 _flag_from_code('PK')),
    'PW': ('Palau',                    _flag_from_code('PW')),
    'PS': ('Palestine',                _flag_from_code('PS')),
    'PA': ('Panama',                   _flag_from_code('PA')),
    'PG': ('Papua New Guinea',         _flag_from_code('PG')),
    'PY': ('Paraguay',                 _flag_from_code('PY')),
    'PE': ('Peru',                     _flag_from_code('PE')),
    'PH': ('Philippines',              _flag_from_code('PH')),
    'PL': ('Poland',                   _flag_from_code('PL')),
    'PT': ('Portugal',                 _flag_from_code('PT')),
    'QA': ('Qatar',                    _flag_from_code('QA')),
    'RO': ('Romania',                  _flag_from_code('RO')),
    'RU': ('Russia',                   _flag_from_code('RU')),
    'RW': ('Rwanda',                   _flag_from_code('RW')),
    'KN': ('Saint Kitts and Nevis',    _flag_from_code('KN')),
    'LC': ('Saint Lucia',              _flag_from_code('LC')),
    'VC': ('Saint Vincent',            _flag_from_code('VC')),
    'WS': ('Samoa',                    _flag_from_code('WS')),
    'SM': ('San Marino',               _flag_from_code('SM')),
    'ST': ('Sao Tome and Principe',    _flag_from_code('ST')),
    'SA': ('Saudi Arabia',             _flag_from_code('SA')),
    'SN': ('Senegal',                  _flag_from_code('SN')),
    'RS': ('Serbia',                   _flag_from_code('RS')),
    'SC': ('Seychelles',               _flag_from_code('SC')),
    'SL': ('Sierra Leone',             _flag_from_code('SL')),
    'SG': ('Singapore',                _flag_from_code('SG')),
    'SK': ('Slovakia',                 _flag_from_code('SK')),
    'SI': ('Slovenia',                 _flag_from_code('SI')),
    'SB': ('Solomon Islands',          _flag_from_code('SB')),
    'SO': ('Somalia',                  _flag_from_code('SO')),
    'ZA': ('South Africa',             _flag_from_code('ZA')),
    'SS': ('South Sudan',              _flag_from_code('SS')),
    'ES': ('Spain',                    _flag_from_code('ES')),
    'LK': ('Sri Lanka',                _flag_from_code('LK')),
    'SD': ('Sudan',                    _flag_from_code('SD')),
    'SR': ('Suriname',                 _flag_from_code('SR')),
    'SE': ('Sweden',                   _flag_from_code('SE')),
    'CH': ('Switzerland',              _flag_from_code('CH')),
    'SY': ('Syria',                    _flag_from_code('SY')),
    'TW': ('Taiwan',                   _flag_from_code('TW')),
    'TJ': ('Tajikistan',               _flag_from_code('TJ')),
    'TZ': ('Tanzania',                 _flag_from_code('TZ')),
    'TH': ('Thailand',                 _flag_from_code('TH')),
    'TL': ('Timor-Leste',              _flag_from_code('TL')),
    'TG': ('Togo',                     _flag_from_code('TG')),
    'TO': ('Tonga',                    _flag_from_code('TO')),
    'TT': ('Trinidad and Tobago',      _flag_from_code('TT')),
    'TN': ('Tunisia',                  _flag_from_code('TN')),
    'TR': ('Turkey',                   _flag_from_code('TR')),
    'TM': ('Turkmenistan',             _flag_from_code('TM')),
    'TV': ('Tuvalu',                   _flag_from_code('TV')),
    'UG': ('Uganda',                   _flag_from_code('UG')),
    'UA': ('Ukraine',                  _flag_from_code('UA')),
    'AE': ('UAE',                      _flag_from_code('AE')),
    'GB': ('United Kingdom',           _flag_from_code('GB')),
    'US': ('United States',            _flag_from_code('US')),
    'UY': ('Uruguay',                  _flag_from_code('UY')),
    'UZ': ('Uzbekistan',               _flag_from_code('UZ')),
    'VU': ('Vanuatu',                  _flag_from_code('VU')),
    'VA': ('Vatican',                  _flag_from_code('VA')),
    'VE': ('Venezuela',                _flag_from_code('VE')),
    'VN': ('Vietnam',                  _flag_from_code('VN')),
    'YE': ('Yemen',                    _flag_from_code('YE')),
    'ZM': ('Zambia',                   _flag_from_code('ZM')),
    'ZW': ('Zimbabwe',                 _flag_from_code('ZW')),
}


def _resolve_country_name(raw: str) -> str:
    """Given +880, BD, or a country name, return 'FLAG Name' string.

    - If raw starts with '+' and digits → look up phone code map,
      try longest prefix first (e.g. +880 before +8).
    - If raw is exactly 2 letters → treat as ISO country code (BD, US, IN …).
    - Otherwise → look up by name, prepend matching flag emoji.
    - If nothing matches, return raw as-is (no flag stripped).
    """
    s = raw.strip()
    if not s:
        return s

    # Already has a flag emoji at the start — return unchanged
    cp = ord(s[0])
    if 0x1F1E6 <= cp <= 0x1F1FF or cp > 0x1F000:
        return s

    # Phone-code lookup (+880, +44, +1 …)
    if s.startswith('+') and s[1:].replace(' ', '').isdigit():
        code = '+' + s[1:].replace(' ', '')
        for length in (5, 4, 3, 2, 1):
            candidate = code[:length + 1]
            if candidate in _PHONE_CODE_MAP:
                name, flag = _PHONE_CODE_MAP[candidate]
                return f"{flag} {name}"
        return s  # unknown phone code — keep as-is

    # 2-letter ISO code lookup (BD, US, IN, GB …)
    if len(s) == 2 and s.isalpha():
        entry = _ISO_CODE_MAP.get(s.upper())
        if entry:
            name, flag = entry
            return f"{flag} {name}"

    # Name lookup — _COUNTRY_NAME_FLAG_MAP (primary)
    key = s.lower().strip()
    flag = _COUNTRY_NAME_FLAG_MAP.get(key, '')
    if flag:
        return f"{flag} {s.title()}"

    # Name lookup — COUNTRY_FLAGS (broader coverage fallback)
    flag2 = COUNTRY_FLAGS.get(key, '')
    if flag2:
        return f"{flag2} {s.title()}"

    # No match — return as-is without any prefix
    return s


def _get_flag_for_country(name: str) -> str:
    """Detect country flag emoji from a name like 'Myanmar V2' → '🇲🇲'.
    Returns '' (empty) if no country matches — never falls back to 🌍."""
    if not name:
        return ''
    # Already starts with a flag/emoji — do NOT add another
    cp = ord(name[0])
    if 0x1F1E6 <= cp <= 0x1F1FF or 0x1F000 <= cp <= 0x1FFFF:
        return ''
    key = name.lower().strip()
    # 1. Direct match
    if key in COUNTRY_FLAGS:
        return COUNTRY_FLAGS[key]
    # 2. Multi-word prefix match (longest first) e.g. "United States V2"
    for ckey in sorted(COUNTRY_FLAGS.keys(), key=lambda k: -len(k)):
        if key == ckey or key.startswith(ckey + ' '):
            return COUNTRY_FLAGS[ckey]
    # 3. Individual word fallback (skip short/numeric words)
    for word in re.sub(r'[^a-z\s]', '', key).split():
        if len(word) > 3 and word in COUNTRY_FLAGS:
            return COUNTRY_FLAGS[word]
    return ''


# ── Panel registry (ordered) ──────────────────────────────────────────────────
# Used by Panel Polling menu for index-based callback_data.
PANEL_LIST = [
    ('SMS Hadi',        monitor),
    ('Konekta Premium', konekta_monitor),
    ('Msi sms',         msi_sms_monitor),
    ('Number Panel',    number_panel_monitor),
    ('Purple sms',      purple_sms_monitor),
    ('Proof sms',       proof_sms_monitor),
    ('Lamix sms',       lamix_sms_monitor),
    ('Seven 1 Tel',     seven1tel_monitor),
    ('Flex sms',        mait_sms_monitor),
    ('Zento sms',       zento_sms_monitor),
    ('Wolf sms',        wolf_sms_monitor),
    ('Shark sms',       shark_sms_monitor),
    ('KM Carrier sms',  km_carrier_sms_monitor),
]

# ── Multiple Panels section (second accounts / extra panels) ──────────────────
MULTIPLE_PANEL_LIST = [
    ('SMS Hadi 2', sms_hadi2_monitor),
]

# ── Combined list for operations that need ALL panels ─────────────────────────
ALL_PANEL_LIST = PANEL_LIST + MULTIPLE_PANEL_LIST

# ── Panel categorization ──────────────────────────────────────────────────────
PANEL_CATEGORY = {
    'SMS Hadi':        'client',
    'Konekta Premium': 'client',
    'Msi sms':         'client',
    'Number Panel':    'client',
    'Purple sms':      'client',
    'Proof sms':       'client',
    'Lamix sms':       'client',
    'Seven 1 Tel':     'agent',
    'Flex sms':        'agent',
    'Zento sms':       'client',
    'Wolf sms':        'agent',
    'Shark sms':       'agent',
    'SMS Hadi 2':      'client',
    'KM Carrier sms':  'agent',
}

# ── Panel config usernames (always from code/config, never from DB) ───────────
PANEL_CONFIG_USERNAMES: dict[str, str] = {
    'SMS Hadi':        SMS_HADI_USERNAME,
    'Konekta Premium': KONEKTA_USERNAME,
    'Msi sms':         MSI_SMS_USERNAME,
    'Number Panel':    NUMBER_PANEL_USERNAME,
    'Purple sms':      PURPLE_SMS_USERNAME,
    'Proof sms':       PROOF_SMS_USERNAME,
    'Lamix sms':       LAMIX_SMS_USERNAME,
    'Seven 1 Tel':     SEVEN1TEL_USERNAME,
    'Flex sms':        MAIT_SMS_USERNAME,
    'Zento sms':       ZENTO_SMS_USERNAME,
    'Wolf sms':        WOLF_SMS_USERNAME,
    'Shark sms':       SHARK_SMS_USERNAME,
    'SMS Hadi 2':      SMS_HADI2_USERNAME,
    'KM Carrier sms':  KM_CARRIER_SMS_USERNAME,
}


def _panels_in_category(category: str) -> list[str]:
    """Return the names of panels belonging to the given category, in
    PANEL_LIST order."""
    return [pname for pname, _m in PANEL_LIST
            if PANEL_CATEGORY.get(pname) == category]


def _md_escape(s: str) -> str:
    """Escape characters that have special meaning in Telegram legacy Markdown
    so panel names cannot accidentally break a message."""
    if not s:
        return ""
    return (
        str(s)
        .replace("\\", "\\\\")
        .replace("*", "\\*")
        .replace("_", "\\_")
        .replace("`", "\\`")
        .replace("[", "\\[")
    )



def _wipe_monitor_session(monitor):
    """Wipe session/cookies/sesskey/csrf from a monitor object and put it
    into manual-only mode. Admin must use ⌛ Retry Interval to log in again."""
    for attr in ('session', 'sesskey', 'cookies', '_csrf', '_token',
                 'csrf_token', 'auth_token', '_session_id', '_cookie_jar'):
        try:
            if hasattr(monitor, attr):
                setattr(monitor, attr, None)
        except Exception:
            pass
    try:
        if hasattr(monitor, 'logged_in'):
            monitor.logged_in = False
    except Exception:
        pass
    try:
        if hasattr(monitor, '_seen_keys'):
            monitor._seen_keys.clear()
    except Exception:
        pass
    try:
        if hasattr(monitor, '_is_first_poll'):
            monitor._is_first_poll = True
    except Exception:
        pass
    try:
        monitor._manual_only = True
    except Exception:
        pass


async def _build_session_cleanup_view():
    """Build the Session Cleanup view — list every panel name in mono format,
    and ask admin to type the name they want to clean. No buttons."""
    statuses = await run_db(_get_all_panel_statuses)
    statuses_by_name = {s['panel_name']: s for s in (statuses or [])}

    lines = []
    for pname, _m in ALL_PANEL_LIST:
        s     = statuses_by_name.get(pname)
        is_en = await run_db(_is_panel_enabled, pname)
        if not is_en:
            icon = "🚫"
        elif s and s.get('logged_in'):
            icon = "✅"
        else:
            icon = "❌"
        lines.append(f"{icon} `{pname}`")

    msg = (
        "*Session Cleanup*\n\n"
        + "\n".join(lines)
        + "\n\n"
        "Type the *name* of the panel whose session you want to clean "
        "(copy it from the list above)."
    )
    return msg


async def _notify_admins_session_cleaned(bot, panel_name: str):
    """Notify all admins that a panel's session was cleaned. Bot will NOT
    auto-login the panel — admin must use ⌛ Retry Interval to re-login."""
    try:
        from database import _get_all_admins_with_details
        admins = _get_all_admins_with_details()
        text_msg = (
            f"*Session Cleanup Done*\n\n"
            f"*{panel_name}* session, cookies and sesskey have been cleared.\n\n"
            f"⚠️ The bot will NOT log in automatically.\n"
            f"Use ⌛ *Retry Interval* to log in again."
        )
        for admin in admins:
            uid = admin.get("user_id")
            if uid:
                try:
                    await bot.send_message(chat_id=uid, text=text_msg, parse_mode="Markdown")
                except Exception:
                    pass
    except Exception:
        pass


async def _notify_admins_panel_toggled(bot, panel_name: str, enabled: bool):
    """Notify all admins that a panel was enabled or disabled."""
    try:
        admins = await run_db(_get_all_admins_with_details)
        if enabled:
            text_msg = (
                f"*Panel Enabled*\n\n"
                f"✅ *{panel_name}* is now enabled. The bot will start monitoring this panel."
            )
        else:
            text_msg = (
                f"*Panel Disabled*\n\n"
                f"🚫 *{panel_name}* is now disabled. The bot has stopped monitoring this panel."
            )
        for admin in admins:
            uid = admin.get("user_id")
            if uid:
                try:
                    await bot.send_message(chat_id=uid, text=text_msg, parse_mode="Markdown")
                except Exception:
                    pass
    except Exception:
        pass


async def _build_extra_groups_overview(context) -> str:
    """List every Extra Group's name and chat ID in mono format inside a
    single message. Used by the 📢 Extra Groups button overview."""
    groups = await run_db(_get_all_extra_groups)
    if not groups:
        return (
            "📢 *Extra Groups*\n\n"
            "No extra groups have been added yet.\n\n"
            "Select an option from the keyboard below."
        )
    lines = []
    for g in groups:
        lines.append(f"`{g['title']}`\n`{g['chat_id']}`")
    msg = (
        f"📢 *Extra Groups* (Total: *{len(groups)}*)\n\n"
        + "\n\n".join(lines)
        + "\n\nSelect an option from the keyboard below."
    )
    if len(msg) > 4000:
        msg = msg[:3990] + "\n…"
    return msg


async def _build_panel_toggle_view():
    """Build the Panel Toggle screen — a single message listing all panels
    in mono format (so admin can copy-paste the name) with their current
    enabled/disabled state. Admin types the panel name to toggle it."""
    lines = []
    enabled_count = 0
    for pname, _m in ALL_PANEL_LIST:
        en = bool(await run_db(_is_panel_enabled, pname))
        if en:
            enabled_count += 1
            lines.append(f"✅ `{pname}`")
        else:
            lines.append(f"🚫 `{pname}`")
    total = len(lines)
    msg = (
        "*Panel Toggle*\n\n"
        f"Total: *{total}*  |  ✅ Enabled: *{enabled_count}*  |  "
        f"🚫 Disabled: *{total - enabled_count}*\n\n"
        + "\n".join(lines)
        + "\n\n"
        "Copy the *name* of the panel you want to enable/disable and send it.\n"
        "(Currently Enabled → will be Disabled; Disabled → will be Enabled.)"
    )
    return msg


async def _build_retry_login_view():
    """Build the ⌛ Retry Interval screen — show ONLY the panels that failed
    to log in (in `mono` format). Admin types a panel name to manually
    trigger that panel's login. On success, all admins are notified."""
    statuses = await run_db(_get_all_panel_statuses)
    statuses_by_name = {s['panel_name']: s for s in (statuses or [])}

    failed_lines = []
    for pname, _m in ALL_PANEL_LIST:
        s     = statuses_by_name.get(pname)
        is_en = await run_db(_is_panel_enabled, pname)
        if not is_en:
            continue
        if s and s.get('logged_in'):
            continue
        failed_lines.append(f"❌ `{pname}`")

    if not failed_lines:
        msg = (
            "⌛ *Retry Interval*\n\n"
            "🎉 All enabled panels are now logged in successfully.\n"
            "No failed panels — nothing to retry."
        )
        return msg, False

    msg = (
        "⌛ *Retry Interval — Failed Panels*\n\n"
        + "\n".join(failed_lines)
        + "\n\n"
        "Copy the *name* of the panel you want to retry login for "
        "(from the list above) and send it.\n\n"
        "All admins will be notified when login succeeds."
    )
    return msg, True


# ── Message Queue (Telegram rate-limit safe sender) ───────────────────────────
# Queues outgoing messages and sends them at max 25/sec with auto-retry.
# Usage: await enqueue_message(bot, chat_id, text, **kwargs)

_msg_queue: asyncio.Queue | None = None   # created lazily in post_init (correct event loop)
_MSG_RATE   = 30          # max messages per second (Telegram hard limit is 30)
_MSG_RETRY  = 5           # number of retries on failure
_MSG_DELAY  = 1.0 / _MSG_RATE   # minimum delay between sends


async def _message_queue_worker():
    """Background coroutine: drains _msg_queue and sends at a safe rate."""
    while True:
        try:
            item = await _msg_queue.get()
            if item is None:
                _msg_queue.task_done()
                break
            bot, chat_id, text, kwargs = item
            for attempt in range(1, _MSG_RETRY + 1):
                try:
                    await bot.send_message(chat_id=chat_id, text=text, **kwargs)
                    break
                except Exception as exc:
                    err_str = str(exc).lower()
                    if 'flood' in err_str or 'too many' in err_str:
                        wait = 5 * attempt
                        logger.warning(f"[MsgQueue] FloodWait → sleeping {wait}s")
                        await asyncio.sleep(wait)
                    elif attempt < _MSG_RETRY:
                        await asyncio.sleep(1.0 * attempt)
                    else:
                        logger.error(f"[MsgQueue] Failed to send to {chat_id}: {exc}")
            await asyncio.sleep(_MSG_DELAY)
            _msg_queue.task_done()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"[MsgQueue] Worker error: {e}")


async def enqueue_message(bot, chat_id: int, text: str, **kwargs):
    """Put a message into the send queue (non-blocking)."""
    if _msg_queue is not None:
        await _msg_queue.put((bot, chat_id, text, kwargs))


# ── Memory Cleanup ─────────────────────────────────────────────────────────────
# Runs every 30 minutes to purge stale entries from in-memory dicts,
# preventing unbounded RAM growth when the bot runs for days.

_CLEANUP_INTERVAL = 1800   # 30 minutes
_SEMAPHORE_TTL    = 3600   # remove semaphores idle > 1 hour
_semaphore_last_used: dict[int, float] = {}   # user_id -> last access timestamp


async def _memory_cleanup_loop():
    """Periodic background task: clean up stale in-memory state."""
    while True:
        await asyncio.sleep(_CLEANUP_INTERVAL)
        try:
            now = time.monotonic()

            # Clean _user_semaphores ── remove entries idle for > TTL
            stale_sems = [
                uid for uid, last in _semaphore_last_used.items()
                if (now - last) > _SEMAPHORE_TTL
            ]
            for uid in stale_sems:
                _user_semaphores.pop(uid, None)
                _semaphore_last_used.pop(uid, None)

            # Clean _cn_timestamps ── remove entries older than 60 s
            stale_ts = [uid for uid, ts_list in _cn_timestamps.items()
                        if not ts_list or (now - max(ts_list)) > 60]
            for uid in stale_ts:
                _cn_timestamps.pop(uid, None)

            # Clean _cn_cooldown ── remove expired cooldowns
            stale_cd = [uid for uid, until in _cn_cooldown.items() if now > until]
            for uid in stale_cd:
                _cn_cooldown.pop(uid, None)

            cleaned = len(stale_sems) + len(stale_ts) + len(stale_cd)
            if cleaned:
                logger.info(f"[MemCleanup] Removed {cleaned} stale memory entries "
                            f"| queue_size={_msg_queue.qsize()}")
        except Exception as e:
            logger.error(f"[MemCleanup] Error: {e}")


# ── Change Number rate limiter ─────────────────────────────────────────────────
# Tracks recent press timestamps and active cooldowns per user
_cn_timestamps: dict[int, list[float]] = {}   # user_id -> list of recent press times
_cn_cooldown:   dict[int, float]       = {}   # user_id -> cooldown_until (unix timestamp)

_CN_WINDOW   = 1.0   # seconds — if pressed ≥2 times within this window → cooldown
_CN_MAX_HITS = 2     # max presses allowed inside the window before cooldown
_CN_COOLDOWN = 3.0   # seconds to wait after triggering the limit

# ── Panel emoji mapping ────────────────────────────────────────────────────────

_PANEL_EMOJIS = {
    'SMS Hadi':        '',
    'Konekta Premium': '👑',
    'Msi sms':         '📱',
    'Number Panel':    '',
    'Purple sms':      '',
    'Proof sms':       '✅',
    'Lamix sms':       '🌐',
    'Seven 1 Tel':     '📱',
    'Flex sms':        '💬',
    'Zento sms':       '🟢',
    'SMS Hadi 2':      '',
    'KM Carrier sms':  '📡',
}

def _panel_label(name: str) -> str:
    """Return emoji + panel name for display on keyboard buttons."""
    emoji = _PANEL_EMOJIS.get(name, '')
    return f"{emoji} {name}"

def _panel_name_from_label(label: str) -> str:
    """Strip leading emoji/status prefix from a panel button label to get the raw panel name.

    Handles all cases:
      'SMS Hadi 2'  → 'SMS Hadi 2'
      '✅ Wolf sms'    → 'Wolf sms'
      '🚫 Shark sms'  → 'Shark sms'
      '💬 Flex sms'   → 'Flex sms'
      'SMS Hadi 2'    → 'SMS Hadi 2'   (no prefix, returned as-is)
    """
    label = label.strip('`').strip()
    # Strip known status prefixes first
    for prefix in ("✅ ", "🚫 ", "✅", "🚫"):
        if label.startswith(prefix):
            label = label[len(prefix):].strip()
            break
    # Strip any remaining leading emoji (non-ASCII, non-alphanumeric first token)
    parts = label.split(' ', 1)
    if len(parts) == 2:
        first = parts[0]
        # If the first token has no ASCII letter or digit it is an emoji — strip it
        if not any(c.isascii() and (c.isalpha() or c.isdigit()) for c in first):
            return parts[1].strip()
    return label


def _resolve_panel_user(panel_dict: dict, pname: str) -> str:
    """Return a clean display username for a panel.

    Rejects the stored value if it is empty, matches the panel name, or
    accidentally matches the keyboard button label (emoji + name) — all of
    which indicate the username was never set or was corrupted.
    """
    stored = (panel_dict.get('username') or '').strip()
    # Treat stored value as invalid when it equals the panel name or label
    if stored in ('', pname, _panel_label(pname)):
        stored = ''
    # Fall back to the hard-coded config username, then to '—'
    fallback = (PANEL_CONFIG_USERNAMES.get(pname) or '').strip()
    if fallback in ('', pname, _panel_label(pname)):
        fallback = ''
    return stored or fallback or '—'


_PANEL_PAGE_SIZE = 6

def _build_panel_page_keyboard(panels: list, page: int) -> ReplyKeyboardMarkup:
    """Build a paginated ReplyKeyboardMarkup for the panel list (6 per page)."""
    start       = page * _PANEL_PAGE_SIZE
    end         = start + _PANEL_PAGE_SIZE
    page_panels = panels[start:end]
    panel_btns  = [KeyboardButton(_panel_label(p['name'])) for p in page_panels]
    rows        = [panel_btns[i:i+2] for i in range(0, len(panel_btns), 2)]
    nav = []
    if end < len(panels):
        remaining = len(panels) - end
        nav.append(KeyboardButton(f"➡️ More Panels ({remaining} remaining)"))
    if page > 0:
        nav.append(KeyboardButton("⬅️ First Page"))
    if nav:
        rows.append(nav)
    rows.append([KeyboardButton("Back to Admin Panel")])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


# ── Per-user concurrency limiter ──────────────────────────────────────────────
# Each user gets at most 3 concurrent handler coroutines so a single user
# cannot flood the bot with thousands of rapid-fire requests.
_user_semaphores: dict[int, asyncio.Semaphore] = {}


def _get_user_sem(user_id: int) -> asyncio.Semaphore:
    if user_id not in _user_semaphores:
        _user_semaphores[user_id] = asyncio.Semaphore(3)
    _semaphore_last_used[user_id] = time.monotonic()
    return _user_semaphores[user_id]


def run_db(func, *args, **kwargs):
    return asyncio.to_thread(func, *args, **kwargs)


# ── Auto user tracker ─────────────────────────────────────────────────────────
# Records every user who interacts with the bot — even if they never typed
# /start. This makes Broadcast and Force Start reach all users who clicked
# any user-panel button or sent a message, and the admin User Count reflects
# the true number of users.

async def _ensure_user_tracked(update: Update) -> None:
    """Idempotently add the user to our DB on ANY interaction. Safe to call
    on every update — _add_user only inserts new rows or refreshes name fields
    for existing rows."""
    try:
        u = update.effective_user if update else None
        if not u or u.is_bot:
            return
        # Only track 1:1 chats with the bot — not group/channel users
        chat = update.effective_chat
        if chat and chat.type and chat.type != "private":
            return
        await run_db(_add_user, u.id, u.username, u.first_name, u.last_name, None)
    except Exception as e:
        # Never let tracking break a real handler
        logger.warning(f"[AutoTrack] failed: {e}")


# ── Powerful bulk-send engine (for Broadcast and Force Start) ────────────────
# Concurrent sender with categorised result reporting and live progress.

async def _bulk_send(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    title: str,
    user_ids: list,
    sender,
    concurrency: int = 25,
    max_attempts: int = 3,
    retry_blocked: bool = False,
):
    """Concurrently call `sender(uid)` for every uid in user_ids and report
    progress / categorised stats live. `sender` is an async callable that
    raises on failure and returns on success.

    max_attempts:    total attempts per user before giving up
    retry_blocked:   if True, also re-attempt users who returned Forbidden
                     (useful for Force Start so a freshly un-blocked user
                     receives /start on a subsequent retry inside the loop)"""
    from telegram.error import (
        Forbidden, BadRequest, RetryAfter, TimedOut, NetworkError,
    )

    total = len(user_ids)
    sem   = asyncio.Semaphore(max(1, concurrency))
    stats = {
        "sent": 0, "blocked": 0, "deactivated": 0,
        "not_found": 0, "other": 0, "done": 0,
    }
    lock = asyncio.Lock()

    status_msg = await update.message.reply_text(
        f"{title}\n\n📊 Total: *{total}* users\n⌛ Starting broadcast…",
        parse_mode='Markdown',
    )

    async def _progress_updater():
        while True:
            await asyncio.sleep(2)
            async with lock:
                done = stats["done"]
                snapshot = dict(stats)
            try:
                await status_msg.edit_text(
                    f"{title}\n\n"
                    f"📊 Total: *{total}*\n"
                    f"📤 Sent: *{done}* / {total}\n"
                    f"✅ Success: *{snapshot['sent']}*\n"
                    f"🚫 Blocked: *{snapshot['blocked']}*\n"
                    f"Deactivated: *{snapshot['deactivated']}*\n"
                    f"❓ Chat not found: *{snapshot['not_found']}*\n"
                    f"⚠️ Other errors: *{snapshot['other']}*",
                    parse_mode='Markdown',
                )
            except Exception:
                pass
            if done >= total:
                return

    async def _send_one(uid):
        async with sem:
            blocked_so_far = False
            sent_ok = False
            for attempt in range(max_attempts):
                try:
                    await sender(uid)
                    async with lock:
                        stats["sent"] += 1
                        if blocked_so_far:
                            # Adjust: this user was previously counted blocked
                            # in this same loop — but we recovered. Keep stats
                            # accurate.
                            pass
                    sent_ok = True
                    break
                except RetryAfter as e:
                    await asyncio.sleep(getattr(e, "retry_after", 1) + 0.5)
                except (TimedOut, NetworkError):
                    await asyncio.sleep(1.0 + attempt)
                except Forbidden:
                    blocked_so_far = True
                    if retry_blocked and attempt < max_attempts - 1:
                        # Wait a bit and try again — user may un-block
                        await asyncio.sleep(0.7 + attempt * 0.3)
                        continue
                    async with lock:
                        stats["blocked"] += 1
                    break
                except BadRequest as e:
                    text_e = str(e).lower()
                    async with lock:
                        if "deactivated" in text_e:
                            stats["deactivated"] += 1
                        elif "chat not found" in text_e or "user not found" in text_e:
                            stats["not_found"] += 1
                        else:
                            stats["other"] += 1
                    break
                except Exception:
                    async with lock:
                        stats["other"] += 1
                    break
            async with lock:
                stats["done"] += 1

    progress_task = asyncio.create_task(_progress_updater())
    await asyncio.gather(*[_send_one(uid) for uid in user_ids])
    try:
        await asyncio.wait_for(progress_task, timeout=3)
    except Exception:
        progress_task.cancel()

    reach_pct = (stats["sent"] / total * 100) if total else 0.0
    try:
        await status_msg.edit_text(
            f"{title} — *Done!* ✅\n\n"
            f"📊 Total users: *{total}*\n"
            f"✅ Success: *{stats['sent']}*  ({reach_pct:.1f}%)\n"
            f"🚫 Blocked: *{stats['blocked']}*\n"
            f"Deactivated: *{stats['deactivated']}*\n"
            f"❓ Chat not found: *{stats['not_found']}*\n"
            f"⚠️ Other errors: *{stats['other']}*",
            parse_mode='Markdown',
        )
    except Exception:
        pass
    return stats


async def _run_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Powerful broadcast: supports two modes.

    1. Forward Mode — if the admin forwards a message to the bot, it is
       forwarded (with the original "Forwarded from …" header visible) to
       every tracked user via bot.forward_message().

    2. Copy Mode (existing behaviour) — if the admin types / sends their own
       message, it is copied (no forward header) to every tracked user via
       bot.copy_message().

    Both modes support any content type: text, photo, video, document, voice,
    sticker, etc.  Concurrent dispatch with categorised result reporting."""
    user_ids = await run_db(_get_all_users)
    src_chat = update.effective_chat.id
    src_msg  = update.message.message_id

    # Detect whether the incoming message is a forwarded one.
    # PTB v20+ exposes forward_origin; older fields forward_from /
    # forward_from_chat are kept as fallback.
    msg = update.message
    is_forwarded = bool(
        getattr(msg, 'forward_origin', None)
        or getattr(msg, 'forward_from', None)
        or getattr(msg, 'forward_from_chat', None)
        or getattr(msg, 'forward_sender_name', None)
    )

    if is_forwarded:
        broadcast_title = "📢 *Broadcast Running… (Forward Mode)*"

        async def _sender(uid):
            await context.bot.forward_message(
                chat_id=uid,
                from_chat_id=src_chat,
                message_id=src_msg,
            )
    else:
        broadcast_title = "📢 *Broadcast Running…*"

        async def _sender(uid):
            await context.bot.copy_message(
                chat_id=uid,
                from_chat_id=src_chat,
                message_id=src_msg,
            )

    await _bulk_send(
        update, context,
        title=broadcast_title,
        user_ids=user_ids,
        sender=_sender,
        concurrency=25,
    )


async def _run_force_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Powerful Force Start — forces a fresh /start for every tracked user.

    Behaviour:
      • Sends the exact same /start welcome + user reply-keyboard that the
        bot would send if the user themselves typed /start. Nothing else —
        no extra text, no inline buttons.
      • Targets EVERY user in the DB, including users who have only ever
        clicked a user-panel button (auto-tracked) and users who have never
        used /start themselves.
      • Even attempts users who previously blocked the bot — with extra
        retries — so the moment a user un-blocks, the next attempt succeeds.
      • Concurrent dispatch with categorised live progress and final report.

    Telegram-imposed limit (cannot be bypassed by any bot, anywhere):
      A bot can ONLY message users that have at least one prior chat with
      the bot. Users who have never opened a chat with this bot at all are
      unreachable until they themselves tap /start once. Blocked users stay
      Forbidden until they un-block. We attempt and report both honestly."""
    # Build the exact /start view that show_main_menu sends
    start_text = (
        "🤖 *Welcome to Number Bot!*\n\n"
        "Stay with us, I hope you can learn something good. "
        "Join the live regularly. "
        "Join all my channels and groups.\n\n"
        "‍*Bot Owner:* @limonff143\n\n"
        "*Available Options:*\n"
        "📞 Get Number      — Get phone numbers by country\n"
        "🌍 Available Country — View available numbers statistics\n\n"
        "Choose an option below:"
    )
    user_kb = get_user_keyboard()

    user_ids = await run_db(_get_all_users)

    async def _sender(uid):
        # Make sure the user is in our DB (idempotent — same as the real /start)
        try:
            await run_db(_add_user, uid, None, None, None, None)
        except Exception:
            pass
        # Send only the /start welcome with the user reply keyboard — no
        # inline buttons, no extra messages, no links.
        await context.bot.send_message(
            chat_id=uid,
            text=start_text,
            parse_mode='Markdown',
            reply_markup=user_kb,
            disable_web_page_preview=True,
        )

    await _bulk_send(
        update, context,
        title="🌟 *Force Start Running…*",
        user_ids=user_ids,
        sender=_sender,
        concurrency=25,
        # Extra retry attempts — gives blocked users that just un-blocked a
        # better chance to receive the /start instantly.
        max_attempts=5,
        retry_blocked=True,
    )


# ── Latest Message helpers ─────────────────────────────────────────────────────

def _safe_inline(s: str) -> str:
    """Remove backticks from a value so it is safe inside single-backtick inline code."""
    return str(s).replace('`', "'")


def _format_panel_latest(rec: dict, pname: str = "") -> str:
    header   = f"*{_md_escape(pname)} — Latest Message*\n\n" if pname else "✉️ *Latest Message from SMS CDR Stats*\n\n"
    msg_body = rec.get('message') or '—'
    all_otps = _extract_all_otps(msg_body) if msg_body != '—' else (rec.get('otp') or '—')
    dt_val   = _safe_inline(rec.get('datetime') or rec.get('msg_timestamp') or rec.get('received_at') or '—')
    country  = _safe_inline(rec.get('country') or '—')
    number   = _safe_inline(rec.get('number') or '—')
    website  = _safe_inline(rec.get('website') or rec.get('website_name') or '—')
    otp_val  = _safe_inline(all_otps)
    return (
        f"{header}"
        f"Date/Time: `{dt_val}`\n"
        f"🌍 Country: `{country}`\n"
        f"📱 Number: `+{number}`\n"
        f"🌐 Service: `{website}`\n"
        f"OTP: `{otp_val}`\n\n"
        f"💬 Full Message :\n```\n{msg_body}\n```"
    )






# ── Show helpers ──────────────────────────────────────────────────────────────

async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = (
        "🤖 *Welcome to Number Bot!*\n\n"
        "Stay with us, I hope you can learn something good. "
        "Join the live regularly. "
        "Join all my channels and groups.\n\n"
        "‍*Bot Owner:* @limonff143\n\n"
        "*Available Options:*\n"
        "📞 Get Number      — Get phone numbers by country\n"
        "🌍 Available Country — View available numbers statistics\n\n"
        "Choose an option below:"
    )
    if update.callback_query:
        await update.callback_query.answer()
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=msg, parse_mode='Markdown',
            reply_markup=get_user_keyboard(),
        )
    else:
        await update.message.reply_text(msg, parse_mode='Markdown',
                                        reply_markup=get_user_keyboard())


async def _check_user_channels(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Check if user has joined all required channels.
    Returns True if verified (all joined or no channels configured).
    Sends a join prompt and returns False if any channel is missing.
    """
    channels = await run_db(_get_required_channels)
    if not channels:
        return True

    user_id = update.effective_user.id
    not_joined = []
    for ch in channels:
        ch_id = ch.get('id', '')
        if not ch_id:
            continue
        try:
            member = await context.bot.get_chat_member(chat_id=ch_id, user_id=user_id)
            if member.status in ('left', 'kicked', 'banned'):
                not_joined.append(ch)
        except Exception:
            not_joined.append(ch)

    if not not_joined:
        return True

    # Build join buttons
    buttons = [
        [InlineKeyboardButton(f"📢 {ch['name']} তে Join করুন", url=ch['url'], api_kwargs={"style": "primary"})]
        for ch in not_joined
    ]
    markup = InlineKeyboardMarkup(buttons)
    msg = (
        "⛔ *User Panel ব্যবহার করতে হলে নিচের সব Channel এ Join করতে হবে।*\n\n"
        "Join করার পর আবার বাটন চাপুন।"
    )
    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.message.reply_text(
            msg, parse_mode='Markdown', reply_markup=markup)
    else:
        await update.message.reply_text(
            msg, parse_mode='Markdown', reply_markup=markup)
    return False


async def show_countries(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # ── Required channels verification ────────────────────────────────────────
    if not await _check_user_channels(update, context):
        return

    # ── If services exist, show service list first ─────────────────────────────
    services = await run_db(_get_services)
    if services:
        svc_emojis = await run_db(_get_all_service_emojis)
        markup = services_inline_keyboard(services, emoji_overrides=svc_emojis)
        txt = "*Select a Service*\n\nChoose a service to see available countries:"
        if update.callback_query:
            await update.callback_query.edit_message_text(
                txt, reply_markup=markup, parse_mode='Markdown')
        else:
            await update.message.reply_text(
                txt, reply_markup=markup, parse_mode='Markdown')
        return

    # ── No services configured — show all countries directly (legacy) ──────────
    countries = await run_db(_get_countries)
    if not countries:
        txt = "❌ No countries available at the moment."
        if update.callback_query:
            await update.callback_query.edit_message_text(txt)
        else:
            await update.message.reply_text(txt)
        return

    counts = await run_db(_get_all_country_counts)
    data = [
        (row[0], row[1], counts.get(row[0], (0, 0))[1])
        for row in countries
    ]

    markup = countries_inline_keyboard(data)
    if not markup:
        txt = "❌ No numbers available at the moment."
        if update.callback_query:
            await update.callback_query.edit_message_text(txt)
        else:
            await update.message.reply_text(txt)
        return

    txt = "*Available Countries*\n\nSelect a country to get numbers:"
    if update.callback_query:
        await update.callback_query.edit_message_text(
            txt, reply_markup=markup, parse_mode='Markdown')
    else:
        await update.message.reply_text(
            txt, reply_markup=markup, parse_mode='Markdown')


async def show_countries_for_service(update: Update, context: ContextTypes.DEFAULT_TYPE, service_name: str):
    country_ids = await run_db(_get_countries_for_service, service_name)
    if not country_ids:
        txt = f"❌ No countries have been added to *{service_name}* yet."
        if update.callback_query:
            await update.callback_query.edit_message_text(txt, parse_mode='Markdown')
        else:
            await update.message.reply_text(txt, parse_mode='Markdown')
        return

    countries = await run_db(_get_countries)
    counts    = await run_db(_get_all_country_counts)

    data = []
    for cid, cname in countries:
        if cid not in country_ids:
            continue
        avail = counts.get(cid, (0, 0))[1]
        if avail <= 0:
            continue
        data.append((cid, cname, avail))

    markup = countries_inline_keyboard(data, back_to_services=True)
    if not markup:
        txt = f"❌ No numbers are currently available for *{service_name}*."
        if update.callback_query:
            await update.callback_query.edit_message_text(txt, parse_mode='Markdown')
        else:
            await update.message.reply_text(txt, parse_mode='Markdown')
        return

    txt = f"*{service_name}* — Select a Country:"
    if update.callback_query:
        await update.callback_query.edit_message_text(
            txt, reply_markup=markup, parse_mode='Markdown')
    else:
        await update.message.reply_text(
            txt, reply_markup=markup, parse_mode='Markdown')


async def show_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # ── Required channels verification ────────────────────────────────────────
    if not await _check_user_channels(update, context):
        return

    countries  = await run_db(_get_countries)
    counts     = await run_db(_get_all_country_counts)
    bonuses    = await run_db(_get_all_country_otp_bonuses)
    global_cfg = await run_db(_get_otp_bonus_settings)
    global_bonus = global_cfg.get('amount', 0.0)

    active = [(cid, cname) for cid, cname in countries if counts.get(cid, (0, 0))[0] > 0]

    if not active:
        msg = "*No countries available yet.*"
    else:
        lines = [
            f"*Total Countries: {len(active)}*",
            f"*{'─' * 20}*",
        ]
        for cid, cname in active:
            total, avail = counts.get(cid, (0, 0))
            bonus_val = bonuses.get(cid, global_bonus)
            _flag = _get_flag_for_country(cname)
            _prefix = f"{_flag} " if _flag else ""
            lines.append(
                f"*Country : {_prefix}{cname}*\n"
                f"*Numbers : {avail}*\n"
                f"*OTP Bonus: {bonus_val:.2f} ৳ BDT*"
            )
            lines.append(f"*{'─' * 20}*")
        msg = "\n".join(lines)

    refresh_markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 Refresh", callback_data="refresh_stats", api_kwargs={"style": "primary"})]
    ])

    if update.callback_query:
        await update.callback_query.edit_message_text(msg, parse_mode='Markdown', reply_markup=refresh_markup)
    else:
        await update.message.reply_text(msg, parse_mode='Markdown', reply_markup=refresh_markup)


# ── /cancel ───────────────────────────────────────────────────────────────────

async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user:
        return
    username = update.effective_user.username
    user_id  = update.effective_user.id


    # Clear all other awaiting states and return to appropriate menu
    _awaiting_keys = [
        'awaiting_country_name', 'awaiting_numbers_file', 'awaiting_new_country_name',
        'awaiting_add_numbers_country', 'awaiting_reset_country_name',
        'awaiting_delete_country_name', 'awaiting_specific_number_delete',
        'awaiting_notify_time', 'awaiting_new_admin',
        'awaiting_ref_bonus', 'awaiting_min_withdraw',
        'awaiting_otp_bonus_amount',
        'awaiting_balance_user_id', 'awaiting_balance_amount', 'balance_edit_target_id',
        'awaiting_withdraw_method', 'awaiting_withdraw_account', 'awaiting_withdraw_amount',
        'awaiting_number_limit',
        'withdraw_method', 'withdraw_account', 'withdraw_amount',
        'awaiting_reset_users_confirm',
        'awaiting_panel_interval',
        'awaiting_panel_retry',
        'awaiting_session_cleanup_panel',
        'awaiting_retry_login_panel',
        'awaiting_reload_interval_panel',
        'awaiting_reload_interval_seconds',
        'awaiting_cred_panel',
        'awaiting_cred_username',
        'panel_toggle_active',
        'panel_list_active', 'panel_list_multiple_active', 'panel_view_active',
        'panel_list_source', 'panel_category_active', 'panel_list_category',
        'current_country_name', 'delete_target_country_id', 'delete_target_country_name',
        'edit_country_id', 'edit_country_name',
        'add_panel_step',
    ]
    cleared = any(context.user_data.get(k) for k in _awaiting_keys)
    for k in _awaiting_keys:
        context.user_data.pop(k, None)
    # Also clean up the Add Panel wizard global session
    _PANEL_SETUP_SESSIONS.pop(user_id, None)

    if cleared:
        msg = "❌ *Operation cancelled.*"
    else:
        msg = "ℹ️ No operation was in progress to cancel."

    markup = get_admin_keyboard() if _is_admin(username, user_id) else get_user_keyboard()
    await update.message.reply_text(msg, parse_mode='Markdown', reply_markup=markup)


# ── Admin start ───────────────────────────────────────────────────────────────

async def admin_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user:
        return
    username = update.effective_user.username
    user_id  = update.effective_user.id
    if not _is_admin(username, user_id):
        await update.message.reply_text("❌ You are not authorized to use admin commands.")
        return

    await update.message.reply_text(
        "*Admin Panel*\n\nWelcome! Use the buttons below.",
        parse_mode='Markdown',
        reply_markup=get_admin_keyboard()
    )


# ── /userpanel ────────────────────────────────────────────────────────────────

async def handle_userpanel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user:
        return
    username = update.effective_user.username
    user_id  = update.effective_user.id
    if not _is_admin(username, user_id):
        await update.message.reply_text("⛔ This command is for admins only.")
        return
    context.user_data['admin_in_user_panel'] = True
    await update.message.reply_text(
        "*User Panel Mode*\n\n"
        "You can now interact with the bot as a regular user.\n"
        "Send `/start` to return to the Admin Panel.",
        parse_mode='Markdown',
        reply_markup=get_user_keyboard(),
    )


# ── /start ────────────────────────────────────────────────────────────────────

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user:
        return
    user     = update.effective_user
    username = user.username
    user_id  = user.id

    # Check referral code from deep link: /start ref_XXXXXXXX
    referred_by_id = None
    if context.args:
        arg = context.args[0]
        if arg.startswith("ref_"):
            token = arg[4:]
            referrer_id = None
            if token.isdigit():
                referrer_id = int(token)
            else:
                referrer_id = await run_db(_get_user_by_ref_code, token)
            if referrer_id and referrer_id != user_id:
                referred_by_id = referrer_id

    await run_db(_add_user, user.id, user.username, user.first_name, user.last_name, referred_by_id)

    # Credit referral bonus if applicable
    if referred_by_id:
        settings = await run_db(_get_referral_settings)
        if settings['enabled']:
            credited = await run_db(_credit_referral, referred_by_id, user_id, settings['bonus'])
            if credited:
                try:
                    referrer_info = await run_db(_get_user_info_by_id, referred_by_id)
                    name = referrer_info['first_name'] if referrer_info else "friend"
                    await context.bot.send_message(
                        chat_id=referred_by_id,
                        text=(
                            f"🎉 *Referral Bonus Received!*\n\n"
                            f"A new user joined via your referral link.\n"
                            f"💰 Bonus added: *৳ {settings['bonus']:.2f}*\n\n"
                            f"Press '💰 My Balance' to see your total balance."
                        ),
                        parse_mode='Markdown',
                    )
                except Exception:
                    pass

    welcome = (
        "🤖 *Welcome to Number Bot!*\n\n"
        "Stay with us, I hope you can learn something good. "
        "Join the live regularly. "
        "Join all my channels and groups.\n\n"
        "‍*Bot Owner:* @limonff143"
    )
    context.user_data.pop('admin_in_user_panel', None)
    reply_kb = get_admin_keyboard() if _is_admin(username, user_id) else get_user_keyboard()
    await update.message.reply_text(welcome, parse_mode='Markdown')
    await update.message.reply_text("📌 Use the menu below:", reply_markup=reply_kb)


# ── Callback handler ──────────────────────────────────────────────────────────

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user:
        return
    # Auto-track every interaction so users who never typed /start are still
    # counted in the admin User Count and reachable by Broadcast/Force Start.
    await _ensure_user_tracked(update)
    query    = update.callback_query
    data     = query.data
    user_id  = query.from_user.id
    username = query.from_user.username

    # ── Delete Dynamic Panel ────────────────────────────────────────────────────
    if data.startswith("delete_panel:"):
        if not _is_admin(username, user_id):
            await query.answer("❌ Unauthorized", show_alert=True)
            return
        pname = data.split(":", 1)[1]
        await query.answer()
        await query.edit_message_reply_markup(reply_markup=None)
        # Remove from DB
        deleted = await run_db(_delete_dynamic_panel, pname)
        # Remove from ALL_PANEL_LIST
        idx = next((i for i, (n, _) in enumerate(ALL_PANEL_LIST) if n == pname), None)
        if idx is not None:
            ALL_PANEL_LIST.pop(idx)
        # Stop monitor and remove from registry
        mon = DYNAMIC_PANEL_REGISTRY.pop(pname, None)
        if mon is not None:
            try:
                mon.stop()
            except Exception:
                pass
        # Clean up other dicts
        PANEL_CATEGORY.pop(pname, None)
        PANEL_CONFIG_USERNAMES.pop(pname, None)
        context.user_data.pop('panel_view_active', None)
        context.user_data.pop('last_panel_view', None)
        context.user_data.pop('panel_list_active', None)
        context.user_data.pop('panel_list_source', None)
        if deleted:
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=f"✅ *{_md_escape(pname)}* সফলভাবে delete হয়েছে।",
                parse_mode='Markdown',
                reply_markup=get_panel_management_keyboard())
        else:
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text=f"⚠️ Panel DB থেকে সরানো যায়নি, কিন্তু monitor বন্ধ করা হয়েছে।",
                reply_markup=get_panel_management_keyboard())
        return

    if data == "col_noop":
        await query.answer()
        return

    # ── Live Column Config — Retry fetch ──────────────────────────────────────
    if data.startswith("col_retry:"):
        if not _is_admin(username, user_id):
            await query.answer("❌ Unauthorized", show_alert=True)
            return
        await query.answer("🔄 Retrying…")
        pname = data[len("col_retry:"):]
        if not pname:
            await query.edit_message_text("❌ Panel name missing.", parse_mode='Markdown')
            return
        try:
            await query.edit_message_text(
                f"🔄 *{_md_escape(pname)}* — Retrying Column Discovery\n\n"
                "⌛ Panel থেকে live data আনা হচ্ছে… _(1–3 সেকেন্ড)_",
                parse_mode='Markdown',
            )
        except Exception:
            pass
        result = await asyncio.to_thread(discover_panel_columns, pname, ALL_PANEL_LIST)
        if result['status'] != 'ok':
            status = result['status']
            err    = result.get('error', 'Unknown error.')
            if status == 'redirected':
                err_msg = (
                    "❌ *Error: Panel session expired.*\n\n"
                    "Panel টি login page-এ redirect করেছে। "
                    "Bot নিজেই re-login করার চেষ্টা করছে।\n"
                    "_একটু পর আবার চেষ্টা করুন।_"
                )
            elif status == 'timeout':
                err_msg = (
                    "❌ *Error: Panel server unresponsive or timed out (20s).*\n\n"
                    "Panel server টি হয়তো বন্ধ বা অনেক slow। "
                    "Site টি browser থেকে manually check করুন।"
                )
            elif status == 'rate_limited':
                err_msg = (
                    "❌ *Error: Rate limited or blocked by Cloudflare.*\n\n"
                    f"`{_md_escape(err)}`\n\n"
                    "_কয়েক মিনিট অপেক্ষা করে আবার চেষ্টা করুন।_"
                )
            elif status == 'no_columns':
                err_msg = (
                    "❌ *Error: No HTML table headers found.*\n\n"
                    "Panel-এ সফলভাবে connect হয়েছে, কিন্তু `<th>` header পাওয়া যায়নি।\n"
                    "এই panel AJAX/JSON দিয়ে data load করে — HTML discovery কাজ করবে না।"
                )
            elif status in ('login_failed', 'login_error'):
                err_msg = (
                    "❌ *Error: Login failed.*\n\n"
                    f"`{_md_escape(err)}`\n\n"
                    "_Credentials ও panel status চেক করুন।_"
                )
            else:
                err_msg = (
                    "❌ *Error: Could not fetch columns.*\n\n"
                    f"`{_md_escape(err)}`"
                )
            retry_kb = InlineKeyboardMarkup([[
                InlineKeyboardButton("🔄 Retry Fetch", callback_data=f"col_retry:{pname}")
            ]])
            try:
                await query.edit_message_text(err_msg, parse_mode='Markdown', reply_markup=retry_kb)
            except Exception:
                pass
            return
        # Success — show column picker
        cols = result['columns']
        await run_db(_save_panel_discovered_columns, pname, cols)
        context.user_data['col_cfg_panel']   = pname
        context.user_data['col_cfg_columns'] = cols
        cfg      = await run_db(_get_panel_column_config, pname) or {}
        cur_num  = cfg.get('number_col', '?')
        cur_body = cfg.get('body_col',   '?')
        def _cn_r(idx2, cols2):
            try:
                return cols2[int(idx2)]
            except (TypeError, ValueError, IndexError):
                return str(idx2)
        num_lbl  = f"`[{cur_num}]` {_md_escape(_cn_r(cur_num, cols))}"  if cur_num  != '?' else '`—` _(not set)_'
        body_lbl = f"`[{cur_body}]` {_md_escape(_cn_r(cur_body, cols))}" if cur_body != '?' else '`—` _(not set)_'
        col_lines = "\n".join(f"  `[{i}]` {_md_escape(c)}" for i, c in enumerate(cols))
        msg = (
            f"⚙️ *{_md_escape(pname)} — Live Column Config*\n\n"
            f"🔎 *{len(cols)} টি Column পাওয়া গেছে:*\n{col_lines}\n\n"
            f"📱 *Phone Number Column:* {num_lbl}\n"
            f"📨 *SMS Body Column:* {body_lbl}\n\n"
            "নিচের বোতাম দিয়ে সিলেক্ট করুন:"
        )
        rs = 5
        def _br_r(prefix, cols2):
            rows2 = []
            for i in range(0, len(cols2), rs):
                rows2.append([
                    InlineKeyboardButton(f"{prefix}[{j}]", callback_data=f"col_{prefix.strip()}:{j}")
                    for j in range(i, min(i + rs, len(cols2)))
                ])
            return rows2
        inline_kb = InlineKeyboardMarkup(
            [[InlineKeyboardButton("──── 📱 Phone Number Column ────", callback_data="col_noop")]]
            + _br_r("📱", cols)
            + [[InlineKeyboardButton("──── 📨 SMS Body Column ────", callback_data="col_noop")]]
            + _br_r("📨", cols)
        )
        try:
            await query.edit_message_text(msg, parse_mode='Markdown', reply_markup=inline_kb)
        except Exception:
            pass
        return

    # ── Live Column Config — Phone / Body column assignment ────────────────────
    if data.startswith("col_📱:") or data.startswith("col_📨:"):
        if not _is_admin(username, user_id):
            await query.answer("❌ Unauthorized", show_alert=True)
            return
        await query.answer()
        parts = data.split(":", 1)
        col_type = parts[0]   # "col_📱" or "col_📨"
        try:
            idx = int(parts[1])
        except (IndexError, ValueError):
            return

        pname = context.user_data.get('col_cfg_panel')
        cols  = context.user_data.get('col_cfg_columns', [])
        if not pname:
            await query.answer(
                "⚠️ Session expired। আবার ⚙️ Live Column Config বোতাম চাপুন।",
                show_alert=True,
            )
            return

        cfg = await run_db(_get_panel_column_config, pname) or {}
        if col_type == "col_📱":
            number_col = idx
            body_col   = cfg.get('body_col', None)
            label      = "📱 Phone Number"
        else:
            number_col = cfg.get('number_col', None)
            body_col   = idx
            label      = "📨 SMS Body"

        await run_db(_set_panel_column_config, pname, number_col, body_col, cols)
        col_name = cols[idx] if idx < len(cols) else str(idx)
        await query.answer(f"✅ {label} → Index [{idx}] ({col_name}) সেট হয়েছে।", show_alert=True)

        # Refresh the inline message with updated assignments
        cfg = await run_db(_get_panel_column_config, pname) or {}
        cur_num  = cfg.get('number_col', '?')
        cur_body = cfg.get('body_col',   '?')

        def _cn(idx2, cols2):
            try:
                return cols2[int(idx2)]
            except (TypeError, ValueError, IndexError):
                return str(idx2)

        num_lbl  = f"`[{cur_num}]` {_md_escape(_cn(cur_num, cols))}"  if cur_num  != '?' else '`—` _(not set)_'
        body_lbl = f"`[{cur_body}]` {_md_escape(_cn(cur_body, cols))}" if cur_body != '?' else '`—` _(not set)_'
        col_lines = "\n".join(f"  `[{i}]` {_md_escape(c)}" for i, c in enumerate(cols))

        msg = (
            f"⚙️ *{_md_escape(pname)} — Live Column Config*\n\n"
            f"🔎 *{len(cols)} টি Column:*\n{col_lines}\n\n"
            f"📱 *Phone Number Column:* {num_lbl}\n"
            f"📨 *SMS Body Column:* {body_lbl}\n\n"
            "✅ Config save হয়েছে। পরিবর্তন করতে আবার বোতাম চাপুন:"
        )

        row_size = 5
        def _br(prefix, cols2, rs):
            rows_out = []
            for i in range(0, len(cols2), rs):
                rows_out.append([
                    InlineKeyboardButton(f"{prefix}[{j}]", callback_data=f"col_{prefix.strip()}:{j}")
                    for j in range(i, min(i + rs, len(cols2)))
                ])
            return rows_out

        inline_kb = InlineKeyboardMarkup(
            [[InlineKeyboardButton("──── 📱 Phone Number Column ────", callback_data="col_noop")]]
            + _br("📱", cols, row_size)
            + [[InlineKeyboardButton("──── 📨 SMS Body Column ────", callback_data="col_noop")]]
            + _br("📨", cols, row_size)
        )
        try:
            await query.edit_message_text(msg, parse_mode='Markdown', reply_markup=inline_kb)
        except Exception:
            pass
        return

    if data == "cancel_delete_panel":
        await query.answer("🚫 Delete cancel করা হয়েছে।")
        await query.edit_message_reply_markup(reply_markup=None)
        return

    # ── Add Panel: Confirm ─────────────────────────────────────────────────────
    if data.startswith("confirm_add_panel:"):
        if not _is_admin(username, user_id):
            await query.answer("❌ Unauthorized", show_alert=True)
            return
        try:
            orig_uid = int(data.split(":", 1)[1])
        except (ValueError, IndexError):
            orig_uid = user_id
        if user_id != orig_uid:
            await query.answer("❌ এটা আপনার wizard না।", show_alert=True)
            return
        setup = _PANEL_SETUP_SESSIONS.get(user_id, {})
        if not setup or not setup.get('name'):
            await query.answer("❌ Session expired। /cancel দিয়ে আবার শুরু করুন।", show_alert=True)
            return
        await query.answer()
        await query.edit_message_reply_markup(reply_markup=None)
        final_cfg = {k: v for k, v in setup.items()
                     if k not in ('session', 'html', 'final_url', 'captcha_type')}
        final_cfg['captcha_type'] = setup.get('captcha_type', 'none')
        ok = await run_db(_save_dynamic_panel, final_cfg)
        if not ok:
            await context.bot.send_message(
                chat_id=query.message.chat_id,
                text="❌ DB save error। /cancel করুন এবং আবার চেষ্টা করুন।",
                reply_markup=get_panel_management_keyboard())
            context.user_data.pop('add_panel_step', None)
            _PANEL_SETUP_SESSIONS.pop(user_id, None)
            return
        pname = final_cfg['name']
        mon   = create_and_register_dynamic_monitor(final_cfg)
        ALL_PANEL_LIST.append((pname, mon))
        PANEL_CATEGORY[pname]         = 'dynamic'
        PANEL_CONFIG_USERNAMES[pname] = final_cfg.get('username', '')
        try:
            await run_db(_get_panel_by_name, pname)
        except Exception:
            pass
        try:
            existing_sess = setup.get('session')
            if existing_sess:
                mon.session   = existing_sess
                mon.logged_in = True
            mon.start(context.bot)
        except Exception as _e:
            logger.warning(f"[AddPanel] Could not start monitor for {pname}: {_e}")
        context.user_data.pop('add_panel_step', None)
        _PANEL_SETUP_SESSIONS.pop(user_id, None)
        try:
            all_admins = await run_db(_get_all_admins_with_details)
            notif = (
                f"➕ *New Panel Added!*\n\n"
                f"*{_md_escape(pname)}*\n"
                f"Username: `{final_cfg.get('username', '')}`\n"
                f"Login: `{_md_escape(final_cfg.get('login_url', ''))}`\n"
                f"AJAX: `{_md_escape(final_cfg.get('ajax_url', ''))}`\n\n"
                "✅ Monitor চালু হয়েছে!"
            )
            for adm in all_admins:
                adm_uid = adm.get('user_id')
                if adm_uid:
                    try:
                        await context.bot.send_message(
                            chat_id=adm_uid, text=notif, parse_mode='Markdown')
                    except Exception:
                        pass
        except Exception:
            pass
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=(
                f"🎉 *{_md_escape(pname)}* panel সফলভাবে যোগ হয়েছে!\n\n"
                "Monitor চালু হয়েছে — SMS আসলেই notify হবে। ✅"
            ),
            parse_mode='Markdown',
            reply_markup=get_panel_management_keyboard())
        return

    # ── Add Panel: Cancel ──────────────────────────────────────────────────────
    if data.startswith("cancel_add_panel:"):
        try:
            orig_uid = int(data.split(":", 1)[1])
        except (ValueError, IndexError):
            orig_uid = user_id
        if user_id != orig_uid:
            await query.answer("❌ এটা আপনার wizard না।", show_alert=True)
            return
        await query.answer("🚫 Add Panel cancel করা হয়েছে।")
        await query.edit_message_reply_markup(reply_markup=None)
        context.user_data.pop('add_panel_step', None)
        _PANEL_SETUP_SESSIONS.pop(user_id, None)
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text="❌ Panel add cancel করা হয়েছে।",
            reply_markup=get_panel_management_keyboard())
        return

    # ── Cancel Reload Interval ─────────────────────────────────────────────────
    if data.startswith("cancel_reload_interval:"):
        if not _is_admin(username, user_id):
            await query.answer("❌ Unauthorized", show_alert=True)
            return
        pname = data.split(":", 1)[1]
        context.user_data.pop('awaiting_reload_interval_seconds', None)
        await query.answer("🚫 Cancelled.")
        await query.edit_message_reply_markup(reply_markup=None)
        is_en = bool(await run_db(_is_panel_enabled, pname))
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text="🚫 Cancelled.",
            reply_markup=get_panel_action_keyboard(is_en))
        return

    # ── Cancel Required Channel add/delete ────────────────────────────────────
    if data == "cancel_req_channel":
        if not _is_admin(username, user_id):
            await query.answer("❌ Unauthorized", show_alert=True)
            return
        context.user_data.pop('awaiting_channel_name', None)
        context.user_data.pop('awaiting_channel_link', None)
        context.user_data.pop('pending_channel_name', None)
        context.user_data.pop('awaiting_channel_delete_index', None)
        await query.answer("🚫 Cancelled.")
        await query.edit_message_reply_markup(reply_markup=None)
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text="🚫 Cancelled.",
            reply_markup=get_required_channels_keyboard())
        return

    # ── Cancel Broadcast ───────────────────────────────────────────────────────
    if data == "cancel_broadcast":
        if not _is_admin(username, user_id):
            await query.answer("❌ Unauthorized", show_alert=True)
            return
        context.user_data.pop('awaiting_broadcast_message', None)
        await query.answer("🚫 Broadcast cancelled.")
        await query.edit_message_reply_markup(reply_markup=None)
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text="🚫 Broadcast cancelled.",
            reply_markup=get_admin_keyboard())
        return

    # ── Refresh Stats ──────────────────────────────────────────────────────────
    if data == "refresh_stats":
        await query.answer("🔄 Refreshing...")
        await show_stats(update, context)
        return

    # ── Copy OTP toast ─────────────────────────────────────────────────────────
    if data.startswith("copy_otp:"):
        otp_val = data[len("copy_otp:"):]
        await query.answer(text=otp_val, show_alert=False)
        return

    await query.answer()

    # ── Top Users Refresh ──────────────────────────────────────────────────────
    if data == "top_users_refresh":
        from datetime import timezone, timedelta as _td
        _tz_bd = timezone(_td(hours=6))
        now_str = datetime.now(_tz_bd).strftime("%d %b %Y, %I:%M %p")
        top5 = await run_db(_get_top_users_detailed, 5)
        lines = ["*🥇 Top 5 Users*", ""]
        for i, u in enumerate(top5, 1):
            name = u['display_name'] or f"ID:{u['user_id']}"
            bal  = u.get('balance', 0.0)
            lines.append(f"`{'─'*28}`")
            lines.append(f"`⭐️ #{i}  {name}`")
            lines.append(f"`💎 Uid        : {u['user_id']}`")
            lines.append(f"`✉️ OTP Msgs   : {u['msgs_received']}`")
            lines.append(f"`💰 Balance    : {bal:.2f} ৳`")
            lines.append("")
        lines.append(f"`{'─'*28}`")
        lines.append(f"`⌛ {now_str} (UTC+6)`")
        refresh_markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Refresh", callback_data="top_users_refresh", api_kwargs={"style": "success"})]
        ])
        try:
            await query.edit_message_text(
                "\n".join(lines),
                parse_mode='Markdown',
                reply_markup=refresh_markup,
            )
        except Exception:
            pass
        return

    # ── Admin User Stats Refresh ───────────────────────────────────────────────
    if data == "admin_user_stats_refresh":
        if not _is_admin(username, user_id):
            await query.answer("❌ Unauthorized", show_alert=True)
            return
        from datetime import timezone, timedelta as _td
        _tz_bd = timezone(_td(hours=6))
        now_str = datetime.now(_tz_bd).strftime("%d %b %Y, %I:%M %p")
        top5      = await run_db(_get_top_users_detailed, 5)
        svc_emojis = await run_db(_get_all_service_emojis)
        lines = ["`📈 Top 5 User Stats`", ""]
        for i, u in enumerate(top5, 1):
            name = u['display_name'] or f"ID:{u['user_id']}"
            bal  = u.get('balance', 0.0)
            lines.append(f"`{'─'*28}`")
            lines.append(f"⭐️ #{i}  `{name}`")
            lines.append(f"🆔 UID             : `{u['user_id']}`")
            lines.append(f"📞 Numbers Used   : {u['numbers_used']}")
            lines.append(f"✉️ Msgs Received  : {u['msgs_received']}")
            lines.append(f"Referrals      : {u['referral_count']}")
            lines.append(f"💰 Balance        : {bal:.2f} ৳")
            svc = u.get('service_usage', {})
            if svc:
                for sname, cnt in svc.items():
                    db_eid = svc_emojis.get(sname)
                    if db_eid:
                        plain = _get_service_emoji(sname) or "📱"
                        svc_icon = f'<tg-emoji emoji-id="{db_eid}">{plain}</tg-emoji>'
                    else:
                        svc_icon = _get_service_sticker_html(sname)
                    lines.append(f"   • {svc_icon} {sname}: {cnt}")
            lines.append("")
        lines.append(f"`{'─'*28}`")
        lines.append(f"⌛ {now_str} (UTC+6)")
        refresh_markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("Refresh", callback_data="admin_user_stats_refresh", api_kwargs={"style": "success"})]
        ])
        try:
            await query.edit_message_text(
                "\n".join(lines),
                parse_mode='Markdown',
                reply_markup=refresh_markup,
            )
        except Exception:
            pass
        await query.answer("✅ Updated!")
        return

    # ── Navigation ────────────────────────────────────────────────────────────
    if data == "get_numbers":
        context.user_data.pop('current_service', None)
        await show_countries(update, context)
        return

    if data.startswith("service_"):
        service_name = data[len("service_"):]
        context.user_data['current_service'] = service_name
        await show_countries_for_service(update, context, service_name)
        return

    if data == "view_stats":
        await show_stats(update, context)
        return

    if data == "back_to_main":
        await show_main_menu(update, context)
        return

    # ── Get a number for a country ────────────────────────────────────────────
    if data.startswith("country_") or data.startswith("another_"):
        prefix     = "country_" if data.startswith("country_") else "another_"
        country_id = int(data[len(prefix):])

        # Rate limit only for "Change Number" (another_) button
        if data.startswith("another_"):
            now = time.time()
            # Check if user is currently in cooldown
            if _cn_cooldown.get(user_id, 0) > now:
                remaining = int(_cn_cooldown[user_id] - now) + 1
                await query.answer(
                    f"⌛ Slow down! Wait {remaining} second(s).",
                    show_alert=True
                )
                return
            # Record this press and remove timestamps outside the window
            presses = _cn_timestamps.get(user_id, [])
            presses = [t for t in presses if now - t < _CN_WINDOW]
            presses.append(now)
            _cn_timestamps[user_id] = presses
            # If hit limit, start cooldown
            if len(presses) >= _CN_MAX_HITS:
                _cn_cooldown[user_id] = now + _CN_COOLDOWN
                _cn_timestamps[user_id] = []
                await query.answer(
                    f"⌛ Slow down! Wait {int(_CN_COOLDOWN)} second(s).",
                    show_alert=True
                )
                return

        countries    = await run_db(_get_countries)
        country_name = next((r[1] for r in countries if r[0] == country_id), "Unknown")

        limit   = await run_db(_get_number_limit)
        numbers = await run_db(_get_available_numbers_by_country, country_id, limit)

        if not numbers:
            await query.edit_message_text("❌ No numbers available for this country.")
            return

        for num in numbers:
            await run_db(_assign_number_to_user, user_id, num, country_id)

        otp_link = await run_db(_get_setting, "bot_link_getotp", OTP_GROUP_LINK)
        _cur_svc = context.user_data.get('current_service')
        _back_cb = f"service_{_cur_svc}" if _cur_svc else "get_numbers"
        markup = country_number_keyboard(country_id, otp_link, numbers=numbers, back_callback=_back_cb)

        _cntry_flag = _get_flag_for_country(country_name)
        _cntry_prefix = f"{_cntry_flag} " if _cntry_flag else ""
        await query.edit_message_text(
            f"{_cntry_prefix}*{country_name}*\n\n"
            f"Click a number button below to copy it:\n\n"
            "⌛ Waiting for OTP...",
            reply_markup=markup,
            parse_mode='Markdown',
        )
        return

    # ── User Withdraw callbacks (available to all users) ──────────────────────
    if data.startswith("wd_method_"):
        method_map = {
            "wd_method_binance": "Binance",
            "wd_method_bkash":   "bKash",
            "wd_method_nagad":   "Nagad",
        }
        method = method_map.get(data, "Unknown")
        context.user_data['awaiting_withdraw_method'] = False
        context.user_data['withdraw_method']          = method
        context.user_data['awaiting_withdraw_account']= True
        await query.edit_message_text(
            f"📱 *Enter your {method} number/account*\n\n"
            f"Enter your {method} number or account number:",
            parse_mode='Markdown'
        )
        return

    if data == "wd_cancel":
        context.user_data.pop('awaiting_withdraw_method',  None)
        context.user_data.pop('awaiting_withdraw_account', None)
        context.user_data.pop('awaiting_withdraw_amount',  None)
        context.user_data.pop('withdraw_method',           None)
        context.user_data.pop('withdraw_account',          None)
        context.user_data.pop('withdraw_amount',           None)
        await query.edit_message_text("❌ Withdraw cancelled.")
        return

    if data == "wd_confirm":
        amount  = context.user_data.get('withdraw_amount', 0)
        method  = context.user_data.get('withdraw_method', '')
        account = context.user_data.get('withdraw_account', '')
        if not (amount and method and account):
            await query.answer("❌ Incomplete information.", show_alert=True)
            return
        balance = await run_db(_get_user_balance, user_id)
        if balance < amount:
            await query.edit_message_text("❌ Insufficient balance! Withdraw cancelled.")
            return
        await run_db(_update_user_balance, user_id, -amount)
        req_id = await run_db(_create_withdraw_request, user_id, amount, method, account)
        for key in ('withdraw_amount', 'withdraw_method', 'withdraw_account',
                    'awaiting_withdraw_amount', 'awaiting_withdraw_account'):
            context.user_data.pop(key, None)
        await query.edit_message_text(
            f"✅ *Withdraw Request Submitted Successfully!*\n\n"
            f"Request ID: `#{req_id}`\n"
            f"💰 Amount: *৳ {amount:.2f}*\n"
            f"📱 Method: *{method}*\n"
            f"📞 Account: `{account}`\n\n"
            f"An admin will verify and send the funds. Thank you!",
            parse_mode='Markdown'
        )
        all_users = await run_db(_get_all_users_with_info)
        user_info = await run_db(_get_user_info_by_id, user_id)
        name = f"@{user_info['username']}" if user_info and user_info['username'] else str(user_id)
        markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Approve", callback_data=f"wd_approve_{req_id}"),
             InlineKeyboardButton("❌ Reject",  callback_data=f"wd_reject_{req_id}")],
        ])
        notif_text = (
            f"💸 *New Withdraw Request!*\n\n"
            f"User: *{name}* (`{user_id}`)\n"
            f"💰 Amount: *৳ {amount:.2f}*\n"
            f"📱 Method: *{method}*\n"
            f"📞 Account: `{account}`"
        )
        for uid, uname, *_ in all_users:
            if _is_admin(uname, uid):
                try:
                    await context.bot.send_message(
                        chat_id=uid, text=notif_text,
                        parse_mode='Markdown', reply_markup=markup)
                except Exception:
                    pass
        return

    # ── Admin-only callbacks ──────────────────────────────────────────────────
    if not _is_admin(username, user_id):
        await query.answer("❌ You are not authorized.", show_alert=True)
        return

    # delete_country_completely_<id>
    if data.startswith("delete_country_completely_"):
        cid = int(data.split("_")[-1])
        countries    = await run_db(_get_countries)
        cname        = next((r[1] for r in countries if r[0] == cid), "Unknown")
        nd, cd       = await run_db(_delete_country, cid)
        back_markup  = InlineKeyboardMarkup([[
            InlineKeyboardButton("Back to Delete Menu", callback_data="back_to_delete"),
        ]])
        if cd:
            await query.edit_message_text(
                f"✅ *'{cname}' country and {nd} number(s) deleted!*",
                parse_mode='Markdown', reply_markup=back_markup)
        else:
            await query.edit_message_text(
                f"❌ Failed to delete '{cname}'!",
                reply_markup=back_markup)
        return

    # delete_all_<id>
    if data.startswith("delete_all_"):
        cid      = int(data.split("_")[-1])
        countries = await run_db(_get_countries)
        cname     = next((r[1] for r in countries if r[0] == cid), "Unknown")
        deleted   = await run_db(_delete_all_numbers_from_country, cid)
        await query.edit_message_text(
            f"✅ *{deleted} number(s) deleted from {cname}!*",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("Back to Delete Menu", callback_data="back_to_delete"),
            ]]))
        return

    # delete_country_<id>  (show options menu)
    if data.startswith("delete_country_"):
        cid      = int(data.split("_")[-1])
        countries = await run_db(_get_countries)
        cname     = next((r[1] for r in countries if r[0] == cid), "Unknown")
        total, avail = await run_db(_get_numbers_count_by_country, cid)
        markup = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"🗑️ Delete all numbers ({total})",
                                  callback_data=f"delete_all_{cid}")],
            [InlineKeyboardButton("🌟 Delete entire country",
                                  callback_data=f"delete_country_completely_{cid}")],
            [InlineKeyboardButton("✏️ Delete specific number",
                                  callback_data=f"delete_specific_{cid}")],
            [InlineKeyboardButton("Back",
                                  callback_data="back_to_delete")],
        ])
        await query.edit_message_text(
            f"*{cname}*\n\nTotal: {total} numbers | Available: {avail}\n\n"
            "⚠️ *Warning:* This will be permanently deleted!",
            reply_markup=markup, parse_mode='Markdown')
        return

    # add_admin_inline — triggered from Manage Admins inline button
    if data == "add_admin_inline":
        if not _is_admin(username, user_id):
            await query.answer("❌ Unauthorized.", show_alert=True)
            return
        context.user_data['awaiting_new_admin'] = True
        await query.answer()
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=(
                "*Add New Admin*\n\n"
                "নতুন এডমিনের *User ID (UID)* লিখুন:"
            ),
            parse_mode='Markdown',
        )
        return

    # admin_info_<username> — show admin details + confirm remove button
    if data.startswith("admin_info_"):
        if not _is_admin(username, user_id):
            await query.answer("❌ Unauthorized.", show_alert=True)
            return
        aname   = data[len("admin_info_"):]
        admins  = await run_db(_get_all_admins_with_details)
        adm     = next((a for a in admins if a['username'] == aname), None)
        if not adm:
            await query.answer("❌ Admin not found.", show_alert=True)
            return
        fname     = adm.get('first_name') or '—'
        lname     = adm.get('last_name')  or ''
        full_name = f"{fname} {lname}".strip()
        uid_val   = adm.get('user_id')
        added_at  = adm.get('added_at') or '—'
        msg = (
            f"*Admin Details*\n\n"
            f"┣━━━━━━━━━━━━━━━━━━━━━\n"
            f"┃ 💰 Name: *{full_name}*\n"
            f"┃ UID: `{uid_val or '—'}`\n"
            f"┃ Admin since: `{added_at}`\n"
            f"┗━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Remove this admin?"
        )
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🗑️ Yes, Remove", callback_data=f"confirm_remove_admin_{aname}"),
             InlineKeyboardButton("❌ Cancel", callback_data="back_to_admin")],
        ])
        await query.edit_message_text(msg, parse_mode='Markdown', reply_markup=keyboard)
        return

    # confirm_remove_admin_<username>
    if data.startswith("confirm_remove_admin_"):
        if not _is_admin(username, user_id):
            await query.answer("❌ Unauthorized.", show_alert=True)
            return
        admin_name = data[len("confirm_remove_admin_"):]
        ok, msg    = await run_db(_remove_admin, admin_name)
        if ok:
            await query.edit_message_text(
                f"✅ *Admin Removed Successfully!*\n\nAdmin no longer exists.",
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("Back to Admin Panel", callback_data="back_to_admin")
                ]]))
        else:
            await query.edit_message_text(
                f"❌ {msg}",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("Back to Admin Panel", callback_data="back_to_admin")
                ]]))
        return

    # remove_admin_<username> (legacy — kept for safety)
    if data.startswith("remove_admin_"):
        if not _is_admin(username, user_id):
            await query.answer("❌ Unauthorized.", show_alert=True)
            return
        admin_name = data[len("remove_admin_"):]
        ok, msg    = await run_db(_remove_admin, admin_name)
        if ok:
            await query.edit_message_text(
                f"✅ Admin @{admin_name} removed successfully!",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("Back to Admin Panel", callback_data="back_to_admin")
                ]]))
        else:
            await query.edit_message_text(
                f"❌ {msg}",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("Back to Admin Panel", callback_data="back_to_admin")
                ]]))
        return

    # protected_admin_<username>
    if data.startswith("protected_admin_"):
        pname = data[len("protected_admin_"):]
        await query.answer(f"🛡️ {pname} is a protected admin and cannot be removed!",
                           show_alert=True)
        return

    # back_to_delete
    if data == "back_to_delete":
        countries = await run_db(_get_countries)
        if not countries:
            await query.edit_message_text("❌ No countries available.")
            return
        counts = await run_db(_get_all_country_counts)
        keyboard = []
        for row in countries:
            cid, cname = row[0], row[1]
            total, _   = counts.get(cid, (0, 0))
            keyboard.append([InlineKeyboardButton(
                f"🗑️ {cname} ({total})", callback_data=f"delete_country_{cid}")])
        keyboard.append([InlineKeyboardButton(
            "Back to Admin Panel", callback_data="back_to_admin")])
        await query.edit_message_text(
            "*Delete Numbers/Countries*\n\nSelect a country:",
            reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        return

    # back_to_admin
    if data == "back_to_admin":
        await query.answer()
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text="*Admin Panel*",
            parse_mode='Markdown',
            reply_markup=get_admin_keyboard(),
        )
        return

    # ── Referral callbacks ─────────────────────────────────────────────────────

    if data == "ref_toggle":
        if not _is_admin(username, user_id):
            await query.answer("❌ Unauthorized.", show_alert=True)
            return
        settings = await run_db(_get_referral_settings)
        new_state = not settings['enabled']
        await run_db(_toggle_referral, new_state)
        status = "✅ Active" if new_state else "❌ Inactive"
        settings2 = await run_db(_get_referral_settings)
        min_wd2   = await run_db(_get_min_withdraw)
        pending2  = await run_db(_get_pending_withdraws)
        markup = InlineKeyboardMarkup([
            [InlineKeyboardButton(
                "✅ Enable" if not new_state else "❌ Disable",
                callback_data="ref_toggle"
            )],
            [InlineKeyboardButton("💰 Change Bonus Amount",      callback_data="ref_set_bonus")],
            [InlineKeyboardButton("📤 Set Minimum Withdraw Amount", callback_data="ref_set_min_withdraw")],
            [InlineKeyboardButton("Edit User Balance",   callback_data="ref_edit_balance")],
            [InlineKeyboardButton(f"💸 Pending Withdraws ({len(pending2)})", callback_data="ref_pending_withdraws")],
        ])
        await query.edit_message_text(
            f"🎁 *Referral Settings Updated!*\n\n"
            f"┣━━━━━━━━━━━━━━━━━━━━━\n"
            f"┃ Status: *{status}*\n"
            f"┃ 💰 Bonus per referral: *৳ {settings2['bonus']:.2f}*\n"
            f"┃ 📤 Min Withdraw: *৳ {min_wd2:.2f}*\n"
            f"┗━━━━━━━━━━━━━━━━━━━━━",
            parse_mode='Markdown',
            reply_markup=markup
        )
        return

    if data == "ref_set_bonus":
        if not _is_admin(username, user_id):
            await query.answer("❌ Unauthorized.", show_alert=True)
            return
        context.user_data['awaiting_ref_bonus'] = True
        await query.edit_message_text(
            "💰 *Change Bonus Amount*\n\n"
            "Enter how much bonus to give per referral:\n"
            "_(Example: 10 or 25.50)_",
            parse_mode='Markdown'
        )
        return

    if data == "ref_edit_balance":
        if not _is_admin(username, user_id):
            await query.answer("❌ Unauthorized.", show_alert=True)
            return
        context.user_data['awaiting_balance_user_id'] = True
        await query.edit_message_text(
            "*Edit User Balance*\n\n"
            "Enter the *Telegram User ID* of the user whose balance you want to edit:",
            parse_mode='Markdown'
        )
        return

    if data == "ref_set_min_withdraw":
        if not _is_admin(username, user_id):
            await query.answer("❌ Unauthorized.", show_alert=True)
            return
        context.user_data['awaiting_min_withdraw'] = True
        await query.edit_message_text(
            "📤 *Set Minimum Withdraw Amount*\n\n"
            "Enter the minimum amount a user can withdraw:\n"
            "_(Example: 50 or 100)_",
            parse_mode='Markdown'
        )
        return

    if data == "ref_pending_withdraws":
        if not _is_admin(username, user_id):
            await query.answer("❌ Unauthorized.", show_alert=True)
            return
        pending = await run_db(_get_pending_withdraws)
        if not pending:
            await query.answer("✅ No pending withdrawals.", show_alert=True)
            return
        for req in pending[:5]:
            name = f"@{req['username']}" if req['username'] else req['first_name'] or str(req['user_id'])
            markup = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Approve", callback_data=f"wd_approve_{req['id']}"),
                 InlineKeyboardButton("❌ Reject",  callback_data=f"wd_reject_{req['id']}")],
            ])
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=(
                    f"💸 *Withdraw Request #{req['id']}*\n\n"
                    f"User: *{name}* (`{req['user_id']}`)\n"
                    f"💰 Amount: *৳ {req['amount']:.2f}*\n"
                    f"📱 Method: *{req['method']}*\n"
                    f"📞 Account: `{req['account']}`\n"
                    f"⌛ Time: {req['created_at']}"
                ),
                parse_mode='Markdown',
                reply_markup=markup
            )
        await query.answer()
        return


    if data.startswith("wd_approve_"):
        if not _is_admin(username, user_id):
            await query.answer("❌ Unauthorized.", show_alert=True)
            return
        req_id = int(data.split("_")[-1])
        req = await run_db(_get_withdraw_request_by_id, req_id)
        await run_db(_update_withdraw_status, req_id, 'approved')
        await query.edit_message_text(
            f"✅ *Withdraw #{req_id} Approved!*\n\nApproved by admin.",
            parse_mode='Markdown'
        )
        if req:
            try:
                await context.bot.send_message(
                    chat_id=req['user_id'],
                    text=(
                        f"✅ *Your Withdraw Request #{req_id} has been approved!*\n\n"
                        f"💰 Amount: *৳ {req['amount']:.2f}*\n"
                        f"📱 Method: *{req['method']}*\n"
                        f"📞 Account: `{req['account']}`\n\n"
                        f"An admin will send the funds to your account soon. Thank you!"
                    ),
                    parse_mode='Markdown'
                )
            except Exception:
                pass
        return

    if data.startswith("wd_reject_"):
        if not _is_admin(username, user_id):
            await query.answer("❌ Unauthorized.", show_alert=True)
            return
        req_id = int(data.split("_")[-1])
        req = await run_db(_get_withdraw_request_by_id, req_id)
        await run_db(_update_withdraw_status, req_id, 'rejected')
        if req and req['status'] == 'pending':
            await run_db(_update_user_balance, req['user_id'], req['amount'])
            try:
                await context.bot.send_message(
                    chat_id=req['user_id'],
                    text=(
                        f"❌ *Your Withdraw Request #{req_id} has been rejected!*\n\n"
                        f"💰 ৳ {req['amount']:.2f} has been refunded to your balance."
                    ),
                    parse_mode='Markdown'
                )
            except Exception:
                pass
        await query.edit_message_text(
            f"❌ *Withdraw #{req_id} Rejected!*\n\nRejected by admin. User's balance has been refunded.",
            parse_mode='Markdown'
        )
        return

    # ── OTP Bonus callbacks ────────────────────────────────────────────────────

    if data == "otp_bonus_toggle":
        if not _is_admin(username, user_id):
            await query.answer("❌ Unauthorized.", show_alert=True)
            return
        settings  = await run_db(_get_otp_bonus_settings)
        new_state = not settings['enabled']
        await run_db(_toggle_otp_bonus, new_state)
        settings2 = await run_db(_get_otp_bonus_settings)
        status    = "✅ Active" if new_state else "❌ Inactive"
        markup = InlineKeyboardMarkup([
            [InlineKeyboardButton(
                "✅ Enable" if not new_state else "❌ Disable",
                callback_data="otp_bonus_toggle"
            )],
            [InlineKeyboardButton("💰 Set Bonus Amount per OTP", callback_data="otp_bonus_set_amount")],
            [InlineKeyboardButton("Edit User Balance",     callback_data="ref_edit_balance")],
        ])
        await query.edit_message_text(
            f"⭐ *OTP Bonus Settings Updated!*\n\n"
            f"┣━━━━━━━━━━━━━━━━━━━━━\n"
            f"┃ Status: *{status}*\n"
            f"┃ 💰 Bonus per OTP: *৳ {settings2['amount']:.2f}*\n"
            f"┗━━━━━━━━━━━━━━━━━━━━━",
            parse_mode='Markdown',
            reply_markup=markup
        )
        return

    if data == "otp_bonus_set_amount":
        if not _is_admin(username, user_id):
            await query.answer("❌ Unauthorized.", show_alert=True)
            return
        context.user_data['awaiting_otp_bonus_amount'] = True
        await query.edit_message_text(
            "💰 *Set OTP Bonus Amount*\n\n"
            "Enter how much bonus a user receives per OTP notification:\n"
            "_(Example: 2 or 5.50)_",
            parse_mode='Markdown'
        )
        return

    # reset_country_<id>
    if data.startswith("reset_country_"):
        cid      = int(data.split("_")[-1])
        countries = await run_db(_get_countries)
        cname     = next((r[1] for r in countries if r[0] == cid), "Unknown")
        reset     = await run_db(_reset_country_numbers, cid)
        total, av = await run_db(_get_numbers_count_by_country, cid)
        await query.edit_message_text(
            f"✅ *Reset Successful!*\n\n"
            f"🌍 Country: *{cname}*\n"
            f"🔄 Reset: *{reset}* number(s)\n"
            f"📊 Now Available: *{av}/{total}*\n\n"
            "All used numbers are available again.",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔄 Back to Reset Menu", callback_data="back_to_reset"),
            ]]))
        return

    # reset_all_countries
    if data == "reset_all_countries":
        total_reset = await run_db(_reset_all_numbers)
        await query.edit_message_text(
            f"✅ *Full Reset Successful!*\n\n"
            f"🔄 Total Reset: *{total_reset}* number(s) across all countries\n\n"
            "All used numbers are available again.",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔄 Back to Reset Menu", callback_data="back_to_reset"),
            ]]))
        return

    # back_to_reset — re-show reset countries menu
    if data == "back_to_reset":
        countries = await run_db(_get_countries)
        if not countries:
            await query.edit_message_text("❌ No countries available.")
            return
        counts = await run_db(_get_all_country_counts)
        btns = []
        for row in countries:
            cid, cname   = row[0], row[1]
            total, avail = counts.get(cid, (0, 0))
            used         = total - avail
            btns.append(InlineKeyboardButton(
                f"🔄 {cname} (Used: {used}/{total})",
                callback_data=f"reset_country_{cid}"))
        keyboard = [btns[i:i+2] for i in range(0, len(btns), 2)]
        keyboard.append([InlineKeyboardButton(
            "🔄 Reset ALL Countries", callback_data="reset_all_countries")])
        keyboard.append([InlineKeyboardButton(
            "Back to Admin Panel", callback_data="back_to_admin")])
        await query.edit_message_text(
            "*🔄 𝑹𝒆𝒔𝒆𝒕 𝑵𝒖𝒎𝒃𝒆𝒓*\n\nSelect a country to reset:",
            reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        return

    # cob_sel_<id> — country OTP bonus: select country
    if data.startswith("cob_sel_"):
        if not _is_admin(username, user_id):
            await query.answer("❌ Unauthorized.", show_alert=True)
            return
        cid        = int(data[len("cob_sel_"):])
        countries  = await run_db(_get_countries)
        counts     = await run_db(_get_all_country_counts)
        cname      = next((r[1] for r in countries if r[0] == cid), "Unknown")
        total, _   = counts.get(cid, (0, 0))
        current    = await run_db(_get_country_otp_bonus, cid)
        global_cfg = await run_db(_get_otp_bonus_settings)
        if current is not None:
            status_line = f"⭐ Custom Bonus: *৳ {current:.2f}*"
        else:
            status_line = f"🌐 Global Default: *৳ {global_cfg['amount']:.2f}* _(no custom set)_"
        markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("✏️ Set Bonus Amount", callback_data=f"cob_set_{cid}")],
            [InlineKeyboardButton("🔄 Reset to Global Default",  callback_data=f"cob_rst_{cid}")],
            [InlineKeyboardButton("Back to Country List",   callback_data="cob_list")],
        ])
        await query.edit_message_text(
            f"🌍 *Country OTP Bonus*\n\n"
            f"┣━━━━━━━━━━━━━━━━━━━━━\n"
            f"┃ 🌍 Country: `{cname}`\n"
            f"┃ Total Numbers: *{total}*\n"
            f"┃ {status_line}\n"
            f"┗━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Select what you want to do:",
            parse_mode='Markdown',
            reply_markup=markup
        )
        return

    # cob_set_<id> — set bonus amount for country
    if data.startswith("cob_set_"):
        if not _is_admin(username, user_id):
            await query.answer("❌ Unauthorized.", show_alert=True)
            return
        cid   = int(data[len("cob_set_"):])
        countries = await run_db(_get_countries)
        cname = next((r[1] for r in countries if r[0] == cid), "Unknown")
        context.user_data['awaiting_country_otp_bonus'] = cid
        context.user_data['awaiting_country_otp_name']  = cname
        await query.edit_message_text(
            f"✏️ *`{cname}`* — Set OTP Bonus\n\n"
            f"How much bonus to give when an OTP is received on this country's number?\n"
            f"_(Example: 3 or 5.50)_",
            parse_mode='Markdown'
        )
        return

    # cob_rst_<id> — reset country bonus to global default
    if data.startswith("cob_rst_"):
        if not _is_admin(username, user_id):
            await query.answer("❌ Unauthorized.", show_alert=True)
            return
        cid  = int(data[len("cob_rst_"):])
        countries = await run_db(_get_countries)
        cname = next((r[1] for r in countries if r[0] == cid), "Unknown")
        await run_db(_reset_country_otp_bonus, cid)
        global_cfg = await run_db(_get_otp_bonus_settings)
        await query.edit_message_text(
            f"✅ *`{cname}`* — Bonus Reset!\n\n"
            f"Global Default will now be used.\n"
            f"🌐 Global Default: *৳ {global_cfg['amount']:.2f}*",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("Back to Country List", callback_data="cob_list")
            ]])
        )
        return

    # cob_list — re-show country OTP bonus list
    if data == "cob_list":
        if not _is_admin(username, user_id):
            await query.answer("❌ Unauthorized.", show_alert=True)
            return
        try:
            countries  = await run_db(_get_countries)
            counts     = await run_db(_get_all_country_counts)
            bonuses    = await run_db(_get_all_country_otp_bonuses)
            global_cfg = await run_db(_get_otp_bonus_settings)
            cob_btns   = []
            lines      = []
            for row in countries:
                cid, cname  = row[0], row[1]
                total, _    = counts.get(cid, (0, 0))
                if total == 0:
                    continue
                custom = bonuses.get(cid)
                bonus_str = f"৳{custom:.2f}" if custom is not None else "default"
                _cob_flag = _get_flag_for_country(cname)
                _cob_label = f"{_cob_flag} {cname}" if _cob_flag else cname
                cob_btns.append(InlineKeyboardButton(
                    f"{_cob_label} ({total}) — {bonus_str}",
                    callback_data=f"cob_sel_{cid}"
                ))
                if custom is not None:
                    lines.append(f"  `{cname}` ({total}): ৳ {custom:.2f} (custom)")
                else:
                    lines.append(f"  `{cname}` ({total}): ৳ {global_cfg['amount']:.2f} (default)")
            if not cob_btns:
                await query.edit_message_text(
                    "❌ No numbers have been added to any country yet.",
                    parse_mode='Markdown'
                )
                return
            keyboard = [cob_btns[i:i+2] for i in range(0, len(cob_btns), 2)]
            keyboard.append([InlineKeyboardButton("Close", callback_data="cob_close")])
            summary = "\n".join(lines) if lines else "(no settings)"
            msg = (
                f"🌍 *Country OTP Bonus Settings*\n\n"
                f"┣━━━━━━━━━━━━━━━━━━━━━\n"
                f"┃ 🌐 Global Default: *৳ {global_cfg['amount']:.2f}*\n"
                f"┗━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"{summary}\n\n"
                f"Select a country:"
            )
            await query.edit_message_text(
                msg,
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        except Exception as e:
            logger.error(f"cob_list callback error: {e}", exc_info=True)
            await query.answer(f"❌ Load failed: {e}", show_alert=True)
        return

    # cob_close
    if data == "cob_close":
        await query.edit_message_text("✅ Country OTP Bonus menu closed.")
        return

    # edit_numbers
    if data == "edit_numbers":
        countries = await run_db(_get_countries)
        if not countries:
            await query.edit_message_text("❌ No countries available.")
            return
        counts = await run_db(_get_all_country_counts)
        keyboard = []
        for row in countries:
            cid, cname = row[0], row[1]
            total, _   = counts.get(cid, (0, 0))
            keyboard.append([InlineKeyboardButton(
                f"✏️ {cname} ({total})", callback_data=f"edit_country_{cid}")])
        keyboard.append([InlineKeyboardButton(
            "Back to Admin Panel", callback_data="back_to_admin")])
        await query.edit_message_text(
            "*Edit Numbers*\n\nSelect a country to add more numbers:",
            reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        return

    # edit_country_<id>
    if data.startswith("edit_country_"):
        cid      = int(data.split("_")[-1])
        countries = await run_db(_get_countries)
        cname     = next((r[1] for r in countries if r[0] == cid), "Unknown")
        context.user_data['edit_country_id']   = cid
        context.user_data['edit_country_name'] = cname
        await query.edit_message_text(
            f"*Edit Numbers for {cname}*\n\n"
            "Please send a TXT file with numbers (one number per line):",
            parse_mode='Markdown')
        return

    # delete_specific_<id>
    if data.startswith("delete_specific_"):
        cid      = int(data.split("_")[-1])
        countries = await run_db(_get_countries)
        cname     = next((r[1] for r in countries if r[0] == cid), "Unknown")
        context.user_data['awaiting_specific_number_delete'] = True
        context.user_data['delete_target_country_id']        = cid
        await query.edit_message_text(
            f"*Delete Specific Number from {cname}*\n\n"
            "Send the phone number to delete (without + symbol):",
            parse_mode='Markdown')
        return

    # ── Panel List callbacks ───────────────────────────────────────────────────

    # Helper: build ALL panels keyboard and send it
    async def _send_all_panels_keyboard(chat_id: int):
        all_panels_db = await run_db(_get_panels)
        all_names = [pname for pname, _m in ALL_PANEL_LIST]
        panels = [p for p in all_panels_db if p['name'] in all_names]
        panels.sort(key=lambda p: all_names.index(p['name']) if p['name'] in all_names else 999)
        context.user_data.pop('panel_view_active', None)
        context.user_data.pop('panel_list_multiple_active', None)
        context.user_data.pop('panel_list_source', None)
        context.user_data['panel_list_active'] = True
        panel_btns = [KeyboardButton(_panel_label(p['name'])) for p in panels]
        rows = [panel_btns[i:i+2] for i in range(0, len(panel_btns), 2)]
        rows.append([KeyboardButton("Back to Admin Panel")])
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"📌 *Panel List* ({len(panels)} panels)\n\nSelect a panel:",
            parse_mode='Markdown',
            reply_markup=ReplyKeyboardMarkup(rows, resize_keyboard=True))

    # panel_list — show ALL panels in mobile keyboard
    if data == "panel_list":
        await query.answer()
        await _send_all_panels_keyboard(query.message.chat_id)
        return

    # panel_list_main — show ALL panels in mobile keyboard
    if data == "panel_list_main":
        await query.answer()
        await _send_all_panels_keyboard(query.message.chat_id)
        return

    # panel_list_multiple — show ALL panels in mobile keyboard
    if data == "panel_list_multiple":
        await query.answer()
        await _send_all_panels_keyboard(query.message.chat_id)
        return

    # panel_list_kb — show ALL panels in mobile keyboard
    if data == "panel_list_kb":
        await query.answer()
        await _send_all_panels_keyboard(query.message.chat_id)
        return

    # panel_list_kb_multiple — show ALL panels in mobile keyboard
    if data == "panel_list_kb_multiple":
        await query.answer()
        await _send_all_panels_keyboard(query.message.chat_id)
        return

    # panel_view_<name> — show Latest Message button directly
    if data.startswith("panel_view_"):
        pname  = data[len("panel_view_"):]
        panel  = await run_db(_get_panel_by_name, pname)
        if not panel:
            await query.edit_message_text("❌ Panel not found.")
            return
        keyboard = [
            [InlineKeyboardButton("✉️ Latest Message",
                                  callback_data=f"panel_msgs_{pname}")],
            [InlineKeyboardButton("change user/pass",
                                  callback_data=f"panel_edit_cred_{pname}")],
            [InlineKeyboardButton("Back to Panel List", callback_data="panel_list")],
        ]
        db_user = _resolve_panel_user(panel, pname)
        await query.edit_message_text(
            f"*{_md_escape(pname)}*\n\n"
            f"Username: `{db_user}`\n"
            f"🔗 URL: `{panel['base_url']}`\n\n"
            "Choose an option:",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup(keyboard))
        return

    # panel_edit_cred_<name> — start credential editing flow via inline button
    if data.startswith("panel_edit_cred_"):
        pname = data[len("panel_edit_cred_"):]
        await query.answer()
        context.user_data['awaiting_cred_panel']    = pname
        context.user_data.pop('awaiting_cred_username', None)
        context.user_data['panel_list_active']      = True
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=(
                f"🔑 *Change User/Pass — {_md_escape(pname)}*\n\n"
                f"Send the *new username* for this panel:"
            ),
            parse_mode='Markdown',
            reply_markup=ReplyKeyboardRemove(),
        )
        return

    # panel_msgs_<name> — show SINGLE latest message (no auto-refresh)
    if data.startswith("panel_msgs_"):
        pname = data[len("panel_msgs_"):]
        rec = await asyncio.to_thread(get_panel_latest_today, pname)
        if not rec:
            await query.edit_message_text(
                f"*{_md_escape(pname)} — No SMS Today*\n\n"
                "No SMS messages have been received from this panel today.\n\n"
                "Panel may not be logged in or has no SMS yet.",
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("Back", callback_data=f"panel_view_{pname}")
                ]]))
            return
        text = _format_panel_latest(rec, pname=pname)
        if len(text) > 4000:
            text = text[:3990] + "\n…"
        try:
            await query.edit_message_text(text, parse_mode='Markdown')
        except Exception:
            plain = text.replace('`', "'").replace('*', '').replace('_', '').replace('[', '(')
            if len(plain) > 4000:
                plain = plain[:3990] + "\n…"
            await query.edit_message_text(plain)
        return

    # ── Extra Groups callbacks ─────────────────────────────────────────────────
    if data == "eg_add":
        if not _is_admin(username, user_id):
            await query.answer("❌ Unauthorized.", show_alert=True)
            return
        context.user_data['awaiting_extra_group_id'] = True
        await query.message.reply_text(
            "Enter the Group Chat ID. (Example: -1001234567890)"
        )
        return

    if data == "eg_remove_list":
        if not _is_admin(username, user_id):
            await query.answer("❌ Unauthorized.", show_alert=True)
            return
        groups = await run_db(_get_all_extra_groups)
        if not groups:
            await query.edit_message_text("🗑️ No groups available to remove.")
            return
        kb_rows = []
        for g in groups:
            kb_rows.append([InlineKeyboardButton(
                f"🗑️ {g['title']} ({g['chat_id']})",
                callback_data=f"eg_del_{g['chat_id']}",
            )])
        kb_rows.append([InlineKeyboardButton("Back", callback_data="eg_back")])
        await query.edit_message_text(
            "🗑️ *Remove Group*\n\nWhich group do you want to remove?",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup(kb_rows),
        )
        return

    if data == "eg_back":
        if not _is_admin(username, user_id):
            await query.answer("❌ Unauthorized.", show_alert=True)
            return
        groups = await run_db(_get_all_extra_groups)
        if not groups:
            msg = "📢 *Extra Groups*\n\nNo extra groups have been added yet."
        else:
            lines = []
            for g in groups:
                try:
                    await context.bot.get_chat(g['chat_id'])
                    icon = "🟢"
                except Exception:
                    icon = "🔴"
                lines.append(f"{icon} *{g['title']}*\n   └ `{g['chat_id']}`")
            msg = "📢 *Extra Groups*\n\n" + "\n\n".join(lines)
        eg_kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Add Group",    callback_data="eg_add"),
             InlineKeyboardButton("🗑️ Remove Group", callback_data="eg_remove_list")],
        ])
        await query.edit_message_text(msg, parse_mode='Markdown', reply_markup=eg_kb)
        return

    if data.startswith("eg_del_"):
        if not _is_admin(username, user_id):
            await query.answer("❌ Unauthorized.", show_alert=True)
            return
        cid = data[len("eg_del_"):]
        await run_db(_remove_extra_group, cid)
        await query.edit_message_text("✅ Group removed.")
        return



# ── Admin keyboard button handler ─────────────────────────────────────────────

async def handle_button_click(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user:
        return
    if not update.message or not update.message.text:
        return
    if update.update_id and _is_duplicate_update(update.update_id):
        return
    username = update.effective_user.username
    user_id  = update.effective_user.id
    text     = update.message.text

    if not _is_admin(username, user_id):
        await update.message.reply_text("❌ You are not authorized to use admin commands.")
        return

    # Any admin button press cancels ALL pending input states (except Broadcast itself,
    # which re-sets awaiting_broadcast_message immediately after).
    _ALL_AWAITING_STATES = [
        'awaiting_broadcast_message',
        'awaiting_service_edit', 'awaiting_service_map',
        'awaiting_extra_group_id', 'awaiting_extra_group_remove_id',
        'awaiting_reset_country_name', 'awaiting_add_numbers_country',
        'awaiting_reset_users_confirm', 'awaiting_number_limit',
        'awaiting_notify_time', 'awaiting_country_name', 'awaiting_new_country_name',
        'awaiting_numbers_file', 'awaiting_admin_username',
        'awaiting_otp_bonus_amount', 'awaiting_otp_daily_limit',
        'awaiting_country_otp_bonus_amount',
        'awaiting_edit_bot_link',
        'awaiting_cred_panel', 'awaiting_cred_username',
        'awaiting_retry_login_panel', 'awaiting_reload_interval_panel',
        'awaiting_reload_interval_seconds', 'awaiting_session_cleanup_panel',
        'awaiting_panel_retry', 'panel_toggle_active',
        'awaiting_delete_country_name', 'awaiting_specific_number_delete',
        'awaiting_user_info_id',
        'awaiting_withdraw_method', 'awaiting_withdraw_account', 'awaiting_withdraw_amount',
        'awaiting_channel_name', 'awaiting_channel_link', 'awaiting_channel_delete_index',
    ]
    if text != "📢 Broadcast":
        for _flag in _ALL_AWAITING_STATES:
            context.user_data.pop(_flag, None)

    if text == "🌍 𝑪𝒐𝒖𝒏𝒕𝒓𝒚 𝑴𝒂𝒏𝒂𝒈𝒆𝒓":
        await update.message.reply_text(
            "*🌍 𝑪𝒐𝒖𝒏𝒕𝒓𝒚 𝑴𝒂𝒏𝒂𝒈𝒆𝒓*\n\nChoose an option:",
            parse_mode='Markdown', reply_markup=get_manage_numbers_keyboard())

    elif text == "⚙️ Add Service":
        context.user_data.pop('awaiting_service_map', None)
        context.user_data['awaiting_service_edit'] = True
        services = await run_db(_get_services)
        if services:
            svc_lines = "\n".join(f"• `{s}`" for s in services)
        else:
            svc_lines = "_কোনো service এখনও add করা হয়নি।_"
        await update.message.reply_text(
            "⚙️ *Service Manager*\n\n"
            "*বর্তমান Services:*\n"
            f"{svc_lines}\n\n"
            "➕ নতুন service যোগ করতে নাম লিখুন:\n"
            "_উদাহরণ:_ `Netflix`\n\n"
            "✨ *Animated emoji sticker সহ যোগ করতে:*\n"
            "_emoji sticker টি message এ রেখে পাশে নাম লিখুন_\n"
            "_উদাহরণ:_ 🎬 `Netflix`\n\n"
            "🗑️ Delete করতে লিখুন:\n"
            "_উদাহরণ:_ `delete Netflix`\n\n"
            "_Cancel করতে যেকোনো menu বাটন চাপুন।_",
            parse_mode='Markdown',
            reply_markup=get_manage_numbers_keyboard(),
        )

    elif text == "🗺️ Service Map":
        context.user_data.pop('awaiting_service_edit', None)
        context.user_data['awaiting_service_map'] = True
        services  = await run_db(_get_services)
        countries = await run_db(_get_countries)
        counts    = await run_db(_get_all_country_counts)
        bonuses   = await run_db(_get_all_country_otp_bonuses)
        global_cfg    = await run_db(_get_otp_bonus_settings)
        global_bonus  = global_cfg.get('amount', 0.0)

        svc_part = "\n".join(f"`{s}`" for s in services) if services else "_কোনো service নেই_"

        country_lines = []
        for cid, cname in countries:
            total, _ = counts.get(cid, (0, 0))
            bonus_val = bonuses.get(cid, global_bonus)
            country_lines.append(f"• `{cname}` — {total} numbers — OTP Bonus: ${bonus_val:.2f}")

        ctry_part = "\n".join(country_lines) if country_lines else "_কোনো country নেই_"

        await update.message.reply_text(
            "🗺️ *Service Map*\n\n"
            "📋 *Services:*\n"
            f"{svc_part}\n\n"
            "🌍 *Countries:*\n"
            f"{ctry_part}\n\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            "➕ Map করতে লিখুন:\n"
            "_উদাহরণ:_ `WhatsApp Bangladesh`\n\n"
            "➖ Unmap করতে লিখুন:\n"
            "_উদাহরণ:_ `unmap WhatsApp Bangladesh`\n\n"
            "_Cancel করতে যেকোনো menu বাটন চাপুন।_",
            parse_mode='Markdown',
            reply_markup=get_manage_numbers_keyboard(),
        )

    elif text == "Manage Admins":
        admins = await run_db(_get_all_admins_with_details)
        from config import PROTECTED_ADMINS, PROTECTED_ADMIN_IDS
        rows = []
        for adm in admins:
            db_uname = adm['username'] or ''
            uid      = adm.get('user_id')
            fname    = adm.get('first_name') or 'Unknown'
            is_prot  = db_uname in PROTECTED_ADMINS or (uid and uid in PROTECTED_ADMIN_IDS)
            display  = f"{fname} (UID: {uid})" if uid else fname
            if is_prot:
                rows.append([InlineKeyboardButton(f"🛡️ {display}", callback_data=f"protected_admin_{db_uname}")])
            else:
                rows.append([InlineKeyboardButton(f"❌ {display}", callback_data=f"admin_info_{db_uname}")])
        rows.append([InlineKeyboardButton("➕ Add New Admin", callback_data="add_admin_inline")])
        rows.append([InlineKeyboardButton("◀ Back to Admin Panel", callback_data="back_to_admin")])
        await update.message.reply_text(
            "*Manage Admins*\n\n"
            "নিচে বর্তমান এডমিনদের তালিকা।\n"
            "❌ বাটনে চাপলে রিমুভ করা যাবে।\n"
            "➕ বাটনে চাপলে নতুন এডমিন যোগ করা যাবে।",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup(rows)
        )

    elif text == "📢 Extra Groups":
        for _flag in [
            'awaiting_extra_group_id', 'awaiting_extra_group_remove_id',
        ]:
            context.user_data.pop(_flag, None)
        msg = await _build_extra_groups_overview(context)
        await update.message.reply_text(
            msg, parse_mode='Markdown',
            reply_markup=get_extra_groups_keyboard(),
        )

    elif text == "➕ Add Group":
        context.user_data.pop('awaiting_extra_group_remove_id', None)
        context.user_data['awaiting_extra_group_id'] = True
        await update.message.reply_text(
            "➕ *Add Group*\n\n"
            "Send the *Chat ID* of the group you want to add.\n"
            "_(Example: `-1001234567890`)_\n\n"
            "⚠️ Make sure the bot is already in that group (admin recommended).",
            parse_mode='Markdown',
        )

    elif text == "🗑️ Remove Group":
        context.user_data.pop('awaiting_extra_group_id', None)
        groups = await run_db(_get_all_extra_groups)
        if not groups:
            await update.message.reply_text(
                "🗑️ *Remove Group*\n\nNo extra groups have been added yet.",
                parse_mode='Markdown',
            )
            return
        lines = [f"• `{g['chat_id']}` — {g['title']}" for g in groups]
        context.user_data['awaiting_extra_group_remove_id'] = True
        await update.message.reply_text(
            "🗑️ *Remove Group*\n\n"
            "Send the exact *Chat ID* of the group you want to remove.\n\n"
            + "\n".join(lines),
            parse_mode='Markdown',
        )

    elif text in ("⌛ Retry Interval", "⌛ Retry Login"):
        # ── If called from inside a panel view, retry login for that panel directly
        if context.user_data.get('panel_view_active'):
            pname = context.user_data['panel_view_active']
            match_m = next((m for n, m in ALL_PANEL_LIST if n == pname), None)
            if not match_m:
                await update.message.reply_text("❌ Panel not found.")
                return
            statuses = await run_db(_get_all_panel_statuses)
            s = next((x for x in (statuses or []) if x['panel_name'] == pname), None)
            is_en = bool(await run_db(_is_panel_enabled, pname))
            if s and s.get('logged_in'):
                await update.message.reply_text(
                    f"ℹ️ *{_md_escape(pname)}* is already logged in successfully.",
                    parse_mode='Markdown',
                    reply_markup=get_panel_action_keyboard(is_en))
                return
            await update.message.reply_text(
                f"⌛ Attempting login for *{_md_escape(pname)}*…",
                parse_mode='Markdown')

            def _do_panel_view_retry(monitor):
                try:
                    monitor._manual_only = False
                except Exception:
                    pass
                try:
                    ok = bool(monitor._login())
                except Exception:
                    ok = False
                if ok and hasattr(monitor, '_extract_sesskey'):
                    try:
                        monitor._extract_sesskey()
                    except Exception:
                        pass
                return ok

            try:
                ok = await asyncio.to_thread(_do_panel_view_retry, match_m)
            except Exception:
                ok = False
            try:
                if ok:
                    await run_db(_update_panel_status, pname, True, None, None)
                else:
                    await run_db(_update_panel_status, pname, False, None, 'Manual retry login failed')
            except Exception:
                pass
            is_en = bool(await run_db(_is_panel_enabled, pname))
            if ok:
                try:
                    await _notify_admins_login_success(context.bot, pname)
                except Exception:
                    pass
                await update.message.reply_text(
                    f"✅ *{_md_escape(pname)}* logged in successfully\\.\n"
                    f"_All admins have been notified\\._",
                    parse_mode='MarkdownV2',
                    reply_markup=get_panel_action_keyboard(is_en))
            else:
                await update.message.reply_text(
                    f"❌ *{_md_escape(pname)}* login failed\\.\n"
                    "Try again later\\.",
                    parse_mode='MarkdownV2',
                    reply_markup=get_panel_action_keyboard(is_en))
            return
        # ── Regular Admin Tools flow: show failed panels list ─────────────────
        msg, has_failed = await _build_retry_login_view()
        if has_failed:
            context.user_data['awaiting_retry_login_panel'] = True
        else:
            context.user_data.pop('awaiting_retry_login_panel', None)
        await update.message.reply_text(
            msg,
            parse_mode='Markdown',
            reply_markup=get_admin_tools_keyboard())

    elif text in ("Session Cleanup", "🧹 Session Cleanup"):
        pname = context.user_data.get('panel_view_active')
        if pname:
            # ── Context-aware: clean this specific panel directly ─────────────
            is_en = await run_db(_is_panel_enabled, pname)
            if not is_en:
                await update.message.reply_text(
                    f"🚫 *{_md_escape(pname)}* is currently disabled.",
                    parse_mode='Markdown',
                    reply_markup=get_panel_action_keyboard(False))
                return
            match = next(((n, m) for n, m in ALL_PANEL_LIST if n == pname), None)
            if not match:
                await update.message.reply_text("❌ Panel not found.")
                return
            _, m = match
            await update.message.reply_text(
                f"⌛ Clearing session for *{_md_escape(pname)}*…",
                parse_mode='Markdown')
            monitor = next((mon for n, mon in ALL_PANEL_LIST if n == pname), None)
            if monitor:
                await asyncio.to_thread(_wipe_monitor_session, monitor)
            await run_db(_update_panel_status, pname, False, None,
                         'Session cleared — awaiting manual re-login')
            await _notify_admins_session_cleaned(context.bot, pname)
            await update.message.reply_text(
                f"✅ *{_md_escape(pname)}* session cleaned.\n\n"
                "Use ⌛ *Retry Interval* to log in again.",
                parse_mode='Markdown',
                reply_markup=get_panel_action_keyboard(True))
        else:
            # ── Global: show full list and ask for panel name ─────────────────
            context.user_data['awaiting_session_cleanup_panel'] = True
            msg = await _build_session_cleanup_view()
            await update.message.reply_text(msg, parse_mode='Markdown')

    elif text in ("Panel Toggle", "🔀 Panel Toggle", "🔀 All Panel Toggle"):
        pname = context.user_data.get('panel_view_active')
        if pname:
            # ── Context-aware: toggle this specific panel directly ────────────
            currently_enabled = bool(await run_db(_is_panel_enabled, pname))
            new_state = not currently_enabled
            await run_db(_set_panel_enabled, pname, new_state)
            await _notify_admins_panel_toggled(context.bot, pname, new_state)
            status_str = "✅ Enabled" if new_state else "🚫 Disabled"
            await update.message.reply_text(
                f"📌 *{_md_escape(pname)}*\n\nStatus updated: *{status_str}*",
                parse_mode='Markdown',
                reply_markup=get_panel_action_keyboard(new_state))
        else:
            # ── Global: show full list and ask for panel name ─────────────────
            msg = await _build_panel_toggle_view()
            context.user_data['panel_toggle_active'] = True
            await update.message.reply_text(
                msg,
                parse_mode='Markdown',
                reply_markup=get_admin_tools_keyboard())

    elif text == "🌟 Force Start":
        context.user_data.clear()
        await _run_force_start(update, context)

    elif text == "🔗 Edit Bot Links":
        context.user_data.clear()
        lnk_number   = await run_db(_get_setting, "otp_btn_number",   "")
        lnk_channel  = await run_db(_get_setting, "otp_btn_channel",  "")
        lnk_support  = await run_db(_get_setting, "bot_link_support", "")
        lnk_otpgroup = await run_db(_get_setting, "bot_link_getotp",   OTP_GROUP_LINK)
        msg = (
            "🔗 *Edit Bot Links*\n\n"
            f"📱 *NUMBER:* `{lnk_number or 'Not set'}`\n"
            f"📢 *CHANNEL:* `{lnk_channel or 'Not set'}`\n"
            f"*Support Group:* `{lnk_support or 'Not set'}`\n"
            f"📢 *OTP Group:* `{lnk_otpgroup}`\n\n"
            "Select the link you want to change:"
        )
        await update.message.reply_text(msg, parse_mode='Markdown',
                                        reply_markup=get_edit_bot_links_keyboard())

    elif text in ("📱 NUMBER Link", "📢 CHANNEL Link", "Support Group Link", "📢 OTP Group Link"):
        if not _is_admin(username, user_id):
            await update.message.reply_text("❌ Unauthorized access.")
            return
        which_map = {
            "📱 NUMBER Link":       "number",
            "📢 CHANNEL Link":      "channel_otp",
            "Support Group Link": "support_group",
            "📢 OTP Group Link":    "otp_group",
        }
        which = which_map[text]
        context.user_data['awaiting_edit_bot_link'] = which
        await update.message.reply_text(
            f"🔗 Send the new link for *{text}*:",
            parse_mode='Markdown',
            reply_markup=get_edit_bot_links_keyboard())

    elif text == "Back to Admin Tools":
        context.user_data.pop('panel_toggle_active', None)
        await update.message.reply_text(
            "*Admin Tools*\n\nSelect an option below:",
            parse_mode='Markdown',
            reply_markup=get_admin_tools_keyboard())

    elif text == "📢 Broadcast":
        context.user_data.clear()
        context.user_data['awaiting_broadcast_message'] = True
        user_count = await run_db(_get_user_count)
        await update.message.reply_text(
            f"📢 *Broadcast Message — Powerful Mode*\n\n"
            f"Total users (including auto-tracked): *{user_count}*\n"
            f"🌟 Concurrent send + auto-retry + categorised report\n\n"
            "✏️ *Mode 1 — Normal Broadcast:*\n"
            "Write / send any message (text, photo, video, voice…).\n"
            "Users will receive it *without* a forward header.\n\n"
            "✉️ *Mode 2 — Forward Broadcast:*\n"
            "Forward any message from a channel or group here.\n"
            "Users will receive it *with* the original 'Forwarded from …' header.",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Cancel Broadcast", callback_data="cancel_broadcast")]
            ]))

    elif text == "Settings":
        await update.message.reply_text(
            "*Settings*\n\nSelect a setting below:",
            parse_mode='Markdown',
            reply_markup=get_settings_keyboard())

    elif text == "Admin Tools":
        await update.message.reply_text(
            "*Admin Tools*\n\nSelect an option below:",
            parse_mode='Markdown',
            reply_markup=get_admin_tools_keyboard())

    elif text == "⏰ নোটিফাই টাইম":
        if not _is_admin(username, user_id):
            await update.message.reply_text("❌ Unauthorized access.")
            return
        current = await run_db(_get_notify_window)
        context.user_data.clear()
        context.user_data['awaiting_notify_time'] = True
        await update.message.reply_text(
            f"⏰ *নোটিফাই টাইম সেটিং*\n\n"
            f"বর্তমান সময়: *{current} মিনিট*\n\n"
            f"User নাম্বার নেওয়ার পর এই সময়ের মধ্যে OTP আসলে তাকে পাঠানো হবে।\n"
            f"এই সময়ের পরে OTP শুধু Group এ যাবে।\n\n"
            f"নতুন সময় মিনিটে লিখুন:\n"
            f"_(উদাহরণ: 5, 10, 15, 30)_",
            parse_mode='Markdown',
            reply_markup=get_admin_tools_keyboard())

    elif text == "Back to Admin Panel":
        await update.message.reply_text(
            "*Admin Panel*", parse_mode='Markdown',
            reply_markup=get_admin_keyboard())

    elif text == "📱 𝑨𝒅𝒅 𝑵𝒖𝒎𝒃𝒆𝒓":
        countries = await run_db(_get_countries)
        if not countries:
            await update.message.reply_text(
                "❌ No countries available. Please use *🌐Add 𝑪𝒐𝒖𝒏𝒕𝒓𝒚* first.",
                parse_mode='Markdown',
                reply_markup=get_manage_numbers_keyboard())
            return
        counts = await run_db(_get_all_country_counts)
        lines = ["*📱 Add Number*", "", "*Available countries:*"]
        for cid, cname in countries:
            total, avail = counts.get(cid, (0, 0))
            used = total - avail
            lines.append(f"• `{cname}` — Total: {total} | Used: {used} | Available: {avail}")
        lines.append("")
        lines.append("Send the country name, ISO code (e.g. JP) or phone code (e.g. +81):")
        context.user_data.clear()
        context.user_data['awaiting_add_numbers_country'] = True
        await update.message.reply_text(
            "\n".join(lines), parse_mode='Markdown',
            reply_markup=get_manage_numbers_keyboard())

    elif text == "🌐Add 𝑪𝒐𝒖𝒏𝒕𝒓𝒚":
        countries = await run_db(_get_countries)
        counts    = await run_db(_get_all_country_counts)
        context.user_data.clear()
        context.user_data['awaiting_new_country_name'] = True

        lines = ["*🌐 Country Manager*", ""]
        if not countries:
            lines.append("_No countries added yet._")
            lines.append("")
        else:
            grand_total = grand_avail = 0
            for cid, cname in countries:
                total, avail = counts.get(cid, (0, 0))
                used = total - avail
                grand_total += total
                grand_avail += avail
                lines.append(
                    f"`{cname}`\n"
                    f"  ➕ Added: `{total}`  ✅ Available: `{avail}`  🔴 Used: `{used}`"
                )
                lines.append("`" + "─" * 30 + "`")
            grand_used = grand_total - grand_avail
            lines.append(
                f"\n📌 *Total Countries:* `{len(countries)}`\n"
                f"*Total Numbers:* `{grand_total}`\n"
                f"✅ *Available:* `{grand_avail}`  🔴 *Used:* `{grand_used}`"
            )
            lines.append("")

        lines.append("✏️ Type a country name (e.g. Bangladesh), ISO code (BD) or phone code (+880).")
        lines.append("🗑️ To delete: `delete` [country name]")

        msg = "\n".join(lines)
        if len(msg) > 4000:
            msg = msg[:3990] + "\n`…`"
        await update.message.reply_text(msg, parse_mode='Markdown',
                                        reply_markup=get_manage_numbers_keyboard())

    elif text == "__DISABLED_NUMBER_STATS__":
        stats = await run_db(_get_country_stats)
        if not stats:
            await update.message.reply_text(
                "📊 *Number Stats*\n\n❌ No countries or numbers found.",
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("Back", callback_data="back_to_admin")]]
                )
            )
            return

        grand_total = grand_avail = 0
        lines = []
        for country_name, total, avail in stats:
            grand_total += total
            grand_avail += avail
            used = total - avail
            _stat_flag = _get_flag_for_country(country_name)
            _stat_prefix = f"{_stat_flag} " if _stat_flag else ""
            lines.append(
                f"{_stat_prefix}*{country_name}*\n"
                f"   ➕ Total: `{total}`  |  ✅ Available: `{avail}`  |  🔴 Used: `{used}`"
            )

        summary = (
            f"\n\n━━━━━━━━━━━━━━━━━━\n"
            f"📌 *Total Countries:* `{len(stats)}`\n"
            f"*Grand Total Numbers:* `{grand_total}`\n"
            f"✅ *Grand Total Available:* `{grand_avail}`\n"
            f"🔴 *Grand Total Used:* `{grand_total - grand_avail}`"
        )
        body = "\n\n".join(lines)
        full_text = f"📊 *Number Stats*\n\n{body}{summary}"

        back_kb = InlineKeyboardMarkup(
            [[InlineKeyboardButton("Back", callback_data="back_to_admin")]]
        )

        if len(full_text) <= 4096:
            await update.message.reply_text(full_text, parse_mode='Markdown', reply_markup=back_kb)
        else:
            # Split into safe chunks (≤ 4096 chars each), restore header on each chunk
            header = "📊 *Number Stats*\n\n"
            chunks: list[str] = []
            current = header
            for line in lines:
                addition = line + "\n\n"
                if len(current) + len(addition) > 4096:
                    chunks.append(current.strip())
                    current = header + addition
                else:
                    current += addition
            # Attach summary to last chunk (or as its own chunk if too long)
            if len(current) + len(summary) <= 4096:
                current += summary
                chunks.append(current.strip())
            else:
                chunks.append(current.strip())
                chunks.append(summary.strip())

            for i, chunk in enumerate(chunks):
                kb = back_kb if i == len(chunks) - 1 else None
                await update.message.reply_text(chunk, parse_mode='Markdown', reply_markup=kb)


    elif text == "Users":
        await update.message.reply_text(
            "*Users*\n\nSelect an option below:",
            parse_mode='Markdown',
            reply_markup=get_users_keyboard())

    elif text == "🔍 User Info":
        if not _is_admin(username, user_id):
            return
        context.user_data['awaiting_user_info_id'] = True
        await update.message.reply_text(
            "🔍 *User Info Lookup*\n\n"
            "Enter the Telegram *User ID* (numeric) of the user you want to view:",
            parse_mode='Markdown',
            reply_markup=get_users_keyboard())

    elif text == "📈 User Stats":
        from datetime import timezone, timedelta as _td
        _tz_bd = timezone(_td(hours=6))
        now_bd = datetime.now(_tz_bd)
        now_str = now_bd.strftime("%d %b %Y, %I:%M %p")

        top5       = await run_db(_get_top_users_detailed, 5)
        svc_emojis = await run_db(_get_all_service_emojis)

        lines = [
            "`📈 Top 5 User Stats`",
            "",
        ]
        for i, u in enumerate(top5, 1):
            name = u['display_name'] or f"ID:{u['user_id']}"
            bal  = u.get('balance', 0.0)
            lines.append(f"`{'─'*28}`")
            lines.append(f"⭐️ #{i}  `{name}`")
            lines.append(f"🆔 UID             : `{u['user_id']}`")
            lines.append(f"📞 Numbers Used   : {u['numbers_used']}")
            lines.append(f"✉️ Msgs Received  : {u['msgs_received']}")
            lines.append(f"Referrals      : {u['referral_count']}")
            lines.append(f"💰 Balance        : {bal:.2f} ৳")
            svc = u.get('service_usage', {})
            if svc:
                for sname, cnt in svc.items():
                    db_eid = svc_emojis.get(sname)
                    if db_eid:
                        plain = _get_service_emoji(sname) or "📱"
                        svc_icon = f'<tg-emoji emoji-id="{db_eid}">{plain}</tg-emoji>'
                    else:
                        svc_icon = _get_service_sticker_html(sname)
                    lines.append(f"   • {svc_icon} {sname}: {cnt}")
            lines.append("")

        lines.append(f"`{'─'*28}`")
        lines.append(f"⌛ {now_str} (UTC+6)")

        msg = "\n".join(lines)
        refresh_markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("Refresh", callback_data="admin_user_stats_refresh", api_kwargs={"style": "success"})]
        ])
        await update.message.reply_text(msg, parse_mode='Markdown',
                                        reply_markup=refresh_markup)

        # Send ZIP with full service usage breakdown
        try:
            zip_buf, stamp = await run_db(generate_user_stats_zip)
            await update.message.reply_document(
                document=zip_buf,
                filename=f"user_stats_{stamp}.zip",
                caption=(
                    "📦 *User Stats ZIP*\n\n"
                    "Sheet 1 — *Top Users* (সব ইউজার + service breakdown)\n"
                    "Sheet 2 — *Service Usage* (প্রতিটি ইউজার × প্রতিটি service)"
                ),
                parse_mode='Markdown',
            )
        except Exception as _zip_err:
            await update.message.reply_text(f"⚠️ ZIP generate করতে সমস্যা: {_zip_err}")

    elif text == "User Count":
        user_count = await run_db(_get_user_count)
        now = datetime.now().strftime("%d %B %Y, %I:%M %p")
        msg = (
            f"╔══════════════════════╗\n"
            f"║   USER STATISTICS   ║\n"
            f"╚══════════════════════╝\n\n"
            f"📊 *Total Registered Users*\n"
            f"┣━━━━━━━━━━━━━━━━━━━━━\n"
            f"┃  Total Users: *{user_count}*\n"
            f"┗━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"⌛ *Report Time:* {now}\n\n"
            f"Excel file with all user data is attached below."
        )
        await update.message.reply_text(msg, parse_mode='Markdown',
                                        reply_markup=get_users_keyboard())
        if user_count > 0:
            excel_buffer = await run_db(generate_users_excel)
            await update.message.reply_document(
                document=excel_buffer,
                filename=f"users_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                caption=f"📌 Full user list — {user_count} users total")
        else:
            await update.message.reply_text("⚠️ No users registered yet.",
                                            reply_markup=get_users_keyboard())

    elif text == "🔄 𝑹𝒆𝒔𝒆𝒕 𝑵𝒖𝒎𝒃𝒆𝒓":
        countries = await run_db(_get_countries)
        counts    = await run_db(_get_all_country_counts)
        # Only countries that actually have numbers
        active = [(cid, cname) for cid, cname in countries
                  if counts.get(cid, (0, 0))[0] > 0]
        if not active:
            await update.message.reply_text(
                "❌ No countries with numbers available.",
                reply_markup=get_manage_numbers_keyboard())
            return
        lines = ["*🔄 𝑹𝒆𝒔𝒆𝒕 𝑵𝒖𝒎𝒃𝒆𝒓*", "", "*Available countries:*"]
        for cid, cname in active:
            total, avail = counts.get(cid, (0, 0))
            lines.append(f"• `{cname}` — Total: {total}")
        lines.append("")
        lines.append("যে country এর নাম পাঠাবেন সেই country এর সব নাম্বার বট থেকে DELETE হবে।")
        lines.append("")
        lines.append("⚠️ *এই কাজ পূর্বাবস্থায় ফেরানো যাবে না।*")
        context.user_data.clear()
        context.user_data['awaiting_reset_country_name'] = True
        await update.message.reply_text(
            "\n".join(lines), parse_mode='Markdown',
            reply_markup=get_manage_numbers_keyboard())

    elif text == "𝑮𝒆𝒕 𝑵𝒖𝒎𝒃𝒆𝒓":
        await show_countries(update, context)

    elif text == "Panel management":
        context.user_data.clear()
        await update.message.reply_text(
            "⚙️ *Panel Management*\n\nযে অপশনটি চান সেটি বেছে নিন:",
            parse_mode='Markdown',
            reply_markup=get_panel_management_keyboard()
        )

    elif text == "📋 Panel List":
        context.user_data['panel_list_active'] = True
        context.user_data['panel_page'] = 0
        context.user_data.pop('panel_list_source', None)

        all_panels_db = await run_db(_get_panels)
        all_names     = [pname for pname, _m in ALL_PANEL_LIST]
        panels        = [p for p in all_panels_db if p['name'] in all_names]
        panels.sort(key=lambda p: all_names.index(p['name']) if p['name'] in all_names else 999)
        context.user_data['panel_list_cache'] = [p['name'] for p in panels]

        await update.message.reply_text(
            f"📌 *Panel List* ({len(panels)} panels)\n\nSelect a panel:",
            parse_mode='Markdown',
            reply_markup=_build_panel_page_keyboard(panels, 0)
        )

    elif text == "📦 Added Panels":
        dyn_panels = await run_db(_get_dynamic_panels)
        if not dyn_panels:
            await update.message.reply_text(
                "📦 এখনো কোনো Added Panel নেই।\n\n"
                "➕ Add Panel দিয়ে নতুন panel যোগ করুন।",
                reply_markup=get_panel_management_keyboard())
            return
        context.user_data['panel_list_active'] = True
        context.user_data['panel_list_source'] = 'dynamic'
        context.user_data['panel_page'] = 0
        context.user_data['panel_list_cache'] = [p['name'] for p in dyn_panels]
        btns = [KeyboardButton(_panel_label(p['name'])) for p in dyn_panels]
        rows = [btns[i:i+2] for i in range(0, len(btns), 2)]
        rows.append([KeyboardButton("Back to Panel List")])
        await update.message.reply_text(
            f"📦 *Added Panels* ({len(dyn_panels)} টি)\n\nএকটি panel বেছে নিন:",
            parse_mode='Markdown',
            reply_markup=ReplyKeyboardMarkup(rows, resize_keyboard=True)
        )

    elif text == "🗑️ Delete Panel":
        pname = context.user_data.get('panel_view_active') or context.user_data.get('last_panel_view')
        if not pname:
            await update.message.reply_text("❌ কোনো panel select করা নেই।",
                                            reply_markup=get_panel_management_keyboard())
            return
        dp = await run_db(_get_dynamic_panel, pname)
        if not dp:
            await update.message.reply_text(
                f"❌ *{_md_escape(pname)}* একটি built-in panel — এটি delete করা যাবে না।",
                parse_mode='Markdown',
                reply_markup=get_panel_management_keyboard())
            return
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ হ্যাঁ, Delete করুন", callback_data=f"delete_panel:{pname}"),
            InlineKeyboardButton("❌ Cancel", callback_data="cancel_delete_panel"),
        ]])
        await update.message.reply_text(
            f"⚠️ *নিশ্চিত করুন*\n\n"
            f"Panel *{_md_escape(pname)}* স্থায়ীভাবে delete হয়ে যাবে।\n"
            f"এটি আর SMS monitor করবে না।\n\n"
            f"আপনি কি নিশ্চিত?",
            parse_mode='Markdown',
            reply_markup=kb)

    elif text == "➕ Add Panel":
        # Clear any stale wizard state for this admin
        _PANEL_SETUP_SESSIONS.pop(user_id, None)
        context.user_data['add_panel_step'] = 'name'
        await update.message.reply_text(
            "➕ *Add New Panel — Step 1/5*\n\n"
            "Panel এর একটি নাম দিন:\n"
            "_Example: My SMS Panel_\n\n"
            "❌ Cancel করতে /cancel পাঠান।",
            parse_mode='Markdown',
            reply_markup=ReplyKeyboardRemove(),
        )

    elif text == "🔄 Reload Interval":
        # ── If called from inside a panel view, skip straight to seconds input
        if context.user_data.get('panel_view_active'):
            pname = context.user_data['panel_view_active']
            idx = next((i for i, (n, _) in enumerate(ALL_PANEL_LIST) if n == pname), None)
            cur = await run_db(_get_panel_interval, pname)
            cur_txt = f"{cur}s" if cur else "default"
            context.user_data['awaiting_reload_interval_seconds'] = (pname, idx)
            await update.message.reply_text(
                f"🔄 *Reload Interval — {_md_escape(pname)}*\n\n"
                f"Current interval: `{cur_txt}`\n\n"
                "Send the new interval in *seconds* (whole number), e.g. `30`.\n\n"
                "The panel will reload every N seconds.",
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_reload_interval:{pname}")]
                ]))
            return
        # ── Regular Admin Tools flow: list all panels ─────────────────────────
        context.user_data['awaiting_reload_interval_panel'] = True
        lines = []
        for pname, _m in ALL_PANEL_LIST:
            cur = await run_db(_get_panel_interval, pname)
            interval_txt = f"`{cur}s`" if cur else "`default`"
            lines.append(f"• `{pname}` — {interval_txt}")
        await update.message.reply_text(
            "🔄 *Reload Interval*\n\n"
            "Below are all configured panels. Each panel polls its source "
            "for new SMS messages every N seconds.\n\n"
            + "\n".join(lines)
            + "\n\nSend the *exact name* of the panel whose reload interval you "
            "want to change:",
            parse_mode='Markdown')

    elif text in ("⭐ OTP Bonus Settings", "⭐ OTP Bonus"):
        settings = await run_db(_get_otp_bonus_settings)
        status   = "✅ Active" if settings['enabled'] else "❌ Inactive"
        await update.message.reply_text(
            f"⭐ *OTP Bonus Settings*\n\n"
            f"┣━━━━━━━━━━━━━━━━━━━━━\n"
            f"┃ Status: *{status}*\n"
            f"┃ 💰 Bonus per OTP: *৳ {settings['amount']:.2f}*\n"
            f"┗━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"ℹ️ Select an option from the keyboard below.",
            parse_mode='Markdown',
            reply_markup=get_otp_bonus_keyboard()
        )

    elif text == "OTP Bonus Toggle":
        settings  = await run_db(_get_otp_bonus_settings)
        new_state = not settings['enabled']
        await run_db(_toggle_otp_bonus, new_state)
        settings2 = await run_db(_get_otp_bonus_settings)
        status    = "✅ Active" if new_state else "❌ Inactive"
        await update.message.reply_text(
            f"⭐ *OTP Bonus Updated!*\n\n"
            f"┣━━━━━━━━━━━━━━━━━━━━━\n"
            f"┃ Status: *{status}*\n"
            f"┃ 💰 Bonus per OTP: *৳ {settings2['amount']:.2f}*\n"
            f"┗━━━━━━━━━━━━━━━━━━━━━",
            parse_mode='Markdown',
            reply_markup=get_otp_bonus_keyboard()
        )

    elif text == "💰 Set Bonus Amount":
        context.user_data['awaiting_otp_bonus_amount'] = True
        await update.message.reply_text(
            "💰 *Set OTP Bonus Amount*\n\n"
            "Enter how much bonus a user receives per OTP notification:\n"
            "_(Example: 2 or 5.50)_",
            parse_mode='Markdown',
            reply_markup=get_otp_bonus_keyboard()
        )

    elif text == "Edit Balance":
        context.user_data['awaiting_balance_user_id'] = True
        await update.message.reply_text(
            "*Edit User Balance*\n\n"
            "Enter the *Telegram User ID* of the user whose balance you want to edit:",
            parse_mode='Markdown'
        )

    elif text == "Back to Settings":
        await update.message.reply_text(
            "*Settings*\n\nSelect a setting below:",
            parse_mode='Markdown',
            reply_markup=get_settings_keyboard()
        )

    # ── Required Channels ─────────────────────────────────────────────────────
    elif text == "📢 Required Channels":
        channels = await run_db(_get_required_channels)
        if channels:
            lines = [f"{i+1}. *{c['name']}* — `{c['url']}`" for i, c in enumerate(channels)]
            ch_text = "\n".join(lines)
        else:
            ch_text = "_কোনো channel এখনও add করা হয়নি।_"
        await update.message.reply_text(
            "📢 *Required Channels*\n\n"
            "এই channel গুলোতে join না করলে user panel ব্যবহার করা যাবে না।\n\n"
            f"{ch_text}",
            parse_mode='Markdown',
            reply_markup=get_required_channels_keyboard())

    elif text == "➕ Add Channel":
        context.user_data['awaiting_channel_name'] = True
        await update.message.reply_text(
            "📢 *Add Required Channel*\n\n"
            "Channel এর *নাম* লিখুন:\n"
            "_উদাহরণ:_ `My Official Channel`",
            parse_mode='Markdown',
            reply_markup=ReplyKeyboardRemove())
        await update.message.reply_text(
            "নিচের বাটন দিয়ে cancel করতে পারবেন:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Cancel", callback_data="cancel_req_channel")]
            ]))

    elif text == "🗑️ Delete Channel":
        channels = await run_db(_get_required_channels)
        if not channels:
            await update.message.reply_text(
                "❌ কোনো channel এখনও add করা হয়নি।",
                reply_markup=get_required_channels_keyboard())
            return
        lines = [f"{i+1}. *{c['name']}* — `{c['url']}`" for i, c in enumerate(channels)]
        context.user_data['awaiting_channel_delete_index'] = True
        await update.message.reply_text(
            "🗑️ *Delete Required Channel*\n\n"
            + "\n".join(lines)
            + "\n\nডিলিট করতে *নম্বর* (1, 2, 3...) লিখুন:",
            parse_mode='Markdown',
            reply_markup=ReplyKeyboardRemove())
        await update.message.reply_text(
            "নিচের বাটন দিয়ে cancel করতে পারবেন:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Cancel", callback_data="cancel_req_channel")]
            ]))

    elif text in ("🎁 Referral Settings", "🎁 Referral"):
        settings = await run_db(_get_referral_settings)
        min_wd   = await run_db(_get_min_withdraw)
        status   = "✅ Active" if settings['enabled'] else "❌ Inactive"
        top      = await run_db(_get_top_referrers, 5)
        pending  = await run_db(_get_pending_withdraws)
        top_text = ""
        for i, r in enumerate(top, 1):
            name = f"@{r['username']}" if r['username'] else r['first_name'] or str(r['user_id'])
            top_text += f"  {i}. {name} — {r['count']} referral(s) | ৳ {r['earned']:.2f}\n"
        await update.message.reply_text(
            f"🎁 *Referral Settings*\n\n"
            f"┣━━━━━━━━━━━━━━━━━━━━━\n"
            f"┃ Status: *{status}*\n"
            f"┃ 💰 Bonus per referral: *৳ {settings['bonus']:.2f}*\n"
            f"┃ 📤 Min Withdraw: *৳ {min_wd:.2f}*\n"
            f"┃ 💸 Pending Withdraws: *{len(pending)}*\n"
            f"┗━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🥇 *Top Referrers:*\n{top_text if top_text else '  No one has referred yet.'}\n"
            f"Select an option from the keyboard below.",
            parse_mode='Markdown',
            reply_markup=get_referral_keyboard()
        )

    elif text == "Referral Toggle":
        settings  = await run_db(_get_referral_settings)
        new_state = not settings['enabled']
        await run_db(_toggle_referral, new_state)
        settings2 = await run_db(_get_referral_settings)
        min_wd2   = await run_db(_get_min_withdraw)
        status    = "✅ Active" if new_state else "❌ Inactive"
        await update.message.reply_text(
            f"🎁 *Referral Updated!*\n\n"
            f"┣━━━━━━━━━━━━━━━━━━━━━\n"
            f"┃ Status: *{status}*\n"
            f"┃ 💰 Bonus per referral: *৳ {settings2['bonus']:.2f}*\n"
            f"┃ 📤 Min Withdraw: *৳ {min_wd2:.2f}*\n"
            f"┗━━━━━━━━━━━━━━━━━━━━━",
            parse_mode='Markdown',
            reply_markup=get_referral_keyboard()
        )

    elif text == "💰 Set Referral Bonus":
        context.user_data['awaiting_ref_bonus'] = True
        await update.message.reply_text(
            "💰 *Change Bonus Amount*\n\n"
            "Enter how much bonus to give per referral:\n"
            "_(Example: 10 or 25.50)_",
            parse_mode='Markdown',
            reply_markup=get_referral_keyboard()
        )

    elif text == "📤 Set Min Withdraw":
        context.user_data['awaiting_min_withdraw'] = True
        await update.message.reply_text(
            "📤 *Set Minimum Withdraw Amount*\n\n"
            "Enter the minimum amount a user can withdraw:\n"
            "_(Example: 50 or 100)_",
            parse_mode='Markdown',
            reply_markup=get_referral_keyboard()
        )

    elif text == "💸 Pending Withdraws":
        pending = await run_db(_get_pending_withdraws)
        if not pending:
            await update.message.reply_text(
                "✅ No pending withdrawals.",
                reply_markup=get_referral_keyboard()
            )
        else:
            for req in pending[:5]:
                name   = f"@{req['username']}" if req['username'] else req['first_name'] or str(req['user_id'])
                markup = InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Approve", callback_data=f"wd_approve_{req['id']}"),
                     InlineKeyboardButton("❌ Reject",  callback_data=f"wd_reject_{req['id']}")],
                ])
                await update.message.reply_text(
                    f"💸 *Withdraw Request #{req['id']}*\n\n"
                    f"User: *{name}* (`{req['user_id']}`)\n"
                    f"💰 Amount: *৳ {req['amount']:.2f}*\n"
                    f"📱 Method: *{req['method']}*\n"
                    f"📞 Account: `{req['account']}`\n"
                    f"⌛ Time: {req['created_at']}",
                    parse_mode='Markdown',
                    reply_markup=markup
                )
            if len(pending) > 5:
                await update.message.reply_text(
                    f"ℹ️ Total *{len(pending)}* pending, showing first 5.",
                    parse_mode='Markdown',
                    reply_markup=get_referral_keyboard()
                )

    elif text == "📊 𝑩𝒐𝒕 𝑺𝒕𝒂𝒕𝒊𝒔𝒕𝒊𝒄𝒔":
        stats = await run_db(_get_bot_overview_stats)
        now_str = datetime.now().strftime("%d %b %Y, %I:%M %p")

        # ── Country breakdown ──────────────────────────────────────────────────
        country_lines = []
        for i, cr in enumerate(stats["country_rows"], 1):
            country_lines.append(
                f"*{i}.* *{cr['name']}*\n"
                f"    ▸ *Total Added:* *{cr['total']}*\n"
                f"    ▸ *Available:* *{cr['available']}*"
            )
        countries_block = (
            "\n\n".join(country_lines) if country_lines else "*No countries added yet.*"
        )

        # ── Panel login status ─────────────────────────────────────────────────
        panel_statuses = await run_db(_get_all_panel_statuses)
        statuses_by_name = {s['panel_name']: s for s in (panel_statuses or [])}
        total_panels = len(ALL_PANEL_LIST)
        success_count = 0
        failed_count = 0
        disabled_count = 0
        for pname, _m in ALL_PANEL_LIST:
            is_en = await run_db(_is_panel_enabled, pname)
            if not is_en:
                disabled_count += 1
                continue
            s = statuses_by_name.get(pname)
            if s and s.get('logged_in'):
                success_count += 1
            else:
                failed_count += 1

        msg = (
            f"📊 *Bot Statistics*\n"
            f"⌛ *Updated: {now_str}*\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"

            f"*Total Users: {stats['total_users']}*\n\n"

            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"*Total Panels: {total_panels}*\n"
            f"✅ *Login Success: {success_count}*\n"
            f"❌ *Login Failed: {failed_count}*\n"
            + (f"🚫 *Disabled: {disabled_count}*\n" if disabled_count > 0 else "")
            + f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🌍 *Total Countries: {stats['total_countries']}*\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"{countries_block}\n\n"

            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🎁 *Total Referrals: {stats['total_referrals']}*\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━"
        )

        if len(msg) > 4096:
            msg = msg[:4090] + "\n…"

        await update.message.reply_text(
            msg, parse_mode='Markdown',
            reply_markup=get_admin_keyboard()
        )

    elif text == "Number Limit":
        current = await run_db(_get_number_limit)
        await update.message.reply_text(
            f"*Number Limit Settings*\n\n"
            f"┣━━━━━━━━━━━━━━━━━━━━━\n"
            f"┃ 📊 Current Limit: *{current}* number(s)\n"
            f"┗━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"ℹ️ Enter how many numbers each user gets at a time.\n"
            f"Example: *1* gives 1 number, *3* gives 3 numbers.",
            parse_mode='Markdown',
            reply_markup=get_settings_keyboard()
        )
        context.user_data['awaiting_number_limit'] = True

    elif text == "🌍 Country OTP Bonus":
        try:
            countries  = await run_db(_get_countries)
            counts     = await run_db(_get_all_country_counts)
            bonuses    = await run_db(_get_all_country_otp_bonuses)
            global_cfg = await run_db(_get_otp_bonus_settings)
            cob2_btns  = []
            lines      = []
            for row in countries:
                cid, cname = row[0], row[1]
                total, _   = counts.get(cid, (0, 0))
                if total == 0:
                    continue
                custom    = bonuses.get(cid)
                bonus_str = f"৳{custom:.2f}" if custom is not None else "default"
                _cob2_flag = _get_flag_for_country(cname)
                _cob2_label = f"{_cob2_flag} {cname}" if _cob2_flag else cname
                cob2_btns.append(InlineKeyboardButton(
                    f"{_cob2_label} ({total}) — {bonus_str}",
                    callback_data=f"cob_sel_{cid}"
                ))
                if custom is not None:
                    lines.append(f"  `{cname}` ({total}): ৳ {custom:.2f} (custom)")
                else:
                    lines.append(f"  `{cname}` ({total}): ৳ {global_cfg['amount']:.2f} (default)")
            if not cob2_btns:
                await update.message.reply_text(
                    "❌ No numbers have been added to any country yet.",
                    reply_markup=get_manage_numbers_keyboard()
                )
                return
            keyboard = [cob2_btns[i:i+2] for i in range(0, len(cob2_btns), 2)]
            keyboard.append([InlineKeyboardButton("Close", callback_data="cob_close")])
            summary = "\n".join(lines) if lines else "(no settings)"
            msg = (
                f"🌍 *Country OTP Bonus Settings*\n\n"
                f"┣━━━━━━━━━━━━━━━━━━━━━\n"
                f"┃ 🌐 Global Default: *৳ {global_cfg['amount']:.2f}*\n"
                f"┗━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"{summary}\n\n"
                f"Select a country:"
            )
            await update.message.reply_text(
                msg,
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        except Exception as e:
            logger.error(f"Country OTP Bonus handler error: {e}", exc_info=True)
            await update.message.reply_text(
                f"❌ Failed to load Country OTP Bonus: `{e}`",
                parse_mode='Markdown',
                reply_markup=get_manage_numbers_keyboard()
            )

    elif text == "🗑️ Reset All Users":
        context.user_data['awaiting_reset_users_confirm'] = True
        await update.message.reply_text(
            "⚠️ *Warning — Data Reset!*\n\n"
            "When this operation runs:\n\n"
            "🛍️ *First, a ZIP backup file will be sent* containing:\n"
            "  • All user info and balances\n"
            "  • Referral logs\n"
            "  • Withdraw requests\n"
            "  • OTP bonus logs\n"
            "  • Number assignments & OTP deliveries\n"
            "  • SMS logs (last 5000)\n\n"
            "🗑️ *Then the following data will be deleted:*\n"
            "  • All user balances → reset to 0\n"
            "  • Referral logs\n"
            "  • Withdraw requests\n"
            "  • OTP bonus logs\n"
            "  • Number assignments & OTP deliveries\n"
            "  • SMS message history\n\n"
            "✅ *What will NOT be deleted (unchanged):*\n"
            "  • All user accounts and their info\n"
            "  • Countries, numbers, admins, panels, settings\n\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            "To confirm, type exactly:\n"
            "`YES DELETE`\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            "Type anything else to cancel.",
            parse_mode='Markdown'
        )

    elif text == "Export Users":
        import os as _os, zipfile as _zf, io as _io
        data_dir = _os.path.join(_os.path.dirname(__file__), "data")
        if not _os.path.isdir(data_dir):
            await update.message.reply_text("❌ data ফোল্ডার পাওয়া যায়নি।")
        else:
            buf = _io.BytesIO()
            with _zf.ZipFile(buf, 'w', _zf.ZIP_DEFLATED) as zf:
                for fname in _os.listdir(data_dir):
                    fpath = _os.path.join(data_dir, fname)
                    if _os.path.isfile(fpath):
                        zf.write(fpath, arcname=f"data/{fname}")
            buf.seek(0)
            await update.message.reply_document(
                document=buf,
                filename="bot_data.zip",
                caption="*Bot Data Export*\n\n`data/` ফোল্ডারের সমস্ত ফাইল।",
                parse_mode="Markdown"
            )


# ── User keyboard button handler ──────────────────────────────────────────────

async def handle_user_button_click(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user:
        return
    if not update.message or not update.message.text:
        return
    if update.update_id and _is_duplicate_update(update.update_id):
        return
    # Auto-track every user-panel button click — even if user never /start'd
    await _ensure_user_tracked(update)
    text    = update.message.text
    user_id = update.effective_user.id

    # Any user button press cancels ALL pending input states automatically
    for _flag in [
        'awaiting_withdraw_method', 'awaiting_withdraw_account', 'awaiting_withdraw_amount',
    ]:
        context.user_data.pop(_flag, None)

    if text in ("𝑮𝒆𝒕 𝑵𝒖𝒎𝒃𝒆𝒓", "Get Numbers"):
        await show_countries(update, context)
    elif text == "𝑨𝒗𝒂𝒊𝒍𝒂𝒃𝒍𝒆 𝑪𝒐𝒖𝒏𝒕𝒓𝒚":
        await show_stats(update, context)
    elif text == "Support Group":
        await handle_support_platform(update, context)
    elif text == "𝑴𝒚 𝑩𝒂𝒍𝒂𝒏𝒄𝒆":
        balance      = await run_db(_get_user_balance, user_id)
        ref_count    = await run_db(_get_referral_count, user_id)
        total_earned = await run_db(_get_referral_total_earned, user_id)
        settings     = await run_db(_get_referral_settings)
        min_wd       = await run_db(_get_min_withdraw)
        otp_stats    = await run_db(_get_user_otp_bonus_stats, user_id)
        bot_info     = await context.bot.get_me()
        ref_link     = f"https://t.me/{bot_info.username}?start=ref_{user_id}"
        from telegram import CopyTextButton as _CopyBtn
        ref_markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔗 Copy Referral Link", copy_text=_CopyBtn(text=ref_link), api_kwargs={"style": "success"})]
        ])
        await update.message.reply_text(
            f"💰 *Your Balance*\n\n"
            f"┣━━━━━━━━━━━━━━━━━━━━━\n"
            f"┃ 💵 Current Balance: *৳ {balance:.2f}*\n"
            f"┃ 📤 Min Withdraw: *৳ {min_wd:.2f}*\n"
            f"┗━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🎁 *Referral Bonus*\n"
            f"┣━━━━━━━━━━━━━━━━━━━━━\n"
            f"┃ Total Referrals: *{ref_count}*\n"
            f"┃ 💸 Total Referral Earnings: *৳ {total_earned:.2f}*\n"
            f"┃ 💰 Bonus per Referral: *৳ {settings['bonus']:.2f}*\n"
            f"┗━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"⭐ *OTP Bonus*\n"
            f"┣━━━━━━━━━━━━━━━━━━━━━\n"
            f"┃ 📥 OTP Bonuses Today: *{otp_stats['today_count']}* time(s)\n"
            f"┃ 💵 Today's OTP Earnings: *৳ {otp_stats['today_earned']:.2f}*\n"
            f"┃ 📊 Total OTP Bonuses: *{otp_stats['total_count']}* time(s)\n"
            f"┃ 💰 Total OTP Earnings: *৳ {otp_stats['total_earned']:.2f}*\n"
            f"┗━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🔗 *Your Referral Link:*\n`{ref_link}`\n\n"
            f"Share the link with friends and earn bonuses! 🎉",
            parse_mode='Markdown',
            reply_markup=ref_markup,
        )
    elif text == "𝑾𝒊𝒕𝒉𝒅𝒓𝒂𝒘":
        balance  = await run_db(_get_user_balance, user_id)
        min_wd   = await run_db(_get_min_withdraw)
        if balance < min_wd:
            await update.message.reply_text(
                f"❌ *Withdrawal Not Available*\n\n"
                f"💰 Your current balance: *৳ {balance:.2f}*\n"
                f"📤 Minimum withdraw amount: *৳ {min_wd:.2f}*\n\n"
                f"Refer more friends to increase your balance!",
                parse_mode='Markdown'
            )
            return
        context.user_data['awaiting_withdraw_method'] = True
        markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("💎 Binance", callback_data="wd_method_binance")],
            [InlineKeyboardButton("📱 bKash",   callback_data="wd_method_bkash")],
            [InlineKeyboardButton("📱 Nagad",   callback_data="wd_method_nagad")],
            [InlineKeyboardButton("❌ Cancel",  callback_data="wd_cancel")],
        ])
        await update.message.reply_text(
            f"💸 *Withdraw Request*\n\n"
            f"💰 Your balance: *৳ {balance:.2f}*\n"
            f"📤 Minimum: *৳ {min_wd:.2f}*\n\n"
            f"Select your preferred payment method:",
            parse_mode='Markdown',
            reply_markup=markup
        )

    elif text == "𝑹𝒂𝒏𝒈𝒆 𝑺𝒆𝒂𝒓𝒄𝒉":
        context.user_data['awaiting_range_search'] = True
        await update.message.reply_text(
            "🔍 *Range Search*\n\n"
            "যে Country এর নম্বর চাই তার নাম লিখুন।\n"
            "উদাহরণ: `Venezuela`, `Russia`, `India`",
            parse_mode='Markdown',
            reply_markup=get_user_keyboard(),
        )

    elif text == "𝑻𝒐𝒑 𝑼𝒔𝒆𝒓𝒔":
        from datetime import timezone, timedelta as _td
        _tz_bd = timezone(_td(hours=6))
        now_str = datetime.now(_tz_bd).strftime("%d %b %Y, %I:%M %p")

        my_info   = await run_db(_get_user_info_by_id, user_id)
        my_otp    = await run_db(_get_user_otp_bonus_stats, user_id)
        my_bal    = my_info['balance'] if my_info else 0.0
        my_name   = (f"@{my_info['username']}" if my_info and my_info.get('username')
                     else (my_info.get('first_name') if my_info else str(user_id)))
        my_msgs   = my_otp.get('total_count', 0) if my_otp else 0

        top5 = await run_db(_get_top_users_detailed, 5)

        lines = [
            f"`{'─'*28}`",
            f"`My Stats`",
            f"`Name       : {my_name}`",
            f"`💎 UID        : {user_id}`",
            f"`✉️ OTP Msgs   : {my_msgs}`",
            f"`💰 Balance    : {my_bal:.2f} ৳`",
            "",
            "*🥇 Top 5 Users*",
            "",
        ]
        for i, u in enumerate(top5, 1):
            uname = u['display_name'] or f"ID:{u['user_id']}"
            bal   = u.get('balance', 0.0)
            lines.append(f"`{'─'*28}`")
            lines.append(f"`⭐️ #{i}  {uname}`")
            lines.append(f"`✉️ OTP Msgs   : {u['msgs_received']}`")
            lines.append(f"`💰 Balance    : {bal:.2f} ৳`")
            lines.append("")

        lines.append(f"`{'─'*28}`")
        lines.append(f"`⌛ {now_str} (UTC+6)`")

        refresh_markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Refresh", callback_data="top_users_refresh", api_kwargs={"style": "success"})]
        ])
        await update.message.reply_text(
            "\n".join(lines),
            parse_mode='Markdown',
            reply_markup=refresh_markup,
        )


async def handle_support_platform(update: Update, context: ContextTypes.DEFAULT_TYPE):
    support_link = await run_db(_get_setting, "bot_link_support", "")
    msg = (
        "🌟 *Welcome to Our Support Platform!*\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "💬 Click the button below to join our support group!"
    )
    markup = InlineKeyboardMarkup([[
        InlineKeyboardButton("Join Support Group", url=support_link, api_kwargs={"style": "success", "icon_custom_emoji_id": "6068727156467309816"})
    ]])
    await update.message.reply_text(msg, parse_mode='Markdown', reply_markup=markup)


# ── Document handler (add numbers / edit numbers) ─────────────────────────────

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user:
        return
    await _ensure_user_tracked(update)
    username = update.effective_user.username
    user_id  = update.effective_user.id
    if not _is_admin(username, user_id):
        await update.message.reply_text("❌ Unauthorized access.")
        return

    edit_mode = (context.user_data.get('edit_country_id') is not None
                 and context.user_data.get('edit_country_name'))
    add_mode  = (context.user_data.get('awaiting_numbers_file')
                 and context.user_data.get('current_country_name'))

    if edit_mode:
        try:
            cid   = context.user_data['edit_country_id']
            cname = context.user_data['edit_country_name']
            doc   = await update.message.document.get_file()
            raw   = await doc.download_as_bytearray()
            for enc in ('utf-8', 'utf-16', 'latin-1'):
                try:
                    text_content = bytes(raw).decode(enc)
                    break
                except (UnicodeDecodeError, ValueError):
                    continue
            else:
                text_content = bytes(raw).decode('latin-1', errors='replace')
            nums  = [n.strip() for n in text_content.replace('\r', '').split('\n') if n.strip()]
            if not nums:
                await update.message.reply_text("❌ No valid numbers found in the file.")
                return
            added        = await run_db(_add_numbers_to_country, cid, nums)
            total, avail = await run_db(_get_numbers_count_by_country, cid)
            context.user_data.pop('edit_country_id', None)
            context.user_data.pop('edit_country_name', None)
            await update.message.reply_text(
                f"✅ *{added}* number(s) added to *{cname}*!\n\n"
                f"📊 Total: {total} | Available: {avail}",
                parse_mode='Markdown',
                reply_markup=get_manage_numbers_keyboard())
        except Exception as e:
            logger.error(f"handle_document edit_mode: {e}")
            await update.message.reply_text(f"❌ Error: {e}")

    elif add_mode:
        try:
            cname = context.user_data['current_country_name']
            doc   = await update.message.document.get_file()
            raw   = await doc.download_as_bytearray()
            for enc in ('utf-8', 'utf-16', 'latin-1'):
                try:
                    text_content = bytes(raw).decode(enc)
                    break
                except (UnicodeDecodeError, ValueError):
                    continue
            else:
                text_content = bytes(raw).decode('latin-1', errors='replace')
            nums  = [n.strip() for n in text_content.replace('\r', '').split('\n') if n.strip()]
            if not nums:
                await update.message.reply_text("❌ No valid numbers found in the file.")
                return
            await run_db(_add_country, cname)
            cid = await run_db(_get_country_id_by_name, cname)
            if not cid:
                await update.message.reply_text("❌ Error: Country not found after creation.")
                return
            added        = await run_db(_add_numbers_to_country, cid, nums)
            total, avail = await run_db(_get_numbers_count_by_country, cid)
            context.user_data.pop('awaiting_numbers_file', None)
            context.user_data.pop('current_country_name', None)
            await update.message.reply_text(
                f"✅ *{added}* number(s) added to *{cname}*!\n\n"
                f"📊 Total: {total} | Available: {avail}",
                parse_mode='Markdown',
                reply_markup=get_manage_numbers_keyboard())
        except Exception as e:
            logger.error(f"handle_document add_mode: {e}")
            await update.message.reply_text(f"❌ Error: {e}")
    else:
        await update.message.reply_text(
            "❌ Please press *📱 Add Number* first, enter the country name, then send the file.",
            parse_mode='Markdown')


# ── General text handler ──────────────────────────────────────────────────────

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user:
        return
    if update.update_id and _is_duplicate_update(update.update_id):
        return
    # Auto-track every text interaction so the user appears in admin User Count
    # and in the Broadcast/Force Start audience even without /start.
    await _ensure_user_tracked(update)
    username = update.effective_user.username
    user_id  = update.effective_user.id
    text     = (update.message.text or "").strip()
    if not text:
        return

    # ── Slash-command suggestions when "/" is typed ───────────────────────────
    if text == "/":
        in_user_panel = context.user_data.get('admin_in_user_panel', False)
        if _is_admin(username, user_id) and not in_user_panel:
            # Admin in admin panel — show full admin commands
            await update.message.reply_text(
                "🌟 *Admin Commands*\n\n"
                "`/start` — 🤖 Return to Admin Panel\n"
                "`/userpanel` — Switch to User Panel Mode\n"
                "`/cancel` — ❌ Cancel any ongoing operation\n"
                "`/support` — 💬 Open Support Platform\n\n"
                "_Type any command or click above to use it._",
                parse_mode='Markdown')
        else:
            # Regular user OR admin in user-panel mode — show user commands
            await update.message.reply_text(
                "🌟 *Commands*\n\n"
                "`/start` — 🤖 Go to Main Menu\n"
                "`/cancel` — ❌ Cancel any ongoing operation\n"
                "`/support` — 💬 Open Support Platform\n\n"
                "_Type any command or click above to use it._",
                parse_mode='Markdown')
        return

    # ── Cancel any pending "awaiting input" state when a known menu button
    #    is pressed (so navigation never gets stuck in a stale prompt). ────────
    _MENU_BUTTONS = {
        "Back to Admin Panel", "Back to Panel List",
        "Back", "Back to Settings",
        "Admin Tools", "🌍 𝑪𝒐𝒖𝒏𝒕𝒓𝒚 𝑴𝒂𝒏𝒂𝒈𝒆𝒓", "Manage Admins",
        "Users", "User Count", "📈 User Stats", "🔍 User Info", "𝑮𝒆𝒕 𝑵𝒖𝒎𝒃𝒆𝒓", "Panel management",
        "📋 Panel List", "➕ Add Panel", "📦 Added Panels", "🗑️ Delete Panel",
        "Settings", "⌛ Retry Interval", "⌛ Retry Login", "🧹 Session Cleanup", "Session Cleanup",
        "🔀 Panel Toggle", "🔀 All Panel Toggle", "Panel Toggle", "🔄 Reload Interval",
        "✉️ Latest Message", "📤 Group এ পাঠাও",
        "📢 Broadcast", "🌟 Force Start", "🔗 Edit Bot Links",
        "📱 NUMBER Link", "📢 CHANNEL Link",
        "Back to Admin Tools",
        "Number Limit", "🎁 OTP Bonus", "⭐ OTP Bonus", "📢 Extra Groups", "Export Users",
        "➕ Add Group", "🗑️ Remove Group",
        "🌐Add 𝑪𝒐𝒖𝒏𝒕𝒓𝒚", "📱 𝑨𝒅𝒅 𝑵𝒖𝒎𝒃𝒆𝒓", "🔄 𝑹𝒆𝒔𝒆𝒕 𝑵𝒖𝒎𝒃𝒆𝒓",
        "⚙️ Add Service", "🗺️ Service Map",
        "⚙️ Live Column Config",
        "change user/pass",
        "📊 View Stats", "📊 Login & View Stats", "/start", "/cancel",
        "OTP Bonus Toggle", "💰 Set Bonus Amount",
        "Referral Toggle", "💰 Set Referral Bonus", "📤 Set Min Withdraw",
        "💸 Pending Withdraws", "Edit Balance",
        "📊 𝑩𝒐𝒕 𝑺𝒕𝒂𝒕𝒊𝒔𝒕𝒊𝒄𝒔",
        "📢 Required Channels", "➕ Add Channel", "🗑️ Delete Channel",
    }
    if text in _MENU_BUTTONS:
        for _flag in [
            'awaiting_extra_group_id', 'awaiting_extra_group_remove_id',
            'awaiting_reset_country_name',
            'awaiting_add_numbers_country', 'awaiting_reset_users_confirm',
            'awaiting_notify_time', 'awaiting_number_limit', 'awaiting_country_name',
            'awaiting_new_country_name', 'awaiting_numbers_file',
            'awaiting_admin_username',
            'awaiting_otp_bonus_amount', 'awaiting_otp_daily_limit',
            'awaiting_country_otp_bonus_amount',
            'awaiting_broadcast_message',
            'awaiting_edit_bot_link',
            'awaiting_user_info_id',
            'awaiting_service_edit', 'awaiting_service_map',
            'add_panel_step',
        ]:
            context.user_data.pop(_flag, None)
        context.user_data.pop('current_country_name', None)
        context.user_data.pop('edit_country_id', None)
        context.user_data.pop('edit_country_name', None)
        if text != '➕ Add Panel':
            _PANEL_SETUP_SESSIONS.pop(user_id, None)

    # ── Add Panel wizard ──────────────────────────────────────────────────────
    if context.user_data.get('add_panel_step') and _is_admin(username, user_id):
        step  = context.user_data['add_panel_step']
        setup = _PANEL_SETUP_SESSIONS.setdefault(user_id, {})

        # ── Step 1: receive panel name ────────────────────────────────────────
        if step == 'name':
            pname = text.strip()
            if not pname:
                await update.message.reply_text("❌ নাম দিতে হবে। আবার চেষ্টা করুন:")
                return
            existing = [n for n, _ in ALL_PANEL_LIST]
            if pname in existing:
                await update.message.reply_text(
                    f"❌ *{_md_escape(pname)}* নামে panel ইতিমধ্যে আছে। অন্য নাম দিন:",
                    parse_mode='Markdown')
                return
            setup['name'] = pname
            context.user_data['add_panel_step'] = 'login_url'
            await update.message.reply_text(
                f"✅ Panel name: *{_md_escape(pname)}*\n\n"
                "➕ *Add New Panel — Step 2/5*\n\n"
                "Login page এর URL দিন:\n"
                "_Example: http://123.45.67.89/client/login_\n\n"
                "❌ Cancel করতে /cancel পাঠান।",
                parse_mode='Markdown')
            return

        # ── Step 2: receive login URL, analyze page ───────────────────────────
        if step == 'login_url':
            url = text.strip()
            if not url.startswith('http'):
                await update.message.reply_text("❌ Valid HTTP/HTTPS URL দিন:")
                return
            await update.message.reply_text("🔍 Login page analyze করছি…")
            result = await asyncio.to_thread(_ap_analyze_login_page, url)
            if not result['ok']:
                await update.message.reply_text(
                    f"❌ Page load করা যায়নি:\n`{result.get('error', '')}`\n\n"
                    "আবার URL দিন বা /cancel করুন।",
                    parse_mode='Markdown')
                return
            setup.update({
                'login_url':          url,
                'signin_url':         result['signin_url'],
                'captcha_type':       result['captcha_type'],
                'captcha_field':      result['captcha_field'],
                'captcha_question':   result['captcha_question'],
                'recaptcha_sitekey':  result['recaptcha_sitekey'],
                'session':            result['session'],
                'html':               result['html'],
                'final_url':          result['final_url'],
            })
            # Build full analysis message
            all_inputs = result.get('all_inputs', [])
            field_lines = []
            for inp in all_inputs:
                itype = inp['type']
                iname = inp['name']
                iph   = f" — \"{inp['placeholder']}\"" if inp.get('placeholder') else ''
                ival  = f" = `{inp['value'][:30]}`" if inp.get('value') and itype == 'hidden' else ''
                field_lines.append(f"  • `{iname}` ({itype}){iph}{ival}")
            fields_txt = '\n'.join(field_lines[:15]) if field_lines else '  _No fields detected_'

            cap_type = result['captcha_type']
            if cap_type == 'recaptcha':
                sk = result.get('recaptcha_sitekey', '')
                cap_info = (
                    f"⚠️ *Google reCAPTCHA*\n"
                    f"  Site Key: `{sk[:40] if sk else 'unknown'}`\n"
                    f"  Field: `g-recaptcha-response`\n"
                    f"  → Admin কে manually token দিতে হবে"
                )
            elif cap_type == 'hcaptcha':
                cap_info = (
                    "⚠️ *hCaptcha*\n"
                    "  Field: `h-captcha-response`\n"
                    "  → Admin কে manually token দিতে হবে"
                )
            elif cap_type == 'math':
                cap_info = (
                    f"✅ *Math Captcha* (auto-solvable)\n"
                    f"  Question: `{result.get('captcha_question', '')}`\n"
                    f"  Field: `{result.get('captcha_field', 'capt')}`"
                )
            elif cap_type == 'unknown':
                cap_info = (
                    f"❓ *Unknown Captcha*\n"
                    f"  Field: `{result.get('captcha_field', '')}`\n"
                    f"  → Admin কে manually answer দিতে হবে"
                )
            else:
                cap_info = "✅ No captcha detected"

            page_err = result.get('page_error', '')
            err_line = f"\n⚠️ Page message: `{_md_escape(page_err[:80])}`" if page_err else ''

            await update.message.reply_text(
                f"✅ *Login Page Analysis*\n\n"
                f"🌐 Title: *{_md_escape(result['title'])}*\n"
                f"🔗 Form action: `{_md_escape(result['signin_url'])}`\n"
                f"🔗 Final URL: `{_md_escape(result['final_url'])}`"
                f"{err_line}\n\n"
                f"📝 *Form Fields ({len(field_lines)}):\n*"
                f"{fields_txt}\n\n"
                f"🔒 *Captcha:*\n{cap_info}",
                parse_mode='Markdown')

            context.user_data['add_panel_step'] = 'username'
            await update.message.reply_text(
                "➕ *Add New Panel — Step 3/5*\n\n"
                "Username দিন:\n\n"
                "❌ Cancel করতে /cancel পাঠান।",
                parse_mode='Markdown')
            return

        # ── Step 3: receive username ──────────────────────────────────────────
        if step == 'username':
            setup['username'] = text.strip()
            context.user_data['add_panel_step'] = 'password'
            await update.message.reply_text(
                "➕ *Add New Panel — Step 4/5*\n\n"
                "Password দিন:",
                parse_mode='Markdown')
            return

        # ── Step 4: receive password → attempt login (like existing monitors) ──
        if step == 'password':
            setup['password'] = text.strip()
            cap_type = setup.get('captcha_type', 'none')
            status_msg = {
                'math':     '🔐 Login attempt করছি (math captcha auto-solving…)',
                'recaptcha':'🔐 Login attempt করছি (reCAPTCHA bypass try করছি…)',
                'hcaptcha': '🔐 Login attempt করছি (hCaptcha bypass try করছি…)',
            }.get(cap_type, '🔐 Login attempt করছি…')
            await update.message.reply_text(status_msg)

            # ── Try up to 3 times — exactly like existing panel monitors ─────
            login_result: dict = {'ok': False}
            for _attempt in range(3):
                login_result = await asyncio.to_thread(_ap_attempt_login, setup)
                if login_result['ok']:
                    break
                if login_result.get('error'):
                    break  # Network/server error — no point retrying

            if login_result['ok']:
                setup['session'] = login_result.get('session', setup['session'])
                setup['html']    = login_result.get('html', '')
                cap_solved = login_result.get('captcha_auto_solved', False)
                context.user_data['add_panel_step'] = 'ajax_url'
                await update.message.reply_text(
                    f"✅ Login সফল হয়েছে! 🎉"
                    f"{' (captcha auto-solved ✅)' if cap_solved else ''}\n\n"
                    "➕ *Add New Panel — Step 5/5*\n\n"
                    "SMS monitoring page এর URL দিন:\n"
                    "_Example: http://123.45.67.89/client/SMSCDRStats_\n\n"
                    "এটি সাধারণত Reports বা SMS CDR Stats page এর URL।\n\n"
                    "❌ Cancel করতে /cancel পাঠান।",
                    parse_mode='Markdown')
                return

            # ── Login failed — determine next action ──────────────────────────
            err = login_result.get('error', '')
            if err:
                # Network/server error
                await update.message.reply_text(
                    f"❌ Connection error:\n`{_md_escape(str(err))}`\n\n"
                    "আবার চেষ্টা করুন — password দিন (বা /cancel):",
                    parse_mode='Markdown')
                return  # stay on 'password' step

            if cap_type in ('recaptcha', 'hcaptcha'):
                # reCAPTCHA enforced server-side — offer session cookie method
                context.user_data['add_panel_step'] = 'captcha_manual'
                setup['captcha_manual_mode'] = 'cookie'
                await update.message.reply_text(
                    f"⚠️ *reCAPTCHA bypass সম্ভব হয়নি।*\n\n"
                    f"এই panel এ Google reCAPTCHA enforce করা আছে।\n"
                    f"Browser এ manually login করে session cookie paste করুন:\n\n"
                    f"1️⃣ Browser এ login করুন:\n`{_md_escape(setup.get('login_url', ''))}`\n\n"
                    f"2️⃣ Login হওয়ার পর Chrome/Firefox এ:\n"
                    f"   *F12 → Application → Cookies → {setup.get('login_url','').split('/')[2] if '/' in setup.get('login_url','') else 'site'}*\n\n"
                    f"3️⃣ `PHPSESSID` (বা যেটা আছে) এর value copy করুন\n\n"
                    f"4️⃣ এই format এ paste করুন:\n"
                    f"`PHPSESSID=abcdef123456`\n\n"
                    f"_(একাধিক cookie থাকলে: `name1=val1; name2=val2`)_\n\n"
                    f"❌ Cancel: /cancel",
                    parse_mode='Markdown')
            elif cap_type == 'math':
                # Math solve failed (unusual) — ask manually
                context.user_data['add_panel_step'] = 'captcha_manual'
                setup['captcha_manual_mode'] = 'math'
                q = setup.get('captcha_question', '')
                await update.message.reply_text(
                    f"⚠️ Math captcha auto-solve ব্যর্থ।\n\n"
                    f"Question: `{q if q else 'See login page'}`\n"
                    f"Field: `{setup.get('captcha_field', 'capt')}`\n\n"
                    "Captcha এর answer দিন (শুধু সংখ্যা):",
                    parse_mode='Markdown')
            elif cap_type == 'unknown':
                context.user_data['add_panel_step'] = 'captcha_manual'
                setup['captcha_manual_mode'] = 'math'
                q = setup.get('captcha_question', '')
                await update.message.reply_text(
                    f"🔒 *Captcha Required*\n\n"
                    f"Field: `{setup.get('captcha_field', 'capt')}`\n"
                    f"Question: `{q if q else 'Login page দেখুন'}`\n\n"
                    "Captcha answer দিন:",
                    parse_mode='Markdown')
            else:
                # No captcha — just wrong credentials
                await update.message.reply_text(
                    "❌ Login failed — username বা password ভুল।\n\n"
                    "আবার username দিন (বা /cancel):",
                    parse_mode='Markdown')
                context.user_data['add_panel_step'] = 'username'
            return

        # ── Step captcha_manual ────────────────────────────────────────────────
        if step == 'captcha_manual':
            val = text.strip()
            if not val:
                await update.message.reply_text("❌ Value দিতে হবে। আবার দিন:")
                return

            manual_mode = setup.get('captcha_manual_mode', 'math')

            if manual_mode == 'cookie':
                # ── Session cookie method ────────────────────────────────────
                await update.message.reply_text("🔐 Session cookie দিয়ে verify করছি…")
                cookie_result = await asyncio.to_thread(_ap_check_cookie_session, setup, val)
                if not cookie_result['ok']:
                    err = cookie_result.get('error', '')
                    await update.message.reply_text(
                        f"❌ Cookie valid না / session verify হয়নি।\n"
                        f"`{_md_escape(str(err)) if err else 'Session expired বা invalid'}`\n\n"
                        "আবার cookie দিন (format: `PHPSESSID=value`) বা /cancel:",
                        parse_mode='Markdown')
                    return
                setup['session'] = cookie_result['session']
                setup['html']    = cookie_result.get('html', '')
                setup['login_method'] = 'cookie'
            else:
                # ── Manual captcha answer method ─────────────────────────────
                setup['manual_captcha_value'] = val
                await update.message.reply_text("🔐 Captcha দিয়ে login করছি…")
                login_result = await asyncio.to_thread(_ap_attempt_login, setup)
                setup.pop('manual_captcha_value', None)
                if not login_result['ok']:
                    err = login_result.get('error', '')
                    q   = setup.get('captcha_question', '')
                    await update.message.reply_text(
                        f"❌ Login fail হয়েছে।\n"
                        f"`{_md_escape(str(err)) if err else 'Wrong captcha বা credentials'}`\n\n"
                        f"Question: `{q if q else 'See login page'}`\n\n"
                        "আবার answer দিন (বা /cancel):",
                        parse_mode='Markdown')
                    return
                setup['session'] = login_result.get('session', setup['session'])
                setup['html']    = login_result.get('html', '')

            context.user_data['add_panel_step'] = 'ajax_url'
            await update.message.reply_text(
                "✅ Login সফল হয়েছে! 🎉\n\n"
                "➕ *Add New Panel — Step 5/5*\n\n"
                "SMS monitoring page এর URL দিন:\n"
                "_Example: http://123.45.67.89/client/SMSCDRStats_\n\n"
                "এটি সাধারণত Reports বা SMS CDR Stats page এর URL।\n\n"
                "❌ Cancel করতে /cancel পাঠান।",
                parse_mode='Markdown')
            return

        # ── Step 3: receive ajax/stats URL, analyze columns ───────────────────
        if step == 'ajax_url':
            stats_url = text.strip()
            if not stats_url.startswith('http'):
                await update.message.reply_text("❌ Valid HTTP/HTTPS URL দিন:")
                return
            setup['stats_url'] = stats_url
            await update.message.reply_text("🔍 SMS data analyze করছি…")
            ajax_result = await asyncio.to_thread(_ap_analyze_ajax, setup, stats_url)
            if not ajax_result['ok']:
                await update.message.reply_text(
                    f"❌ Page load করা যায়নি:\n`{ajax_result.get('error', '')}`\n\n"
                    "আবার URL দিন বা /cancel করুন।",
                    parse_mode='Markdown')
                return
            col_headers = ajax_result.get('col_headers', [])
            setup.update({
                'ajax_url':    ajax_result['ajax_url'],
                'path_prefix': ajax_result['path_prefix'],
                'col_map':     dict(ajax_result['col_map']),
                'col_count':   ajax_result.get('col_count', 0),
                'col_headers': col_headers,
            })
            sample_rows = ajax_result.get('sample_rows', [])
            col_map     = ajax_result['col_map']
            col_count   = ajax_result.get('col_count', 0)
            auth_type   = ajax_result.get('auth_type', 'cookie')

            # ── Build numbered column list: name + actual sample value ──────────
            _NUM_EMOJI = ['1️⃣','2️⃣','3️⃣','4️⃣','5️⃣','6️⃣','7️⃣','8️⃣','9️⃣','🔟']
            # Pick one real data row for preview (prefer a row with a phone-like number)
            _preview_row: list = []
            for _sr in sample_rows:
                if isinstance(_sr, list) and len(_sr) > 2:
                    _preview_row = _sr
                    break

            col_list_txt = ""
            detection_note = ""

            if col_headers:
                lines = []
                total = len(col_headers)
                # Align count: if preview row has more cols than headers, show extras
                total_display = max(total, len(_preview_row))
                for i in range(total_display):
                    num_str  = _NUM_EMOJI[i] if i < len(_NUM_EMOJI) else f"`{i+1}.`"
                    hdr      = col_headers[i] if i < total else f"Column {i+1}"
                    if _preview_row and i < len(_preview_row):
                        raw      = str(_preview_row[i])
                        sample   = raw[:38] + ('…' if len(raw) > 38 else '')
                        lines.append(
                            f"{num_str} *{_md_escape(hdr)}*\n"
                            f"      ↳ `{_md_escape(sample)}`"
                        )
                    else:
                        lines.append(f"{num_str} *{_md_escape(hdr)}*")
                col_list_txt = (
                    f"\n\n📊 *Total Columns Found: {total_display}*\n"
                    f"_(প্রতিটির নিচে real sample data দেখানো হয়েছে)_\n\n"
                    + "\n".join(lines)
                )
            elif _preview_row:
                # No headers from HTML — fall back to raw AJAX sample row only
                lines = []
                for i, val in enumerate(_preview_row):
                    num_str  = _NUM_EMOJI[i] if i < len(_NUM_EMOJI) else f"`{i+1}.`"
                    raw      = str(val)
                    sample   = raw[:45] + ('…' if len(raw) > 45 else '')
                    lines.append(f"{num_str} `{_md_escape(sample)}`")
                col_list_txt = (
                    f"\n\n📊 *Total Columns Found: {len(_preview_row)}*\n"
                    f"_(HTML header পাওয়া যায়নি — AJAX data দেখানো হচ্ছে)_\n\n"
                    + "\n".join(lines)
                )
                detection_note = "\n⚠️ _HTML থেকে column নাম পাওয়া যায়নি। Sample data দেখে column চেনো।_"
            else:
                col_list_txt = "\n\n⚠️ *Column detect করা যায়নি।* Report page-এর সঠিক URL দিন অথবা manual index দিন।"
                detection_note = ""

            context.user_data['add_panel_step'] = 'col_phone'
            await update.message.reply_text(
                f"✅ *Step 3 — SMS Data Fetched*\n\n"
                f"• AJAX URL: `{_md_escape(ajax_result['ajax_url'])}`\n"
                f"• Auth: `{auth_type}` | Path: `{ajax_result['path_prefix']}`"
                f"{col_list_txt}"
                f"{detection_note}\n\n"
                f"─────────────────\n"
                f"📱 *Step 4/6 — ফোন নাম্বার কলাম*\n"
                f"ফোন নাম্বারটি কত নম্বর কলামে আছে?\n"
                f"শুধু নম্বরটি লিখুন _(যেমন: `3`)_\n\n"
                f"❌ Cancel: /cancel",
                parse_mode='Markdown')
            return

        # ─────────────────────────────────────────────────────────────────────
        # Shared helpers for steps 4-6
        # ─────────────────────────────────────────────────────────────────────

        def _col_sample(setup_ref: dict, idx: int) -> str:
            """Short sample value from AJAX data for display."""
            rows = setup_ref.get('sample_rows', [])
            row  = next((r for r in rows if isinstance(r, list) and len(r) > idx), None)
            if not row:
                return '—'
            raw = str(row[idx])[:38]
            return f"`{_md_escape(raw)}`"

        def _col_label_fn(setup_ref: dict, idx: int) -> str:
            hdrs = setup_ref.get('col_headers', [])
            if hdrs and 0 <= idx < len(hdrs):
                return f"*{_md_escape(hdrs[idx])}* (কলাম {idx+1})"
            return f"কলাম {idx+1}"

        def _max_col(setup_ref: dict) -> int:
            hdrs = setup_ref.get('col_headers', [])
            cnt  = setup_ref.get('col_count', 0)
            return len(hdrs) if hdrs else cnt

        def _build_col_ref(setup_ref: dict, mark_idx: int = -1) -> str:
            """
            Build a compact numbered column reference from detected headers +
            sample data.  mark_idx highlights an already-chosen column with ✅.
            E.g.:
              📋 Detected Columns:
              1️⃣ Date/Time  → `2024-06-29 18:30:00`
              2️⃣ Range       → `Bangladesh MTN`
              3️⃣ Number      → `8801712345678`   ← ✅ already set
              ...
            """
            hdrs = setup_ref.get('col_headers', [])
            rows = setup_ref.get('sample_rows', [])
            preview = next((r for r in rows if isinstance(r, list) and len(r) > 1), [])
            _NUM = ['1️⃣','2️⃣','3️⃣','4️⃣','5️⃣','6️⃣','7️⃣','8️⃣','9️⃣','🔟']

            if not hdrs and not preview:
                return ""

            total = max(len(hdrs), len(preview))
            lines = []
            for i in range(total):
                num  = _NUM[i] if i < len(_NUM) else f"{i+1}."
                name = _md_escape(hdrs[i]) if i < len(hdrs) else f"Column {i+1}"
                sval = ''
                if i < len(preview):
                    raw  = str(preview[i])[:32]
                    sval = f" → `{_md_escape(raw)}`"
                tick = " ✅" if i == mark_idx else ""
                lines.append(f"{num} *{name}*{sval}{tick}")

            return "\n📋 *Detected Columns:*\n" + "\n".join(lines)

        # ── Step 4: Phone Number column ───────────────────────────────────────
        if step == 'col_phone':
            try:
                choice = int(text.strip())
                mx = _max_col(setup)
                if mx and not (1 <= choice <= mx):
                    await update.message.reply_text(
                        f"❌ 1 থেকে {mx} এর মধ্যে নম্বর দিন। আবার লিখুন:")
                    return
                idx = choice - 1
                setup['phone_idx'] = idx
                setup['col_map']['number'] = idx
                col_ref = _build_col_ref(setup, mark_idx=idx)
                context.user_data['add_panel_step'] = 'col_otp'
                await update.message.reply_text(
                    f"✅ *ফোন নাম্বারের কলাম সেট করা হয়েছে।*\n"
                    f"📱 কলাম {choice} → {_col_sample(setup, idx)}\n"
                    f"{col_ref}\n\n"
                    f"─────────────────\n"
                    f"💬 *Step 5/6 — OTP / SMS Text কলাম*\n"
                    f"ওটিপি মেসেজ (SMS Text) কত নম্বর কলামে আছে?\n"
                    f"শুধু নম্বরটি লিখুন _(যেমন: `6`)_\n\n"
                    f"❌ Cancel: /cancel",
                    parse_mode='Markdown')
            except ValueError:
                await update.message.reply_text("❌ শুধু সংখ্যা দিন (যেমন: 1, 2, 3):")
            return

        # ── Step 5: OTP / SMS Body column ────────────────────────────────────
        if step == 'col_otp':
            try:
                choice = int(text.strip())
                mx = _max_col(setup)
                if mx and not (1 <= choice <= mx):
                    await update.message.reply_text(
                        f"❌ 1 থেকে {mx} এর মধ্যে নম্বর দিন। আবার লিখুন:")
                    return
                idx = choice - 1
                setup['otp_idx'] = idx
                setup['col_map']['sms_body'] = idx
                col_ref = _build_col_ref(setup, mark_idx=idx)
                context.user_data['add_panel_step'] = 'col_service'
                await update.message.reply_text(
                    f"✅ *OTP / SMS Text কলাম সেট করা হয়েছে।*\n"
                    f"💬 কলাম {choice} → {_col_sample(setup, idx)}\n"
                    f"{col_ref}\n\n"
                    f"─────────────────\n"
                    f"🌍 *Step 6/6 — Service / App Name কলাম*\n"
                    f"সার্ভিসের নাম (App Name) কত নম্বর কলামে আছে?\n"
                    f"শুধু নম্বরটি লিখুন _(যেমন: `2`)_\n\n"
                    f"❌ Cancel: /cancel",
                    parse_mode='Markdown')
            except ValueError:
                await update.message.reply_text("❌ শুধু সংখ্যা দিন (যেমন: 1, 2, 3):")
            return

        # ── Step 6: Service / Country column → Final Summary ─────────────────
        if step == 'col_service':
            try:
                choice = int(text.strip())
                mx = _max_col(setup)
                if mx and not (1 <= choice <= mx):
                    await update.message.reply_text(
                        f"❌ 1 থেকে {mx} এর মধ্যে নম্বর দিন। আবার লিখুন:")
                    return
                idx = choice - 1
                setup['service_idx'] = idx
                setup['col_map']['service_idx'] = idx
                context.user_data['add_panel_step'] = 'awaiting_confirm'

                phone_idx = setup.get('phone_idx', 0)
                otp_idx   = setup.get('otp_idx', 0)
                svc_idx   = idx
                pw_mask   = '•' * min(len(setup.get('password', '')), 8)

                summary = (
                    f"✅ *সবগুলো কলাম সঠিকভাবে সেট করা হয়েছে\\!*\n\n"
                    f"📋 *Configuration Summary*\n\n"
                    f"📛 *Panel:* {_md_escape(setup.get('name', ''))}\n"
                    f"🔗 *Login URL:* `{_md_escape(setup.get('login_url', ''))}`\n"
                    f"👤 *Username:* `{_md_escape(setup.get('username', ''))}`\n"
                    f"🔑 *Password:* `{pw_mask}`\n"
                    f"📡 *AJAX URL:* `{_md_escape(setup.get('ajax_url', ''))}`\n\n"
                    f"📊 *Column Mapping:*\n"
                    f"📱 Phone → {_col_label_fn(setup, phone_idx)}\n"
                    f"     Sample: {_col_sample(setup, phone_idx)}\n"
                    f"💬 OTP/SMS → {_col_label_fn(setup, otp_idx)}\n"
                    f"     Sample: {_col_sample(setup, otp_idx)}\n"
                    f"🌍 Service → {_col_label_fn(setup, svc_idx)}\n"
                    f"     Sample: {_col_sample(setup, svc_idx)}\n\n"
                    f"সব ঠিক থাকলে *Confirm* করুন।"
                )
                kb = InlineKeyboardMarkup([[
                    InlineKeyboardButton(
                        "✅ Confirm and Save",
                        callback_data=f"confirm_add_panel:{user_id}"),
                    InlineKeyboardButton(
                        "❌ Cancel",
                        callback_data=f"cancel_add_panel:{user_id}"),
                ]])
                await update.message.reply_text(summary, parse_mode='Markdown', reply_markup=kb)
            except ValueError:
                await update.message.reply_text("❌ শুধু সংখ্যা দিন (যেমন: 1, 2, 3):")
            return

        # ── Step awaiting_confirm: guard — user must click inline button ───────
        if step == 'awaiting_confirm':
            await update.message.reply_text(
                "⏳ উপরের *✅ Confirm and Save* অথবা *❌ Cancel* বাটনে ক্লিক করুন।\n\n"
                "Cancel করতে /cancel পাঠান।",
                parse_mode='Markdown')
            return

    # ── Extra Group add flow ───────────────────────────────────────────────────
    if context.user_data.get('awaiting_extra_group_id'):
        context.user_data.pop('awaiting_extra_group_id')
        raw = text
        try:
            chat = await context.bot.get_chat(raw)
            await run_db(_add_extra_group, str(chat.id), chat.title or raw)
            await update.message.reply_text(
                f"✅ *Group Added!*\n\n"
                f"💰 Name: `{chat.title}`\nID: `{chat.id}`",
                parse_mode='Markdown',
            )
        except Exception as e:
            await update.message.reply_text(
                f"❌ Group not found or bot is not in that group.\nError: {e}"
            )
        # Re-show the overview + Extra Groups keyboard
        msg = await _build_extra_groups_overview(context)
        await update.message.reply_text(
            msg, parse_mode='Markdown',
            reply_markup=get_extra_groups_keyboard(),
        )
        return

    # ── Extra Group remove flow ────────────────────────────────────────────────
    if context.user_data.get('awaiting_extra_group_remove_id'):
        context.user_data.pop('awaiting_extra_group_remove_id')
        raw = (text or "").strip()
        groups = await run_db(_get_all_extra_groups)
        match = next(
            (g for g in groups if str(g['chat_id']) == raw),
            None,
        )
        if not match:
            await update.message.reply_text(
                f"❌ Chat ID `{raw}` not found in the list. Please try again.",
                parse_mode='Markdown',
            )
        else:
            await run_db(_remove_extra_group, str(match['chat_id']))
            await update.message.reply_text(
                f"✅ *Group Removed.*\n\n"
                f"💰 Name: `{match['title']}`\nID: `{match['chat_id']}`",
                parse_mode='Markdown',
            )
        msg = await _build_extra_groups_overview(context)
        await update.message.reply_text(
            msg, parse_mode='Markdown',
            reply_markup=get_extra_groups_keyboard(),
        )
        return

    # ── Required Channel: awaiting channel name ──────────────────────────────
    if context.user_data.get('awaiting_channel_name'):
        context.user_data.pop('awaiting_channel_name')
        name = text.strip()
        if not name:
            await update.message.reply_text(
                "❌ নাম খালি হতে পারবে না। আবার চেষ্টা করুন।",
                reply_markup=get_required_channels_keyboard())
            return
        context.user_data['pending_channel_name'] = name
        context.user_data['awaiting_channel_link'] = True
        await update.message.reply_text(
            f"✅ নাম সেট হয়েছে: *{name}*\n\n"
            "এখন Channel এর *link বা username* দিন:\n"
            "_উদাহরণ:_ `@MyChannel` অথবা `https://t.me/MyChannel`",
            parse_mode='Markdown',
            reply_markup=ReplyKeyboardRemove())
        await update.message.reply_text(
            "নিচের বাটন দিয়ে cancel করতে পারবেন:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Cancel", callback_data="cancel_req_channel")]
            ]))
        return

    # ── Required Channel: awaiting channel link ───────────────────────────────
    if context.user_data.get('awaiting_channel_link'):
        context.user_data.pop('awaiting_channel_link')
        raw_link = text.strip()
        ch_name  = context.user_data.pop('pending_channel_name', 'Channel')

        # Normalize to a resolvable username/invite link
        if raw_link.startswith('https://t.me/') and '+' not in raw_link:
            username_or_id = '@' + raw_link.split('/')[-1]
        elif raw_link.startswith('@'):
            username_or_id = raw_link
        else:
            username_or_id = raw_link  # pass as-is (invite links, etc.)

        # Auto-resolve chat ID
        try:
            chat = await context.bot.get_chat(username_or_id)
            ch_id = str(chat.id)
            # Build a clean public URL
            if chat.username:
                ch_url = f"https://t.me/{chat.username}"
            else:
                ch_url = raw_link
        except Exception as e:
            await update.message.reply_text(
                f"❌ Channel খুঁজে পাওয়া যায়নি।\n\n"
                f"নিশ্চিত করুন বটকে channel এ add করা হয়েছে।\n"
                f"Error: `{e}`",
                parse_mode='Markdown',
                reply_markup=get_required_channels_keyboard())
            return

        added = await run_db(_add_required_channel, ch_name, ch_url, ch_id)
        if added:
            await update.message.reply_text(
                f"✅ *Channel Added!*\n\n"
                f"📢 নাম: *{ch_name}*\n"
                f"🔗 Link: {ch_url}\n"
                f"🆔 Chat ID: `{ch_id}`",
                parse_mode='Markdown',
                reply_markup=get_required_channels_keyboard())
        else:
            await update.message.reply_text(
                f"⚠️ এই channel টি আগেই add করা আছে।",
                reply_markup=get_required_channels_keyboard())
        return

    # ── Required Channel: awaiting delete index ───────────────────────────────
    if context.user_data.get('awaiting_channel_delete_index'):
        context.user_data.pop('awaiting_channel_delete_index')
        channels = await run_db(_get_required_channels)
        try:
            idx = int(text.strip()) - 1
            if idx < 0 or idx >= len(channels):
                raise ValueError()
        except ValueError:
            await update.message.reply_text(
                f"❌ সঠিক নম্বর দিন (1 থেকে {len(channels)})।",
                reply_markup=get_required_channels_keyboard())
            return
        deleted_name = channels[idx]['name']
        await run_db(_delete_required_channel, idx)
        await update.message.reply_text(
            f"✅ *{deleted_name}* channel ডিলিট করা হয়েছে।",
            parse_mode='Markdown',
            reply_markup=get_required_channels_keyboard())
        return

    # ── Service Edit flow ─────────────────────────────────────────────────────
    if context.user_data.get('awaiting_service_edit'):
        if not _is_admin(username, user_id):
            await update.message.reply_text("❌ Unauthorized.")
            return

        # ── Extract custom emoji ID (animated sticker) from message entities ──
        custom_emoji_id: str | None = None
        entities = update.message.entities or []
        for ent in entities:
            if ent.type == "custom_emoji" and ent.custom_emoji_id:
                custom_emoji_id = ent.custom_emoji_id
                break

        # ── Strip custom emoji chars from text to get clean service name ──────
        raw_full = update.message.text or ""
        clean = raw_full
        for ent in sorted(entities, key=lambda e: e.offset, reverse=True):
            if ent.type == "custom_emoji":
                clean = clean[:ent.offset] + clean[ent.offset + ent.length:]
        raw = clean.strip()

        if raw.lower().startswith("delete "):
            svc_name = raw[7:].strip()
            deleted = await run_db(_delete_service, svc_name)
            if deleted:
                reply = f"✅ *{svc_name}* service সফলভাবে delete হয়েছে।"
            else:
                reply = f"❌ *{svc_name}* নামে কোনো service পাওয়া যায়নি।"
        else:
            added = await run_db(_add_service, raw)
            if added:
                if custom_emoji_id:
                    await run_db(_set_service_emoji, raw, custom_emoji_id)
                reply = f"✅ *{raw}* service সফলভাবে add হয়েছে।"
            else:
                reply = f"⚠️ *{raw}* service আগে থেকেই আছে।"
        services = await run_db(_get_services)
        svc_lines = "\n".join(f"• `{s}`" for s in services) if services else "_কোনো service নেই_"
        await update.message.reply_text(
            f"{reply}\n\n"
            "*বর্তমান Services:*\n"
            f"{svc_lines}\n\n"
            "➕ নতুন যোগ করতে নাম লিখুন | 🗑️ Delete করতে: `delete নাম`",
            parse_mode='Markdown',
            reply_markup=get_manage_numbers_keyboard(),
        )
        return

    # ── Service Map flow ──────────────────────────────────────────────────────
    if context.user_data.get('awaiting_service_map'):
        if not _is_admin(username, user_id):
            await update.message.reply_text("❌ Unauthorized.")
            return
        raw = text.strip()
        is_unmap = raw.lower().startswith("unmap ")
        payload  = raw[6:].strip() if is_unmap else raw

        words = payload.split()
        if len(words) < 2:
            await update.message.reply_text(
                "❌ ফরম্যাট ঠিক নেই।\n\n"
                "Map করতে: `WhatsApp Bangladesh`\n"
                "Unmap করতে: `unmap WhatsApp Bangladesh`",
                parse_mode='Markdown',
                reply_markup=get_manage_numbers_keyboard(),
            )
            return

        countries = await run_db(_get_countries)

        # Try matching country from the right: 1 word first, then 2, then 3…
        # This correctly handles multi-word service names like "Facebook New ID".
        found_country = None
        svc_name = None
        for n in range(1, len(words)):
            cname_raw = ' '.join(words[len(words) - n:])
            svc_try   = ' '.join(words[:len(words) - n])
            if not svc_try:
                continue
            match = next(
                ((cid, cname) for cid, cname in countries
                 if cname.lower() == cname_raw.lower()
                 or _strip_flag_prefix(cname).lower() == cname_raw.lower()),
                None,
            )
            if match:
                found_country = match
                svc_name = svc_try
                break

        if not found_country:
            await update.message.reply_text(
                f"❌ *{cname_raw}* নামে কোনো country পাওয়া যায়নি।\n"
                "Country-এর নাম হুবহু লিখুন।",
                parse_mode='Markdown',
                reply_markup=get_manage_numbers_keyboard(),
            )
            return

        cid, cname = found_country
        if is_unmap:
            ok = await run_db(_unmap_service_country, svc_name, cid)
            if ok:
                reply = f"✅ *{cname}* — *{svc_name}* থেকে remove হয়েছে।"
            else:
                reply = f"⚠️ *{cname}* এই service-এ ছিল না অথবা service নাম ভুল।"
        else:
            ok = await run_db(_map_service_country, svc_name, cid)
            if ok:
                reply = f"✅ *{cname}* — *{svc_name}*-এ add হয়েছে।"
            else:
                reply = f"⚠️ *{cname}* আগে থেকেই এই service-এ আছে অথবা service নাম ভুল।"

        await update.message.reply_text(
            f"{reply}\n\n"
            "Map করতে: `ServiceName CountryName`\n"
            "Unmap করতে: `unmap ServiceName CountryName`",
            parse_mode='Markdown',
            reply_markup=get_manage_numbers_keyboard(),
        )
        return

    # ── Latest Message — robust top-level handler ─────────────────────────────
    # Only handles the case where panel_view_active is NOT set (e.g. after a
    # bot restart). When panel_view_active IS set, the panel_view block below
    # handles it and returns the correct panel action keyboard.
    if text == "✉️ Latest Message" and not context.user_data.get('panel_view_active'):
        # Try last_panel_view as fallback (persists across the session)
        pname = context.user_data.get('last_panel_view')
        if not pname:
            await update.message.reply_text(
                "ℹ️ *Please select a panel first.*\n\n"
                "📌 *Panel List* → choose a panel → then press ✉️ *Latest Message*.",
                parse_mode='Markdown',
                reply_markup=get_admin_keyboard())
            return

        # Restore panel context so subsequent buttons work normally.
        context.user_data['panel_view_active'] = pname
        context.user_data['last_panel_view']   = pname

        is_en = bool(await run_db(_is_panel_enabled, pname))
        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action='typing')
        rec = await asyncio.to_thread(get_panel_latest_today, pname)

        if not rec:
            await update.message.reply_text(
                f"*{_md_escape(pname)}*\n\n"
                "❌ প্যানেলে কোনো মেসেজ পাওয়া যায়নি।",
                parse_mode='Markdown',
                reply_markup=get_panel_action_keyboard(is_en))
            return

        text_msg = _format_panel_latest(rec, pname=pname)
        if len(text_msg) > 4000:
            text_msg = text_msg[:3990] + "\n…"

        try:
            await update.message.reply_text(text_msg, parse_mode='Markdown',
                                            reply_markup=get_panel_action_keyboard(is_en))
        except Exception:
            plain = text_msg.replace('`', "'").replace('*', '').replace('_', '').replace('[', '(')
            if len(plain) > 4000:
                plain = plain[:3990] + "\n…"
            await update.message.reply_text(plain, reply_markup=get_panel_action_keyboard(is_en))
        return

    # ── Panel view action keyboard flow ───────────────────────────────────────
    if context.user_data.get('panel_view_active'):
        pname = context.user_data['panel_view_active']

        if text == "Back to Panel List":
            context.user_data.pop('panel_view_active', None)
            context.user_data.pop('panel_list_category', None)
            context.user_data.pop('panel_list_source', None)
            context.user_data.pop('panel_list_multiple_active', None)
            context.user_data['panel_list_active'] = True
            context.user_data['panel_page'] = 0
            all_panels_db = await run_db(_get_panels)
            all_names = [pname for pname, _m in ALL_PANEL_LIST]
            panels = [p for p in all_panels_db if p['name'] in all_names]
            panels.sort(key=lambda p: all_names.index(p['name']) if p['name'] in all_names else 999)
            context.user_data['panel_list_cache'] = [p['name'] for p in panels]
            await update.message.reply_text(
                f"📌 *Panel List* ({len(panels)} panels)\n\nSelect a panel:",
                parse_mode='Markdown',
                reply_markup=_build_panel_page_keyboard(panels, 0)
            )
            return

        if text == "🔑 Change User/Pass":
            pname = context.user_data.get('panel_view_active')
            if not pname:
                return
            context.user_data['awaiting_cred_panel']    = pname
            context.user_data.pop('awaiting_cred_username', None)
            await update.message.reply_text(
                f"🔑 *Change User/Pass — {_md_escape(pname)}*\n\n"
                f"Send the *new username* for this panel:",
                parse_mode='Markdown',
                reply_markup=ReplyKeyboardRemove(),
            )
            return

        if text == "✉️ Latest Message":
            pname = context.user_data.get('panel_view_active')
            await context.bot.send_chat_action(chat_id=update.effective_chat.id, action='typing')
            rec = await asyncio.to_thread(get_panel_latest_today, pname)
            is_en = bool(await run_db(_is_panel_enabled, pname))
            if not rec:
                await update.message.reply_text(
                    f"*{_md_escape(pname)}*\n\n"
                    "❌ প্যানেলে কোনো মেসেজ পাওয়া যায়নি।",
                    parse_mode='Markdown',
                    reply_markup=get_panel_action_keyboard(is_en))
                return
            text_msg = _format_panel_latest(rec, pname=pname or "")
            if len(text_msg) > 4000:
                text_msg = text_msg[:3990] + "\n…"
            try:
                await update.message.reply_text(text_msg, parse_mode='Markdown',
                                                reply_markup=get_panel_action_keyboard(is_en))
            except Exception:
                plain = text_msg.replace('`', "'").replace('*', '').replace('_', '').replace('[', '(')
                if len(plain) > 4000:
                    plain = plain[:3990] + "\n…"
                await update.message.reply_text(plain, reply_markup=get_panel_action_keyboard(is_en))
            return

        if text == "📤 Group এ পাঠাও":
            pname = context.user_data.get('panel_view_active')
            if not pname:
                return
            await context.bot.send_chat_action(chat_id=update.effective_chat.id, action='typing')
            rec = await asyncio.to_thread(get_panel_latest_cached, pname)
            is_en = bool(await run_db(_is_panel_enabled, pname))
            if not rec:
                await update.message.reply_text(
                    f"*{_md_escape(pname)} — কোনো SMS নেই*\n\n"
                    "এই panel এ আজকে কোনো SMS আসেনি।\n"
                    "Panel login হয়নি বা SMS আসেনি।",
                    parse_mode='Markdown',
                    reply_markup=get_panel_action_keyboard(is_en),
                )
                return
            number   = str(rec.get('number') or '')
            country  = str(rec.get('country') or '')
            website  = str(rec.get('website') or 'Unknown')
            otp      = str(rec.get('otp') or '')
            sms_body = str(rec.get('message') or '')
            try:
                grp_text, grp_markup = _build_group_notify_text(number, country, website, otp, sms_body)
                await _broadcast_to_groups(
                    context.bot, pname, grp_text, grp_markup,
                    dt_str='', number='', sms_body='',
                )
                await update.message.reply_text(
                    f"✅ *{_md_escape(pname)}* এর সর্বশেষ message group এ পাঠানো হয়েছে।",
                    parse_mode='Markdown',
                    reply_markup=get_panel_action_keyboard(is_en),
                )
            except Exception as _ge:
                await update.message.reply_text(
                    f"❌ Group এ পাঠাতে সমস্যা হয়েছে:\n`{_ge}`",
                    parse_mode='Markdown',
                    reply_markup=get_panel_action_keyboard(is_en),
                )
            return

        if text in ("🔴 Disable Panel", "🟢 Enable Panel"):
            pname = context.user_data.get('panel_view_active')
            if not pname:
                return
            currently_enabled = bool(await run_db(_is_panel_enabled, pname))
            new_state = not currently_enabled
            await run_db(_set_panel_enabled, pname, new_state)
            await _notify_admins_panel_toggled(context.bot, pname, new_state)
            status_str = "✅ Enabled" if new_state else "🚫 Disabled"
            await update.message.reply_text(
                f"📌 *{_md_escape(pname)}*\n\n"
                f"Status updated: *{status_str}*",
                parse_mode='Markdown',
                reply_markup=get_panel_action_keyboard(new_state)
            )
            return

        if text == "📊 Panel Status":
            pname = context.user_data.get('panel_view_active')
            if not pname:
                return
            statuses = await run_db(_get_all_panel_statuses)
            status_map = {s['panel_name']: s for s in (statuses or [])}
            s = status_map.get(pname, {})
            is_en = bool(await run_db(_is_panel_enabled, pname))
            panel = await run_db(_get_panel_by_name, pname)
            db_user = _resolve_panel_user(panel, pname) if panel else "—"
            enabled_str  = "✅ Enabled" if is_en else "🚫 Disabled"
            logged_str   = "🟢 Logged In" if s.get('logged_in') else "🔴 Not Logged In"
            last_err  = _md_escape(str(s.get('last_error') or "—"))
            last_seen = s.get('last_seen') or "—"
            base_url  = panel.get('base_url', '—') if panel else '—'
            await update.message.reply_text(
                f"📊 *{_md_escape(pname)} — Status*\n\n"
                f"┣ Status: *{enabled_str}*\n"
                f"┣ Login: *{logged_str}*\n"
                f"┣ Username: `{db_user}`\n"
                f"┣ URL: `{base_url}`\n"
                f"┣ Last Seen: `{last_seen}`\n"
                f"┗ Last Error: `{last_err}`",
                parse_mode='Markdown',
                reply_markup=get_panel_action_keyboard(is_en)
            )
            return

        if text == "⚙️ Live Column Config":
            pname = context.user_data.get('panel_view_active')
            if not pname:
                return
            is_en = bool(await run_db(_is_panel_enabled, pname))
            # Immediate loading feedback — admin sees this within ~100ms
            await update.message.reply_text(
                f"🔄 *{_md_escape(pname)}* — Live Column Config\n\n"
                "⌛ Panel থেকে live data আনা হচ্ছে… _(1–3 সেকেন্ড)_",
                parse_mode='Markdown',
                reply_markup=get_panel_action_keyboard(is_en),
            )
            await context.bot.send_chat_action(chat_id=update.effective_chat.id, action='typing')

            # Fetch with detailed error info
            result = await asyncio.to_thread(discover_panel_columns, pname, ALL_PANEL_LIST)

            if result['status'] != 'ok':
                status = result['status']
                err    = result.get('error', 'Unknown error.')
                if status == 'redirected':
                    err_msg = (
                        f"❌ *Error: Panel session expired.*\n\n"
                        f"Panel টি login page-এ redirect করেছে। "
                        f"Bot নিজেই re-login করার চেষ্টা করছে।\n"
                        f"_একটু পর আবার চেষ্টা করুন।_"
                    )
                elif status == 'timeout':
                    err_msg = (
                        f"❌ *Error: Panel server unresponsive or timed out (20s).*\n\n"
                        f"Panel server টি হয়তো বন্ধ বা অনেক slow। "
                        f"Site টি browser থেকে manually check করুন।"
                    )
                elif status == 'rate_limited':
                    err_msg = (
                        f"❌ *Error: Rate limited or blocked by Cloudflare.*\n\n"
                        f"`{_md_escape(err)}`\n\n"
                        f"_কয়েক মিনিট অপেক্ষা করে আবার চেষ্টা করুন।_"
                    )
                elif status == 'no_columns':
                    err_msg = (
                        f"❌ *Error: No HTML table headers found.*\n\n"
                        f"Panel-এ সফলভাবে connect হয়েছে, কিন্তু stats page-এ কোনো "
                        f"`<th>` header পাওয়া যায়নি।\n"
                        f"এই panel-টি সম্ভবত AJAX/JSON দিয়ে data load করে — "
                        f"HTML column discovery কাজ করবে না।"
                    )
                elif status in ('login_failed', 'login_error'):
                    err_msg = (
                        f"❌ *Error: Login failed.*\n\n"
                        f"`{_md_escape(err)}`\n\n"
                        f"_Panel টি enable এবং credentials সঠিক কিনা নিশ্চিত করুন।_"
                    )
                else:
                    err_msg = (
                        f"❌ *Error: Could not fetch columns.*\n\n"
                        f"`{_md_escape(err)}`"
                    )
                retry_kb = InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔄 Retry Fetch", callback_data=f"col_retry:{pname}")
                ]])
                await update.message.reply_text(err_msg, parse_mode='Markdown', reply_markup=retry_kb)
                return

            cols = result['columns']
            # Save discovered columns to DB
            await run_db(_save_panel_discovered_columns, pname, cols)
            # Store in user_data so the callback can reference them without refetching
            context.user_data['col_cfg_panel']   = pname
            context.user_data['col_cfg_columns'] = cols
            # Load any existing config
            cfg = await run_db(_get_panel_column_config, pname) or {}
            cur_num  = cfg.get('number_col', '?')
            cur_body = cfg.get('body_col',   '?')

            def _col_name(idx, cols):
                try:
                    return cols[int(idx)]
                except (TypeError, ValueError, IndexError):
                    return str(idx)

            num_lbl  = f"`[{cur_num}]` {_md_escape(_col_name(cur_num, cols))}"  if cur_num  != '?' else '`—` _(not set)_'
            body_lbl = f"`[{cur_body}]` {_md_escape(_col_name(cur_body, cols))}" if cur_body != '?' else '`—` _(not set)_'
            col_lines = "\n".join(f"  `[{i}]` {_md_escape(c)}" for i, c in enumerate(cols))

            msg = (
                f"⚙️ *{_md_escape(pname)} — Live Column Config*\n\n"
                f"🔎 *{len(cols)} টি Column পাওয়া গেছে:*\n{col_lines}\n\n"
                f"📱 *Phone Number Column:* {num_lbl}\n"
                f"📨 *SMS Body Column:* {body_lbl}\n\n"
                "নিচের বোতাম দিয়ে কোন index টি Phone Number এবং কোনটি SMS Body সেটি সিলেক্ট করুন:"
            )

            row_size = 5
            def _build_col_rows(prefix, cols, row_size):
                rows_out = []
                for i in range(0, len(cols), row_size):
                    rows_out.append([
                        InlineKeyboardButton(f"{prefix}[{j}]", callback_data=f"col_{prefix.strip()}:{j}")
                        for j in range(i, min(i + row_size, len(cols)))
                    ])
                return rows_out

            phone_rows = _build_col_rows("📱", cols, row_size)
            body_rows  = _build_col_rows("📨", cols, row_size)
            inline_kb  = InlineKeyboardMarkup(
                [[InlineKeyboardButton("──── 📱 Phone Number Column ────", callback_data="col_noop")]]
                + phone_rows
                + [[InlineKeyboardButton("──── 📨 SMS Body Column ────", callback_data="col_noop")]]
                + body_rows
            )
            await update.message.reply_text(msg, parse_mode='Markdown', reply_markup=inline_kb)
            return

        # Check if user clicked another panel button while panel_view was still active
        possible_panel_name = _panel_name_from_label(text)
        possible_panel = await run_db(_get_panel_by_name, possible_panel_name)
        if possible_panel:
            pname = possible_panel['name']
            context.user_data['panel_view_active'] = pname
            context.user_data['last_panel_view']   = pname
            context.user_data['panel_list_active'] = True
            panel = possible_panel
            is_en = bool(await run_db(_is_panel_enabled, pname))
            db_user = _resolve_panel_user(panel, pname)
            enabled_str = "✅ Enabled" if is_en else "🚫 Disabled"
            await update.message.reply_text(
                f"📌 *{_md_escape(pname)}*\n\n"
                f"┣ Status: *{enabled_str}*\n"
                f"┣ Username: `{db_user}`\n"
                f"┗ URL: `{panel['base_url']}`\n\n"
                "Choose an action:",
                parse_mode='Markdown',
                reply_markup=get_panel_action_keyboard(is_en)
            )
            return

        return

    # ── Panel list mobile keyboard flow (ALL panels unified) ─────────────────
    if context.user_data.get('panel_list_active') or context.user_data.get('panel_list_multiple_active'):
        # If the admin is currently waiting for specific text input from another
        # feature, do NOT intercept — let the dedicated handler below process it.
        _SKIP_PANEL_LIST_STATES = (
            'awaiting_edit_bot_link', 'awaiting_broadcast_message',
            'awaiting_otp_bonus_amount', 'awaiting_otp_daily_limit',
            'awaiting_country_otp_bonus', 'awaiting_country_otp_bonus_amount',
            'awaiting_number_limit', 'awaiting_panel_interval',
            'awaiting_balance_user_id', 'awaiting_balance_amount',
            'awaiting_new_admin', 'awaiting_min_withdraw',
            'awaiting_new_country_name', 'awaiting_reset_country_name',
            'awaiting_add_numbers_country', 'awaiting_country_name',
            'awaiting_notify_time', 'awaiting_numbers_file', 'awaiting_admin_username',
            'awaiting_reset_users_confirm', 'awaiting_extra_group_id',
            'awaiting_extra_group_remove_id',
            'awaiting_svc_name',
            'panel_toggle_active',
            'awaiting_ref_bonus', 'awaiting_otp_daily_limit',
            'awaiting_withdraw_account', 'awaiting_withdraw_amount',
            'awaiting_withdraw_method',
            'awaiting_retry_login_panel', 'awaiting_reload_interval_panel',
            'awaiting_reload_interval_seconds', 'awaiting_session_cleanup_panel',
            'awaiting_panel_retry',
            'awaiting_delete_country_name', 'awaiting_specific_number_delete',
            'awaiting_cred_panel', 'awaiting_cred_username',
            'awaiting_user_info_id',
            'col_cfg_panel',
            'add_panel_step',
        )
        # Only intercept if NO awaiting-input state is active.
        # When admin is mid-flow (e.g. entering OTP bonus amount, broadcast
        # text, balance edit, etc.), skip this block entirely so the
        # dedicated handlers further below can process the input correctly.
        if not any(context.user_data.get(s) for s in _SKIP_PANEL_LIST_STATES):
            # ── Pagination buttons ─────────────────────────────────────────────
            if text.startswith("➡️ More Panels") or text == "⬅️ First Page":
                cached_names  = context.user_data.get('panel_list_cache', [])
                all_panels_db = await run_db(_get_panels)
                panels_by_name = {p['name']: p for p in all_panels_db}
                panels = [panels_by_name[n] for n in cached_names if n in panels_by_name]
                if not panels:
                    # fallback: re-fetch order
                    all_names = [pname for pname, _m in ALL_PANEL_LIST]
                    panels = [p for p in all_panels_db if p['name'] in all_names]
                    panels.sort(key=lambda p: all_names.index(p['name']) if p['name'] in all_names else 999)
                    context.user_data['panel_list_cache'] = [p['name'] for p in panels]
                if text == "⬅️ First Page":
                    page = 0
                else:
                    page = context.user_data.get('panel_page', 0) + 1
                context.user_data['panel_page'] = page
                total = len(panels)
                start = page * _PANEL_PAGE_SIZE
                end   = min(start + _PANEL_PAGE_SIZE, total)
                page_label = f"Page {page + 1}" if page > 0 else ""
                suffix = f" — {page_label}" if page_label else ""
                await update.message.reply_text(
                    f"📌 *Panel List* ({total} panels){suffix}\n\nSelect a panel:",
                    parse_mode='Markdown',
                    reply_markup=_build_panel_page_keyboard(panels, page)
                )
                return
            # ──────────────────────────────────────────────────────────────────
            _PANEL_LIST_CANCEL_BUTTONS = {
                "Back", "Back to Panel List",
                # Admin main keyboard
                "🌍 𝑪𝒐𝒖𝒏𝒕𝒓𝒚 𝑴𝒂𝒏𝒂𝒈𝒆𝒓", "Manage Admins",
                "Users", "Panel management", "📋 Panel List", "➕ Add Panel",
                "📢 Broadcast", "Settings",
                "📊 𝑩𝒐𝒕 𝑺𝒕𝒂𝒕𝒊𝒔𝒕𝒊𝒄𝒔",
                # Admin tools keyboard
                "🌟 Force Start",
                "⌛ Retry Interval", "⌛ Retry Login",
                "🔄 Reload Interval", "📢 Extra Groups",
                "🔗 Edit Bot Links", "Channel Join",
                "Back to Admin Panel",
                # Settings keyboard
                "⭐ OTP Bonus", "🎁 Referral",
                "Number Limit", "🗑️ Reset All Users",
                "Export Users", "Admin Tools",
                # Manage numbers keyboard
                "📱 𝑨𝒅𝒅 𝑵𝒖𝒎𝒃𝒆𝒓", "🌐Add 𝑪𝒐𝒖𝒏𝒕𝒓𝒚",
                "🔄 𝑹𝒆𝒔𝒆𝒕 𝑵𝒖𝒎𝒃𝒆𝒓", "🌍 Country OTP Bonus",
                "⚙️ Add Service", "🗺️ Service Map",
                "⚙️ Live Column Config",
                # Manage admins keyboard (buttons removed — now inline)
                # Users keyboard
                "User Count", "📈 User Stats", "🔍 User Info",
                # OTP bonus keyboard
                "OTP Bonus Toggle", "💰 Set Bonus Amount", "Edit Balance", "Back to Settings",
                # Referral keyboard
                "Referral Toggle", "💰 Set Referral Bonus",
                "📤 Set Min Withdraw", "💸 Pending Withdraws",
                # Edit bot links keyboard
                "📱 NUMBER Link", "📢 CHANNEL Link",
                "Support Group Link", "📢 OTP Group Link",
                "Back to Admin Tools",
                # Extra groups keyboard
                "➕ Add Group", "🗑️ Remove Group",
                # Channel join keyboard
                "➕ Add Channel", "✏️ Edit Channel",
                "🗑️ Delete Channel", "⌛ Check Interval",
                # Required channels
                "📢 Required Channels",
                # Panel management extra buttons
                "📦 Added Panels",
                # User keyboard
                "𝑮𝒆𝒕 𝑵𝒖𝒎𝒃𝒆𝒓", "𝑨𝒗𝒂𝒊𝒍𝒂𝒃𝒍𝒆 𝑪𝒐𝒖𝒏𝒕𝒓𝒚",
                "𝑴𝒚 𝑩𝒂𝒍𝒂𝒏𝒄𝒆", "𝑾𝒊𝒕𝒉𝒅𝒓𝒂𝒘",
                "𝑻𝒐𝒑 𝑼𝒔𝒆𝒓𝒔",
            }
            if text in _PANEL_LIST_CANCEL_BUTTONS:
                context.user_data.pop('panel_list_active', None)
                context.user_data.pop('panel_list_multiple_active', None)
                context.user_data.pop('panel_list_category', None)
                context.user_data.pop('panel_view_active', None)
                context.user_data.pop('panel_list_source', None)
                # Fall through — let the normal handler process this button
            elif text in ("Back", "Back to Panel List"):
                context.user_data.pop('panel_list_active', None)
                context.user_data.pop('panel_list_multiple_active', None)
                context.user_data.pop('panel_list_category', None)
                context.user_data.pop('panel_view_active', None)
                context.user_data.pop('panel_list_source', None)
                if _is_admin(username, user_id):
                    await update.message.reply_text(
                        "Back to Admin Panel.",
                        reply_markup=get_admin_keyboard())
                return
            else:
                # Treat text as a panel name (strip emoji prefix / backticks if present)
                panel_name_lookup = _panel_name_from_label(text)
                panel = await run_db(_get_panel_by_name, panel_name_lookup)

                # Fallback: check dynamic (user-added) panels
                if not panel:
                    dp = await run_db(_get_dynamic_panel, panel_name_lookup)
                    if dp:
                        panel = {
                            'name':     dp.get('name', ''),
                            'username': dp.get('username', ''),
                            'base_url': dp.get('login_url', dp.get('stats_url', '')),
                        }

                if panel:
                    pname = panel['name']
                    context.user_data['panel_view_active'] = pname
                    context.user_data['last_panel_view']   = pname
                    context.user_data['panel_list_active'] = True
                    context.user_data.pop('panel_list_multiple_active', None)
                    is_en = bool(await run_db(_is_panel_enabled, pname))
                    db_user = _resolve_panel_user(panel, pname)
                    enabled_str = "✅ Enabled" if is_en else "🚫 Disabled"
                    is_dynamic = context.user_data.get('panel_list_source') == 'dynamic'
                    panel_kb = get_dynamic_panel_action_keyboard(is_en) if is_dynamic else get_panel_action_keyboard(is_en)
                    await update.message.reply_text(
                        f"📌 *{_md_escape(pname)}*\n\n"
                        f"┣ Status: *{enabled_str}*\n"
                        f"┣ Username: `{db_user}`\n"
                        f"┗ URL: `{_md_escape(panel['base_url'])}`\n\n"
                        "Choose an action:",
                        parse_mode='Markdown',
                        reply_markup=panel_kb
                    )
                else:
                    # Unknown input — re-show appropriate panel list
                    if context.user_data.get('panel_list_source') == 'dynamic':
                        dyn_panels = await run_db(_get_dynamic_panels)
                        btns = [KeyboardButton(_panel_label(p['name'])) for p in dyn_panels]
                        rows = [btns[i:i+2] for i in range(0, len(btns), 2)]
                        rows.append([KeyboardButton("Back to Panel List")])
                        await update.message.reply_text(
                            f"📦 *Added Panels* ({len(dyn_panels)} টি)\n\n❌ Panel পাওয়া যায়নি:",
                            parse_mode='Markdown',
                            reply_markup=ReplyKeyboardMarkup(rows, resize_keyboard=True)
                        )
                    else:
                        all_panels_db = await run_db(_get_panels)
                        all_names = [pname for pname, _m in ALL_PANEL_LIST]
                        panels = [p for p in all_panels_db if p['name'] in all_names]
                        panels.sort(key=lambda p: all_names.index(p['name']) if p['name'] in all_names else 999)
                        panel_btns = [KeyboardButton(_panel_label(p['name'])) for p in panels]
                        rows = [panel_btns[i:i+2] for i in range(0, len(panel_btns), 2)]
                        rows.append([KeyboardButton("Back to Admin Panel")])
                        await update.message.reply_text(
                            f"📌 *Panel List* ({len(panels)} panels)\n\n❌ Panel not found. Select from below:",
                            parse_mode='Markdown',
                            reply_markup=ReplyKeyboardMarkup(rows, resize_keyboard=True)
                        )
                return
        # else: panel_list_active is set but an input-awaiting state is also
        # active — fall through to the dedicated handlers below.

    # ── Panel Toggle: admin typed (or copy-pasted) a panel name to toggle it ─
    if context.user_data.get('panel_toggle_active'):
        if not _is_admin(username, user_id):
            await update.message.reply_text("❌ Unauthorized access.")
            context.user_data.pop('panel_toggle_active', None)
            return
        # Strip mono backticks (if admin copied the formatted text), surrounding
        # whitespace and any leading status emoji.
        raw = (text or "").strip()
        pname_typed = _panel_name_from_label(raw)
        match = None
        for pname, _m in ALL_PANEL_LIST:
            if pname.lower() == pname_typed.lower():
                match = pname
                break
        if not match:
            await update.message.reply_text(
                "❌ No panel found with that name.\n"
                "Copy the exact *name* from the list and try again.",
                parse_mode='Markdown',
                reply_markup=get_admin_tools_keyboard())
            return
        # Toggle the panel
        currently_enabled = bool(await run_db(_is_panel_enabled, match))
        new_state = not currently_enabled
        await run_db(_set_panel_enabled, match, new_state)
        # Notify all admins (this also covers the requested toggle notification).
        await _notify_admins_panel_toggled(context.bot, match, new_state)
        # Clear the flag and return to the Admin Tools keyboard.
        context.user_data.pop('panel_toggle_active', None)
        action_word = "Enabled ✅" if new_state else "Disabled 🚫"
        await update.message.reply_text(
            f"*{_md_escape(match)}* is now {action_word}.\n\n"
            "Press *Panel Toggle* again to toggle another panel.",
            parse_mode='Markdown',
            reply_markup=get_admin_tools_keyboard())
        return

    # ── Edit Panel Credentials: step 1 — admin typed new username ────────────
    if context.user_data.get('awaiting_cred_panel') and not context.user_data.get('awaiting_cred_username'):
        if not _is_admin(username, user_id):
            await update.message.reply_text("❌ Unauthorized access.")
            context.user_data.pop('awaiting_cred_panel', None)
            return
        pname     = context.user_data['awaiting_cred_panel']
        new_uname = (text or "").strip()
        if not new_uname:
            await update.message.reply_text(
                "❌ Username cannot be empty. Send the new username:",
                parse_mode='Markdown')
            return
        context.user_data['awaiting_cred_username'] = new_uname
        await update.message.reply_text(
            f"✅ Username: `{new_uname}`\n\n"
            f"Now send the *new password* for *{_md_escape(pname)}*:",
            parse_mode='Markdown',
        )
        return

    # ── Edit Panel Credentials: step 2 — admin typed new password ────────────
    if context.user_data.get('awaiting_cred_panel') and context.user_data.get('awaiting_cred_username'):
        if not _is_admin(username, user_id):
            await update.message.reply_text("❌ Unauthorized access.")
            context.user_data.pop('awaiting_cred_panel', None)
            context.user_data.pop('awaiting_cred_username', None)
            return
        pname     = context.user_data.pop('awaiting_cred_panel')
        new_uname = context.user_data.pop('awaiting_cred_username')
        new_pass  = (text or "").strip()
        if not new_pass:
            await update.message.reply_text(
                "❌ Password cannot be empty. Send the new password:",
                parse_mode='Markdown')
            context.user_data['awaiting_cred_panel']    = pname
            context.user_data['awaiting_cred_username'] = new_uname
            return
        # Save to DB
        ok = await run_db(_update_panel_credentials, pname, new_uname, new_pass)
        if not ok:
            await update.message.reply_text(
                f"❌ Panel *{_md_escape(pname)}* not found in database.",
                parse_mode='Markdown',
                reply_markup=get_admin_keyboard())
            return
        # Clear session on the matching monitor so it re-logins with new creds
        for mname, mon in ALL_PANEL_LIST:
            if mname == pname:
                try:
                    mon.logged_in = False
                    mon.session   = None
                except Exception:
                    pass
                break
        # Notify all admins
        try:
            all_admins = await run_db(_get_all_admins_with_details)
            notify_text = (
                f"✏️ *Panel Credentials Updated*\n\n"
                f"*{_md_escape(pname)}*\n"
                f"New Username: `{new_uname}`\n\n"
                f"🔄 Session cleared — panel will re-login automatically."
            )
            for adm in all_admins:
                adm_uid = adm.get("user_id")
                if adm_uid and adm_uid != user_id:
                    try:
                        await context.bot.send_message(chat_id=adm_uid, text=notify_text, parse_mode='Markdown')
                    except Exception:
                        pass
        except Exception:
            pass
        # Return to panel list (mono text)
        context.user_data['panel_list_active'] = True
        context.user_data.pop('panel_view_active', None)
        all_panels_db = await run_db(_get_panels)
        all_names = [pname for pname, _m in ALL_PANEL_LIST]
        panels_list = [p for p in all_panels_db if p['name'] in all_names]
        panels_list.sort(key=lambda p: all_names.index(p['name']) if p['name'] in all_names else 999)
        lines = [f"`{p['name']}`" for p in panels_list]
        await update.message.reply_text(
            f"✅ Credentials updated for *{_md_escape(pname)}*.\n\n"
            f"📌 *Panel List* ({len(panels_list)} panels)\n\n"
            + "\n".join(lines)
            + "\n\n_Type the exact panel name to see its ✉️ Latest Message._",
            parse_mode='Markdown',
            reply_markup=get_admin_keyboard(),
        )
        return

    # ── Edit Bot Links: admin typed the new URL ───────────────────────────────
    if context.user_data.get('awaiting_edit_bot_link'):
        if not _is_admin(username, user_id):
            await update.message.reply_text("❌ Unauthorized access.")
            context.user_data.pop('awaiting_edit_bot_link', None)
            return
        which = context.user_data.pop('awaiting_edit_bot_link')
        new_url = (text or "").strip()
        if not new_url.startswith("http"):
            await update.message.reply_text(
                "❌ Invalid link. Please send a link starting with `https://` or `http://`.",
                parse_mode='Markdown')
            context.user_data['awaiting_edit_bot_link'] = which
            return
        key_map = {
            "number":        "otp_btn_number",
            "channel_otp":   "otp_btn_channel",
            "support_group": "bot_link_support",
            "otp_group":     "bot_link_getotp",
        }
        label_map = {
            "number":        "📱 NUMBER",
            "channel_otp":   "📢 CHANNEL",
            "support_group": "Support Group",
            "otp_group":     "📢 OTP Group",
        }
        await run_db(_set_setting, key_map[which], new_url)
        lnk_number   = await run_db(_get_setting, "otp_btn_number",   "")
        lnk_channel  = await run_db(_get_setting, "otp_btn_channel",  "")
        lnk_support  = await run_db(_get_setting, "bot_link_support", "")
        lnk_otpgroup = await run_db(_get_setting, "bot_link_getotp",   OTP_GROUP_LINK)
        await update.message.reply_text(
            f"✅ *{label_map[which]}* link updated!\n\n"
            f"🔗 New link: `{new_url}`\n\n"
            f"📌 *Current Links:*\n"
            f"📱 NUMBER: `{lnk_number or 'Not set'}`\n"
            f"📢 CHANNEL: `{lnk_channel or 'Not set'}`\n"
            f"Support Group: `{lnk_support or 'Not set'}`\n"
            f"📢 OTP Group: `{lnk_otpgroup}`",
            parse_mode='Markdown',
            reply_markup=get_edit_bot_links_keyboard())
        return

    # ── Service Manager: admin types text commands ───────────────────────────
    _OTHER_ACTIVE_STATES = (
        'awaiting_number_limit', 'awaiting_otp_bonus_amount', 'awaiting_otp_daily_limit',
        'awaiting_country_otp_bonus', 'awaiting_country_otp_bonus_amount',
        'awaiting_ref_bonus', 'awaiting_balance_user_id', 'awaiting_balance_amount',
        'awaiting_min_withdraw', 'awaiting_broadcast_message',
        'awaiting_new_country_name', 'awaiting_reset_country_name',
        'awaiting_add_numbers_country', 'awaiting_country_name',
        'awaiting_notify_time', 'awaiting_numbers_file', 'awaiting_admin_username',
        'awaiting_new_admin', 'awaiting_reset_users_confirm',
        'awaiting_extra_group_id', 'awaiting_extra_group_remove_id',
        'awaiting_edit_bot_link',
        'awaiting_cred_panel', 'awaiting_cred_username',
        'awaiting_retry_login_panel', 'awaiting_reload_interval_panel',
        'awaiting_reload_interval_seconds', 'awaiting_session_cleanup_panel',
        'awaiting_panel_retry', 'panel_toggle_active',
        'awaiting_delete_country_name', 'awaiting_specific_number_delete',
        'awaiting_user_info_id',
        'awaiting_withdraw_method', 'awaiting_withdraw_account', 'awaiting_withdraw_amount',
        'panel_list_active', 'panel_list_multiple_active',
    )
    # ── Broadcast: admin typed the message to send to all users ─────────────
    if context.user_data.get('awaiting_broadcast_message'):
        if not _is_admin(username, user_id):
            await update.message.reply_text("❌ Unauthorized access.")
            context.user_data.pop('awaiting_broadcast_message', None)
            return
        context.user_data.pop('awaiting_broadcast_message', None)
        await _run_broadcast(update, context)
        return

    # ── Retry Login: admin typed a failed-panel name to manually re-login ────
    if context.user_data.get('awaiting_retry_login_panel'):
        if not _is_admin(username, user_id):
            await update.message.reply_text("❌ Unauthorized access.")
            context.user_data.pop('awaiting_retry_login_panel', None)
            return
        typed = _panel_name_from_label((text or "").strip())
        match = None
        match_m = None
        for pname, m in ALL_PANEL_LIST:
            if pname.lower() == typed.lower():
                match = pname
                match_m = m
                break
        if not match:
            await update.message.reply_text(
                "❌ No panel found with that name.\n"
                "Copy the exact name from the list and try again.",
                parse_mode='Markdown')
            return
        # Verify the panel actually needs a retry (not already logged in)
        statuses = await run_db(_get_all_panel_statuses)
        s = next((x for x in (statuses or []) if x['panel_name'] == match), None)
        if s and s.get('logged_in'):
            context.user_data.pop('awaiting_retry_login_panel', None)
            await update.message.reply_text(
                f"ℹ️ *{match}* is already logged in successfully.",
                parse_mode='Markdown',
                reply_markup=get_admin_tools_keyboard())
            return

        await update.message.reply_text(
            f"⌛ Attempting login for *{match}*…",
            parse_mode='Markdown')

        def _do_retry_login(monitor):
            # Reset manual-only so the background loop will resume after success
            try:
                monitor._manual_only = False
            except Exception:
                pass
            try:
                ok = bool(monitor._login())
            except Exception:
                ok = False
            if ok and hasattr(monitor, '_extract_sesskey'):
                try:
                    monitor._extract_sesskey()
                except Exception:
                    pass
            return ok

        try:
            ok = await asyncio.to_thread(_do_retry_login, match_m)
        except Exception:
            ok = False

        try:
            if ok:
                await run_db(_update_panel_status, match, True, None, None)
            else:
                await run_db(_update_panel_status, match, False, None,
                             'Manual retry login failed')
        except Exception:
            pass

        if ok:
            # Notify ALL admins about the successful login (per requirement)
            try:
                await _notify_admins_login_success(context.bot, match)
            except Exception:
                pass
            # Refresh the failed-panels view so admin can pick the next one
            new_msg, has_failed = await _build_retry_login_view()
            if has_failed:
                context.user_data['awaiting_retry_login_panel'] = True
            else:
                context.user_data.pop('awaiting_retry_login_panel', None)
            await update.message.reply_text(
                f"✅ *{match}* logged in successfully.\n"
                f"_All admins have been notified._\n\n" + new_msg,
                parse_mode='Markdown',
                reply_markup=get_admin_tools_keyboard())
        else:
            # Keep the awaiting flag so the admin can try another panel
            await update.message.reply_text(
                f"❌ *{match}* login failed.\n"
                f"Try again later, or send another panel name.",
                parse_mode='Markdown')
        return

    # ── Reload Interval flow: step 1 — admin typed a panel name ──────────────
    if context.user_data.get('awaiting_reload_interval_panel'):
        if not _is_admin(username, user_id):
            await update.message.reply_text("❌ Unauthorized access.")
            context.user_data.pop('awaiting_reload_interval_panel', None)
            return
        typed = _panel_name_from_label((text or "").strip())
        match = None
        match_idx = None
        for idx, (pname, _m) in enumerate(ALL_PANEL_LIST):
            if pname.lower() == typed.lower():
                match = pname
                match_idx = idx
                break
        if not match:
            await update.message.reply_text(
                "❌ No panel with that name. Copy the exact name from the list and try again.",
                parse_mode='Markdown')
            return
        cur = await run_db(_get_panel_interval, match)
        cur_secs = cur if cur is not None else 0
        cur_txt = f"{cur_secs}s" if cur_secs else "default"
        context.user_data.pop('awaiting_reload_interval_panel', None)
        context.user_data['awaiting_reload_interval_seconds'] = (match, match_idx)
        await update.message.reply_text(
            f"🔄 *Set Reload Interval — {match}*\n\n"
            f"Current interval: `{cur_txt}`\n\n"
            "Send the new interval in *seconds* (whole number), e.g. `30`.\n\n"
            "After every N seconds the bot will reload this panel and check "
            "for new SMS messages.",
            parse_mode='Markdown')
        return

    # ── Reload Interval flow: step 2 — admin typed seconds ───────────────────
    if context.user_data.get('awaiting_reload_interval_seconds'):
        if not _is_admin(username, user_id):
            await update.message.reply_text("❌ Unauthorized access.")
            context.user_data.pop('awaiting_reload_interval_seconds', None)
            return
        pname, idx = context.user_data['awaiting_reload_interval_seconds']
        try:
            seconds = int((text or "").strip())
            if seconds < 1:
                raise ValueError("too low")
        except ValueError:
            await update.message.reply_text(
                "❌ Invalid value. Please send a whole number (seconds), "
                "e.g. `30`.",
                parse_mode='Markdown')
            return
        context.user_data.pop('awaiting_reload_interval_seconds', None)
        # Persist to DB
        await run_db(_set_panel_interval, pname, seconds)
        # Apply live to the running monitor
        try:
            _, m = ALL_PANEL_LIST[idx]
            if hasattr(m, 'set_interval'):
                m.set_interval(seconds)
            else:
                m.interval = seconds
        except Exception:
            pass
        # Notify all admins
        try:
            from database import _get_all_admins_with_details
            admins = _get_all_admins_with_details()
            notify_text = (
                f"🔄 *Reload Interval Updated*\n\n"
                f"*{pname}* will now reload every *{seconds}* second(s).\n"
                f"The change took effect immediately."
            )
            for admin in admins:
                uid = admin.get("user_id")
                if uid:
                    try:
                        await context.bot.send_message(
                            chat_id=uid, text=notify_text,
                            parse_mode='Markdown')
                    except Exception:
                        pass
        except Exception:
            pass
        # Return to panel view if that's where we came from
        if context.user_data.get('panel_view_active'):
            is_en = bool(await run_db(_is_panel_enabled, pname))
            await update.message.reply_text(
                f"✅ *{_md_escape(pname)}* reload interval set to `{seconds}s`.\n"
                f"_All admins have been notified._",
                parse_mode='Markdown',
                reply_markup=get_panel_action_keyboard(is_en))
        else:
            await update.message.reply_text(
                f"✅ *{pname}* reload interval set to `{seconds}s`.\n"
                f"_All admins have been notified._",
                parse_mode='Markdown',
                reply_markup=get_admin_tools_keyboard())
        return

    # ── Session Cleanup: admin sent a panel name ──────────────────────────────
    if context.user_data.get('awaiting_session_cleanup_panel'):
        if not _is_admin(username, user_id):
            await update.message.reply_text("❌ Unauthorized access.")
            context.user_data.pop('awaiting_session_cleanup_panel', None)
            return
        # Match the typed name against known panels (case-insensitive, trimmed)
        typed = _panel_name_from_label((text or "").strip())
        match = None
        for pname, m in ALL_PANEL_LIST:
            if pname.lower() == typed.lower():
                match = (pname, m)
                break
        if not match:
            await update.message.reply_text(
                f"❌ *No panel found with name:* `{typed}`\n\n"
                "Copy the exact name from the list and try again.",
                parse_mode='Markdown')
            return

        pname, m = match
        context.user_data.pop('awaiting_session_cleanup_panel', None)

        is_en = await run_db(_is_panel_enabled, pname)
        if not is_en:
            await update.message.reply_text(
                f"🚫 *{pname}* is currently disabled.",
                parse_mode='Markdown',
                reply_markup=get_admin_tools_keyboard())
            return

        await update.message.reply_text(
            f"⌛ Clearing session, cookies and sesskey for *{pname}*…",
            parse_mode='Markdown')

        def _do_session_clean(monitor):
            """Wipe session/cookies/sesskey/csrf etc. and put the monitor into
            manual-only mode so it does NOT auto re-login. The admin must use
            ⌛ Retry Interval to log it back in."""
            # Clear common session-related attributes on the monitor object
            for attr in ('session', 'sesskey', 'cookies', '_csrf', '_token',
                         'csrf_token', 'auth_token', '_session_id',
                         '_cookie_jar'):
                try:
                    if hasattr(monitor, attr):
                        setattr(monitor, attr, None)
                except Exception:
                    pass
            try:
                if hasattr(monitor, 'logged_in'):
                    monitor.logged_in = False
            except Exception:
                pass
            # Reset any cached "seen" set so first poll resyncs cleanly
            try:
                if hasattr(monitor, '_seen_keys'):
                    monitor._seen_keys.clear()
            except Exception:
                pass
            # Reset "first poll" so it doesn't spam after re-login
            try:
                if hasattr(monitor, '_is_first_poll'):
                    monitor._is_first_poll = True
            except Exception:
                pass
            # Put monitor in manual-only mode — no automatic login until admin
            # explicitly triggers it via ⌛ Retry Interval.
            try:
                monitor._manual_only = True
            except Exception:
                pass
            return True

        try:
            await asyncio.to_thread(_do_session_clean, m)
        except Exception:
            pass

        try:
            await run_db(_update_panel_status, pname, False, None,
                         'Session cleared — awaiting manual re-login')
        except Exception:
            pass

        await _notify_admins_session_cleaned(context.bot, pname)

        result_msg = (
            f"✅ *Session Cleanup Done*\n\n"
            f"*{pname}*\n"
            f"  • Session: cleared\n"
            f"  • Cookie: cleared\n"
            f"  • Sesskey: cleared\n"
            f"  • Auto re-login: 🚫 disabled\n\n"
            f"To login again, go to ⌛ *Retry Interval* and send this panel's name.\n\n"
            f"_All admins have been notified._"
        )
        await update.message.reply_text(
            result_msg,
            parse_mode='Markdown',
            reply_markup=get_admin_tools_keyboard())
        return

    # ── Panel login-retry interval set flow ───────────────────────────────────
    if context.user_data.get('awaiting_panel_retry'):
        if not _is_admin(username, user_id):
            await update.message.reply_text("❌ Unauthorized access.")
            return
        pname, idx = context.user_data.pop('awaiting_panel_retry')
        raw = text.strip().lower()
        seconds = None
        try:
            if raw.endswith('h'):
                seconds = int(float(raw[:-1]) * 3600)
            elif raw.endswith('m'):
                seconds = int(float(raw[:-1]) * 60)
            elif raw.endswith('s'):
                seconds = int(float(raw[:-1]))
            else:
                seconds = int(raw)
            if seconds < 1:
                raise ValueError("too low")
        except (ValueError, TypeError):
            await update.message.reply_text(
                "❌ Invalid value. Example: `30` (seconds), `5m` (minutes), `2h` (hours).",
                parse_mode='Markdown')
            context.user_data['awaiting_panel_retry'] = (pname, idx)
            return
        await run_db(_set_panel_retry_interval, pname, seconds)
        # Apply live to the running monitor
        try:
            _, m = ALL_PANEL_LIST[idx]
            m.set_retry_interval(seconds)
        except Exception:
            pass
        if seconds >= 3600:
            human = f"{seconds // 3600}h"
        elif seconds >= 60:
            human = f"{seconds // 60}m"
        else:
            human = f"{seconds}s"
        await update.message.reply_text(
            f"✅ *{pname}* retry interval set to: `{human}` ({seconds}s).\n\n"
            "This panel will retry login every `" + human + "` after a failure. "
            "All admins will be notified as soon as login succeeds.",
            parse_mode='Markdown',
            reply_markup=get_admin_keyboard())
        return

    # ── Panel SMS-poll interval set flow ──────────────────────────────────────
    if context.user_data.get('awaiting_panel_interval'):
        if not _is_admin(username, user_id):
            await update.message.reply_text("❌ Unauthorized access.")
            return
        pname, idx = context.user_data.pop('awaiting_panel_interval')
        try:
            seconds = int(text.strip())
            if seconds < 1:
                raise ValueError("too low")
        except ValueError:
            await update.message.reply_text(
                "❌ Invalid value. Please send a whole number (seconds), e.g. `30`.",
                parse_mode='Markdown')
            context.user_data['awaiting_panel_interval'] = (pname, idx)
            return
        await run_db(_set_panel_interval, pname, seconds)
        # Apply live to the running monitor
        try:
            _, m = ALL_PANEL_LIST[idx]
            m.set_interval(seconds)
        except Exception:
            pass
        await update.message.reply_text(
            f"✅ *{pname}* polling interval updated to `{seconds}s`.\n\n"
            "The change takes effect immediately — no restart needed.",
            parse_mode='Markdown',
            reply_markup=get_admin_keyboard())
        return

    if context.user_data.get('awaiting_country_name'):
        if not _is_admin(username, user_id):
            await update.message.reply_text("❌ Unauthorized access.")
            return
        _plain_name = _resolve_country_name(text.strip())
        context.user_data['current_country_name']  = _plain_name
        context.user_data['awaiting_country_name'] = False
        context.user_data['awaiting_numbers_file'] = True
        await update.message.reply_text(
            f"✅ Country: *{_plain_name}*\n\nNow send a TXT file with phone numbers (one per line):",
            parse_mode='Markdown')
        return

    if context.user_data.get('awaiting_notify_time'):
        if not _is_admin(username, user_id):
            await update.message.reply_text("❌ Unauthorized access.")
            return
        try:
            minutes = int(text.strip())
            if minutes < 1:
                raise ValueError
        except ValueError:
            await update.message.reply_text(
                "❌ সঠিক সংখ্যা লিখুন। (উদাহরণ: 5, 10, 15, 30)",
                reply_markup=get_admin_tools_keyboard())
            return
        context.user_data.pop('awaiting_notify_time', None)
        await run_db(_set_notify_window, minutes)
        await update.message.reply_text(
            f"✅ *নোটিফাই টাইম আপডেট হয়েছে।*\n\n"
            f"⏰ নতুন সময়: *{minutes} মিনিট*\n\n"
            f"এখন থেকে user নাম্বার নেওয়ার পর {minutes} মিনিটের মধ্যে OTP আসলে তাকে পাঠানো হবে।",
            parse_mode='Markdown',
            reply_markup=get_admin_tools_keyboard())
        return

    if context.user_data.get('awaiting_reset_country_name'):
        if not _is_admin(username, user_id):
            await update.message.reply_text("❌ Unauthorized access.")
            return
        target = text.strip()
        if not target:
            await update.message.reply_text(
                "❌ Country নাম লিখুন।",
                parse_mode='Markdown',
                reply_markup=get_manage_numbers_keyboard())
            return
        countries = await run_db(_get_countries)
        found     = next(((r[0], r[1]) for r in countries
                          if r[1].lower() == target.lower()
                          or _strip_flag_prefix(r[1]).lower() == target.lower()), None)
        if not found:
            await update.message.reply_text(
                f"❌ '{target}' নামে কোনো country পাওয়া যায়নি। আবার চেষ্টা করুন।",
                reply_markup=get_manage_numbers_keyboard())
            return
        context.user_data.pop('awaiting_reset_country_name', None)
        cid, cname = found
        deleted = await run_db(_delete_country_numbers, cid)
        await update.message.reply_text(
            f"🗑️ *{cname}* — সব নাম্বার DELETE হয়েছে।\n\n"
            f"✅ মোট *{deleted}* টি নাম্বার বট থেকে সরানো হয়েছে।",
            parse_mode='Markdown',
            reply_markup=get_manage_numbers_keyboard())
        return

    if context.user_data.get('awaiting_add_numbers_country'):
        if not _is_admin(username, user_id):
            await update.message.reply_text("❌ Unauthorized access.")
            return
        countries = await run_db(_get_countries)
        raw_input = text.strip()

        # First try exact match (full name or stripped name)
        found = next(((r[0], r[1]) for r in countries
                      if r[1].lower() == raw_input.lower()
                      or _strip_flag_prefix(r[1]).lower() == raw_input.lower()), None)

        # If no exact match, resolve ISO code / phone code / alias and try again
        if not found:
            resolved = _resolve_country_name(raw_input)
            if resolved != raw_input:
                resolved_plain = _strip_flag_prefix(resolved).lower()
                found = next(((r[0], r[1]) for r in countries
                              if _strip_flag_prefix(r[1]).lower() == resolved_plain
                              or r[1].lower() == resolved.lower()), None)

        if not found:
            await update.message.reply_text(
                f"❌ Country *{raw_input}* not found. "
                f"Enter the country name, ISO code (e.g. JP) or phone code (e.g. +81).",
                parse_mode='Markdown',
                reply_markup=get_manage_numbers_keyboard())
            return
        context.user_data.pop('awaiting_add_numbers_country', None)
        cid, cname = found
        context.user_data['edit_country_id']   = cid
        context.user_data['edit_country_name'] = cname
        await update.message.reply_text(
            f"✅ Country: *{cname}*\n\n"
            f"Now send a TXT file with phone numbers (one per line):",
            parse_mode='Markdown',
            reply_markup=get_manage_numbers_keyboard())
        return

    if context.user_data.get('awaiting_new_country_name'):
        if not _is_admin(username, user_id):
            await update.message.reply_text("❌ Unauthorized access.")
            return
        raw = text.strip()
        if not raw:
            await update.message.reply_text(
                "❌ Country name cannot be empty. Enter again or send /cancel.")
            return

        # Delete command: "delete <country name>"
        if raw.lower().startswith("delete "):
            target = raw[7:].strip()
            if not target:
                await update.message.reply_text(
                    "❌ Please write the country name after `delete`.\n"
                    "Example: `delete Bangladesh`",
                    parse_mode='Markdown')
                return
            countries = await run_db(_get_countries)
            found = next(
                ((r[0], r[1]) for r in countries
                 if r[1].lower() == target.lower()
                 or _strip_flag_prefix(r[1]).lower() == target.lower()),
                None)
            if not found:
                await update.message.reply_text(
                    f"❌ Country *{target}* not found. Check the name and try again.",
                    parse_mode='Markdown')
                return

            cid, cname = found
            total, _avail = await run_db(_get_numbers_count_by_country, cid)
            await run_db(_delete_country, cid)

            # Stay in the Add Country flow — show updated stats with delete notice
            context.user_data['awaiting_new_country_name'] = True
            countries_upd = await run_db(_get_countries)
            counts_upd    = await run_db(_get_all_country_counts)
            lines = [f"🗑️ *{cname}* deleted. ({total} numbers removed)", ""]
            lines += ["*🌐 Country Manager*", ""]
            if not countries_upd:
                lines.append("_No countries added yet._")
                lines.append("")
            else:
                grand_total = grand_avail = 0
                for cid2, cname2 in countries_upd:
                    total2, avail2 = counts_upd.get(cid2, (0, 0))
                    used2 = total2 - avail2
                    grand_total += total2
                    grand_avail += avail2
                    lines.append(
                        f"`{cname2}`\n"
                        f"  ➕ Added: `{total2}`  ✅ Available: `{avail2}`  🔴 Used: `{used2}`"
                    )
                    lines.append("`" + "─" * 30 + "`")
                grand_used = grand_total - grand_avail
                lines.append(
                    f"\n📌 *Total Countries:* `{len(countries_upd)}`\n"
                    f"*Total Numbers:* `{grand_total}`\n"
                    f"✅ *Available:* `{grand_avail}`  🔴 *Used:* `{grand_used}`"
                )
                lines.append("")
            lines.append("✏️ Type a country name (e.g. Bangladesh), ISO code (BD) or phone code (+880).")
            lines.append("🗑️ To delete: `delete` [country name]")
            msg = "\n".join(lines)
            if len(msg) > 4000:
                msg = msg[:3990] + "\n`…`"
            await update.message.reply_text(msg, parse_mode='Markdown',
                                            reply_markup=get_manage_numbers_keyboard())

            # Notify all admins about the deletion
            try:
                admins = await run_db(_get_all_admins_with_details)
                admin_name = update.effective_user.full_name or username or str(user_id)
                notify_text = (
                    f"🗑️ *Country Deleted*\n\n"
                    f"🌍 Country: *{cname}*\n"
                    f"📞 Numbers removed: `{total}`\n"
                    f"Deleted by: *{admin_name}*\n"
                    f"⌛ Time: {datetime.now().strftime('%d %b %Y, %I:%M %p')}"
                )
                for adm in admins:
                    adm_uid = adm.get('user_id')
                    if adm_uid and adm_uid != user_id:
                        try:
                            await context.bot.send_message(
                                chat_id=adm_uid,
                                text=notify_text,
                                parse_mode='Markdown')
                        except Exception:
                            pass
            except Exception:
                pass
            return

        cname = _resolve_country_name(raw)
        existing_id = await run_db(_get_country_id_by_name, cname)
        if not existing_id:
            existing_id = await run_db(_get_country_id_by_name, _strip_flag_prefix(cname))
        if existing_id:
            await update.message.reply_text(
                f"⚠️ Country *{cname}* already exists. Enter a different name or send /cancel.",
                parse_mode='Markdown')
            return

        await run_db(_add_country, cname)

        # Stay in the Add Country flow — show updated stats with success notice
        context.user_data['awaiting_new_country_name'] = True
        countries_upd = await run_db(_get_countries)
        counts_upd    = await run_db(_get_all_country_counts)
        lines = [f"✅ *{cname}* added successfully!", ""]
        lines += ["*🌐 Country Manager*", ""]
        if not countries_upd:
            lines.append("_No countries added yet._")
            lines.append("")
        else:
            grand_total = grand_avail = 0
            for cid2, cname2 in countries_upd:
                total2, avail2 = counts_upd.get(cid2, (0, 0))
                used2 = total2 - avail2
                grand_total += total2
                grand_avail += avail2
                lines.append(
                    f"`{cname2}`\n"
                    f"  ➕ Added: `{total2}`  ✅ Available: `{avail2}`  🔴 Used: `{used2}`"
                )
                lines.append("`" + "─" * 30 + "`")
            grand_used = grand_total - grand_avail
            lines.append(
                f"\n📌 *Total Countries:* `{len(countries_upd)}`\n"
                f"*Total Numbers:* `{grand_total}`\n"
                f"✅ *Available:* `{grand_avail}`  🔴 *Used:* `{grand_used}`"
            )
            lines.append("")
        lines.append("✏️ Type a country name (e.g. Bangladesh), ISO code (BD) or phone code (+880).")
        lines.append("🗑️ To delete: `delete` [country name]")
        msg = "\n".join(lines)
        if len(msg) > 4000:
            msg = msg[:3990] + "\n`…`"
        await update.message.reply_text(msg, parse_mode='Markdown',
                                        reply_markup=get_manage_numbers_keyboard())

        # Notify all admins about the new country
        try:
            admins = await run_db(_get_all_admins_with_details)
            admin_name = update.effective_user.full_name or username or str(user_id)
            notify_text = (
                f"🌍 *New Country Added*\n\n"
                f"✅ Country: *{cname}*\n"
                f"Added by: *{admin_name}*\n"
                f"⌛ Time: {datetime.now().strftime('%d %b %Y, %I:%M %p')}"
            )
            for adm in admins:
                adm_uid = adm.get('user_id')
                if adm_uid and adm_uid != user_id:
                    try:
                        await context.bot.send_message(
                            chat_id=adm_uid,
                            text=notify_text,
                            parse_mode='Markdown')
                    except Exception:
                        pass
        except Exception:
            pass
        return

    if context.user_data.get('awaiting_delete_country_name'):
        if not _is_admin(username, user_id):
            await update.message.reply_text("❌ Unauthorized access.")
            return
        context.user_data['awaiting_delete_country_name'] = False
        countries = await run_db(_get_countries)
        found     = next(((r[0], r[1]) for r in countries
                          if r[1].lower() == text.lower()
                          or _strip_flag_prefix(r[1]).lower() == text.lower()), None)
        if not found:
            await update.message.reply_text(
                f"❌ Country '{text}' not found. Check the name and try again.",
                reply_markup=get_manage_numbers_keyboard())
            return
        cid, cname = found
        deleted, removed = await run_db(_delete_country, cid)
        status_line = "🌍 Country: *Removed*" if removed else "🌍 Country: *Kept*"
        await update.message.reply_text(
            f"✅ *Deleted Successfully*\n\n"
            f"🌍 Country: *{cname}*\n"
            f"🗑️ Removed: *{deleted}* numbers\n"
            f"{status_line}",
            parse_mode='Markdown',
            reply_markup=get_manage_numbers_keyboard())
        return

    if context.user_data.get('awaiting_specific_number_delete'):
        if not _is_admin(username, user_id):
            await update.message.reply_text("❌ Unauthorized access.")
            return
        context.user_data['awaiting_specific_number_delete'] = False
        deleted = await run_db(_delete_number, text)
        if deleted:
            await update.message.reply_text(
                f"✅ Number `{text}` deleted successfully!",
                parse_mode='Markdown',
                reply_markup=get_manage_numbers_keyboard())
        else:
            await update.message.reply_text(
                f"❌ Number `{text}` not found!",
                parse_mode='Markdown',
                reply_markup=get_manage_numbers_keyboard())
        return

    if context.user_data.get('awaiting_new_admin'):
        if not _is_admin(username, user_id):
            await update.message.reply_text("❌ Unauthorized access.")
            return
        context.user_data['awaiting_new_admin'] = False
        uid_text = text.strip()

        async def _send_manage_admins_view(intro: str):
            """Re-render the unified Manage Admins inline view."""
            admins_list = await run_db(_get_all_admins_with_details)
            from config import PROTECTED_ADMINS, PROTECTED_ADMIN_IDS
            rows = []
            for adm in admins_list:
                db_uname = adm['username'] or ''
                uid_val  = adm.get('user_id')
                fname_   = adm.get('first_name') or 'Unknown'
                is_prot  = db_uname in PROTECTED_ADMINS or (uid_val and uid_val in PROTECTED_ADMIN_IDS)
                display_ = f"{fname_} (UID: {uid_val})" if uid_val else fname_
                if is_prot:
                    rows.append([InlineKeyboardButton(f"🛡️ {display_}", callback_data=f"protected_admin_{db_uname}")])
                else:
                    rows.append([InlineKeyboardButton(f"❌ {display_}", callback_data=f"admin_info_{db_uname}")])
            rows.append([InlineKeyboardButton("➕ Add New Admin", callback_data="add_admin_inline")])
            rows.append([InlineKeyboardButton("◀ Back to Admin Panel", callback_data="back_to_admin")])
            await update.message.reply_text(
                intro + "\n\n*Manage Admins*\n\n"
                "নিচে বর্তমান এডমিনদের তালিকা।\n"
                "❌ বাটনে চাপলে রিমুভ করা যাবে।\n"
                "➕ বাটনে চাপলে নতুন এডমিন যোগ করা যাবে।",
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup(rows),
            )

        if not uid_text.isdigit():
            await _send_manage_admins_view("❌ *Invalid Input!* শুধু সংখ্যা লিখুন। উদাহরণ: `1234567890`")
            return
        ok, row, reason = await run_db(_add_admin_by_uid, int(uid_text))
        if reason == "user_not_found":
            await _send_manage_admins_view(
                f"❌ *User Not Found!*\n\nUID `{uid_text}` ডেটাবেসে নেই।\n"
                f"ওই ইউজারকে আগে `/start` পাঠাতে বলুন।"
            )
        elif reason == "already_admin":
            fname = row.get('first_name') or ''
            await _send_manage_admins_view(
                f"⚠️ *Already an Admin!*\n\nName: *{fname}*\nUID: `{row.get('user_id')}`"
            )
        else:
            fname = row.get('first_name') or ''
            lname = row.get('last_name') or ''
            full_name = f"{fname} {lname}".strip()
            await _send_manage_admins_view(
                f"✅ *Admin Added Successfully!*\n\n"
                f"┣━━━━━━━━━━━━━━━━━━━━━\n"
                f"┃ Name: *{full_name}*\n"
                f"┃ UID: `{row.get('user_id')}`\n"
                f"┗━━━━━━━━━━━━━━━━━━━━━"
            )
        return

    # ── Min withdraw amount input ─────────────────────────────────────────────
    if context.user_data.get('awaiting_min_withdraw'):
        if not _is_admin(username, user_id):
            await update.message.reply_text("❌ Unauthorized access.")
            return
        try:
            amount = float(text.replace(',', '.').strip())
            if amount < 0:
                raise ValueError
            await run_db(_set_min_withdraw, amount)
            context.user_data['awaiting_min_withdraw'] = False
            await update.message.reply_text(
                f"✅ *Minimum Withdraw Updated!*\n\n"
                f"📤 From now on, the minimum withdraw is *৳ {amount:.2f}*.",
                parse_mode='Markdown',
                reply_markup=get_admin_keyboard()
            )
        except ValueError:
            await update.message.reply_text("❌ Enter a valid number. (Example: 50 or 100)")
        return

    # ── Range Search: country name input ─────────────────────────────────────
    if context.user_data.get('awaiting_range_search'):
        context.user_data['awaiting_range_search'] = False
        query_name = text.strip()
        # Find country by exact name (case-insensitive), then partial match fallback
        country_id = await run_db(_get_country_id_by_name, query_name)
        if country_id is None:
            # Partial match: search all countries for one containing the query
            all_countries = await run_db(_get_countries)
            matches = [(r[0], r[1]) for r in all_countries
                       if query_name.lower() in r[1].lower()]
            if len(matches) == 1:
                country_id   = matches[0][0]
                country_name = matches[0][1]
            elif len(matches) > 1:
                names = '\n'.join(f"• {r[1]}" for r in matches[:10])
                await update.message.reply_text(
                    f"⚠️ *একাধিক Country পাওয়া গেছে:*\n\n{names}\n\n"
                    f"আরও নির্দিষ্ট নাম লিখুন।",
                    parse_mode='Markdown',
                    reply_markup=get_user_keyboard(),
                )
                return
            else:
                await update.message.reply_text(
                    f"❌ *'{query_name}'* নামে কোনো Country পাওয়া যায়নি।\n\n"
                    f"সঠিক নাম লিখুন অথবা *𝑨𝒗𝒂𝒊𝒍𝒂𝒃𝒍𝒆 𝑪𝒐𝒖𝒏𝒕𝒓𝒚* বাটনে দেখুন।",
                    parse_mode='Markdown',
                    reply_markup=get_user_keyboard(),
                )
                return
        else:
            all_countries = await run_db(_get_countries)
            country_name = next((r[1] for r in all_countries if r[0] == country_id), query_name)

        limit   = await run_db(_get_number_limit)
        numbers = await run_db(_get_available_numbers_by_country, country_id, limit)
        if not numbers:
            await update.message.reply_text(
                f"❌ *{country_name}* এ এখন কোনো নম্বর নেই।\n\n"
                f"পরে আবার চেষ্টা করুন।",
                parse_mode='Markdown',
                reply_markup=get_user_keyboard(),
            )
            return
        for num in numbers:
            await run_db(_assign_number_to_user, user_id, num, country_id)
        otp_link = await run_db(_get_setting, "bot_link_getotp", OTP_GROUP_LINK)
        markup   = country_number_keyboard(country_id, otp_link, numbers=numbers)
        _flag    = _get_flag_for_country(country_name)
        _prefix  = f"{_flag} " if _flag else ""
        await update.message.reply_text(
            f"{_prefix}*{country_name}*\n\n"
            f"নিচের নম্বর বাটনে চাপলে কপি হবে:\n\n"
            f"⌛ OTP এর জন্য অপেক্ষা করুন...",
            parse_mode='Markdown',
            reply_markup=markup,
        )
        return

    # ── Withdraw: account number input ────────────────────────────────────────
    if context.user_data.get('awaiting_withdraw_account'):
        account = text.strip()
        method  = context.user_data.get('withdraw_method', '')
        context.user_data['awaiting_withdraw_account'] = False
        context.user_data['withdraw_account']          = account
        context.user_data['awaiting_withdraw_amount']  = True
        balance = await run_db(_get_user_balance, user_id)
        min_wd  = await run_db(_get_min_withdraw)
        await update.message.reply_text(
            f"💰 *Enter Withdraw Amount*\n\n"
            f"📱 Method: *{method}*\n"
            f"📞 Account: `{account}`\n"
            f"💵 Your balance: *৳ {balance:.2f}*\n"
            f"📤 Minimum: *৳ {min_wd:.2f}*\n\n"
            f"Enter how much you want to withdraw:",
            parse_mode='Markdown'
        )
        return

    # ── Withdraw: amount input ────────────────────────────────────────────────
    if context.user_data.get('awaiting_withdraw_amount'):
        method  = context.user_data.get('withdraw_method', '')
        account = context.user_data.get('withdraw_account', '')
        try:
            amount  = float(text.replace(',', '.').strip())
            balance = await run_db(_get_user_balance, user_id)
            min_wd  = await run_db(_get_min_withdraw)
            if amount < min_wd:
                await update.message.reply_text(
                    f"❌ Minimum withdraw amount is *৳ {min_wd:.2f}*. Enter a larger amount.",
                    parse_mode='Markdown'
                )
                return
            if amount > balance:
                await update.message.reply_text(
                    f"❌ Your balance is only *৳ {balance:.2f}*. You cannot withdraw that much.",
                    parse_mode='Markdown'
                )
                return
            context.user_data['awaiting_withdraw_amount'] = False
            context.user_data['withdraw_amount']          = amount
            markup = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Confirm", callback_data="wd_confirm")],
                [InlineKeyboardButton("❌ Cancel",        callback_data="wd_cancel")],
            ])
            await update.message.reply_text(
                f"💸 *Withdraw Confirmation*\n\n"
                f"┣━━━━━━━━━━━━━━━━━━━━━\n"
                f"┃ 💰 Amount: *৳ {amount:.2f}*\n"
                f"┃ 📱 Method: *{method}*\n"
                f"┃ 📞 Account: `{account}`\n"
                f"┗━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"Confirm?",
                parse_mode='Markdown',
                reply_markup=markup
            )
        except ValueError:
            await update.message.reply_text("❌ Enter a valid number.")
        return

    # ── Referral bonus amount input ───────────────────────────────────────────
    if context.user_data.get('awaiting_ref_bonus'):
        if not _is_admin(username, user_id):
            await update.message.reply_text("❌ Unauthorized access.")
            return
        try:
            amount = float(text.replace(',', '.').strip())
            if amount < 0:
                raise ValueError
            await run_db(_set_referral_bonus, amount)
            context.user_data['awaiting_ref_bonus'] = False
            await update.message.reply_text(
                f"✅ *Bonus Updated!*\n\n"
                f"💰 From now on, *৳ {amount:.2f}* bonus will be given per referral.",
                parse_mode='Markdown',
                reply_markup=get_admin_keyboard()
            )
        except ValueError:
            await update.message.reply_text(
                "❌ Enter a valid number. (Example: 10 or 25.50)")
        return

    # ── Reset All Users confirmation ──────────────────────────────────────────
    if context.user_data.get('awaiting_reset_users_confirm'):
        if not _is_admin(username, user_id):
            await update.message.reply_text("❌ Unauthorized access.")
            return
        context.user_data['awaiting_reset_users_confirm'] = False
        if text.strip() == "YES DELETE":
            await update.message.reply_text(
                "⌛ *Creating backup...* Please wait.",
                parse_mode='Markdown'
            )
            try:
                zip_buf, stamp = await run_db(export_all_data_as_zip)
                from telegram import InputFile
                await context.bot.send_document(
                    chat_id=user_id,
                    document=InputFile(zip_buf, filename=f"backup_{stamp}.zip"),
                    caption=(
                        f"🛍️ *Full Data Backup*\n"
                        f"📌️ Date: `{stamp}`\n\n"
                        f"This file contains:\n"
                        f"• User info and balances\n"
                        f"• Referral logs\n"
                        f"• Withdraw requests\n"
                        f"• OTP bonus logs\n"
                        f"• Number assignments & OTP deliveries\n"
                        f"• SMS logs"
                    ),
                    parse_mode='Markdown'
                )
            except Exception as e:
                await update.message.reply_text(
                    f"⚠️ Failed to create backup: `{e}`\n\nReset cancelled.",
                    parse_mode='Markdown',
                    reply_markup=get_settings_keyboard()
                )
                return

            summary          = await run_db(_reset_all_user_data, user_id)
            total_users      = summary.get('users', 0)
            total_referrals  = summary.get('referral_count', 0)
            referral_income  = summary.get('referral_income', 0.0)
            otp_income_today = summary.get('otp_income_today', 0.0)
            otp_income_total = summary.get('otp_income_total', 0.0)
            total_withdraws  = summary.get('withdraw_requests', 0)
            total_assignments = summary.get('number_assignments', 0)
            await update.message.reply_text(
                "✅ *Reset Complete!*\n\n"
                f"🛍️ Backup ZIP file sent ✅\n\n"
                f"User accounts: *{total_users}* — *unchanged*\n"
                f"💰 All user balances → *reset to 0*\n\n"
                f"🗑️ Deleted:\n"
                f"  • Referral logs: *{total_referrals}*\n"
                f"  • Referral earnings: *৳ {referral_income:.2f}*\n"
                f"  • OTP earnings (today): *৳ {otp_income_today:.2f}*\n"
                f"  • OTP earnings (total): *৳ {otp_income_total:.2f}*\n"
                f"  • Withdraw requests: *{total_withdraws}*\n"
                f"  • Number assignments: *{total_assignments}*\n"
                f"  • OTP delivery tracking & SMS history\n\n"
                "🛡️ Admins, panels, countries, numbers and settings remain unchanged.",
                parse_mode='Markdown',
                reply_markup=get_settings_keyboard()
            )
        else:
            await update.message.reply_text(
                "❌ *Operation cancelled.*\n\nNo data was deleted.",
                parse_mode='Markdown',
                reply_markup=get_settings_keyboard()
            )
        return

    # ── Number Limit input ────────────────────────────────────────────────────
    if context.user_data.get('awaiting_number_limit'):
        if not _is_admin(username, user_id):
            await update.message.reply_text("❌ Unauthorized access.")
            return
        try:
            limit = int(text.strip())
            if limit < 1:
                raise ValueError
            await run_db(_set_number_limit, limit)
            context.user_data['awaiting_number_limit'] = False
            await update.message.reply_text(
                f"✅ *Number Limit Updated!*\n\n"
                f"From now on, each user will receive *{limit}* number(s) at a time.",
                parse_mode='Markdown',
                reply_markup=get_settings_keyboard()
            )
        except ValueError:
            await update.message.reply_text("❌ Enter a valid whole number. (Example: 1 or 3)")
        return

    # ── OTP Bonus amount input ────────────────────────────────────────────────
    if context.user_data.get('awaiting_otp_bonus_amount'):
        if not _is_admin(username, user_id):
            await update.message.reply_text("❌ Unauthorized access.")
            return
        try:
            amount = float(text.replace(',', '.').strip())
            if amount < 0:
                raise ValueError
            await run_db(_set_otp_bonus_amount, amount)
            context.user_data['awaiting_otp_bonus_amount'] = False
            await update.message.reply_text(
                f"✅ *OTP Bonus Updated!*\n\n"
                f"⭐ From now on, *৳ {amount:.2f}* bonus will be given per OTP notification.",
                parse_mode='Markdown',
                reply_markup=get_admin_keyboard()
            )
        except ValueError:
            await update.message.reply_text("❌ Enter a valid number. (Example: 2 or 5.50)")
        return

    # ── Country OTP Bonus input ───────────────────────────────────────────────
    if context.user_data.get('awaiting_country_otp_bonus') is not None and context.user_data.get('awaiting_country_otp_bonus') is not False:
        if not _is_admin(username, user_id):
            await update.message.reply_text("❌ Unauthorized access.")
            return
        cid   = context.user_data['awaiting_country_otp_bonus']
        cname = context.user_data.get('awaiting_country_otp_name', 'Unknown')
        try:
            amount = float(text.replace(',', '.').strip())
            if amount < 0:
                raise ValueError
            await run_db(_set_country_otp_bonus, cid, amount)
            context.user_data.pop('awaiting_country_otp_bonus', None)
            context.user_data.pop('awaiting_country_otp_name', None)
            await update.message.reply_text(
                f"✅ *{cname}* — OTP Bonus Updated!\n\n"
                f"⭐ From now on, *৳ {amount:.2f}* bonus will be given when an OTP is received on this country's number.",
                parse_mode='Markdown',
                reply_markup=get_settings_keyboard()
            )
        except ValueError:
            await update.message.reply_text("❌ Enter a valid number. (Example: 3 or 5.50)")
        return

    # ── User Info lookup ───────────────────────────────────────────────────────
    if context.user_data.get('awaiting_user_info_id'):
        if not _is_admin(username, user_id):
            context.user_data.pop('awaiting_user_info_id', None)
            return
        try:
            target_id = int(text.strip())
        except ValueError:
            await update.message.reply_text(
                "❌ Invalid ID. Please enter a numeric Telegram User ID.",
                reply_markup=get_users_keyboard())
            return
        context.user_data.pop('awaiting_user_info_id', None)
        info = await run_db(_get_user_info_by_id, target_id)
        if not info:
            await update.message.reply_text(
                f"❌ No user found with ID `{target_id}`.",
                parse_mode='Markdown',
                reply_markup=get_users_keyboard())
            return
        otp_stats   = await run_db(_get_user_otp_bonus_stats, target_id)
        ref_count   = await run_db(_get_referral_count, target_id)
        ref_earned  = await run_db(_get_referral_total_earned, target_id)
        ref_code    = await run_db(_get_user_referral_code, target_id)
        otp_total   = otp_stats.get('total_count', 0) if otp_stats else 0
        otp_today   = otp_stats.get('today_count', 0) if otp_stats else 0
        uname_str   = f"@{info['username']}" if info.get('username') else "—"
        fname_str   = info.get('first_name') or "—"
        msg = (
            f"🔍 *User Info*\n\n"
            f"`{'─'*30}`\n"
            f"`Name         : {fname_str}`\n"
            f"`🔗 Username     : {uname_str}`\n"
            f"`💎 User ID      : {info['user_id']}`\n"
            f"`{'─'*30}`\n"
            f"`💰 Balance      : {info['balance']:.2f} ৳`\n"
            f"`✉️ OTP Total    : {otp_total}`\n"
            f"`✉️ OTP Today    : {otp_today}`\n"
            f"`Referrals    : {ref_count}`\n"
            f"`💵 Ref Earned   : {ref_earned:.2f} ৳`\n"
            f"`Ref Code     : {ref_code or '—'}`\n"
            f"`{'─'*30}`"
        )
        await update.message.reply_text(msg, parse_mode='Markdown',
                                        reply_markup=get_users_keyboard())
        return

    # ── Balance edit: get user ID ──────────────────────────────────────────────
    if context.user_data.get('awaiting_balance_user_id'):
        if not _is_admin(username, user_id):
            await update.message.reply_text("❌ Unauthorized access.")
            return
        try:
            target_id = int(text.strip())
            info = await run_db(_get_user_info_by_id, target_id)
            if not info:
                await update.message.reply_text("❌ No user found with this ID.")
                return
            context.user_data['awaiting_balance_user_id']  = False
            context.user_data['balance_edit_target_id']    = target_id
            context.user_data['awaiting_balance_amount']   = True
            name = f"@{info['username']}" if info['username'] else info['first_name'] or str(target_id)
            await update.message.reply_text(
                f"User found: *{name}*\n"
                f"💰 Current balance: *৳ {info['balance']:.2f}*\n\n"
                f"Enter the new balance (use + to add, - to deduct, or a plain number to set):\n"
                f"_(Example: +50 or -10 or 100)_",
                parse_mode='Markdown'
            )
        except ValueError:
            await update.message.reply_text("❌ Enter a valid User ID (numbers only).")
        return

    # ── Balance edit: get amount ───────────────────────────────────────────────
    if context.user_data.get('awaiting_balance_amount'):
        if not _is_admin(username, user_id):
            await update.message.reply_text("❌ Unauthorized access.")
            return
        target_id = context.user_data.get('balance_edit_target_id')
        try:
            txt = text.strip()
            if txt.startswith('+'):
                amount = float(txt[1:].replace(',', '.'))
                await run_db(_update_user_balance, target_id, amount)
                action = f"*+৳ {amount:.2f}* added"
            elif txt.startswith('-'):
                amount = float(txt[1:].replace(',', '.'))
                await run_db(_update_user_balance, target_id, -amount)
                action = f"*-৳ {amount:.2f}* deducted"
            else:
                amount = float(txt.replace(',', '.'))
                await run_db(_set_user_balance, target_id, amount)
                action = f"*৳ {amount:.2f}* set"
            context.user_data['awaiting_balance_amount']  = False
            context.user_data.pop('balance_edit_target_id', None)
            new_balance = await run_db(_get_user_balance, target_id)
            await update.message.reply_text(
                f"✅ *Balance Updated Successfully!*\n\n"
                f"User ID: `{target_id}`\n"
                f"Change: {action}\n"
                f"💰 New Balance: *৳ {new_balance:.2f}*",
                parse_mode='Markdown',
                reply_markup=get_admin_keyboard()
            )
            try:
                await context.bot.send_message(
                    chat_id=target_id,
                    text=(
                        f"💰 *Your Balance Has Been Updated!*\n\n"
                        f"Change: {action}\n"
                        f"New Balance: *৳ {new_balance:.2f}*"
                    ),
                    parse_mode='Markdown'
                )
            except Exception:
                pass
        except ValueError:
            await update.message.reply_text("❌ Enter a valid number. (Example: +50 or -10 or 100)")
        return

    # Default: show appropriate panel
    if _is_admin(username, user_id):
        await admin_start(update, context)
    else:
        await show_main_menu(update, context)


# ── Error handler ─────────────────────────────────────────────────────────────

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    import traceback
    from telegram.error import Conflict, NetworkError, TimedOut, RetryAfter

    # Transient / expected errors — log as WARNING only, don't notify user
    if isinstance(context.error, Conflict):
        logger.warning("Telegram Conflict: closing old bot session — will recover shortly.")
        return
    if isinstance(context.error, (NetworkError, TimedOut)):
        logger.warning(f"Network error (transient): {context.error}")
        return
    if isinstance(context.error, RetryAfter):
        logger.warning(f"Rate limit — retrying after {context.error.retry_after}s.")
        return

    err_text = "".join(traceback.format_exception(type(context.error), context.error, context.error.__traceback__))
    logger.error(f"Exception while handling an update:\n{err_text}")
    # Only reply to real user messages — never reply to channel posts (effective_user is None there)
    if update and update.effective_message and update.effective_user:
        try:
            await update.effective_message.reply_text(
                f"❌ *An error occurred!*\n\n`{context.error}`",
                parse_mode='Markdown'
            )
        except Exception:
            pass


# ── Post-init: start OTP monitor ──────────────────────────────────────────────




async def post_init(application: Application):
    # Force-close any existing getUpdates session from a previous instance.
    # This prevents the "Conflict" error when restarting the bot.
    try:
        await application.bot.get_updates(offset=-1, timeout=1)
    except Exception:
        pass
    logger.info("[Startup] Previous Telegram session cleared.")

    # Give the bot handler pool 500 threads (handles 20k+ concurrent users).
    # OTP monitors run in their own 24-thread pool (otp_monitor._OTP_EXECUTOR)
    # so the two pools never compete.
    loop = asyncio.get_running_loop()
    loop.set_default_executor(
        concurrent.futures.ThreadPoolExecutor(
            max_workers=500,
            thread_name_prefix="bot-worker",
        )
    )

    # Create the message queue inside the running event loop (avoids cross-loop errors)
    global _msg_queue
    _msg_queue = asyncio.Queue()

    # Resolve and store the bot's username for dynamic use in OTP notifications
    try:
        from otp_monitor import set_bot_username as _set_bot_uname
        _bot_me = await application.bot.get_me()
        if _bot_me.username:
            _set_bot_uname(_bot_me.username)
            logger.info("[Startup] Bot username set to @%s", _bot_me.username)
    except Exception as _e:
        logger.warning("[Startup] Could not fetch bot username: %s", _e)

    # Start Message Queue worker (rate-limited Telegram sender)
    loop.create_task(_message_queue_worker(), name="msg-queue-worker")
    logger.info("[MsgQueue] Message queue worker started (max %d msg/sec)", _MSG_RATE)

    # Start Memory Cleanup loop (runs every 30 minutes)
    loop.create_task(_memory_cleanup_loop(), name="memory-cleanup")
    logger.info("[MemCleanup] Memory cleanup task started (interval=%ds)", _CLEANUP_INTERVAL)


    def _apply_saved_intervals():
        """Load stored polling & retry intervals from DB and apply to each monitor."""
        for pname, m in ALL_PANEL_LIST:
            try:
                saved = _get_panel_interval(pname)
                if saved:
                    m.set_interval(saved)
            except Exception:
                pass
            try:
                saved_retry = _get_panel_retry_interval(pname)
                if saved_retry:
                    m.set_retry_interval(saved_retry)
            except Exception:
                pass

    # Load dynamic panels from DB and append to ALL_PANEL_LIST
    try:
        _dynamic = load_dynamic_panels_from_db()
        for _dpname, _dpmon in _dynamic:
            ALL_PANEL_LIST.append((_dpname, _dpmon))
            PANEL_CATEGORY.setdefault(_dpname, 'dynamic')
            PANEL_CONFIG_USERNAMES.setdefault(_dpname, _dpmon.username)
        if _dynamic:
            logger.info("[Startup] Loaded %d dynamic panel(s) from DB.", len(_dynamic))
    except Exception as _de:
        logger.warning("[Startup] Could not load dynamic panels: %s", _de)

    _apply_saved_intervals()

    async def _staggered_start():
        """Start monitors one by one with short delays to prevent concurrent
        login failures caused by panel-side rate-limiting / single-session
        restrictions."""
        bot = application.bot
        monitor.start(bot)
        await asyncio.sleep(4)
        msi_sms_monitor.start(bot)
        await asyncio.sleep(4)
        proof_sms_monitor.start(bot)
        await asyncio.sleep(4)
        lamix_sms_monitor.start(bot)
        await asyncio.sleep(4)
        purple_sms_monitor.start(bot)
        await asyncio.sleep(4)
        seven1tel_monitor.start(bot)
        await asyncio.sleep(4)
        mait_sms_monitor.start(bot)
        await asyncio.sleep(4)
        zento_sms_monitor.start(bot)
        await asyncio.sleep(4)
        wolf_sms_monitor.start(bot)
        await asyncio.sleep(4)
        shark_sms_monitor.start(bot)
        await asyncio.sleep(4)
        km_carrier_sms_monitor.start(bot)
        await asyncio.sleep(4)
        sms_hadi2_monitor.start(bot)
        await asyncio.sleep(4)
        konekta_monitor.start(bot)     # known to fail if started too early
        await asyncio.sleep(6)
        number_panel_monitor.start(bot)  # known to fail if started too early
        # Start any dynamic panels loaded from DB
        for _dname, _dmon in DYNAMIC_PANEL_REGISTRY.items():
            await asyncio.sleep(4)
            try:
                _dmon.start(bot)
                logger.info("[Startup] Dynamic panel '%s' started.", _dname)
            except Exception as _dse:
                logger.warning("[Startup] Dynamic panel '%s' failed to start: %s", _dname, _dse)

    loop.create_task(_staggered_start())


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    _init_db()

    _bot_request = HTTPXRequest(
        connection_pool_size=128,
        connect_timeout=20.0,
        read_timeout=20.0,
        write_timeout=20.0,
        pool_timeout=10.0,
    )
    _custom_bot = AnimatedEmojiBot(token=BOT_TOKEN, request=_bot_request)

    application = (
        Application.builder()
        .bot(_custom_bot)
        .concurrent_updates(True)
        .post_init(post_init)
        .build()
    )

    # Admin keyboard buttons
    application.add_handler(MessageHandler(
        filters.Text([
            "🌍 𝑪𝒐𝒖𝒏𝒕𝒓𝒚 𝑴𝒂𝒏𝒂𝒈𝒆𝒓", "Manage Admins", "Back to Admin Panel",
            "Users", "User Count", "📈 User Stats", "🔍 User Info",
            "🔄 𝑹𝒆𝒔𝒆𝒕 𝑵𝒖𝒎𝒃𝒆𝒓",
            "Panel management", "📋 Panel List", "➕ Add Panel", "📦 Added Panels", "🗑️ Delete Panel",
            "⌛ Retry Login", "🔀 All Panel Toggle",
            "Settings",
            "Admin Tools",
            "⏰ নোটিফাই টাইম",
            "📢 Extra Groups",
            "➕ Add Group", "🗑️ Remove Group",
            "⌛ Retry Interval",
            "Session Cleanup",
            "Panel Toggle",
            "🧹 Session Cleanup",
            "🔀 Panel Toggle",
            "🔄 Reload Interval",
            "📢 Broadcast",
            "🌟 Force Start",
            "🔗 Edit Bot Links",
            "📱 NUMBER Link",
            "📢 CHANNEL Link",
            "Support Group Link",
            "📢 OTP Group Link",
            "Export Users",

            "🎁 Referral Settings", "🎁 Referral",
            "⭐ OTP Bonus Settings", "⭐ OTP Bonus",
            "Number Limit",
            "🌍 Country OTP Bonus",
            "🗑️ Reset All Users",
            "🌐Add 𝑪𝒐𝒖𝒏𝒕𝒓𝒚",
            "📱 𝑨𝒅𝒅 𝑵𝒖𝒎𝒃𝒆𝒓",
            "⚙️ Add Service", "🗺️ Service Map",
            # OTP Bonus sub-menu
            "OTP Bonus Toggle", "💰 Set Bonus Amount",
            # Referral sub-menu
            "Referral Toggle", "💰 Set Referral Bonus",
            "📤 Set Min Withdraw", "💸 Pending Withdraws",
            # Shared
            "Edit Balance", "Back to Settings",
            "📊 𝑩𝒐𝒕 𝑺𝒕𝒂𝒕𝒊𝒔𝒕𝒊𝒄𝒔",
            "Back to Admin Tools",
            # Required Channels
            "📢 Required Channels", "➕ Add Channel", "🗑️ Delete Channel",
        ]),
        handle_button_click,
    ))

    # User keyboard buttons
    application.add_handler(MessageHandler(
        filters.Text([
            "𝑮𝒆𝒕 𝑵𝒖𝒎𝒃𝒆𝒓", "Get Numbers",
            "𝑨𝒗𝒂𝒊𝒍𝒂𝒃𝒍𝒆 𝑪𝒐𝒖𝒏𝒕𝒓𝒚",
            "𝑴𝒚 𝑩𝒂𝒍𝒂𝒏𝒄𝒆",
            "𝑾𝒊𝒕𝒉𝒅𝒓𝒂𝒘",
            "𝑻𝒐𝒑 𝑼𝒔𝒆𝒓𝒔",
            "Support Group",
        ]),
        handle_user_button_click,
    ))

    application.add_handler(CommandHandler("start",     start_command))
    application.add_handler(CommandHandler("userpanel", handle_userpanel_command))
    application.add_handler(CommandHandler("cancel",    cancel_command))
    application.add_handler(CommandHandler("support",   handle_support_platform))
    application.add_handler(CallbackQueryHandler(handle_callback))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    application.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    application.add_error_handler(error_handler)

    print("🤖 Bot is running…")
    # Exclude channel_post / edited_channel_post — the bot is admin in some channels
    # and those updates have effective_user=None which caused spurious errors.
    application.run_polling(
        drop_pending_updates=True,
        allowed_updates=[
            "message",
            "edited_message",
            "callback_query",
            "inline_query",
            "chosen_inline_result",
            "my_chat_member",
            "chat_member",
        ],
    )


if __name__ == '__main__':
    main()
