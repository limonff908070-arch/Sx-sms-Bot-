"""
flags.py — Country flag helpers shared across bot.py, keyboards.py, etc.
Provides COUNTRY_FLAGS (name → flag char) and animated sticker ID lookup.
"""
from __future__ import annotations


def _flag_from_code(code: str) -> str:
    return ''.join(chr(0x1F1E6 + ord(c) - ord('A')) for c in code.upper())


COUNTRY_FLAGS: dict[str, str] = {
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


def strip_flag_emoji(name: str) -> str:
    """Strip a leading flag emoji (and trailing space) from a country name.

    Flag emoji are two regional-indicator chars (U+1F1E6..U+1F1FF).
    Other leading emoji (cp > 0x1F000) are stripped as a single char.
    """
    s = (name or '').strip()
    if not s:
        return s
    cp = ord(s[0])
    if 0x1F1E6 <= cp <= 0x1F1FF:   # regional indicator — flag is 2 chars
        return s[2:].lstrip()
    if cp > 0x1F000:                # other leading emoji — 1 char
        return s[1:].lstrip()
    return s



import re as _re

_SORTED_COUNTRY_KEYS: list[str] = sorted(COUNTRY_FLAGS.keys(), key=lambda k: -len(k))


def get_flag_char(country_name: str) -> str:
    """Return the Unicode flag emoji char for a country name, or '' if unknown.

    Supports:
    - Exact match:              'bangladesh'      → 🇧🇩
    - Multi-word prefix match:  'united states v2'→ 🇺🇸  (longest-first greedy)
    - Word fallback:            'myanmar premium' → 🇲🇲  (first long word match)
    """
    s = (country_name or '').strip()
    if not s:
        return ''
    key = s.lower()

    flag = COUNTRY_FLAGS.get(key, '')
    if flag:
        return flag

    for ckey in _SORTED_COUNTRY_KEYS:
        if key.startswith(ckey + ' '):
            return COUNTRY_FLAGS[ckey]

    for word in _re.sub(r'[^a-z\s]', '', key).split():
        if len(word) > 3 and word in COUNTRY_FLAGS:
            return COUNTRY_FLAGS[word]

    return ''


def get_animated_flag_id(country_name: str) -> str | None:
    """Return the animated sticker ID for a country's flag, or None if unknown.

    Chain: country_name → flag_char (COUNTRY_FLAGS) → sticker_id (EMOJI_MAP)
    Works for plain names ('Myanmar'), names with suffixes ('Myanmar V2'),
    and names that already carry a flag prefix ('🇲🇲 Myanmar').
    """
    from custom_emojis import EMOJI_MAP
    flag_char = get_flag_char(country_name)
    if not flag_char:
        return None
    return EMOJI_MAP.get(flag_char)


# Service button sticker map — only unique IDs per service.
# Shared IDs from _APP_STICKER_MAP are assigned to the primary service only;
# secondary services with the same ID are intentionally omitted (no sticker shown).
SERVICE_BUTTON_EMOJI_MAP: dict[str, str] = {
    # ── APPEmojiSXSponsor (actual brand stickers — 6298*/6300* range) ─────────
    'whatsapp':  '6298480844214379008',  # user-confirmed ✓
    'tiktok':    '6298708640689824023',
    'twitter/x': '6298350676640538162',
    'twitter':   '6298350676640538162',
    'x':         '6298350676640538162',
    'spotify':   '6300761828330840482',
    # ── Common aliases ────────────────────────────────────────────────────────
    'wa':        '6298480844214379008',
    # NOTE: Services not listed here (facebook, instagram, telegram, etc.) have
    # no confirmed brand sticker in the available packs — showing NO icon is
    # better than showing a wrong one (6068*/6069* are generic colour emojis).
}


def get_service_emoji_id(service_name: str) -> str | None:
    """Return the animated sticker ID for a service button, or None if unknown.

    Uses SERVICE_BUTTON_EMOJI_MAP — each entry has a unique sticker ID so the
    correct icon is shown for each service. Services not in the map return None
    (no icon), which is preferable to showing a wrong sticker.
    """
    key = (service_name or '').strip().lower()
    return SERVICE_BUTTON_EMOJI_MAP.get(key)
