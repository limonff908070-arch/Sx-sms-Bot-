from telegram import (
    InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, KeyboardButton, CopyTextButton,
)
from flags import get_animated_flag_id, get_service_emoji_id, strip_flag_emoji


ADMIN_BUTTON_LAYOUT = [
    ["🌍 𝑪𝒐𝒖𝒏𝒕𝒓𝒚 𝑴𝒂𝒏𝒂𝒈𝒆𝒓", "Manage Admins"],
    ["Users",          "Panel management"],
    ["📢 Broadcast",      "Settings"],
    ["📊 𝑩𝒐𝒕 𝑺𝒕𝒂𝒕𝒊𝒔𝒕𝒊𝒄𝒔"],
]


def get_admin_keyboard():
    rows = []
    for layout_row in ADMIN_BUTTON_LAYOUT:
        rows.append([KeyboardButton(b) for b in layout_row])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def get_panel_management_keyboard():
    keyboard = [
        [KeyboardButton("📋 Panel List"),  KeyboardButton("📦 Added Panels")],
        [KeyboardButton("➕ Add Panel"),   KeyboardButton("Back to Admin Panel")],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def get_edit_bot_links_keyboard():
    keyboard = [
        [KeyboardButton("📱 NUMBER Link"),       KeyboardButton("📢 CHANNEL Link")],
        [KeyboardButton("Support Group Link"), KeyboardButton("📢 OTP Group Link")],
        [KeyboardButton("Back to Admin Tools")],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def get_extra_groups_keyboard():
    keyboard = [
        [KeyboardButton("➕ Add Group"),  KeyboardButton("🗑️ Remove Group")],
        [KeyboardButton("Back to Admin Tools")],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def get_admin_tools_keyboard():
    keyboard = [
        [KeyboardButton("🌟 Force Start"),     KeyboardButton("⌛ Retry Interval")],
        [KeyboardButton("📢 Extra Groups"),    KeyboardButton("🔗 Edit Bot Links")],
        [KeyboardButton("⏰ নোটিফাই টাইম"),   KeyboardButton("Back to Admin Panel")],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def get_users_keyboard():
    keyboard = [
        [KeyboardButton("User Count"), KeyboardButton("📈 User Stats")],
        [KeyboardButton("🔍 User Info"),  KeyboardButton("Back to Admin Panel")],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def get_settings_keyboard():
    keyboard = [
        [KeyboardButton("⭐ OTP Bonus"),           KeyboardButton("🎁 Referral")],
        [KeyboardButton("Number Limit"),          KeyboardButton("🗑️ Reset All Users")],
        [KeyboardButton("Export Users"),          KeyboardButton("Admin Tools")],
        [KeyboardButton("📢 Required Channels"),  KeyboardButton("Back to Admin Panel")],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def get_required_channels_keyboard():
    keyboard = [
        [KeyboardButton("➕ Add Channel"),   KeyboardButton("🗑️ Delete Channel")],
        [KeyboardButton("Back to Settings")],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)



def get_otp_bonus_keyboard():
    keyboard = [
        [KeyboardButton("OTP Bonus Toggle"),  KeyboardButton("💰 Set Bonus Amount")],
        [KeyboardButton("Edit Balance"),       KeyboardButton("Back to Settings")],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def get_referral_keyboard():
    keyboard = [
        [KeyboardButton("Referral Toggle"),   KeyboardButton("💰 Set Referral Bonus")],
        [KeyboardButton("📤 Set Min Withdraw"),   KeyboardButton("Edit Balance")],
        [KeyboardButton("💸 Pending Withdraws"),  KeyboardButton("Back to Settings")],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def get_manage_numbers_keyboard():
    keyboard = [
        [KeyboardButton("📱 𝑨𝒅𝒅 𝑵𝒖𝒎𝒃𝒆𝒓"),    KeyboardButton("🌐Add 𝑪𝒐𝒖𝒏𝒕𝒓𝒚")],
        [KeyboardButton("🔄 𝑹𝒆𝒔𝒆𝒕 𝑵𝒖𝒎𝒃𝒆𝒓"), KeyboardButton("🌍 Country OTP Bonus")],
        [KeyboardButton("⚙️ Add Service"),     KeyboardButton("🗺️ Service Map")],
        [KeyboardButton("Back to Admin Panel")],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def get_manage_admins_keyboard():
    keyboard = [
        [KeyboardButton("Back to Admin Panel")],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def get_panel_action_keyboard(is_enabled: bool = True):
    toggle_label = "🔴 Disable Panel" if is_enabled else "🟢 Enable Panel"
    keyboard = [
        [KeyboardButton("✉️ Latest Message"),    KeyboardButton("📤 Group এ পাঠাও")],
        [KeyboardButton("🔑 Change User/Pass"),  KeyboardButton("📊 Panel Status")],
        [KeyboardButton(toggle_label),           KeyboardButton("🔄 Reload Interval")],
        [KeyboardButton("🧹 Session Cleanup"),   KeyboardButton("⌛ Retry Login")],
        [KeyboardButton("⚙️ Live Column Config"), KeyboardButton("🔀 All Panel Toggle")],
        [KeyboardButton("Back to Panel List")],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def get_dynamic_panel_action_keyboard(is_enabled: bool = True):
    toggle_label = "🔴 Disable Panel" if is_enabled else "🟢 Enable Panel"
    keyboard = [
        [KeyboardButton("✉️ Latest Message"),    KeyboardButton("📤 Group এ পাঠাও")],
        [KeyboardButton("🔑 Change User/Pass"),  KeyboardButton("📊 Panel Status")],
        [KeyboardButton(toggle_label),           KeyboardButton("🔄 Reload Interval")],
        [KeyboardButton("🧹 Session Cleanup"),   KeyboardButton("⌛ Retry Login")],
        [KeyboardButton("⚙️ Live Column Config"), KeyboardButton("🔀 All Panel Toggle")],
        [KeyboardButton("🗑️ Delete Panel"),      KeyboardButton("Back to Panel List")],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def get_user_keyboard():
    keyboard = [
        [
            KeyboardButton("𝑮𝒆𝒕 𝑵𝒖𝒎𝒃𝒆𝒓",      api_kwargs={"style": "success", "icon_custom_emoji_id": "6068652724684070452"}),
            KeyboardButton("𝑨𝒗𝒂𝒊𝒍𝒂𝒃𝒍𝒆 𝑪𝒐𝒖𝒏𝒕𝒓𝒚", api_kwargs={"style": "success", "icon_custom_emoji_id": "6068759755269086959"}),
        ],
        [
            KeyboardButton("𝑴𝒚 𝑩𝒂𝒍𝒂𝒏𝒄𝒆", api_kwargs={"style": "primary", "icon_custom_emoji_id": "6068607339764653118"}),
            KeyboardButton("𝑾𝒊𝒕𝒉𝒅𝒓𝒂𝒘",   api_kwargs={"style": "danger",  "icon_custom_emoji_id": "6069031789907681337"}),
        ],
        [
            KeyboardButton("𝑻𝒐𝒑 𝑼𝒔𝒆𝒓𝒔",   api_kwargs={"style": "primary", "icon_custom_emoji_id": "6068892242125266188"}),
            KeyboardButton("Support Group", api_kwargs={"style": "primary", "icon_custom_emoji_id": "6068727156467309816"}),
        ],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def country_number_keyboard(country_id, otp_link, numbers=None, back_callback: str = "get_numbers"):
    rows = []
    if numbers:
        for num in numbers:
            display = f"+{num}" if not str(num).startswith("+") else str(num)
            rows.append([
                InlineKeyboardButton(
                    display,
                    copy_text=CopyTextButton(text=display),
                    api_kwargs={"style": "success"},
                )
            ])
    rows.append([InlineKeyboardButton(
        "🔄 Change Number",
        callback_data=f"another_{country_id}",
        api_kwargs={"style": "primary"},
    )])
    rows.append([InlineKeyboardButton(
        "GET OTP",
        url=otp_link,
        api_kwargs={"style": "success", "icon_custom_emoji_id": "6068876810307772648"},
    )])
    rows.append([InlineKeyboardButton(
        "◀ Back to Countries",
        callback_data=back_callback,
        api_kwargs={"style": "danger"},
    )])
    return InlineKeyboardMarkup(rows)


def countries_inline_keyboard(countries_data, back_to_services: bool = False):
    """
    countries_data: list of (country_id, country_name, available_count)
    Returns InlineKeyboardMarkup or None if no available numbers.
    Each country button shows on its own row with an animated flag sticker.
    If back_to_services=True, a "Back to Services" button is appended.
    """
    btns = []
    for country_id, country_name, available in countries_data:
        if available <= 0:
            continue
        clean_name = strip_flag_emoji(country_name)
        animated_id = get_animated_flag_id(clean_name)
        api_kw = {"style": "success"}
        if animated_id:
            api_kw["icon_custom_emoji_id"] = animated_id
        btn_kw = dict(
            text=f"{clean_name} ({available})",
            callback_data=f"country_{country_id}",
            api_kwargs=api_kw,
        )
        btns.append(InlineKeyboardButton(**btn_kw))
    if not btns:
        return None
    keyboard = [btns[i:i+2] for i in range(0, len(btns), 2)]
    if back_to_services:
        keyboard.append([InlineKeyboardButton(
            text="◀ Back to Services",
            callback_data="get_numbers",
            api_kwargs={"style": "success"},
        )])
    return InlineKeyboardMarkup(keyboard)


def services_inline_keyboard(services: list[str], emoji_overrides: dict[str, str] | None = None):
    """
    emoji_overrides: {service_name: custom_emoji_id} from DB — takes priority
    over the built-in SERVICE_BUTTON_EMOJI_MAP fallback.
    """
    if not services:
        return None
    overrides = emoji_overrides or {}
    keyboard = []
    for svc in services:
        emoji_id = overrides.get(svc) or get_service_emoji_id(svc)
        api_kw = {"style": "success"}
        if emoji_id:
            api_kw["icon_custom_emoji_id"] = emoji_id
        btn_kw = dict(
            text=svc,
            callback_data=f"service_{svc}",
            api_kwargs=api_kw,
        )
        keyboard.append([InlineKeyboardButton(**btn_kw)])
    return InlineKeyboardMarkup(keyboard)
