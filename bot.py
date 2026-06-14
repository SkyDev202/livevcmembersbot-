import os
import json
import asyncio
import logging
from datetime import datetime
from pathlib import Path

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ChatJoinRequest,
    MessageEntity,
)
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ChatJoinRequestHandler,
    ChatMemberHandler,
    MessageHandler,
    filters,
    ContextTypes,
)
from telegram.constants import ParseMode, ChatMemberStatus
from telegram.error import TelegramError, BadRequest


# ═══════════════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════════════
BOT_TOKEN = "8665772753:AAFRtbriEZmQ2k68ofk7B2uynUxmUKNEiho"
ADMIN_IDS        = [8776447116, 8872309116]
ADMIN_ID         = ADMIN_IDS[0]
CHANNEL_ID       = -1002088619780
DATA_FILE        = "bot_datx45a.json"

FORWARD_SOURCE_CHANNEL_ID = int(os.getenv("FORWARD_SOURCE_CHANNEL_ID", "-1002701185142"))

FORWARD_MSG_IDS_DEFAULT = [60, 61]
def _parse_forward_msg_ids() -> list[int]:
    raw = os.getenv("FORWARD_MSG_IDS", ",".join(map(str, FORWARD_MSG_IDS_DEFAULT)))
    ids: list[int] = []
    for part in raw.replace(" ", "").split(","):
        if part and part.lstrip("-").isdigit():
            ids.append(int(part))
    return ids or [10, 11]

FORWARD_MSG_IDS  = _parse_forward_msg_ids()

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════
#  PREMIUM EMOJI HELPERS
# ═══════════════════════════════════════════════════════
def pe(emoji_id: str, fallback: str = "✨") -> str:
    return f'<tg-emoji emoji-id="{emoji_id}">{fallback}</tg-emoji>'

E_CROWN   = pe("5217822164362739968", "👑")
E_STAR    = pe("5438496463044752972", "⭐")
E_CHECK   = pe("5206607081334906820", "✔️")
E_CROSS   = pe("5210952531676504517", "❌")
E_FIRE    = pe("5424972470023104089", "🔥")
E_BELL    = pe("5458603043203327669", "🔔")
E_DIAMOND = pe("5427168083074628963", "💎")
E_PARTY   = pe("5461151367559141950", "🎉")
E_WARN    = pe("5447644880824181073", "⚠️")
E_LOCK    = pe("5296369303661067030", "🔒")
E_GLOBE   = pe("5447410659077661506", "🌐")
E_SEARCH  = pe("5231012545799666522", "🔍")
E_GEAR    = pe("5341715473882955310", "⚙️")
E_SHIELD  = pe("5251203410396458957", "🛡")
E_EYES    = pe("5210956306952758910", "👀")
E_MEGA    = pe("5424818078833715060", "📣")
E_LINK    = pe("5271604874419647061", "🔗")
E_INFO    = pe("5334544901428229844", "ℹ️")
E_THUMB   = pe("5337080053119336309", "👍")
E_GREEN   = pe("5416081784641168838", "🟢")
E_RED     = pe("5411225014148014586", "🔴")
E_LIGHT   = pe("5456140674028019486", "⚡")
E_PIN     = pe("5397782960512444700", "📌")
E_NEW     = pe("5382357040008021292", "🆕")
E_CHART   = pe("5231200819986047254", "📊")
E_TRASH   = pe("5445267414562389170", "🗑")
E_STOP    = pe("5260293700088511294", "⛔")
E_BAN     = pe("5240241223632954241", "🚫")
E_ALERT   = pe("5395695537687123235", "🚨")
E_SPARK   = pe("5325547803936572038", "✨")
E_ARROW   = pe("5416117059207572332", "➡️")
E_PLUS    = pe("5397916757333654639", "➕")
E_HOUR    = pe("5386367538735104399", "⌛")
E_REFRESH = pe("5375338737028841420", "🔄")
E_COOL    = pe("5222079954421818267", "🆒")
E_FREE    = pe("5406756500108501710", "🆓")
E_MAIL    = pe("5253742260054409879", "✉️")
E_CHAT    = pe("5443038326535759644", "💬")
E_BOOK    = pe("5222444124698853913", "🔖")
E_HOME    = pe("5416041192905265756", "🏠")
E_DOWN    = pe("5406745015365943482", "⬇️")
E_UP      = pe("5449683594425410231", "🔼")
E_100     = pe("5341498088408234504", "💯")
E_BANG    = pe("5276032951342088188", "💥")
E_EDIT    = pe("5395444784611480792", "✏️")
E_FLAG    = pe("5460755126761312667", "🚩")
E_SHOP    = pe("5406683434124859552", "🛍")
E_COMET   = pe("5224607267797606837", "☄️")
E_MEDAL1  = pe("5440539497383087970", "🥇")
E_OK      = pe("5222079954421818267", "🆒")
E_PLAY    = pe("5264919878082509254", "▶️")

# ═══════════════════════════════════════════════════════
#  DATA PERSISTENCE
# ═══════════════════════════════════════════════════════
_DEFAULTS: dict = {
    "pending_requests": {},
    "accepted_users":   [],
    "declined_users":   [],
    "members":          [],
    "left_members":     [],
    "banned_users":     [],
    "daily_users":      {},
    "pinned_messages":  {},
    "stats": {
        "total_requests": 0,
        "total_accepted": 0,
        "total_declined": 0,
        "total_left":     0,
        "total_users":    0,
        "active_users":   0,
        "dead_users":     0,
        "broadcasts_sent": 0,
        "welcome_media_set": False,
    },
    "settings": {
        "welcome_msg":        None,
        "welcome_entities":   None,
        "request_msg":        None,
        "request_entities":   None,
        "accepted_msg":       None,
        "accepted_entities":  None,
        "declined_msg":       None,
        "declined_entities":  None,
        "left_msg":           None,
        "left_entities":      None,
        "auto_accept":        False,
        "auto_accept_delay":  0,
        "activity_channel_id": CHANNEL_ID,
        "forward_source_channel_id": FORWARD_SOURCE_CHANNEL_ID,
        "host_message_ids": FORWARD_MSG_IDS,
        "pin_source_msg_ids": [FORWARD_MSG_IDS[0]] if FORWARD_MSG_IDS else [],
        "auto_unpin_source_msg_ids": [],
        "admin_join_leave_notify": False,
        "pin_start_msg": True,
        "pin_accepted_msg": False,
        "pin_declined_msg": True,
        "pin_welcome_msg": False,
        "start_msg_with_forward_tag": False,
        "max_pins_per_user": 1,
        "broadcast_target": "all",
        "broadcast_mode": "copy",
        "pin_broadcast": False,
        "broadcast_include_banned": False,
        "broadcast_buttons": [],      # list of {text, url} dicts
        "broadcast_button_pending": None,  # temp storage while building buttons
        "welcome_media_type": None,
        "welcome_media_file_id": None,
        "welcome_media_caption": None,
        "welcome_media_entities": None,
    },
    "broadcast_history": [],
}


def load_data() -> dict:
    if Path(DATA_FILE).exists():
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                stored = json.load(f)
            import copy
            for k, v in _DEFAULTS.items():
                if k not in stored:
                    stored[k] = copy.deepcopy(v)
            for k, v in _DEFAULTS["settings"].items():
                stored["settings"].setdefault(k, v)
            for k, v in _DEFAULTS["stats"].items():
                stored["stats"].setdefault(k, v)
            return stored
        except Exception as e:
            logger.error(f"Failed to load data: {e}")
    import copy
    return copy.deepcopy(_DEFAULTS)


def save_data(data: dict):
    try:
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, default=str)
    except Exception as e:
        logger.error(f"Failed to save data: {e}")


bot_data: dict = load_data()
# Migration: if an old DB stored the request channel as forward source, reset it to host channel.
try:
    _settings = bot_data.setdefault("settings", {})
    if int(_settings.get("forward_source_channel_id") or CHANNEL_ID) == int(CHANNEL_ID):
        _settings["forward_source_channel_id"] = int(FORWARD_SOURCE_CHANNEL_ID)
        save_data(bot_data)
except Exception as _e:
    logger.warning(f"forward source migration skipped: {_e}")


def get_activity_channel_id() -> int:
    """Return the channel ID used for all bot activities.
    Admin can change this from Settings. Falls back to CHANNEL_ID.
    """
    try:
        return int(bot_data.get("settings", {}).get("activity_channel_id") or CHANNEL_ID)
    except Exception:
        return int(CHANNEL_ID)


def set_activity_channel_id(channel_id: int):
    bot_data.setdefault("settings", {})["activity_channel_id"] = int(channel_id)
    save_data(bot_data)


def get_forward_source_channel_id() -> int:
    """Return the host/source channel ID used for copy_message.
    This can be different from the activity/request channel.
    """
    try:
        return int(bot_data.get("settings", {}).get("forward_source_channel_id") or FORWARD_SOURCE_CHANNEL_ID)
    except Exception:
        return int(FORWARD_SOURCE_CHANNEL_ID)


def set_forward_source_channel_id(channel_id: int):
    bot_data.setdefault("settings", {})["forward_source_channel_id"] = int(channel_id)
    save_data(bot_data)



def _parse_int_list(raw: str) -> list[int]:
    ids: list[int] = []
    for part in raw.replace(" ", "").split(","):
        if part and part.lstrip("-").isdigit():
            value = int(part)
            if value not in ids:
                ids.append(value)
    return ids


def get_host_message_ids() -> list[int]:
    ids = bot_data.get("settings", {}).get("host_message_ids")
    if isinstance(ids, list):
        parsed = _unique_ints(ids)
        if parsed:
            return parsed
    return list(FORWARD_MSG_IDS)


def set_host_message_ids(ids: list[int]):
    bot_data.setdefault("settings", {})["host_message_ids"] = _unique_ints(ids)
    save_data(bot_data)


def get_pin_source_msg_ids() -> list[int]:
    ids = bot_data.get("settings", {}).get("pin_source_msg_ids")
    if isinstance(ids, list):
        return _unique_ints(ids)
    host_ids = get_host_message_ids()
    return host_ids[:1]


def set_pin_source_msg_ids(ids: list[int]):
    bot_data.setdefault("settings", {})["pin_source_msg_ids"] = _unique_ints(ids)
    save_data(bot_data)


def get_auto_unpin_source_msg_ids() -> list[int]:
    ids = bot_data.get("settings", {}).get("auto_unpin_source_msg_ids")
    return _unique_ints(ids if isinstance(ids, list) else [])


def set_auto_unpin_source_msg_ids(ids: list[int]):
    bot_data.setdefault("settings", {})["auto_unpin_source_msg_ids"] = _unique_ints(ids)
    save_data(bot_data)


# ═══════════════════════════════════════════════════════
#  USER STATS HELPERS
# ═══════════════════════════════════════════════════════
def _today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _week_str_range() -> list[str]:
    from datetime import timedelta
    today = datetime.now().date()
    return [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7)]


def track_user_seen(uid: int):
    today = _today_str()
    daily = bot_data.setdefault("daily_users", {})
    daily.setdefault(today, [])
    uid_str = str(uid)
    if uid_str not in daily[today]:
        daily[today].append(uid_str)

    from datetime import timedelta
    today_d = datetime.now().date()
    keep_30 = {(today_d - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(30)}
    for k in list(daily.keys()):
        if k not in keep_30:
            del daily[k]


def get_today_users() -> int:
    return len(bot_data.get("daily_users", {}).get(_today_str(), []))


def get_last7_users() -> int:
    daily = bot_data.get("daily_users", {})
    seen: set[str] = set()
    for day in _week_str_range():
        seen.update(daily.get(day, []))
    return len(seen)


def all_tracked_users() -> set[str]:
    users: set[str] = set()
    for key in ("members", "accepted_users", "declined_users", "left_members", "banned_users"):
        users.update(str(u) for u in bot_data.get(key, []))
    users.update(str(u) for u in bot_data.get("pending_requests", {}).keys())
    for seen in bot_data.get("daily_users", {}).values():
        users.update(str(u) for u in seen)
    return users


def compute_active_dead() -> tuple[int, int]:
    users = all_tracked_users()
    daily = bot_data.get("daily_users", {})
    active_set: set[str] = set()
    for day in _week_str_range():
        active_set.update(str(u) for u in daily.get(day, []))
    active = len(active_set & users)
    dead = max(len(users) - active, 0)
    return active, dead


def get_pin_setting(key: str) -> bool:
    return bool(bot_data.get("settings", {}).get(key, False))


def get_max_pins() -> int:
    try:
        return max(int(bot_data.get("settings", {}).get("max_pins_per_user", 1)), 0)
    except Exception:
        return 1

# ═══════════════════════════════════════════════════════
#  ENTITY SERIALIZATION (for premium emoji support)
# ═══════════════════════════════════════════════════════

def serialize_entities(entities) -> list | None:
    """Convert telegram MessageEntity list to JSON-serializable list."""
    if not entities:
        return None
    result = []
    for e in entities:
        d = {
            "type":   e.type.value if hasattr(e.type, "value") else str(e.type),
            "offset": e.offset,
            "length": e.length,
        }
        if e.url:
            d["url"] = e.url
        if e.user:
            d["user_id"] = e.user.id
        if e.language:
            d["language"] = e.language
        if e.custom_emoji_id:
            d["custom_emoji_id"] = e.custom_emoji_id
        result.append(d)
    return result


def deserialize_entities(data_list: list | None) -> list | None:
    """Convert JSON list back to MessageEntity objects."""
    if not data_list:
        return None
    from telegram import User as TGUser
    entities = []
    for d in data_list:
        try:
            entity = MessageEntity(
                type=d["type"],
                offset=d["offset"],
                length=d["length"],
                url=d.get("url"),
                language=d.get("language"),
                custom_emoji_id=d.get("custom_emoji_id"),
            )
            entities.append(entity)
        except Exception as ex:
            logger.warning(f"deserialize_entities skip: {ex}")
    return entities if entities else None


def shift_entities_for_replacements(
    serialized_entities: list | None,
    original_text: str,
    replacements: dict[str, str],
) -> list | None:
    """
    Deserialize saved entities and shift offsets when placeholders are replaced.
    This keeps premium animated emojis and formatting working in admin messages.
    """
    if not serialized_entities:
        return None

    adjusted = []
    for ent in serialized_entities:
        d = dict(ent)
        offset = int(d.get("offset", 0))
        for placeholder, value in replacements.items():
            start = 0
            while True:
                idx = original_text.find(placeholder, start)
                if idx == -1:
                    break
                if idx < offset:
                    offset += len(value) - len(placeholder)
                start = idx + len(placeholder)
        d["offset"] = offset
        adjusted.append(d)

    return deserialize_entities(adjusted)


def apply_placeholders_with_entities(
    original_text: str,
    serialized_entities: list | None,
    **values,
) -> tuple[str, list | None]:
    replacements = {f"{{{k}}}": str(v) for k, v in values.items()}
    text = original_text
    for placeholder, value in replacements.items():
        text = text.replace(placeholder, value)

    entities = shift_entities_for_replacements(
        serialized_entities,
        original_text,
        replacements,
    )
    return text, entities



# ═══════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════
def is_admin(uid: int) -> bool:
    return int(uid) in ADMIN_IDS


async def notify_admins(ctx: ContextTypes.DEFAULT_TYPE, text: str, **kwargs):
    for admin_id in ADMIN_IDS:
        try:
            await ctx.bot.send_message(
                admin_id, text,
                parse_mode=ParseMode.HTML,
                **kwargs
            )
        except TelegramError as e:
            logger.error(f"Could not notify admin {admin_id}: {e}")


async def is_member(uid: int, ctx: ContextTypes.DEFAULT_TYPE) -> bool:
    try:
        m = await ctx.bot.get_chat_member(get_activity_channel_id(), uid)
        return m.status in (
            ChatMemberStatus.MEMBER,
            ChatMemberStatus.ADMINISTRATOR,
            ChatMemberStatus.OWNER,
        )
    except Exception:
        return False


async def safe_send(
    ctx: ContextTypes.DEFAULT_TYPE,
    uid: int,
    text: str,
    entities: list | None = None,
    **kwargs
):
    """
    Send a message; silently ignore if user blocked the bot.
    If entities are provided (for premium emoji support), send without parse_mode.
    Otherwise send with HTML parse_mode.
    """
    try:
        if entities:
            msg = await ctx.bot.send_message(
                uid,
                text,
                entities=entities,
                **kwargs
            )
        else:
            msg = await ctx.bot.send_message(
                uid,
                text,
                parse_mode=ParseMode.HTML,
                **kwargs
            )
        return msg
    except TelegramError as e:
        logger.debug(f"safe_send uid={uid}: {e}")
        return None


def _unique_ints(values) -> list[int]:
    out: list[int] = []
    for v in values:
        try:
            iv = int(v)
        except Exception:
            continue
        if iv not in out:
            out.append(iv)
    return out



async def safe_pin_message(
    ctx: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    message_id: int,
    disable_notification: bool = True,
    source_msg_id: int | None = None,
) -> bool:
    """Pin a DM message and keep the configured pin policy."""
    max_pins = get_max_pins()
    key = str(chat_id)
    pinned = bot_data.setdefault("pinned_messages", {}).setdefault(key, [])
    source_map = bot_data.setdefault("pinned_message_sources", {}).setdefault(key, {})

    try:
        for source_id in get_auto_unpin_source_msg_ids():
            old_mid = source_map.pop(str(source_id), None)
            if old_mid:
                try:
                    await ctx.bot.unpin_chat_message(chat_id=chat_id, message_id=int(old_mid))
                except Exception as e:
                    logger.debug(f"auto unpin chat={chat_id} source={source_id}: {e}")
                pinned[:] = [m for m in pinned if int(m) != int(old_mid)]

        if max_pins == 1:
            try:
                await ctx.bot.unpin_all_chat_messages(chat_id=chat_id)
            except Exception as e:
                logger.debug(f"unpin_all chat={chat_id}: {e}")
            pinned.clear()
            source_map.clear()

        await ctx.bot.pin_chat_message(
            chat_id=chat_id,
            message_id=message_id,
            disable_notification=disable_notification,
        )
        if message_id not in pinned:
            pinned.append(message_id)
        if source_msg_id is not None:
            source_map[str(source_msg_id)] = message_id

        if max_pins > 0:
            while len(pinned) > max_pins:
                old_mid = pinned.pop(0)
                try:
                    await ctx.bot.unpin_chat_message(chat_id=chat_id, message_id=old_mid)
                except Exception as e:
                    logger.debug(f"unpin old chat={chat_id} msg={old_mid}: {e}")
                for source_id, sent_mid in list(source_map.items()):
                    if int(sent_mid) == int(old_mid):
                        source_map.pop(source_id, None)

        save_data(bot_data)
        return True
    except Exception as e:
        logger.debug(f"safe_pin chat={chat_id} msg={message_id}: {e}")
        return False

def get_forward_source_candidates() -> list[int]:
    """
    Return ONLY the configured host/source channel.

    Important:
    The request/activity channel is NOT used as a fallback here.
    This prevents the bot from copying the same message ID from the wrong channel.
    """
    return _unique_ints([get_forward_source_channel_id()])


async def copy_channel_messages_to_user(
    ctx: ContextTypes.DEFAULT_TYPE,
    user_id: int,
    with_forward_tag: bool = False,
    pin_last: bool = False,
) -> int:
    """
    Copy or forward host-channel messages to a user.
    with_forward_tag=False uses copy_message, so there is no source tag.
    with_forward_tag=True uses forward_message, so Telegram shows the source.
    """
    host_msg_ids = get_host_message_ids()
    if not host_msg_ids:
        logger.warning("host_message_ids is empty; nothing to copy.")
        return 0

    sent = 0
    last_mid = None
    source_id = get_forward_source_channel_id()

    sent_by_source: dict[int, int] = {}
    pin_sources = set(get_pin_source_msg_ids())

    for msg_id in host_msg_ids:
        try:
            if with_forward_tag:
                msg = await ctx.bot.forward_message(
                    chat_id=user_id,
                    from_chat_id=source_id,
                    message_id=int(msg_id),
                )
            else:
                msg = await ctx.bot.copy_message(
                    chat_id=user_id,
                    from_chat_id=source_id,
                    message_id=int(msg_id),
                )
            last_mid = msg.message_id
            sent_by_source[int(msg_id)] = msg.message_id
            sent += 1
            logger.info(f"host message OK source={source_id} mid={msg_id} -> uid={user_id}")
            await asyncio.sleep(0.4)
        except BadRequest as e:
            logger.warning(f"host message failed source={source_id} mid={msg_id} -> uid={user_id}: {e}")
        except TelegramError as e:
            logger.warning(f"host message failed source={source_id} mid={msg_id} -> uid={user_id}: {e}")
        except Exception as e:
            logger.warning(f"host message unexpected source={source_id} mid={msg_id} -> uid={user_id}: {e}")

    if sent == 0:
        logger.error(
            f"Host copy FAILED. source={source_id}, msg_ids={host_msg_ids}, user={user_id}. "
            f"Check bot admin rights in host channel, exact channel post IDs, protected content, and source ID."
        )

    if pin_last and sent > 0:
        pinned_any = False
        for source_id, sent_mid in sent_by_source.items():
            if source_id in pin_sources:
                await safe_pin_message(ctx, user_id, sent_mid, source_msg_id=source_id)
                pinned_any = True
        if not pinned_any and last_mid:
            await safe_pin_message(ctx, user_id, last_mid)

    return sent


async def approve_join_request_safe(
    ctx: ContextTypes.DEFAULT_TYPE,
    user_id: int,
) -> tuple[bool, str]:
    """Approve a join request using Bot API only."""
    try:
        await ctx.bot.approve_chat_join_request(get_activity_channel_id(), user_id)
        return True, "bot_api"
    except BadRequest as e:
        bot_error = str(e)
        logger.warning(f"Bot API approve failed uid={user_id}: {bot_error}")
        if "User_already_participant" in bot_error:
            return True, "already_joined"
        if "Hide_requester_missing" in bot_error:
            return False, "request_missing"
        return False, f"Bot API: {bot_error}"
    except TelegramError as e:
        return False, f"Bot API: {e}"
    except Exception as e:
        return False, f"Unexpected: {e}"


async def decline_join_request_safe(
    ctx: ContextTypes.DEFAULT_TYPE,
    user_id: int,
) -> tuple[bool, str]:
    """Decline a join request using Bot API only."""
    try:
        await ctx.bot.decline_chat_join_request(get_activity_channel_id(), user_id)
        return True, "bot_api"
    except BadRequest as e:
        bot_error = str(e)
        logger.warning(f"Bot API decline failed uid={user_id}: {bot_error}")
        if "Hide_requester_missing" in bot_error:
            return False, "request_missing"
        return False, f"Bot API: {bot_error}"
    except TelegramError as e:
        return False, f"Bot API: {e}"
    except Exception as e:
        return False, f"Unexpected: {e}"


# ═══════════════════════════════════════════════════════
#  MESSAGE FORMATTERS
#  Returns (text, entities_or_None)
#  If custom message with saved entities → returns (text, entities)
#  If custom message text only → returns (text, None) with HTML parse_mode
#  If default → returns (html_text, None)
# ═══════════════════════════════════════════════════════

def fmt_accepted_msg(first_name: str) -> tuple[str, list | None]:
    custom      = bot_data["settings"].get("accepted_msg")
    custom_ents = bot_data["settings"].get("accepted_entities")
    if custom:
        return apply_placeholders_with_entities(
            custom,
            custom_ents,
            first_name=first_name,
        )
    return "Request approved ✅", None


def fmt_declined_msg(first_name: str) -> tuple[str, list | None]:
    custom      = bot_data["settings"].get("declined_msg")
    custom_ents = bot_data["settings"].get("declined_entities")
    if custom:
        return apply_placeholders_with_entities(
            custom,
            custom_ents,
            first_name=first_name,
        )
    return (
        f"{E_CROSS} <b>Request Declined</b>\n\n"
        f"{E_WARN} Sorry <b>{first_name}</b>, your join request was <b>declined</b>.\n\n"
        f"{E_INFO} Please contact the admin for more information."
    ), None


def fmt_welcome_msg(first_name: str) -> tuple[str, list | None]:
    custom      = bot_data["settings"].get("welcome_msg")
    custom_ents = bot_data["settings"].get("welcome_entities")
    if custom:
        return apply_placeholders_with_entities(
            custom,
            custom_ents,
            first_name=first_name,
        )
    return (
        f"{E_PARTY} <b>Welcome to the Channel!</b> {E_PARTY}\n\n"
        f"{E_STAR} Hello <b>{first_name}</b>!\n\n"
        f"{E_DIAMOND} You are now a verified member.\n"
        f"{E_FIRE} We're thrilled to have you here!\n\n"
        f"{E_CROWN} Enjoy the content {E_SPARK}"
    ), None


def fmt_request_msg(first_name: str) -> tuple[str, list | None]:
    custom      = bot_data["settings"].get("request_msg")
    custom_ents = bot_data["settings"].get("request_entities")
    if custom:
        return apply_placeholders_with_entities(
            custom,
            custom_ents,
            first_name=first_name,
        )
    return (
        f"{E_BELL} <b>Request Received!</b>\n\n"
        f"{E_STAR} Hello <b>{first_name}</b>!\n\n"
        f"{E_CHECK} Your join request has been <b>received</b>.\n"
        f"{E_HOUR} Please wait while an admin reviews it.\n\n"
        f"{E_INFO} You will be notified once it's processed.\n\n"
        f"{E_SPARK} Thank you for your patience! {E_100}"
    ), None


def fmt_left_msg(first_name: str) -> tuple[str, list | None]:
    custom      = bot_data["settings"].get("left_msg")
    custom_ents = bot_data["settings"].get("left_entities")
    if custom:
        return apply_placeholders_with_entities(
            custom,
            custom_ents,
            first_name=first_name,
             )
    return (
        f"{E_CHAT} <b>Hello {first_name} bhai!</b>\n\n"
        f"{E_INFO} Agar koi problem thi ya aapko help chahiye, toh hum hamesha yahan hain {E_ARROW} @MANISHOPGAMING \n\n"
        f"{E_FREE} <b>SPECIAL GIFT CODE JUST FOR YOU:</b>\n"
        f"{E_SPARK} <code>F65F5A6AB87B0A5AD6141EE73BB9C656</code> {E_SPARK}\n\n"
        f"{E_FIRE} Wapas join karo aur apna reward miss mat karo!\n"
        f"{E_LINK} https://t.me/+jfUDHkhxr644N2Jl \n\n"
        f"{E_LIGHT} Jaldi join karo — niche hack de diya hai, use karo aur profit karo!"
    ), None


# ═══════════════════════════════════════════════════════
#  KEYBOARDS
# ═══════════════════════════════════════════════════════
def admin_home_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📊 Statistics",          callback_data="adm_stats"),
            InlineKeyboardButton("🔮 Pending Requests",    callback_data="adm_pending_0"),
        ],
        [
            InlineKeyboardButton("✅ Accept All",          callback_data="adm_accept_all"),
            InlineKeyboardButton("❌ Decline All",         callback_data="adm_decline_all"),
        ],
        [
            InlineKeyboardButton("📣 Broadcast",           callback_data="adm_broadcast"),
            InlineKeyboardButton("⚙️ Settings",            callback_data="adm_settings"),
        ],
        [
            InlineKeyboardButton("👥 Members",             callback_data="adm_members"),
            InlineKeyboardButton("🚫 Ban / Unban",         callback_data="adm_banned"),
        ],
        [
            InlineKeyboardButton("📌 Hack Pin",            callback_data="adm_pin_manager"),
            InlineKeyboardButton("🚀 Forward Test",        callback_data="adm_fwd_test"),
        ],
        [
            InlineKeyboardButton("🖼️ Welcome Media",       callback_data="adm_welcome_media"),
            InlineKeyboardButton("💎 Broadcast Panel",     callback_data="adm_broadcast"),
        ],
        [
            InlineKeyboardButton("🧩 Message IDs",         callback_data="set_host_message_ids"),
            InlineKeyboardButton("🔄 Refresh",             callback_data="adm_home"),
        ],
        [
            InlineKeyboardButton("⬇️ Get DB",              callback_data="adm_get_db"),
            InlineKeyboardButton("⬆️ Upload DB",           callback_data="adm_upload_db"),
        ],
    ])


def back_kb(cb: str = "adm_home") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("🔙 Back to Panel", callback_data=cb)]]
    )


# ═══════════════════════════════════════════════════════
#  /start
# ═══════════════════════════════════════════════════════
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if is_admin(user.id):
        text = (
            f"{E_CROWN} <b>Admin Control Panel</b> {E_CROWN}\n\n"
            f"{E_STAR} Welcome back, <b>{user.first_name}</b>!\n\n"
            f"{E_CHART} Pending: <b>{len(bot_data['pending_requests'])}</b>\n"
            f"{E_CHECK} Accepted: <b>{bot_data['stats']['total_accepted']}</b>\n"
            f"{E_CROSS} Declined: <b>{bot_data['stats']['total_declined']}</b>\n"
            f"{E_PIN} Hack Pin IDs: <code>{get_pin_source_msg_ids()}</code>"
        )
        await update.message.reply_text(
            text,
            reply_markup=admin_home_kb(),
            parse_mode=ParseMode.HTML,
        )
        return

    uid_str = str(user.id)
    if uid_str in bot_data["banned_users"]:
        return

    track_user_seen(user.id)
    save_data(bot_data)
    await copy_channel_messages_to_user(
        ctx,
        user.id,
        with_forward_tag=bot_data["settings"].get("start_msg_with_forward_tag", False),
        pin_last=get_pin_setting("pin_start_msg"),
    )


# ═══════════════════════════════════════════════════════
#  ADMIN HELP REMOVED

# ═══════════════════════════════════════════════════════
# ═══════════════════════════════════════════════════════
#  JOIN REQUEST HANDLER
# ═══════════════════════════════════════════════════════
async def on_join_request(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    req: ChatJoinRequest = update.chat_join_request
    user    = req.from_user
    uid     = user.id
    uid_str = str(uid)

    # Only process requests from the active/admin-set channel.
    if req.chat.id != get_activity_channel_id():
        logger.info(f"Ignored join request from non-active channel {req.chat.id}; active={get_activity_channel_id()}")
        return
    track_user_seen(uid)
    save_data(bot_data)

    # ── Banned → instant decline ─────────────────────────
    if uid_str in bot_data["banned_users"]:
        try:
            await req.decline()
        except Exception:
            pass
        await safe_send(
            ctx, uid,
            f"{E_STOP} <b>Request Declined</b>\n\n"
            f"{E_BAN} You are banned from this channel.",
        )
        return
    # ── Already recorded → send host-channel copy again (rejoin/duplicate request) ──
    if uid_str in bot_data["pending_requests"]:
        # Rejoin/duplicate request: ALWAYS copy host-channel messages only.
        # No normal fallback message here, so premium emojis stay exactly as in host channel.
        copied_count = await copy_channel_messages_to_user(
            ctx,
            uid,
            with_forward_tag=bot_data["settings"].get("start_msg_with_forward_tag", False),
            pin_last=get_pin_setting("pin_start_msg"),
        )
        if copied_count == 0:
            logger.warning(
                f"No host-channel messages copied for duplicate request uid={uid}. "
                f"Check forward source={get_forward_source_channel_id()} and msg_ids={get_host_message_ids()}"
            )
        return

    # ── Record the request ───────────────────────────────
    bot_data["pending_requests"][uid_str] = {
        "user_id":    uid,
        "first_name": user.first_name or "",
        "last_name":  user.last_name  or "",
        "username":   user.username   or "",
        "date":       datetime.now().isoformat(),
    }
    bot_data["stats"]["total_requests"] += 1
    save_data(bot_data)

    first_name = user.first_name or "there"

    # ── Auto-accept flow ─────────────────────────────────
    if bot_data["settings"].get("auto_accept"):
        delay = bot_data["settings"].get("auto_accept_delay", 0)
        if delay and delay > 0:
            await asyncio.sleep(delay)
        try:
            await req.approve()
        except Exception as e:
            logger.error(f"auto-accept approve uid={uid}: {e}")
            return

        bot_data["pending_requests"].pop(uid_str, None)
        if uid_str not in bot_data["accepted_users"]:
            bot_data["accepted_users"].append(uid_str)
        bot_data["stats"]["total_accepted"] += 1
        save_data(bot_data)

        text, ents = fmt_accepted_msg(first_name)
        sent_msg = await safe_send(ctx, uid, text, entities=ents)
        if sent_msg and get_pin_setting("pin_accepted_msg"):
            await safe_pin_message(ctx, uid, sent_msg.message_id)
        return

    # ── Manual-review flow ───────────────────────────────
    # Join request DM: copy host-channel messages without forward tag.
    # copy_message preserves premium animated emoji entities.
    copied_count = await copy_channel_messages_to_user(
        ctx,
        uid,
        with_forward_tag=bot_data["settings"].get("start_msg_with_forward_tag", False),
        pin_last=get_pin_setting("pin_start_msg"),
    )
    if copied_count == 0:
        logger.warning(
            f"No host-channel messages copied for new request uid={uid}. "
            f"Check forward source={get_forward_source_channel_id()} and msg_ids={get_host_message_ids()}"
        )

    # Notify admins with accept / decline buttons
    admin_text = (
        f"{E_NEW} <b>New Join Request</b>\n\n"
        f"{E_EYES} <b>Name:</b> {user.first_name or ''} {user.last_name or ''}\n"
        f"{E_LINK} <b>Username:</b> "
        f"{'@' + user.username if user.username else 'N/A'}\n"
        f"{E_INFO} <b>ID:</b> <code>{uid}</code>\n"
        f"{E_CHART} <b>Pending:</b> {len(bot_data['pending_requests'])}"
    )
    admin_kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Accept",         callback_data=f"accept_{uid}"),
            InlineKeyboardButton("❌ Decline",        callback_data=f"decline_{uid}"),
        ],
        [
            InlineKeyboardButton("🚫 Ban & Decline",  callback_data=f"ban_{uid}"),
            InlineKeyboardButton("👤 Profile",        url=f"tg://user?id={uid}"),
        ],
    ])
    # Admin notification toggle also controls new join-request alerts.
    # User still receives the copied host-channel message above.
    if bot_data["settings"].get("admin_join_leave_notify", False):
        await notify_admins(ctx, admin_text, reply_markup=admin_kb)


# ═══════════════════════════════════════════════════════
#  CHAT MEMBER HANDLER  (joined / left)
#  FIX: Use ANY_CHAT_MEMBER to capture all member events.
#  FIX: left message now correctly sends via safe_send.
# ═══════════════════════════════════════════════════════
async def on_chat_member(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    # Handle both chat_member and my_chat_member updates
    evt = update.chat_member or update.my_chat_member
    if not evt:
        return

    # Only process events for the active/admin-set channel.
    active_channel_id = get_activity_channel_id()
    if evt.chat.id != active_channel_id:
        logger.info(f"Ignored chat_member update from channel {evt.chat.id}; active={active_channel_id}")
        return

    user       = evt.new_chat_member.user
    if getattr(user, "is_bot", False):
        return
    uid_str    = str(user.id)
    old_status = evt.old_chat_member.status
    new_status = evt.new_chat_member.status
    track_user_seen(user.id)

    LEFT_STATUSES   = {ChatMemberStatus.LEFT, ChatMemberStatus.BANNED}
    ACTIVE_STATUSES = {
        ChatMemberStatus.MEMBER,
        ChatMemberStatus.ADMINISTRATOR,
        ChatMemberStatus.OWNER,
    }

    # ── JOINED ──────────────────────────────────────────
    if old_status in LEFT_STATUSES and new_status in ACTIVE_STATUSES:
        if uid_str not in bot_data["members"]:
            bot_data["members"].append(uid_str)
        bot_data["left_members"] = [
            u for u in bot_data["left_members"] if u != uid_str
        ]
        bot_data["pending_requests"].pop(uid_str, None)

        if uid_str not in bot_data["accepted_users"]:
            bot_data["accepted_users"].append(uid_str)
            bot_data["stats"]["total_accepted"] += 1

        save_data(bot_data)
        await send_welcome_media(ctx, user.id)

        if bot_data["settings"].get("admin_join_leave_notify", False):
            await notify_admins(
                ctx,
                f"{E_GREEN} <b>Member Joined</b>\n\n"
                f"{E_EYES} {user.first_name} "
                f"({'@' + user.username if user.username else 'no username'})\n"
                f"{E_INFO} ID: <code>{user.id}</code>\n"
                f"{E_PIN} Active Channel: <code>{get_activity_channel_id()}</code>",
            )

    # ── LEFT / KICKED ────────────────────────────────────
    elif old_status in ACTIVE_STATUSES and new_status in LEFT_STATUSES:
        bot_data["members"] = [
            u for u in bot_data["members"] if u != uid_str
        ]
        if uid_str not in bot_data["left_members"]:
            bot_data["left_members"].append(uid_str)
        bot_data["stats"]["total_left"] += 1
        save_data(bot_data)

        first_name = user.first_name or "there"

        # FIX: Send left message with custom premium emoji/entity support.
        # Note: Telegram only allows DM if user has started the bot and not blocked it.
        text, ents = fmt_left_msg(first_name)
        sent = await safe_send(ctx, user.id, text, entities=ents)

        if bot_data["settings"].get("admin_join_leave_notify", False):
            await notify_admins(
                ctx,
                f"{E_RED} <b>Member Left</b>\n\n"
                f"{E_EYES} {user.first_name} "
                f"({'@' + user.username if user.username else 'no username'})\n"
                f"{E_INFO} ID: <code>{user.id}</code>\n"
                f"{E_CHAT} Leave DM: <b>{'Sent ✅' if sent else 'Failed ❌ — user must start bot / not block bot'}</b>\n"
                f"{E_PIN} Active Channel: <code>{get_activity_channel_id()}</code>",
            )


async def on_pinned_service_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Delete Telegram's automatic 'pinned a message' service notice when possible."""
    msg = update.effective_message
    if not msg or not msg.pinned_message:
        return
    try:
        await msg.delete()
    except TelegramError as e:
        logger.debug(f"delete pin service message failed chat={msg.chat_id}: {e}")
    except Exception as e:
        logger.debug(f"delete pin service message unexpected chat={msg.chat_id}: {e}")


# ═══════════════════════════════════════════════════════
#  CALLBACK QUERY ROUTER
# ═══════════════════════════════════════════════════════
async def on_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q    = update.callback_query
    user = q.from_user
    data = q.data

    if not is_admin(user.id):
        await q.answer("⛔ Not authorized!", show_alert=True)
        return

    await q.answer()

    if data.startswith("accept_"):
        uid = int(data[7:])
        await _cb_accept_one(q, ctx, uid)

    elif data.startswith("decline_"):
        uid = int(data[8:])
        await _cb_decline_one(q, ctx, uid)

    elif data.startswith("ban_"):
        uid = int(data[4:])
        await _cb_ban_one(q, ctx, uid)

    elif data.startswith("unban_"):
        uid_str = data[6:]
        if uid_str in bot_data["banned_users"]:
            bot_data["banned_users"].remove(uid_str)
            save_data(bot_data)
        await _show_banned(q, ctx)

    elif data == "adm_home":
        await _show_home(q, ctx)

    elif data == "adm_stats":
        await _show_stats(q, ctx)

    elif data.startswith("adm_pending_"):
        page = int(data.split("_")[2])
        await _show_pending(q, ctx, page)

    elif data == "adm_accept_all":
        await _cb_accept_all(q, ctx)

    elif data == "adm_decline_all":
        await _cb_decline_all(q, ctx)

    elif data == "adm_broadcast":
        await _show_broadcast_panel(q, ctx)

    elif data == "broadcast_compose":
        ctx.user_data["awaiting"] = "broadcast"
        await q.edit_message_text(
            f"{E_MEGA} <b>Broadcast — Text</b>\n\n"
            f"Send the message now.\n"
            f"{E_ARROW} Target: <b>{bot_data['settings'].get('broadcast_target', 'all')}</b>\n"
            f"{E_LINK} Buttons: <b>{len(bot_data['settings'].get('broadcast_buttons', []))} attached</b>\n"
            f"{E_SPARK} Premium emojis supported!\n\n"
            f"/cancel to abort.",
            parse_mode=ParseMode.HTML,
        )

    elif data == "broadcast_compose_media":
        ctx.user_data["awaiting"] = "broadcast_media"
        await q.edit_message_text(
            f"{E_MEGA} <b>Broadcast — Media</b>\n\n"
            f"Send a <b>photo, video, document, audio, or GIF</b> now.\n"
            f"Caption is optional (supports premium emojis ✅).\n\n"
            f"{E_ARROW} Target: <b>{bot_data['settings'].get('broadcast_target', 'all')}</b>\n"
            f"{E_LINK} Buttons: <b>{len(bot_data['settings'].get('broadcast_buttons', []))} attached</b>\n\n"
            f"/cancel to abort.",
            parse_mode=ParseMode.HTML,
        )

    elif data == "bcast_manage_buttons":
        await _show_broadcast_buttons_panel(q, ctx)

    elif data == "bcast_add_button":
        ctx.user_data["awaiting"] = "bcast_button_text"
        await q.edit_message_text(
            f"{E_LINK} <b>Add Broadcast Button</b>\n\n"
            f"{E_STAR} Step 1/2: Send the <b>button text</b>.\n"
            f"Example: <code>Join Now 🚀</code>\n\n"
            f"/cancel to abort.",
            parse_mode=ParseMode.HTML,
        )

    elif data == "bcast_clear_buttons":
        bot_data["settings"]["broadcast_buttons"] = []
        save_data(bot_data)
        await _show_broadcast_buttons_panel(q, ctx)

    elif data.startswith("bcast_remove_btn_"):
        try:
            idx = int(data.split("_")[-1])
            btns = bot_data["settings"].get("broadcast_buttons", [])
            if 0 <= idx < len(btns):
                btns.pop(idx)
                bot_data["settings"]["broadcast_buttons"] = btns
                save_data(bot_data)
        except Exception:
            pass
        await _show_broadcast_buttons_panel(q, ctx)

    elif data.startswith("bcast_target_"):
        bot_data["settings"]["broadcast_target"] = data.replace("bcast_target_", "", 1)
        save_data(bot_data)
        await _show_broadcast_panel(q, ctx)

    elif data in {"toggle_bcast_forward", "toggle_pin_broadcast", "toggle_bcast_include_banned"}:
        if data == "toggle_bcast_forward":
            bot_data["settings"]["broadcast_mode"] = (
                "forward" if bot_data["settings"].get("broadcast_mode", "copy") == "copy" else "copy"
            )
        elif data == "toggle_pin_broadcast":
            bot_data["settings"]["pin_broadcast"] = not bot_data["settings"].get("pin_broadcast", False)
        else:
            bot_data["settings"]["broadcast_include_banned"] = not bot_data["settings"].get("broadcast_include_banned", False)
        save_data(bot_data)
        await _show_broadcast_panel(q, ctx)

    elif data == "adm_settings":
        await _show_settings(q, ctx)

    elif data == "toggle_auto_accept":
        bot_data["settings"]["auto_accept"] = not bot_data["settings"].get(
            "auto_accept", False
        )
        save_data(bot_data)
        await _show_settings(q, ctx)

    elif data == "toggle_admin_join_leave_notify":
        bot_data["settings"]["admin_join_leave_notify"] = not bot_data["settings"].get(
            "admin_join_leave_notify", False
        )
        save_data(bot_data)
        await _show_settings(q, ctx)

    elif data in {
        "toggle_pin_start_msg",
        "toggle_pin_accepted_msg",
        "toggle_pin_declined_msg",
        "toggle_pin_welcome_msg",
        "toggle_start_forward_tag",
    }:
        key_map = {
            "toggle_pin_start_msg": "pin_start_msg",
            "toggle_pin_accepted_msg": "pin_accepted_msg",
            "toggle_pin_declined_msg": "pin_declined_msg",
            "toggle_pin_welcome_msg": "pin_welcome_msg",
            "toggle_start_forward_tag": "start_msg_with_forward_tag",
        }
        key = key_map[data]
        bot_data["settings"][key] = not bot_data["settings"].get(key, False)
        save_data(bot_data)
        await _show_settings(q, ctx)

    elif data == "set_max_pins":
        ctx.user_data["awaiting"] = "max_pins_per_user"
        await q.edit_message_text(
            f"{E_PIN} Send max pinned messages per user.\n"
            f"Use <b>0</b> for unlimited, <b>1</b> to keep only latest.\n"
            f"/cancel to abort.",
            parse_mode=ParseMode.HTML,
        )

    elif data == "set_delay":
        ctx.user_data["awaiting"] = "auto_accept_delay"
        await q.edit_message_text(
            f"{E_HOUR} Send the delay in <b>seconds</b> (0 = instant):\n"
            f"/cancel to abort.",
            parse_mode=ParseMode.HTML,
        )

    elif data == "set_activity_channel":
        ctx.user_data["awaiting"] = "activity_channel_id"
        await q.edit_message_text(
            f"{E_MEGA} <b>Set Activity Channel</b>\n\n"
            f"Send the channel ID for all bot activities.\n"
            f"Current: <code>{get_activity_channel_id()}</code>\n\n"
            f"Example: <code>-1002232875049</code>\n\n"
            f"Make sure the bot is admin in that channel and has permission to receive member updates.\n"
            f"/cancel to abort.",
            parse_mode=ParseMode.HTML,
        )


    elif data == "set_forward_source_channel":
        ctx.user_data["awaiting"] = "forward_source_channel_id"
        await q.edit_message_text(
            f"{E_MEGA} <b>Set Host Channel</b>\n\n"
            f"Send the host/source channel ID.\n"
            f"Current: <code>{get_forward_source_channel_id()}</code>\n\n"
            f"Example: <code>-1002701185142</code>\n\n"
            f"/cancel to abort.",
            parse_mode=ParseMode.HTML,
        )

    elif data == "set_host_message_ids":
        ctx.user_data["awaiting"] = "host_message_ids"
        await q.edit_message_text(
            f"{E_MAIL} <b>Set Host Message IDs</b>\n\n"
            f"Current: <code>{get_host_message_ids()}</code>\n"
            f"Send one or more IDs: <code>34</code> or <code>34,35</code>.\n"
            f"/cancel to abort.",
            parse_mode=ParseMode.HTML,
        )

    elif data == "adm_pin_manager":
        await _show_pin_manager(q, ctx)

    elif data == "set_pin_source_msg_ids":
        ctx.user_data["awaiting"] = "pin_source_msg_ids"
        await q.edit_message_text(
            f"{E_PIN} <b>Set Hack Pin Message IDs</b>\n\n"
            f"Host IDs: <code>{get_host_message_ids()}</code>\n"
            f"Currently pinned source IDs: <code>{get_pin_source_msg_ids()}</code>\n"
            f"Send IDs to pin, for example <code>34</code> or <code>34,35</code>.\n"
            f"/cancel to abort.",
            parse_mode=ParseMode.HTML,
        )

    elif data == "set_auto_unpin_source_msg_ids":
        ctx.user_data["awaiting"] = "auto_unpin_source_msg_ids"
        await q.edit_message_text(
            f"{E_TRASH} <b>Auto Unpin Manager</b>\n\n"
            f"Current auto-unpin source IDs: <code>{get_auto_unpin_source_msg_ids()}</code>\n"
            f"Send source message IDs to auto-unpin before new pins, or <code>0</code> for none.\n"
            f"/cancel to abort.",
            parse_mode=ParseMode.HTML,
        )

    elif data.startswith("setmsg_"):
        msg_type = data[7:]
        ctx.user_data["awaiting"] = f"setmsg_{msg_type}"
        await q.edit_message_text(
            f"{E_EDIT} <b>Set {msg_type} message</b>\n\n"
            f"Send the new message.\n"
            f"✅ <b>Premium emojis are fully supported</b> — just paste/send with them.\n"
            f"Placeholders: <code>{{first_name}}</code>\n\n"
            f"/cancel to abort.",
            parse_mode=ParseMode.HTML,
        )

    elif data == "reset_msgs":
        for k in [
            "request_msg",  "request_entities",
            "accepted_msg", "accepted_entities",
            "declined_msg", "declined_entities",
            "welcome_msg",  "welcome_entities",
            "left_msg",     "left_entities",
        ]:
            bot_data["settings"][k] = None
        save_data(bot_data)
        await _show_settings(q, ctx)

    elif data == "adm_members":
        await _show_members(q, ctx)

    elif data == "adm_banned":
        await _show_banned(q, ctx)

    elif data == "adm_welcome_media":
        await _show_welcome_media(q, ctx)

    elif data == "set_welcome_media":
        ctx.user_data["awaiting"] = "welcome_media"
        await q.edit_message_text(
            f"{E_BOOK} <b>Welcome Media Manager</b>\n\n"
            f"Send a photo, video, or document to use as welcome media.\n"
            f"/cancel to abort.",
            parse_mode=ParseMode.HTML,
        )

    elif data == "clear_welcome_media":
        for k in ["welcome_media_type", "welcome_media_file_id", "welcome_media_caption", "welcome_media_entities"]:
            bot_data["settings"][k] = None
        bot_data["stats"]["welcome_media_set"] = False
        save_data(bot_data)
        await _show_welcome_media(q, ctx)

    elif data == "adm_fwd_test":
        await _fwd_test(q, ctx)

    elif data == "adm_get_db":
        await _cb_get_db(q, ctx)

    elif data == "adm_upload_db":
        ctx.user_data["awaiting"] = "upload_db"
        await q.edit_message_text(
            f"{E_UP} <b>Upload Database</b>\n\n"
            f"Send the <code>bot_data.json</code> file now.\n"
            f"⚠️ This will <b>overwrite</b> current data!\n\n"
            f"/cancel to abort.",
            parse_mode=ParseMode.HTML,
        )

    else:
        await q.answer("Unknown action.", show_alert=True)


# ═══════════════════════════════════════════════════════
#  CALLBACK HELPERS
# ═══════════════════════════════════════════════════════
async def _show_home(q, ctx):
    text = (
        f"{E_CROWN} <b>Admin Control Panel</b> {E_CROWN}\n\n"
        f"{E_CHART} <b>Quick Stats</b>\n"
        f"{E_GREEN}  Pending  : <b>{len(bot_data['pending_requests'])}</b>\n"
        f"{E_CHECK}  Accepted : <b>{bot_data['stats']['total_accepted']}</b>\n"
        f"{E_CROSS}  Declined : <b>{bot_data['stats']['total_declined']}</b>\n"
        f"{E_RED}   Left     : <b>{bot_data['stats']['total_left']}</b>\n"
        f"{E_STOP}  Banned   : <b>{len(bot_data['banned_users'])}</b>\n\n"
        f"{E_ARROW} Choose an action below:"
    )
    await q.edit_message_text(
        text,
        reply_markup=admin_home_kb(),
        parse_mode=ParseMode.HTML,
    )


def fmt_bot_stats() -> str:
    active, dead = compute_active_dead()
    total_users = len(all_tracked_users())
    bot_data["stats"]["total_users"] = total_users
    bot_data["stats"]["active_users"] = active
    bot_data["stats"]["dead_users"] = dead
    save_data(bot_data)

    auto_approve = "ON" if bot_data["settings"].get("auto_accept", False) else "OFF"
    welcome_media = "Set" if bot_data["stats"].get("welcome_media_set") else "Not Set"
    broadcasts_sent = bot_data["stats"].get("broadcasts_sent", len(bot_data.get("broadcast_history", [])))

    return (
        f"{E_CHART} <b>BOT STATISTICS</b> {E_CHART}\n\n"
        f"{E_EYES} Total Users : <b>{total_users}</b>\n"
        f"{E_CHECK} Active Users : <b>{active}</b>\n"
        f"{E_CROSS} Dead Users : <b>{dead}</b>\n\n"
        f"{E_NEW} Today Users : <b>{get_today_users()}</b>\n"
        f"{E_CHART} Last 7 Days : <b>{get_last7_users()}</b>\n\n"
        f"{E_PLAY} Auto Approve : <b>{auto_approve}</b>\n"
        f"{E_LIGHT} Bot Status : <b>Online</b>\n\n"
        f"{E_HOUR} Pending Requests : <b>{len(bot_data['pending_requests'])}</b>\n"
        f"{E_CHECK} Total Accepted : <b>{bot_data['stats']['total_accepted']}</b>\n"
        f"{E_CROSS} Total Declined : <b>{bot_data['stats']['total_declined']}</b>\n"
        f"{E_GREEN} Current Members : <b>{len(bot_data['members'])}</b>\n"
        f"{E_RED} Total Left : <b>{bot_data['stats']['total_left']}</b>\n"
        f"{E_STOP} Banned Users : <b>{len(bot_data['banned_users'])}</b>\n"
        f"{E_FIRE} Total Requests : <b>{bot_data['stats']['total_requests']}</b>\n\n"
        f"{E_BOOK} Welcome Media : <b>{welcome_media}</b>\n"
        f"{E_MEGA} Broadcasts Sent : <b>{broadcasts_sent}</b>"
    )


async def _show_stats(q, ctx):
    await q.edit_message_text(
        fmt_bot_stats(), reply_markup=back_kb(), parse_mode=ParseMode.HTML
    )


async def _show_pending(q, ctx, page: int = 0):
    pending  = list(bot_data["pending_requests"].items())
    per_page = 8
    total    = len(pending)
    start    = page * per_page
    end      = start + per_page
    chunk    = pending[start:end]

    if not pending:
        await q.edit_message_text(
            f"{E_CHECK} <b>No pending requests!</b>",
            reply_markup=back_kb(),
            parse_mode=ParseMode.HTML,
        )
        return

    text = (
        f"{E_EYES} <b>Pending Requests — Page {page + 1}</b>"
        f" ({total} total)\n\n"
    )
    buttons: list[list[InlineKeyboardButton]] = []

    for uid_str, info in chunk:
        name = info.get("first_name", "?")
        text += f"{E_STAR} <b>{name}</b> — <code>{uid_str}</code>\n"
        buttons.append([
            InlineKeyboardButton(f"✅ {name}", callback_data=f"accept_{uid_str}"),
            InlineKeyboardButton(f"❌ {name}", callback_data=f"decline_{uid_str}"),
            InlineKeyboardButton("🚫",          callback_data=f"ban_{uid_str}"),
        ])

    nav = []
    if page > 0:
        nav.append(
            InlineKeyboardButton("⬅️ Prev", callback_data=f"adm_pending_{page - 1}")
        )
    if end < total:
        nav.append(
            InlineKeyboardButton("➡️ Next", callback_data=f"adm_pending_{page + 1}")
        )
    if nav:
        buttons.append(nav)

    buttons.append([
        InlineKeyboardButton("✅ Accept All",  callback_data="adm_accept_all"),
        InlineKeyboardButton("❌ Decline All", callback_data="adm_decline_all"),
    ])
    buttons.append([InlineKeyboardButton("🔙 Back", callback_data="adm_home")])

    await q.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(buttons),
        parse_mode=ParseMode.HTML,
    )


async def _cb_accept_one(q, ctx, target_id: int):
    uid_str = str(target_id)

    info = bot_data["pending_requests"].get(uid_str, {})
    first_name = info.get("first_name", "there") or "there"

    ok, method = await approve_join_request_safe(ctx, target_id)
    if not ok:
        try:
            await q.edit_message_text(
                f"{E_CROSS} <b>Accept failed!</b>\n"
                f"{E_INFO} User <code>{target_id}</code> was not approved.\n\n"
                f"<code>{method[:350]}</code>",
                reply_markup=back_kb("adm_pending_0"),
                parse_mode=ParseMode.HTML,
            )
        except BadRequest:
            pass
        return

    bot_data["pending_requests"].pop(uid_str, None)
    if uid_str not in bot_data["accepted_users"]:
        bot_data["accepted_users"].append(uid_str)
    if uid_str not in bot_data["members"]:
        bot_data["members"].append(uid_str)
    bot_data["stats"]["total_accepted"] += 1
    save_data(bot_data)

    text, ents = fmt_accepted_msg(first_name)
    sent_msg = await safe_send(ctx, target_id, text, entities=ents)
    if sent_msg and get_pin_setting("pin_accepted_msg"):
        await safe_pin_message(ctx, target_id, sent_msg.message_id)

    try:
        await q.edit_message_text(
            f"{E_CHECK} <b>Accepted!</b>\n"
            f"{E_INFO} User <code>{target_id}</code> approved via <b>{method}</b>.",
            reply_markup=back_kb("adm_pending_0"),
            parse_mode=ParseMode.HTML,
        )
    except BadRequest:
        pass


async def _cb_decline_one(q, ctx, target_id: int):
    uid_str = str(target_id)

    info       = bot_data["pending_requests"].get(uid_str, {})
    first_name = info.get("first_name", "there") or "there"

    ok, method = await decline_join_request_safe(ctx, target_id)
    if not ok:
        try:
            await q.edit_message_text(
                f"{E_CROSS} <b>Decline failed!</b>\n"
                f"{E_INFO} User <code>{target_id}</code> was not declined.\n\n"
                f"<code>{method[:350]}</code>",
                reply_markup=back_kb("adm_pending_0"),
                parse_mode=ParseMode.HTML,
            )
        except BadRequest:
            pass
        return

    bot_data["pending_requests"].pop(uid_str, None)
    if uid_str not in bot_data["declined_users"]:
        bot_data["declined_users"].append(uid_str)
    bot_data["stats"]["total_declined"] += 1
    save_data(bot_data)

    text, ents = fmt_declined_msg(first_name)
    sent_msg = await safe_send(ctx, target_id, text, entities=ents)
    if sent_msg and get_pin_setting("pin_declined_msg"):
        await safe_pin_message(ctx, target_id, sent_msg.message_id)

    try:
        await q.edit_message_text(
            f"{E_CROSS} <b>Declined!</b>\n"
            f"{E_INFO} User <code>{target_id}</code> declined via <b>{method}</b>.",
            reply_markup=back_kb("adm_pending_0"),
            parse_mode=ParseMode.HTML,
        )
    except BadRequest:
        pass


async def _cb_ban_one(q, ctx, target_id: int):
    uid_str = str(target_id)

    try:
        await ctx.bot.decline_chat_join_request(get_activity_channel_id(), target_id)
    except Exception:
        pass

    bot_data["pending_requests"].pop(uid_str, None)
    if uid_str not in bot_data["banned_users"]:
        bot_data["banned_users"].append(uid_str)
    bot_data["stats"]["total_declined"] += 1
    save_data(bot_data)

    await safe_send(
        ctx, target_id,
        f"{E_STOP} <b>Banned & Declined</b>\n\n"
        f"{E_CROSS} You have been banned from this channel.",
    )

    try:
        await q.edit_message_text(
            f"{E_STOP} <b>Banned!</b>\n"
            f"{E_INFO} User <code>{target_id}</code> banned & declined.",
            reply_markup=back_kb("adm_pending_0"),
            parse_mode=ParseMode.HTML,
        )
    except BadRequest:
        pass


async def _cb_accept_all(q, ctx):
    pending = dict(bot_data["pending_requests"])
    if not pending:
        await q.edit_message_text(
            f"{E_CHECK} No pending requests!",
            reply_markup=back_kb(),
            parse_mode=ParseMode.HTML,
        )
        return

    try:
        await q.edit_message_text(
            f"{E_HOUR} <b>Accepting {len(pending)} requests…</b>",
            parse_mode=ParseMode.HTML,
        )
    except BadRequest:
        pass

    ok = fail = 0
    failed_lines = []
    for uid_str, info in pending.items():
        uid        = int(uid_str)
        first_name = info.get("first_name", "there") or "there"

        approved, method = await approve_join_request_safe(ctx, uid)
        if not approved:
            fail += 1
            failed_lines.append(f"{uid}: {method[:120]}")
            await asyncio.sleep(0.2)
            continue

        bot_data["pending_requests"].pop(uid_str, None)
        if uid_str not in bot_data["accepted_users"]:
            bot_data["accepted_users"].append(uid_str)
        if uid_str not in bot_data["members"]:
            bot_data["members"].append(uid_str)
        bot_data["stats"]["total_accepted"] += 1
        ok += 1

        text, ents = fmt_accepted_msg(first_name)
        sent_msg = await safe_send(ctx, uid, text, entities=ents)
        if sent_msg and get_pin_setting("pin_accepted_msg"):
            await safe_pin_message(ctx, uid, sent_msg.message_id)
        await asyncio.sleep(0.2)

    save_data(bot_data)

    extra = ""
    if failed_lines:
        extra = "\n\n<b>Failed:</b>\n<code>" + "\n".join(failed_lines[:5]) + "</code>"
    try:
        await q.edit_message_text(
            f"{E_CHECK} <b>Bulk Accept Complete!</b>\n\n"
            f"{E_GREEN} Success : {ok}\n"
            f"{E_RED}   Failed  : {fail}"
            f"{extra}",
            reply_markup=back_kb(),
            parse_mode=ParseMode.HTML,
        )
    except BadRequest:
        pass


async def _cb_decline_all(q, ctx):
    pending = dict(bot_data["pending_requests"])
    if not pending:
        await q.edit_message_text(
            f"{E_CHECK} No pending requests!",
            reply_markup=back_kb(),
            parse_mode=ParseMode.HTML,
        )
        return

    try:
        await q.edit_message_text(
            f"{E_HOUR} <b>Declining {len(pending)} requests…</b>",
            parse_mode=ParseMode.HTML,
        )
    except BadRequest:
        pass

    ok = fail = 0
    failed_lines = []
    for uid_str, info in pending.items():
        uid        = int(uid_str)
        first_name = info.get("first_name", "there") or "there"

        declined, method = await decline_join_request_safe(ctx, uid)
        if not declined:
            fail += 1
            failed_lines.append(f"{uid}: {method[:120]}")
            await asyncio.sleep(0.2)
            continue

        bot_data["pending_requests"].pop(uid_str, None)
        if uid_str not in bot_data["declined_users"]:
            bot_data["declined_users"].append(uid_str)
        bot_data["stats"]["total_declined"] += 1
        ok += 1
        text, ents = fmt_declined_msg(first_name)
        sent_msg = await safe_send(ctx, uid, text, entities=ents)
        if sent_msg and get_pin_setting("pin_declined_msg"):
            await safe_pin_message(ctx, uid, sent_msg.message_id)
        await asyncio.sleep(0.2)

    save_data(bot_data)

    extra = ""
    if failed_lines:
        extra = "\n\n<b>Failed:</b>\n<code>" + "\n".join(failed_lines[:5]) + "</code>"
    try:
        await q.edit_message_text(
            f"{E_CROSS} <b>Bulk Decline Complete!</b>\n\n"
            f"{E_GREEN} Success : {ok}\n"
            f"{E_RED}   Failed  : {fail}"
            f"{extra}",
            reply_markup=back_kb(),
            parse_mode=ParseMode.HTML,
        )
    except BadRequest:
        pass


async def _show_settings(q, ctx):
    s     = bot_data["settings"]
    aa    = s.get("auto_accept", False)
    admin_notify = s.get("admin_join_leave_notify", False)
    delay = s.get("auto_accept_delay", 0)

    def yn(v): return "✅ On" if v else "❌ Off"

    text = (
        f"{E_GEAR} <b>Settings</b>\n\n"
        f"{E_PLAY}  Auto Accept : {yn(aa)}\n"
        f"{E_BELL} Admin Join/Leave Notify : {yn(admin_notify)}\n"
        f"{E_HOUR} Auto Delay  : {delay}s\n"
        f"{E_MEGA} Activity Channel : <code>{get_activity_channel_id()}</code>\n"
        f"{E_MAIL} Forward Source : <code>{get_forward_source_channel_id()}</code>\n"
        f"{E_INFO} Host Msg IDs : <code>{get_host_message_ids()}</code>\n"\
        f"{E_PIN} Hack Pin IDs : <code>{get_pin_source_msg_ids()}</code>\n"\
        f"{E_TRASH} Auto Unpin IDs : <code>{get_auto_unpin_source_msg_ids()}</code>\n\n"
        f"{E_PIN} <b>Pin settings</b>\n"
        f"  Start host msg : {yn(s.get('pin_start_msg', True))}\n"
        f"  Accepted msg   : {yn(s.get('pin_accepted_msg', False))}\n"
        f"  Declined msg   : {yn(s.get('pin_declined_msg', True))}\n"
        f"  Welcome msg    : {yn(s.get('pin_welcome_msg', False))}\n"
        f"  Start forward tag : {yn(s.get('start_msg_with_forward_tag', False))}\n"
        f"  Max pins/user  : <b>{get_max_pins()}</b>\n\n"
        f"{E_EDIT} <b>Custom messages</b>\n"
        f"  Request  : {'✅ custom' if s.get('request_msg')  else '❌ default'}\n"
        f"  Accepted : {'✅ custom' if s.get('accepted_msg') else '❌ default'}\n"
        f"  Declined : {'✅ custom' if s.get('declined_msg') else '❌ default'}\n"
        f"  Welcome  : {'✅ custom' if s.get('welcome_msg')  else '❌ default'}\n"
        f"  Left     : {'✅ custom' if s.get('left_msg')     else '❌ default'}\n"
    )
    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                f"{'🔴 Disable' if aa else '🟢 Enable'} Auto Accept",
                callback_data="toggle_auto_accept",
            ),
            InlineKeyboardButton("⏱️ Set Delay", callback_data="set_delay"),
        ],
        [
            InlineKeyboardButton(
                f"{'🔴 Disable' if admin_notify else '🟢 Enable'} Admin Notify",
                callback_data="toggle_admin_join_leave_notify",
            ),
        ],
        [
            InlineKeyboardButton("✏️ Request msg",   callback_data="setmsg_request"),
            InlineKeyboardButton("✏️ Accepted msg",  callback_data="setmsg_accepted"),
        ],
        [
            InlineKeyboardButton("✏️ Declined msg",  callback_data="setmsg_declined"),
            InlineKeyboardButton("✏️ Welcome msg",   callback_data="setmsg_welcome"),
        ],
        [
            InlineKeyboardButton("✏️ Left msg",      callback_data="setmsg_left"),
            InlineKeyboardButton("🗑️ Reset All Msgs", callback_data="reset_msgs"),
        ],
        [
            InlineKeyboardButton("📌 Pin /start",    callback_data="toggle_pin_start_msg"),
            InlineKeyboardButton("📌 Pin Accepted",  callback_data="toggle_pin_accepted_msg"),
        ],
        [
            InlineKeyboardButton("📌 Pin Declined",  callback_data="toggle_pin_declined_msg"),
            InlineKeyboardButton("📌 Pin Welcome",   callback_data="toggle_pin_welcome_msg"),
        ],
        [
            InlineKeyboardButton("🏷️ Forward Tag",   callback_data="toggle_start_forward_tag"),
            InlineKeyboardButton("🔢 Max Pins",      callback_data="set_max_pins"),
        ],
        [
            InlineKeyboardButton("📢 Activity Channel", callback_data="set_activity_channel"),
        ],
        [
            InlineKeyboardButton("📡 Set Host Channel",  callback_data="set_forward_source_channel"),
        ],
        [
            InlineKeyboardButton("🧩 Message IDs",   callback_data="set_host_message_ids"),
            InlineKeyboardButton("📌 Hack Pin Mgr",  callback_data="adm_pin_manager"),
        ],
        [InlineKeyboardButton("🔙 Back to Panel", callback_data="adm_home")],
    ])
    await q.edit_message_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)



async def _show_pin_manager(q, ctx):
    s = bot_data["settings"]
    text = (
        f"{E_PIN} <b>Hack Pin Manager</b>\n\n"
        f"{E_MAIL} Host Channel : <code>{get_forward_source_channel_id()}</code>\n"
        f"{E_INFO} Host Message IDs : <code>{get_host_message_ids()}</code>\n"
        f"{E_PIN} Pin Source IDs : <code>{get_pin_source_msg_ids()}</code>\n"
        f"{E_TRASH} Auto Unpin Source IDs : <code>{get_auto_unpin_source_msg_ids()}</code>\n"
        f"{E_LINK} Forward Tag : <b>{'ON' if s.get('start_msg_with_forward_tag') else 'OFF'}</b>\n"
        f"{E_PIN} Multiple Pins Limit : <b>{get_max_pins()}</b>"
    )
    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📡 Host Channel",    callback_data="set_forward_source_channel"),
            InlineKeyboardButton("🧩 Message IDs",     callback_data="set_host_message_ids"),
        ],
        [
            InlineKeyboardButton("📌 Which Pin",       callback_data="set_pin_source_msg_ids"),
            InlineKeyboardButton("🔢 Max Pins",        callback_data="set_max_pins"),
        ],
        [
            InlineKeyboardButton("🗑️ Auto Unpin",      callback_data="set_auto_unpin_source_msg_ids"),
            InlineKeyboardButton("🏷️ Forward Tag",     callback_data="toggle_start_forward_tag"),
        ],
        [InlineKeyboardButton("🔙 Back to Settings", callback_data="adm_settings")],
    ])
    await q.edit_message_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)


def _broadcast_targets() -> list[str]:
    s = bot_data["settings"]
    target = s.get("broadcast_target", "all")
    include_banned = bool(s.get("broadcast_include_banned", False))
    if target == "pending":
        users = list(bot_data.get("pending_requests", {}).keys())
    elif target == "approved":
        users = list(set(bot_data.get("accepted_users", [])) | set(bot_data.get("members", [])))
    elif target == "rejected":
        users = list(bot_data.get("declined_users", []))
    else:
        users = list(all_tracked_users())
    if not include_banned:
        banned = set(bot_data.get("banned_users", []))
        users = [u for u in users if u not in banned]
    return [str(u) for u in _unique_ints(users)]


async def _show_broadcast_panel(q, ctx):
    s = bot_data["settings"]
    targets = _broadcast_targets()
    pending_count = len(bot_data.get("pending_requests", {}))
    btns = s.get("broadcast_buttons", [])
    target = s.get("broadcast_target", "all")

    # Emoji for each target
    target_emoji = {
        "all":      f"{E_GLOBE} All",
        "pending":  f"{E_HOUR} Pending",
        "approved": f"{E_CHECK} Approved",
        "rejected": f"{E_CROSS} Rejected",
    }

    text = (
        f"{E_MEGA} <b>Broadcast Panel</b>\n\n"
        f"{E_ARROW} Target  : <b>{target_emoji.get(target, target)}</b>\n"
        f"{E_EYES}  Users   : <b>{len(targets)}</b>\n"
        f"{E_HOUR}  Pending : <b>{pending_count}</b>\n"
        f"{E_GEAR}  Mode    : <b>{s.get('broadcast_mode', 'copy').upper()}</b>\n"
        f"{E_PIN}   Pin     : <b>{'✅ ON' if s.get('pin_broadcast') else '❌ OFF'}</b>\n"
        f"{E_BAN}   Banned  : <b>{'Include' if s.get('broadcast_include_banned') else 'Exclude'}</b>\n"
        f"{E_LINK}  Buttons : <b>{len(btns)} set</b>"
        + (f"\n  " + "  ".join(f"[{b['text']}]" for b in btns[:3]) if btns else "")
    )
    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(f"{'✅' if target=='all'      else '⚪'} 🌐 All",       callback_data="bcast_target_all"),
            InlineKeyboardButton(f"{'✅' if target=='pending'  else '⚪'} ⏳ Pending",   callback_data="bcast_target_pending"),
        ],
        [
            InlineKeyboardButton(f"{'✅' if target=='approved' else '⚪'} 💚 Approved",  callback_data="bcast_target_approved"),
            InlineKeyboardButton(f"{'✅' if target=='rejected' else '⚪'} ❌ Rejected",  callback_data="bcast_target_rejected"),
        ],
        [
            InlineKeyboardButton(
                f"{'📤 Forward' if s.get('broadcast_mode','copy')=='forward' else '📋 Copy'} Mode",
                callback_data="toggle_bcast_forward",
            ),
            InlineKeyboardButton(
                f"{'📌 Unpin' if s.get('pin_broadcast') else '📌 Pin'} Msg",
                callback_data="toggle_pin_broadcast",
            ),
        ],
        [
            InlineKeyboardButton(
                f"{'🚫 Excl.' if not s.get('broadcast_include_banned') else '✅ Incl.'} Banned",
                callback_data="toggle_bcast_include_banned",
            ),
            InlineKeyboardButton(
                f"🔗 {'Edit' if btns else 'Add'} Buttons ({len(btns)})",
                callback_data="bcast_manage_buttons",
            ),
        ],
        [InlineKeyboardButton(f"✉️ Send Text Broadcast",    callback_data="broadcast_compose")],
        [InlineKeyboardButton(f"🖼️ Send Media Broadcast",  callback_data="broadcast_compose_media")],
        [InlineKeyboardButton("🔙 Back to Panel", callback_data="adm_home")],
    ])
    await q.edit_message_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)


async def _show_broadcast_buttons_panel(q, ctx):
    """Panel to manage inline buttons attached to broadcast messages."""
    btns = bot_data["settings"].get("broadcast_buttons", [])
    text = (
        f"{E_LINK} <b>Broadcast Buttons</b>\n\n"
        f"These buttons will be attached to your next broadcast.\n"
        f"Max <b>5 buttons</b> supported.\n\n"
    )
    if btns:
        for i, b in enumerate(btns, 1):
            text += f"{i}. <b>{b['text']}</b> → <code>{b['url']}</code>\n"
    else:
        text += f"{E_INFO} No buttons set yet."

    kb_rows = []
    if len(btns) < 5:
        kb_rows.append([InlineKeyboardButton("➕ Add Button", callback_data="bcast_add_button")])
    if btns:
        kb_rows.append([InlineKeyboardButton("🗑️ Clear All Buttons", callback_data="bcast_clear_buttons")])
        for i in range(len(btns)):
            kb_rows.append([InlineKeyboardButton(f"❌ Remove #{i+1} — {btns[i]['text']}", callback_data=f"bcast_remove_btn_{i}")])
    kb_rows.append([InlineKeyboardButton("🔙 Back to Broadcast", callback_data="adm_broadcast")])
    await q.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb_rows), parse_mode=ParseMode.HTML)


def _build_broadcast_reply_markup() -> InlineKeyboardMarkup | None:
    """Build InlineKeyboardMarkup from saved broadcast_buttons, or None if empty."""
    btns = bot_data["settings"].get("broadcast_buttons", [])
    if not btns:
        return None
    rows = []
    for b in btns:
        try:
            rows.append([InlineKeyboardButton(b["text"], url=b["url"])])
        except Exception:
            pass
    return InlineKeyboardMarkup(rows) if rows else None


async def _do_broadcast(ctx, src_chat_id: int, src_msg_id: int, progress_msg, message_obj=None):
    """
    Core broadcast executor.
    Sends copy/forward to all _broadcast_targets(), attaching buttons if set.
    Handles text, photo, video, document, audio, animation (GIF).
    """
    members = _broadcast_targets()
    if not members:
        await progress_msg.edit_text(
            f"{E_WARN} No users for selected broadcast target!",
            parse_mode=ParseMode.HTML,
        )
        return

    await progress_msg.edit_text(
        f"{E_HOUR} Broadcasting to <b>{len(members)}</b> users…",
        parse_mode=ParseMode.HTML,
    )

    ok = fail = blocked = 0
    mode = bot_data["settings"].get("broadcast_mode", "copy")
    pin_broadcast = bot_data["settings"].get("pin_broadcast", False)
    reply_markup = _build_broadcast_reply_markup()

    for uid_str in members:
        try:
            sent = None
            uid_int = int(uid_str)

            if mode == "forward":
                # forward mode: no buttons, no caption edit
                sent = await ctx.bot.forward_message(
                    chat_id=uid_int,
                    from_chat_id=src_chat_id,
                    message_id=src_msg_id,
                )
            else:
                # copy mode: supports adding buttons
                if message_obj and reply_markup:
                    # We need to send with reply_markup — copy_message doesn't support it.
                    # Re-send the message directly with correct type.
                    msg = message_obj
                    caption = msg.caption
                    caption_entities = msg.caption_entities
                    parse_mode = None if caption_entities else ParseMode.HTML

                    if msg.photo:
                        sent = await ctx.bot.send_photo(
                            uid_int,
                            photo=msg.photo[-1].file_id,
                            caption=caption,
                            caption_entities=caption_entities if caption_entities else None,
                            parse_mode=parse_mode if not caption_entities else None,
                            reply_markup=reply_markup,
                        )
                    elif msg.video:
                        sent = await ctx.bot.send_video(
                            uid_int,
                            video=msg.video.file_id,
                            caption=caption,
                            caption_entities=caption_entities if caption_entities else None,
                            parse_mode=parse_mode if not caption_entities else None,
                            reply_markup=reply_markup,
                        )
                    elif msg.document:
                        sent = await ctx.bot.send_document(
                            uid_int,
                            document=msg.document.file_id,
                            caption=caption,
                            caption_entities=caption_entities if caption_entities else None,
                            parse_mode=parse_mode if not caption_entities else None,
                            reply_markup=reply_markup,
                        )
                    elif msg.audio:
                        sent = await ctx.bot.send_audio(
                            uid_int,
                            audio=msg.audio.file_id,
                            caption=caption,
                            caption_entities=caption_entities if caption_entities else None,
                            parse_mode=parse_mode if not caption_entities else None,
                            reply_markup=reply_markup,
                        )
                    elif msg.animation:
                        sent = await ctx.bot.send_animation(
                            uid_int,
                            animation=msg.animation.file_id,
                            caption=caption,
                            caption_entities=caption_entities if caption_entities else None,
                            parse_mode=parse_mode if not caption_entities else None,
                            reply_markup=reply_markup,
                        )
                    elif msg.text or msg.entities:
                        sent = await ctx.bot.send_message(
                            uid_int,
                            text=msg.text or "",
                            entities=msg.entities if msg.entities else None,
                            parse_mode=None if msg.entities else ParseMode.HTML,
                            reply_markup=reply_markup,
                        )
                    else:
                        sent = await ctx.bot.copy_message(
                            chat_id=uid_int,
                            from_chat_id=src_chat_id,
                            message_id=src_msg_id,
                            reply_markup=reply_markup,
                        )
                else:
                    sent = await ctx.bot.copy_message(
                        chat_id=uid_int,
                        from_chat_id=src_chat_id,
                        message_id=src_msg_id,
                        reply_markup=reply_markup,
                    )

            if pin_broadcast and sent:
                await safe_pin_message(ctx, uid_int, sent.message_id)
            ok += 1
        except TelegramError as e:
            err = str(e).lower()
            if "blocked" in err or "deactivated" in err:
                blocked += 1
            else:
                fail += 1
        await asyncio.sleep(0.05)

    snippet = ""
    if message_obj:
        snippet = (message_obj.text or message_obj.caption or "")[:80]

    bot_data["broadcast_history"].append({
        "date":    datetime.now().isoformat(),
        "snippet": snippet,
        "target":  bot_data["settings"].get("broadcast_target", "all"),
        "mode":    mode,
        "ok":      ok,
        "fail":    fail,
        "blocked": blocked,
    })
    bot_data["stats"]["broadcasts_sent"] = bot_data["stats"].get("broadcasts_sent", 0) + 1
    save_data(bot_data)

    await progress_msg.edit_text(
        f"{E_MEGA} <b>Broadcast Complete!</b> {E_PARTY}\n\n"
        f"{E_GREEN} Delivered : <b>{ok}</b>\n"
        f"{E_RED}   Failed   : <b>{fail}</b>\n"
        f"{E_STOP}  Blocked   : <b>{blocked}</b>\n\n"
        f"{E_CHART} Total target was <b>{len(members)}</b> users",
        parse_mode=ParseMode.HTML,
    )


async def _show_welcome_media(q, ctx):
    s = bot_data["settings"]
    status = "Set" if s.get("welcome_media_file_id") else "Not Set"
    media_type = s.get("welcome_media_type") or "-"
    text = (
        f"{E_BOOK} <b>Welcome Media Manager</b>\n\n"
        f"Status : <b>{status}</b>\n"
        f"Type   : <b>{media_type}</b>"
    )
    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🖼️ Set Media",    callback_data="set_welcome_media"),
            InlineKeyboardButton("🗑️ Clear Media",  callback_data="clear_welcome_media"),
        ],
        [InlineKeyboardButton("🔙 Back to Panel", callback_data="adm_home")],
    ])
    await q.edit_message_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)


async def send_welcome_media(ctx: ContextTypes.DEFAULT_TYPE, user_id: int) -> bool:
    s = bot_data.get("settings", {})
    file_id = s.get("welcome_media_file_id")
    media_type = s.get("welcome_media_type")
    if not file_id or not media_type:
        return False

    caption = s.get("welcome_media_caption") or None
    entities = deserialize_entities(s.get("welcome_media_entities"))
    try:
        kwargs = {"caption": caption} if caption else {}
        if entities:
            kwargs["caption_entities"] = entities
        elif caption:
            kwargs["parse_mode"] = ParseMode.HTML

        if media_type == "photo":
            msg = await ctx.bot.send_photo(user_id, file_id, **kwargs)
        elif media_type == "video":
            msg = await ctx.bot.send_video(user_id, file_id, **kwargs)
        else:
            msg = await ctx.bot.send_document(user_id, file_id, **kwargs)

        if msg and get_pin_setting("pin_welcome_msg"):
            await safe_pin_message(ctx, user_id, msg.message_id)
        return True
    except TelegramError as e:
        logger.debug(f"welcome media send failed uid={user_id}: {e}")
        return False


async def _show_members(q, ctx):
    members = bot_data["members"]
    text    = f"{E_CROWN} <b>Members ({len(members)})</b>\n\n"
    if members:
        for i, uid in enumerate(members[:30], 1):
            text += f"{i}. <code>{uid}</code>\n"
        if len(members) > 30:
            text += f"\n<i>…and {len(members) - 30} more</i>"
    else:
        text += f"{E_INFO} No tracked members yet."
    await q.edit_message_text(text, reply_markup=back_kb(), parse_mode=ParseMode.HTML)


async def _show_banned(q, ctx):
    banned  = bot_data["banned_users"]
    text    = f"{E_STOP} <b>Banned Users ({len(banned)})</b>\n\n"
    buttons: list[list[InlineKeyboardButton]] = []
    if banned:
        for i, uid in enumerate(banned[:20], 1):
            text += f"{i}. <code>{uid}</code>\n"
            buttons.append([
                InlineKeyboardButton(f"✅ Unban {uid}", callback_data=f"unban_{uid}")
            ])
    else:
        text += f"{E_CHECK} No banned users."
    buttons.append([InlineKeyboardButton("🔙 Back to Panel", callback_data="adm_home")])
    await q.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(buttons),
        parse_mode=ParseMode.HTML,
    )


async def _fwd_test(q, ctx):
    """Copy configured host-channel messages to the admin (no forward tag)."""
    sent = 0
    errors = []
    source_id = get_forward_source_channel_id()
    host_msg_ids = get_host_message_ids()
    for msg_id in host_msg_ids:
        try:
            await ctx.bot.copy_message(
                chat_id=q.from_user.id,
                from_chat_id=source_id,
                message_id=int(msg_id),
            )
            sent += 1
            await asyncio.sleep(0.4)
        except BadRequest as e:
            errors.append(f"host_source={source_id} msg_id={msg_id}: {e}")
        except TelegramError as e:
            errors.append(f"host_source={source_id} msg_id={msg_id}: {e}")

    err_text = "\n".join(errors[-8:]) if errors else "none"
    try:
        await q.edit_message_text(
            f"{E_CHECK} <b>Forward Test</b>\n\n"
            f"{E_GREEN} Sent  : {sent}/{len(host_msg_ids)}\n"
            f"{E_INFO} Host Source: <code>{source_id}</code>\\n"
            f"{E_CROSS} Errors: {err_text}",
            reply_markup=back_kb(),
            parse_mode=ParseMode.HTML,
        )
    except BadRequest:
        pass


async def _cb_get_db(q, ctx):
    """Send the database file to the admin."""
    if not Path(DATA_FILE).exists():
        try:
            await q.edit_message_text(
                f"{E_CROSS} <b>Database file not found!</b>",
                reply_markup=back_kb(),
                parse_mode=ParseMode.HTML,
            )
        except BadRequest:
            pass
        return

    try:
        await q.edit_message_text(
            f"{E_HOUR} <b>Sending database…</b>",
            parse_mode=ParseMode.HTML,
        )
    except BadRequest:
        pass

    try:
        with open(DATA_FILE, "rb") as f:
            await ctx.bot.send_document(
                chat_id=q.from_user.id,
                document=f,
                filename=DATA_FILE,
                caption=(
                    f"{E_DOWN} <b>Database Export</b>\n"
                    f"{E_INFO} File: <code>{DATA_FILE}</code>\n"
                    f"{E_CHART} Members: <b>{len(bot_data['members'])}</b>\n"
                    f"{E_GREEN} Pending: <b>{len(bot_data['pending_requests'])}</b>"
                ),
                parse_mode=ParseMode.HTML,
            )
        try:
            await q.edit_message_text(
                f"{E_CHECK} <b>Database sent!</b>",
                reply_markup=back_kb(),
                parse_mode=ParseMode.HTML,
            )
        except BadRequest:
            pass
    except Exception as e:
        logger.error(f"get_db error: {e}")
        try:
            await q.edit_message_text(
                f"{E_CROSS} <b>Error sending DB:</b>\n<code>{e}</code>",
                reply_markup=back_kb(),
                parse_mode=ParseMode.HTML,
            )
        except BadRequest:
            pass


# ═══════════════════════════════════════════════════════
#  TEXT / DOCUMENT MESSAGE HANDLER  (admin input states)
# ═══════════════════════════════════════════════════════
async def on_document(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle database file uploads from admins."""
    global bot_data
    user = update.effective_user
    if not is_admin(user.id):
        return

    awaiting = ctx.user_data.get("awaiting", "")

    # ── Broadcast media (document/file) ──────────────────
    if awaiting == "broadcast_media":
        ctx.user_data.pop("awaiting", None)
        prog = await update.message.reply_text(
            f"{E_HOUR} Preparing file broadcast…", parse_mode=ParseMode.HTML
        )
        await _do_broadcast(
            ctx,
            src_chat_id=update.effective_chat.id,
            src_msg_id=update.effective_message.message_id,
            progress_msg=prog,
            message_obj=update.effective_message,
        )
        return

    if awaiting == "welcome_media":
        doc = update.message.document
        if not doc:
            return
        bot_data["settings"]["welcome_media_type"] = "document"
        bot_data["settings"]["welcome_media_file_id"] = doc.file_id
        bot_data["settings"]["welcome_media_caption"] = update.message.caption or None
        bot_data["settings"]["welcome_media_entities"] = serialize_entities(update.message.caption_entities or [])
        bot_data["stats"]["welcome_media_set"] = True
        save_data(bot_data)
        ctx.user_data.pop("awaiting", None)
        await update.message.reply_text(
            f"{E_CHECK} <b>Welcome media saved!</b>",
            reply_markup=admin_home_kb(),
            parse_mode=ParseMode.HTML,
        )
        return

    if awaiting != "upload_db":
        return

    doc = update.message.document
    if not doc:
        return

    # Validate it's a JSON file
    if not (doc.file_name and doc.file_name.endswith(".json")):
        await update.message.reply_text(
            f"{E_CROSS} Please send a <code>.json</code> file.",
            parse_mode=ParseMode.HTML,
        )
        return

    ctx.user_data.pop("awaiting", None)

    try:
        file = await ctx.bot.get_file(doc.file_id)
        downloaded = await file.download_as_bytearray()
        content    = downloaded.decode("utf-8")

        # Validate JSON
        new_data = json.loads(content)

        # Back-fill missing keys to avoid KeyError after restore
        import copy
        for k, v in _DEFAULTS.items():
            if k not in new_data:
                new_data[k] = copy.deepcopy(v)
        for k, v in _DEFAULTS["settings"].items():
            new_data["settings"].setdefault(k, v)
        for k, v in _DEFAULTS["stats"].items():
            new_data["stats"].setdefault(k, v)

        # Write to disk
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(new_data, f, indent=2, default=str)

        # Reload in memory
        bot_data = new_data

        await update.message.reply_text(
            f"{E_CHECK} <b>Database Uploaded Successfully!</b>\n\n"
            f"{E_GREEN} Members : <b>{len(bot_data['members'])}</b>\n"
            f"{E_HOUR}  Pending : <b>{len(bot_data['pending_requests'])}</b>\n"
            f"{E_STOP}  Banned  : <b>{len(bot_data['banned_users'])}</b>",
            reply_markup=admin_home_kb(),
            parse_mode=ParseMode.HTML,
        )
    except json.JSONDecodeError as e:
        await update.message.reply_text(
            f"{E_CROSS} <b>Invalid JSON file!</b>\n<code>{e}</code>",
            reply_markup=admin_home_kb(),
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        logger.error(f"upload_db error: {e}")
        await update.message.reply_text(
            f"{E_CROSS} <b>Upload failed:</b>\n<code>{e}</code>",
            reply_markup=admin_home_kb(),
            parse_mode=ParseMode.HTML,
        )


async def on_media(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Handle welcome photo/video uploads from admins. Also handles broadcast media."""
    user = update.effective_user
    if not is_admin(user.id):
        return

    awaiting = ctx.user_data.get("awaiting", "")
    msg = update.effective_message

    # ── Broadcast media ──────────────────────────────────
    if awaiting == "broadcast_media":
        ctx.user_data.pop("awaiting", None)
        prog = await msg.reply_text(
            f"{E_HOUR} Preparing media broadcast…", parse_mode=ParseMode.HTML
        )
        await _do_broadcast(
            ctx,
            src_chat_id=update.effective_chat.id,
            src_msg_id=msg.message_id,
            progress_msg=prog,
            message_obj=msg,
        )
        return

    # ── Welcome media ────────────────────────────────────
    if awaiting != "welcome_media":
        return

    if msg.photo:
        media_type = "photo"
        file_id = msg.photo[-1].file_id
    elif msg.video:
        media_type = "video"
        file_id = msg.video.file_id
    else:
        return

    bot_data["settings"]["welcome_media_type"] = media_type
    bot_data["settings"]["welcome_media_file_id"] = file_id
    bot_data["settings"]["welcome_media_caption"] = msg.caption or None
    bot_data["settings"]["welcome_media_entities"] = serialize_entities(msg.caption_entities or [])
    bot_data["stats"]["welcome_media_set"] = True
    save_data(bot_data)
    ctx.user_data.pop("awaiting", None)
    await msg.reply_text(
        f"{E_CHECK} <b>Welcome media saved!</b>",
        reply_markup=admin_home_kb(),
        parse_mode=ParseMode.HTML,
    )


async def on_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    text = update.message.text.strip()

    # /cancel always works
    if text.lower() == "/cancel":
        ctx.user_data.pop("awaiting", None)
        await update.message.reply_text(
            f"{E_CHECK} Cancelled.",
            reply_markup=admin_home_kb() if is_admin(user.id) else None,
            parse_mode=ParseMode.HTML,
        )
        return

    # Non-admin
    if not is_admin(user.id):
        if not await is_member(user.id, ctx):
            await update.message.reply_text(
                f"{E_LOCK} <b>Access Denied</b>\n\n"
                f"{E_WARN} Join the channel first to use the bot.",
                parse_mode=ParseMode.HTML,
            )
        return

    awaiting = ctx.user_data.get("awaiting", "")

    if awaiting == "activity_channel_id":
        channel_text = text.replace(" ", "")
        if not channel_text.startswith("-100") or not channel_text[1:].isdigit():
            await update.message.reply_text(
                f"{E_CROSS} Send a valid private channel ID like <code>-1002232875049</code>.",
                parse_mode=ParseMode.HTML,
            )
            return
        set_activity_channel_id(int(channel_text))
        ctx.user_data.pop("awaiting", None)
        await update.message.reply_text(
            f"{E_CHECK} <b>Activity channel updated!</b>\n"
            f"{E_MEGA} New channel: <code>{get_activity_channel_id()}</code>\n\n"
            f"{E_WARN} Make sure bot is admin in this channel, then restart/deploy the bot.",
            reply_markup=admin_home_kb(),
            parse_mode=ParseMode.HTML,
        )


    elif awaiting == "forward_source_channel_id":
        channel_text = text.replace(" ", "")
        if not channel_text.startswith("-100") or not channel_text[1:].isdigit():
            await update.message.reply_text(
                f"{E_CROSS} Send a valid private channel ID like <code>-1002701185142</code>.",
                parse_mode=ParseMode.HTML,
            )
            return
        set_forward_source_channel_id(int(channel_text))
        ctx.user_data.pop("awaiting", None)
        await update.message.reply_text(
            f"{E_CHECK} <b>Host channel updated!</b>\n"
            f"{E_MAIL} New host: <code>{get_forward_source_channel_id()}</code>",
            reply_markup=admin_home_kb(),
            parse_mode=ParseMode.HTML,
        )

    elif awaiting in {"host_message_ids", "pin_source_msg_ids", "auto_unpin_source_msg_ids"}:
        ids = [] if text.strip() == "0" else _parse_int_list(text)
        if awaiting != "auto_unpin_source_msg_ids" and not ids:
            await update.message.reply_text(
                f"{E_CROSS} Send valid message ID(s), for example <code>34</code> or <code>34,35</code>.",
                parse_mode=ParseMode.HTML,
            )
            return
        if awaiting == "host_message_ids":
            set_host_message_ids(ids)
            label = "Host message IDs"
        elif awaiting == "pin_source_msg_ids":
            set_pin_source_msg_ids(ids)
            label = "Hack pin IDs"
        else:
            set_auto_unpin_source_msg_ids(ids)
            label = "Auto-unpin IDs"
        ctx.user_data.pop("awaiting", None)
        await update.message.reply_text(
            f"{E_CHECK} <b>{label} updated!</b>\n<code>{ids}</code>",
            reply_markup=admin_home_kb(),
            parse_mode=ParseMode.HTML,
        )

    elif awaiting == "auto_accept_delay":
        if text.isdigit():
            bot_data["settings"]["auto_accept_delay"] = int(text)
            save_data(bot_data)
            ctx.user_data.pop("awaiting", None)
            await update.message.reply_text(
                f"{E_CHECK} Delay set to <b>{text}s</b>!",
                reply_markup=admin_home_kb(),
                parse_mode=ParseMode.HTML,
            )
        else:
            await update.message.reply_text(
                f"{E_CROSS} Send a valid number.", parse_mode=ParseMode.HTML
            )

    elif awaiting == "max_pins_per_user":
        if text.isdigit():
            bot_data["settings"]["max_pins_per_user"] = int(text)
            save_data(bot_data)
            ctx.user_data.pop("awaiting", None)
            await update.message.reply_text(
                f"{E_CHECK} Max pins set to <b>{text}</b>!",
                reply_markup=admin_home_kb(),
                parse_mode=ParseMode.HTML,
            )
        else:
            await update.message.reply_text(
                f"{E_CROSS} Send a valid number.", parse_mode=ParseMode.HTML
            )

    elif awaiting.startswith("setmsg_"):
        msg_type = awaiting[7:]
        key_map  = {
            "request":  ("request_msg",  "request_entities"),
            "accepted": ("accepted_msg", "accepted_entities"),
            "declined": ("declined_msg", "declined_entities"),
            "welcome":  ("welcome_msg",  "welcome_entities"),
            "left":     ("left_msg",     "left_entities"),
        }
        keys = key_map.get(msg_type)
        if keys:
            text_key, ents_key = keys
            msg = update.effective_message
            msg_text = msg.text if msg.text is not None else (msg.caption or "")
            raw_entities = msg.entities if msg.text is not None else (msg.caption_entities or [])

            # Store exact copied text + entities so premium animated emojis remain premium emojis.
            bot_data["settings"][text_key] = msg_text
            bot_data["settings"][ents_key] = serialize_entities(raw_entities or [])

            save_data(bot_data)

        ctx.user_data.pop("awaiting", None)
        await update.message.reply_text(
            f"{E_CHECK} <b>{msg_type.title()} message updated!</b>\n"
            f"{E_SPARK} Premium emojis preserved: "
            f"{'✅' if bot_data['settings'].get(keys[1] if keys else '') else '—'}",
            reply_markup=admin_home_kb(),
            parse_mode=ParseMode.HTML,
        )

    elif awaiting == "broadcast":
        ctx.user_data.pop("awaiting", None)
        prog = await update.message.reply_text(
            f"{E_HOUR} Preparing broadcast…", parse_mode=ParseMode.HTML
        )
        await _do_broadcast(
            ctx,
            src_chat_id=update.effective_chat.id,
            src_msg_id=update.effective_message.message_id,
            progress_msg=prog,
            message_obj=update.effective_message,
        )

    elif awaiting == "bcast_button_text":
        ctx.user_data["bcast_btn_text_pending"] = text
        ctx.user_data["awaiting"] = "bcast_button_url"
        await update.message.reply_text(
            f"{E_LINK} <b>Step 2/2</b>: Send the <b>URL</b> for the button.\n"
            f"Example: <code>https://t.me/yourchannel</code>\n\n"
            f"/cancel to abort.",
            parse_mode=ParseMode.HTML,
        )

    elif awaiting == "bcast_button_url":
        btn_text = ctx.user_data.pop("bcast_btn_text_pending", "")
        ctx.user_data.pop("awaiting", None)
        url = text.strip()
        if not (url.startswith("http://") or url.startswith("https://") or url.startswith("tg://")):
            await update.message.reply_text(
                f"{E_CROSS} Invalid URL. Must start with <code>https://</code>, <code>http://</code>, or <code>tg://</code>.\n"
                f"Try again or /cancel.",
                parse_mode=ParseMode.HTML,
            )
            return
        btns = bot_data["settings"].setdefault("broadcast_buttons", [])
        if len(btns) < 5:
            btns.append({"text": btn_text, "url": url})
            save_data(bot_data)
            await update.message.reply_text(
                f"{E_CHECK} <b>Button added!</b>\n"
                f"{E_LINK} [{btn_text}] → {url}\n\n"
                f"{E_INFO} Total buttons: <b>{len(btns)}</b>\n"
                f"Go back to Broadcast Panel to add more or send.",
                reply_markup=admin_home_kb(),
                parse_mode=ParseMode.HTML,
            )
        else:
            await update.message.reply_text(
                f"{E_WARN} Max 5 buttons reached.",
                reply_markup=admin_home_kb(),
                parse_mode=ParseMode.HTML,
            )

    else:
        await update.message.reply_text(
            f"{E_INFO} Use /start to open the admin panel.",
            reply_markup=admin_home_kb(),
            parse_mode=ParseMode.HTML,
        )


# ═══════════════════════════════════════════════════════
#  STANDALONE ADMIN COMMANDS
# ═══════════════════════════════════════════════════════
async def cmd_stats(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    await update.message.reply_text(fmt_bot_stats(), parse_mode=ParseMode.HTML)


async def cmd_accept(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if not ctx.args:
        await update.message.reply_text(
            f"{E_INFO} /accept &lt;user_id&gt;", parse_mode=ParseMode.HTML
        )
        return
    uid     = int(ctx.args[0])
    uid_str = str(uid)

    info       = bot_data["pending_requests"].get(uid_str, {})
    first_name = info.get("first_name", "there") or "there"

    try:
        await ctx.bot.approve_chat_join_request(get_activity_channel_id(), uid)
    except BadRequest as e:
        logger.info(f"cmd /accept uid={uid}: {e}")

    bot_data["pending_requests"].pop(uid_str, None)
    if uid_str not in bot_data["accepted_users"]:
        bot_data["accepted_users"].append(uid_str)
    bot_data["stats"]["total_accepted"] += 1
    save_data(bot_data)

    text, ents = fmt_accepted_msg(first_name)
    sent_msg = await safe_send(ctx, uid, text, entities=ents)
    if sent_msg and get_pin_setting("pin_accepted_msg"):
        await safe_pin_message(ctx, uid, sent_msg.message_id)
    await update.message.reply_text(
        f"{E_CHECK} User {uid} accepted!", parse_mode=ParseMode.HTML
    )


async def cmd_decline(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if not ctx.args:
        await update.message.reply_text(
            f"{E_INFO} /decline &lt;user_id&gt;", parse_mode=ParseMode.HTML
        )
        return
    uid     = int(ctx.args[0])
    uid_str = str(uid)

    info       = bot_data["pending_requests"].get(uid_str, {})
    first_name = info.get("first_name", "there") or "there"

    try:
        await ctx.bot.decline_chat_join_request(get_activity_channel_id(), uid)
    except BadRequest as e:
        logger.info(f"cmd /decline uid={uid}: {e}")

    bot_data["pending_requests"].pop(uid_str, None)
    if uid_str not in bot_data["declined_users"]:
        bot_data["declined_users"].append(uid_str)
    bot_data["stats"]["total_declined"] += 1
    save_data(bot_data)

    text, ents = fmt_declined_msg(first_name)
    sent_msg = await safe_send(ctx, uid, text, entities=ents)
    if sent_msg and get_pin_setting("pin_declined_msg"):
        await safe_pin_message(ctx, uid, sent_msg.message_id)
    await update.message.reply_text(
        f"{E_CROSS} User {uid} declined!", parse_mode=ParseMode.HTML
    )


async def cmd_ban(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if not ctx.args:
        await update.message.reply_text(
            f"{E_INFO} /ban &lt;user_id&gt;", parse_mode=ParseMode.HTML
        )
        return
    uid_str = ctx.args[0]
    try:
        await ctx.bot.decline_chat_join_request(get_activity_channel_id(), int(uid_str))
    except Exception:
        pass
    bot_data["pending_requests"].pop(uid_str, None)
    if uid_str not in bot_data["banned_users"]:
        bot_data["banned_users"].append(uid_str)
    save_data(bot_data)
    await update.message.reply_text(
        f"{E_STOP} User {uid_str} banned!", parse_mode=ParseMode.HTML
    )


async def cmd_unban(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if not ctx.args:
        await update.message.reply_text(
            f"{E_INFO} /unban &lt;user_id&gt;", parse_mode=ParseMode.HTML
        )
        return
    uid_str = ctx.args[0]
    if uid_str in bot_data["banned_users"]:
        bot_data["banned_users"].remove(uid_str)
        save_data(bot_data)
        await update.message.reply_text(
            f"{E_CHECK} User {uid_str} unbanned!", parse_mode=ParseMode.HTML
        )
    else:
        await update.message.reply_text(
            f"{E_WARN} User {uid_str} was not banned.", parse_mode=ParseMode.HTML
        )


async def cmd_acceptall(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    pending = dict(bot_data["pending_requests"])
    if not pending:
        await update.message.reply_text(
            f"{E_CHECK} No pending!", parse_mode=ParseMode.HTML
        )
        return
    msg = await update.message.reply_text(
        f"{E_HOUR} Accepting {len(pending)}…", parse_mode=ParseMode.HTML
    )
    ok = 0
    for uid_str, info in pending.items():
        uid        = int(uid_str)
        first_name = info.get("first_name", "there") or "there"
        try:
            await ctx.bot.approve_chat_join_request(get_activity_channel_id(), uid)
        except Exception:
            pass
        if uid_str not in bot_data["accepted_users"]:
            bot_data["accepted_users"].append(uid_str)
        bot_data["stats"]["total_accepted"] += 1
        ok += 1
        text, ents = fmt_accepted_msg(first_name)
        sent_msg = await safe_send(ctx, uid, text, entities=ents)
        if sent_msg and get_pin_setting("pin_accepted_msg"):
            await safe_pin_message(ctx, uid, sent_msg.message_id)
        await asyncio.sleep(0.2)
    bot_data["pending_requests"].clear()
    save_data(bot_data)
    await msg.edit_text(
        f"{E_CHECK} Accepted {ok} requests!", parse_mode=ParseMode.HTML
    )


async def cmd_declineall(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    pending = dict(bot_data["pending_requests"])
    if not pending:
        await update.message.reply_text(
            f"{E_CHECK} No pending!", parse_mode=ParseMode.HTML
        )
        return
    msg = await update.message.reply_text(
        f"{E_HOUR} Declining {len(pending)}…", parse_mode=ParseMode.HTML
    )
    ok = 0
    for uid_str, info in pending.items():
        uid        = int(uid_str)
        first_name = info.get("first_name", "there") or "there"
        try:
            await ctx.bot.decline_chat_join_request(get_activity_channel_id(), uid)
        except Exception:
            pass
        if uid_str not in bot_data["declined_users"]:
            bot_data["declined_users"].append(uid_str)
        bot_data["stats"]["total_declined"] += 1
        ok += 1
        text, ents = fmt_declined_msg(first_name)
        sent_msg = await safe_send(ctx, uid, text, entities=ents)
        if sent_msg and get_pin_setting("pin_declined_msg"):
            await safe_pin_message(ctx, uid, sent_msg.message_id)
        await asyncio.sleep(0.2)
    bot_data["pending_requests"].clear()
    save_data(bot_data)
    await msg.edit_text(
        f"{E_CROSS} Declined {ok} requests!", parse_mode=ParseMode.HTML
    )


async def cmd_pending(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    pending = bot_data["pending_requests"]
    if not pending:
        await update.message.reply_text(
            f"{E_CHECK} No pending requests!", parse_mode=ParseMode.HTML
        )
        return
    text = f"{E_EYES} <b>Pending ({len(pending)})</b>\n\n"
    for i, (uid, info) in enumerate(list(pending.items())[:30], 1):
        text += f"{i}. <b>{info.get('first_name','?')}</b> — <code>{uid}</code>\n"
    if len(pending) > 30:
        text += f"\n<i>…and {len(pending) - 30} more</i>"
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


async def cmd_user(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if not ctx.args:
        await update.message.reply_text(
            f"{E_INFO} /user &lt;user_id&gt;", parse_mode=ParseMode.HTML
        )
        return
    uid  = ctx.args[0]
    info = bot_data["pending_requests"].get(uid, {})
    text = (
        f"{E_SEARCH} <b>User Info</b>\n\n"
        f"{E_INFO} ID       : <code>{uid}</code>\n"
        f"{E_STAR}  Name     : {info.get('first_name','?')} {info.get('last_name','')}\n"
        f"{E_LINK}  Username : @{info.get('username','N/A')}\n\n"
        f"Pending  : {'✅' if uid in bot_data['pending_requests'] else '❌'}\n"
        f"Accepted : {'✅' if uid in bot_data['accepted_users']   else '❌'}\n"
        f"Member   : {'✅' if uid in bot_data['members']          else '❌'}\n"
        f"Left     : {'✅' if uid in bot_data['left_members']     else '❌'}\n"
        f"Banned   : {'✅' if uid in bot_data['banned_users']     else '❌'}\n"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


async def cmd_reload(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    global bot_data
    bot_data = load_data()
    await update.message.reply_text(
        f"{E_REFRESH} Data reloaded!", parse_mode=ParseMode.HTML
    )


async def cmd_getdb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Download the database file via command."""
    if not is_admin(update.effective_user.id):
        return
    if not Path(DATA_FILE).exists():
        await update.message.reply_text(
            f"{E_CROSS} Database file not found!", parse_mode=ParseMode.HTML
        )
        return
    try:
        with open(DATA_FILE, "rb") as f:
            await ctx.bot.send_document(
                chat_id=update.effective_user.id,
                document=f,
                filename=DATA_FILE,
                caption=(
                    f"{E_DOWN} <b>Database Export</b>\n"
                    f"{E_INFO} File: <code>{DATA_FILE}</code>\n"
                    f"{E_GREEN} Members: <b>{len(bot_data['members'])}</b>\n"
                    f"{E_HOUR}  Pending: <b>{len(bot_data['pending_requests'])}</b>"
                ),
                parse_mode=ParseMode.HTML,
            )
    except Exception as e:
        await update.message.reply_text(
            f"{E_CROSS} Error: <code>{e}</code>", parse_mode=ParseMode.HTML
        )


async def cmd_broadcast(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if not ctx.args:
        await update.message.reply_text(
            f"{E_INFO} /broadcast &lt;message&gt;", parse_mode=ParseMode.HTML
        )
        return
    text    = " ".join(ctx.args)
    members = bot_data["members"]
    msg     = await update.message.reply_text(
        f"{E_HOUR} Broadcasting…", parse_mode=ParseMode.HTML
    )
    ok = fail = 0
    for uid_str in members:
        try:
            await ctx.bot.send_message(
                int(uid_str), text, parse_mode=ParseMode.HTML
            )
            ok += 1
        except Exception:
            fail += 1
        await asyncio.sleep(0.05)
    await msg.edit_text(
        f"{E_MEGA} Done! {E_GREEN} {ok} sent, {E_RED} {fail} failed.",
        parse_mode=ParseMode.HTML,
    )


async def cmd_mystatus(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user    = update.effective_user
    uid_str = str(user.id)
    member  = await is_member(user.id, ctx)

    if uid_str in bot_data["banned_users"]:
        status = f"{E_STOP} Banned"
    elif member:
        status = f"{E_GREEN} Active Member"
    elif uid_str in bot_data["pending_requests"]:
        status = f"{E_HOUR} Pending Approval"
    else:
        status = f"{E_RED} Not a Member"

    await update.message.reply_text(
        f"{E_EYES} <b>Your Status</b>\n\n"
        f"{E_INFO} Name   : <b>{user.first_name}</b>\n"
        f"{E_LINK} User   : @{user.username or 'N/A'}\n"
        f"{E_STAR} Status : {status}",
        parse_mode=ParseMode.HTML,
    )


async def cmd_myinfo(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await update.message.reply_text(
        f"{E_BOOK} <b>Your Info</b>\n\n"
        f"{E_INFO} ID        : <code>{user.id}</code>\n"
        f"{E_STAR}  First Name: {user.first_name}\n"
        f"{E_STAR}  Last Name : {user.last_name or 'N/A'}\n"
        f"{E_LINK}  Username  : @{user.username or 'N/A'}\n"
        f"{E_GLOBE} Language  : {user.language_code or 'N/A'}\n"
        f"{E_DIAMOND} Premium : "
        f"{'✅' if getattr(user, 'is_premium', False) else '❌'}",
        parse_mode=ParseMode.HTML,
    )


# ═══════════════════════════════════════════════════════
#  ERROR HANDLER
# ═══════════════════════════════════════════════════════
async def on_error(update: object, ctx: ContextTypes.DEFAULT_TYPE):
    err = ctx.error
    logger.error(f"Unhandled error: {err}", exc_info=err)
    ignore = ("conflict:", "blocked", "deactivated", "chat not found", "message is not modified")
    if any(s in str(err).lower() for s in ignore):
        return
    await notify_admins(
        ctx,
        f"{E_ALERT} <b>Bot Error:</b>\n<code>{str(err)[:400]}</code>",
    )


# ═══════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════
def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN env variable is missing. Add it in Railway Variables.")

    app = Application.builder().token(BOT_TOKEN).build()

    # Commands
    app.add_handler(CommandHandler("start",      cmd_start))
    app.add_handler(CommandHandler("stats",      cmd_stats))
    app.add_handler(CommandHandler("accept",     cmd_accept))
    app.add_handler(CommandHandler("decline",    cmd_decline))
    app.add_handler(CommandHandler("ban",        cmd_ban))
    app.add_handler(CommandHandler("unban",      cmd_unban))
    app.add_handler(CommandHandler("acceptall",  cmd_acceptall))
    app.add_handler(CommandHandler("declineall", cmd_declineall))
    app.add_handler(CommandHandler("pending",    cmd_pending))
    app.add_handler(CommandHandler("user",       cmd_user))
    app.add_handler(CommandHandler("reload",     cmd_reload))
    app.add_handler(CommandHandler("getdb",      cmd_getdb))
    app.add_handler(CommandHandler("broadcast",  cmd_broadcast))

    # Join requests (highest priority)
    app.add_handler(ChatJoinRequestHandler(on_join_request))

    # FIX: Use ANY_CHAT_MEMBER to properly capture member join/leave events
    # in channels. CHAT_MEMBER alone misses many channel member updates.
    app.add_handler(
        ChatMemberHandler(on_chat_member, ChatMemberHandler.ANY_CHAT_MEMBER)
    )

    # Inline buttons
    app.add_handler(CallbackQueryHandler(on_callback))

    # Remove Telegram's automatic pin service notice in user DMs where possible.
    app.add_handler(
        MessageHandler(filters.StatusUpdate.PINNED_MESSAGE, on_pinned_service_message)
    )

    # Document handler for DB upload (must be before text handler)
    app.add_handler(
        MessageHandler(filters.PHOTO | filters.VIDEO, on_media)
    )
    app.add_handler(
        MessageHandler(filters.Document.ALL, on_document)
    )

    # Free-text (admin states + member guard)
    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, on_text)
    )

    # Errors
    app.add_error_handler(on_error)

    print("=" * 54)
    print("  🤖  Advanced Request-Accept Bot  —  RUNNING")
    print(f"  📢  Channel  : {get_activity_channel_id()}")
    print(f"  📨  Fwd Src  : {get_forward_source_channel_id()}")
    print(f"  📨  Host Src : {get_forward_source_channel_id()}")
    print(f"  👑  Admins   : {ADMIN_IDS}")
    print(f"  📨  Fwd IDs  : {get_host_message_ids()}")
    print("=" * 54)

    app.run_polling(
        allowed_updates=[
            Update.MESSAGE,
            Update.CALLBACK_QUERY,
            Update.CHAT_JOIN_REQUEST,
            Update.CHAT_MEMBER,        # channel member updates
            Update.MY_CHAT_MEMBER,     # bot's own member updates
        ],
        drop_pending_updates=True,
    )


if __name__ == "__main__":
    main()
