import asyncio
import json
import os
import random
import re
import sys
import time
import logging
import sqlite3
import io
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple

# ─── Fix Windows Unicode Console ─────────────────────────────────────────────
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(
        sys.stdout.buffer, encoding='utf-8', errors='replace'
    )
    sys.stderr = io.TextIOWrapper(
        sys.stderr.buffer, encoding='utf-8', errors='replace'
    )
    os.environ['PYTHONIOENCODING'] = 'utf-8'

# ─── Install required packages ───────────────────────────────────────────────
try:
    from telethon import TelegramClient, events, functions, types
    from telethon.tl.functions.channels import (
        JoinChannelRequest, GetFullChannelRequest
    )
    from telethon.tl.functions.messages import (
        SendReactionRequest, ImportChatInviteRequest,
        GetMessagesViewsRequest, ReadHistoryRequest,
        CheckChatInviteRequest
    )
    from telethon.tl.functions.phone import (
        JoinGroupCallRequest, LeaveGroupCallRequest,
        CheckGroupCallRequest
    )
    from telethon.tl.types import (
        ReactionEmoji, DataJSON,
        Channel, Chat, ChatInviteAlready, ChatInvite,
    )
    from telethon.errors import (
        SessionPasswordNeededError, PhoneCodeExpiredError,
        PhoneCodeInvalidError, FloodWaitError,
        UserAlreadyParticipantError,
        InviteHashExpiredError, InviteHashInvalidError
    )
    from telegram import (
        Update, InlineKeyboardButton, InlineKeyboardMarkup,
    )
    from telegram.ext import (
        Application, CommandHandler, MessageHandler,
        CallbackQueryHandler, ContextTypes, filters,
    )
    from telegram.constants import ParseMode
except ImportError:
    print("Installing required packages...")
    os.system("pip install telethon python-telegram-bot")
    print("Please restart the script.")
    sys.exit(1)

# ─── Configuration ────────────────────────────────────────────────────────────
BOT_TOKEN = "8714648402:AAE4qGUlMFcgO-8jcxYZNTOsQocRdKcc_GI"
ADMIN_ID = 8624480309
API_ID = 39052980
API_HASH = "5b0b6f9aedd2113a4a591dbcde61be43"

DB_FILE = "vcbot.db"
SESSIONS_DIR = "sessions"

REACTION_EMOJIS = [
    "\U0001f44d", "\u2764\ufe0f", "\U0001f525", "\U0001f970",
    "\U0001f44f", "\U0001f60d", "\U0001f389", "\U0001f4af",
    "\u26a1", "\U0001f3c6", "\U0001f62e", "\U0001f64f"
]

DEVICE_TYPES = [
    {
        "device": "iPhone 14 Pro",
        "system": "iOS 16.5",
        "app": "Telegram iOS 9.6.3"
    },
    {
        "device": "Samsung Galaxy S23",
        "system": "Android 13",
        "app": "Telegram Android 9.6.3"
    },
    {
        "device": "Pixel 7",
        "system": "Android 13",
        "app": "Telegram Android 9.6.3"
    },
    {
        "device": "iPhone 13",
        "system": "iOS 16.4",
        "app": "Telegram iOS 9.6.2"
    },
    {
        "device": "OnePlus 11",
        "system": "Android 13",
        "app": "Telegram Android 9.6.1"
    },
    {
        "device": "MacBook Pro",
        "system": "macOS 13.4",
        "app": "Telegram macOS 9.3.3"
    },
    {
        "device": "Windows PC",
        "system": "Windows 11",
        "app": "Telegram Desktop 4.8.10"
    },
    {
        "device": "iPad Pro",
        "system": "iPadOS 16.5",
        "app": "Telegram iOS 9.6.3"
    },
]


# ─── Safe Logging Handler ─────────────────────────────────────────────────────
class SafeStreamHandler(logging.StreamHandler):
    """
    Windows-safe stream handler that replaces unencodable characters
    instead of crashing.
    """
    def __init__(self):
        # Force UTF-8 stream for Windows
        if sys.platform == "win32":
            stream = io.TextIOWrapper(
                sys.stdout.buffer,
                encoding='utf-8',
                errors='replace',
                line_buffering=True
            )
        else:
            stream = sys.stdout
        super().__init__(stream)

    def emit(self, record):
        try:
            msg = self.format(record)
            # Replace any unencodable chars with '?'
            safe_msg = msg.encode(
                self.stream.encoding or 'utf-8', errors='replace'
            ).decode(self.stream.encoding or 'utf-8', errors='replace')
            self.stream.write(safe_msg + self.terminator)
            self.stream.flush()
        except Exception:
            self.handleError(record)


# ─── Logging Setup ────────────────────────────────────────────────────────────
def setup_logging():
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    # File handler — always UTF-8
    fh = logging.FileHandler('vcbot.log', encoding='utf-8')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(
        logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    )

    # Console handler — Windows safe
    ch = SafeStreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(
        logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    )

    root.addHandler(fh)
    root.addHandler(ch)

    # Silence noisy libs
    logging.getLogger('telethon').setLevel(logging.WARNING)
    logging.getLogger('telegram').setLevel(logging.WARNING)
    logging.getLogger('httpx').setLevel(logging.WARNING)
    logging.getLogger('httpcore').setLevel(logging.WARNING)
    logging.getLogger('apscheduler').setLevel(logging.WARNING)


setup_logging()
logger = logging.getLogger(__name__)


# ─── Database ─────────────────────────────────────────────────────────────────
class DatabaseManager:
    def __init__(self):
        self.init_db()

    def init_db(self):
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()

        c.execute('''
            CREATE TABLE IF NOT EXISTS userbots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                phone TEXT UNIQUE NOT NULL,
                session_file TEXT,
                is_active INTEGER DEFAULT 1,
                added_by INTEGER,
                device_info TEXT,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_active TIMESTAMP
            )
        ''')

        c.execute('''
            CREATE TABLE IF NOT EXISTS allowed_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER UNIQUE NOT NULL,
                username TEXT,
                added_by INTEGER,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        c.execute('''
            CREATE TABLE IF NOT EXISTS vc_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                vc_link TEXT NOT NULL,
                channel_invite TEXT,
                chat_id TEXT,
                started_by INTEGER,
                is_active INTEGER DEFAULT 1,
                started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                ended_at TIMESTAMP
            )
        ''')

        c.execute('''
            CREATE TABLE IF NOT EXISTS bot_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                action TEXT,
                userbot_phone TEXT,
                details TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        c.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        ''')

        # Safe migration
        try:
            c.execute(
                'ALTER TABLE vc_sessions ADD COLUMN channel_invite TEXT'
            )
        except Exception:
            pass

        defaults = [
            ('auto_react', '1'),
            ('auto_watch', '1'),
            ('human_behavior', '1'),
        ]
        for k, v in defaults:
            c.execute(
                'INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)',
                (k, v)
            )

        conn.commit()
        conn.close()

    def get_conn(self):
        conn = sqlite3.connect(DB_FILE)
        conn.row_factory = sqlite3.Row
        return conn

    def add_userbot(self, phone, session_file, added_by, device_info=None):
        conn = self.get_conn()
        try:
            device = device_info or json.dumps(random.choice(DEVICE_TYPES))
            conn.execute(
                '''INSERT OR REPLACE INTO userbots
                   (phone, session_file, added_by, device_info, last_active)
                   VALUES (?, ?, ?, ?, ?)''',
                (phone, session_file, added_by, device, datetime.now())
            )
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"add_userbot error: {e}")
            return False
        finally:
            conn.close()

    def get_all_userbots(self, active_only=False):
        conn = self.get_conn()
        try:
            if active_only:
                return conn.execute(
                    'SELECT * FROM userbots WHERE is_active = 1'
                ).fetchall()
            return conn.execute('SELECT * FROM userbots').fetchall()
        finally:
            conn.close()

    def get_userbot(self, phone):
        conn = self.get_conn()
        try:
            return conn.execute(
                'SELECT * FROM userbots WHERE phone = ?', (phone,)
            ).fetchone()
        finally:
            conn.close()

    def toggle_userbot(self, phone, active):
        conn = self.get_conn()
        try:
            conn.execute(
                'UPDATE userbots SET is_active = ? WHERE phone = ?',
                (1 if active else 0, phone)
            )
            conn.commit()
        finally:
            conn.close()

    def toggle_all_userbots(self, active):
        conn = self.get_conn()
        try:
            conn.execute(
                'UPDATE userbots SET is_active = ?', (1 if active else 0,)
            )
            conn.commit()
        finally:
            conn.close()

    def delete_userbot(self, phone):
        conn = self.get_conn()
        try:
            conn.execute('DELETE FROM userbots WHERE phone = ?', (phone,))
            conn.commit()
        finally:
            conn.close()

    def add_allowed_user(self, user_id, username, added_by):
        conn = self.get_conn()
        try:
            conn.execute(
                '''INSERT OR REPLACE INTO allowed_users
                   (user_id, username, added_by) VALUES (?, ?, ?)''',
                (user_id, username, added_by)
            )
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"add_allowed_user error: {e}")
            return False
        finally:
            conn.close()

    def is_allowed_user(self, user_id):
        if user_id == ADMIN_ID:
            return True
        conn = self.get_conn()
        try:
            return conn.execute(
                'SELECT id FROM allowed_users WHERE user_id = ?', (user_id,)
            ).fetchone() is not None
        finally:
            conn.close()

    def get_allowed_users(self):
        conn = self.get_conn()
        try:
            return conn.execute('SELECT * FROM allowed_users').fetchall()
        finally:
            conn.close()

    def remove_allowed_user(self, user_id):
        conn = self.get_conn()
        try:
            conn.execute(
                'DELETE FROM allowed_users WHERE user_id = ?', (user_id,)
            )
            conn.commit()
        finally:
            conn.close()

    def save_vc_session(
        self, vc_link, chat_id, started_by, channel_invite=None
    ):
        conn = self.get_conn()
        try:
            conn.execute('UPDATE vc_sessions SET is_active = 0')
            conn.execute(
                '''INSERT INTO vc_sessions
                   (vc_link, chat_id, started_by, channel_invite)
                   VALUES (?, ?, ?, ?)''',
                (vc_link, chat_id, started_by, channel_invite)
            )
            conn.commit()
        finally:
            conn.close()

    def get_active_vc(self):
        conn = self.get_conn()
        try:
            return conn.execute(
                '''SELECT * FROM vc_sessions
                   WHERE is_active = 1 ORDER BY id DESC LIMIT 1'''
            ).fetchone()
        finally:
            conn.close()

    def end_vc_session(self):
        conn = self.get_conn()
        try:
            conn.execute(
                '''UPDATE vc_sessions
                   SET is_active = 0, ended_at = ?
                   WHERE is_active = 1''',
                (datetime.now(),)
            )
            conn.commit()
        finally:
            conn.close()

    def log_stat(self, action, phone, details):
        conn = self.get_conn()
        try:
            conn.execute(
                '''INSERT INTO bot_stats
                   (action, userbot_phone, details) VALUES (?, ?, ?)''',
                (action, phone, details)
            )
            conn.commit()
        finally:
            conn.close()

    def get_setting(self, key, default='1'):
        conn = self.get_conn()
        try:
            row = conn.execute(
                'SELECT value FROM settings WHERE key = ?', (key,)
            ).fetchone()
            return row[0] if row else default
        finally:
            conn.close()

    def set_setting(self, key, value):
        conn = self.get_conn()
        try:
            conn.execute(
                'INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)',
                (key, value)
            )
            conn.commit()
        finally:
            conn.close()

    def update_last_active(self, phone):
        conn = self.get_conn()
        try:
            conn.execute(
                'UPDATE userbots SET last_active = ? WHERE phone = ?',
                (datetime.now(), phone)
            )
            conn.commit()
        finally:
            conn.close()

    def get_stats(self):
        conn = self.get_conn()
        try:
            stats = {
                'total_userbots': conn.execute(
                    'SELECT COUNT(*) FROM userbots'
                ).fetchone()[0],
                'active_userbots': conn.execute(
                    'SELECT COUNT(*) FROM userbots WHERE is_active = 1'
                ).fetchone()[0],
                'total_users': conn.execute(
                    'SELECT COUNT(*) FROM allowed_users'
                ).fetchone()[0],
                'total_actions': conn.execute(
                    'SELECT COUNT(*) FROM bot_stats'
                ).fetchone()[0],
            }
            avc = self.get_active_vc()
            stats['active_vc'] = avc['vc_link'] if avc else None
            return stats
        finally:
            conn.close()


# ─── Link Utilities ───────────────────────────────────────────────────────────
def extract_invite_hash(link: str) -> Optional[str]:
    link = link.strip()
    m = re.match(r'https?://t\.me/\+([a-zA-Z0-9_\-]+)', link)
    if m:
        return m.group(1)
    m = re.match(
        r'https?://t\.me/joinchat/([a-zA-Z0-9_\-]+)', link
    )
    if m:
        return m.group(1)
    return None


def extract_username(link: str) -> Optional[str]:
    link = link.strip()
    m = re.match(
        r'https?://t\.me/([a-zA-Z0-9_]+)(?:[/?].*)?$', link
    )
    if m:
        u = m.group(1)
        if u.lower() != 'joinchat':
            return u
    return None


def is_private_link(link: str) -> bool:
    return extract_invite_hash(link) is not None


def format_duration(seconds: int) -> str:
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    if h > 0:
        return f"{h}h {m}m {s}s"
    elif m > 0:
        return f"{m}m {s}s"
    return f"{s}s"


def progress_bar(current: int, total: int, length: int = 10) -> str:
    if total == 0:
        return "-" * length
    filled = int(length * current / max(total, 1))
    return "#" * filled + "-" * (length - filled)


# ─── VC Joiner Core ───────────────────────────────────────────────────────────
class VCJoiner:

    @staticmethod
    async def get_entity_from_invite(
        client: TelegramClient, invite_hash: str
    ) -> Tuple[Optional[Any], str]:
        logger.info(f"[VCJoiner] Processing invite hash: {invite_hash}")

        # Phase 1: Check invite info safely
        try:
            info = await client(CheckChatInviteRequest(hash=invite_hash))
            logger.info(
                f"[VCJoiner] CheckChatInvite: {type(info).__name__}"
            )

            if isinstance(info, ChatInviteAlready):
                chat = info.chat
                logger.info(
                    f"[VCJoiner] Already member: "
                    f"{getattr(chat, 'title', chat.id)}"
                )
                return chat, ""

            if isinstance(info, ChatInvite):
                logger.info(
                    f"[VCJoiner] Not member of: {info.title} — joining..."
                )

        except InviteHashExpiredError:
            return None, "Invite link has expired. Get a new one."
        except InviteHashInvalidError:
            return None, "Invite link is invalid. Check the link."
        except Exception as e:
            logger.warning(
                f"[VCJoiner] CheckChatInvite (non-fatal): "
                f"{type(e).__name__}: {e}"
            )

        # Phase 2: Join
        try:
            result = await client(ImportChatInviteRequest(hash=invite_hash))
            logger.debug(
                f"[VCJoiner] ImportChatInvite result: {type(result).__name__}"
            )

            if hasattr(result, 'chats') and result.chats:
                for chat in result.chats:
                    if isinstance(chat, (Channel, Chat)):
                        logger.info(
                            f"[VCJoiner] Joined: "
                            f"{getattr(chat, 'title', chat.id)}"
                        )
                        await asyncio.sleep(2)
                        return chat, ""

            # Fallback: search dialogs
            logger.warning(
                "[VCJoiner] No chats in result, searching dialogs..."
            )
            entity = await VCJoiner._search_dialogs(client)
            if entity:
                return entity, ""

            return None, "Joined but could not retrieve channel entity."

        except UserAlreadyParticipantError:
            logger.info("[VCJoiner] Already participant — searching dialogs")
            entity = await VCJoiner._search_dialogs(client)
            if entity:
                return entity, ""
            try:
                info = await client(
                    CheckChatInviteRequest(hash=invite_hash)
                )
                if isinstance(info, ChatInviteAlready):
                    return info.chat, ""
            except Exception:
                pass
            return None, "Already in channel but could not find entity."

        except InviteHashExpiredError:
            return None, "Invite link has expired."
        except InviteHashInvalidError:
            return None, "Invite link is invalid."

        except FloodWaitError as e:
            logger.warning(f"[VCJoiner] FloodWait: {e.seconds}s")
            wait = min(e.seconds, 60)
            await asyncio.sleep(wait)
            try:
                result = await client(
                    ImportChatInviteRequest(hash=invite_hash)
                )
                if hasattr(result, 'chats') and result.chats:
                    for chat in result.chats:
                        if isinstance(chat, (Channel, Chat)):
                            return chat, ""
            except UserAlreadyParticipantError:
                entity = await VCJoiner._search_dialogs(client)
                if entity:
                    return entity, ""
            except Exception as e2:
                return None, f"FloodWait retry failed: {e2}"
            return None, f"FloodWait {e.seconds}s — try again later."

        except Exception as e:
            logger.error(
                f"[VCJoiner] ImportChatInvite error: "
                f"{type(e).__name__}: {e}"
            )
            return None, f"{type(e).__name__}: {e}"

    @staticmethod
    async def _search_dialogs(
        client: TelegramClient, limit: int = 20
    ) -> Optional[Any]:
        try:
            dialogs = await client.get_dialogs(limit=limit)
            for dialog in dialogs:
                ent = dialog.entity
                if isinstance(ent, (Channel, Chat)):
                    logger.info(
                        f"[VCJoiner] Found in dialogs: "
                        f"{getattr(ent, 'title', ent.id)}"
                    )
                    return ent
        except Exception as e:
            logger.error(f"[VCJoiner] _search_dialogs: {e}")
        return None

    @staticmethod
    async def get_entity_from_username(
        client: TelegramClient, username: str
    ) -> Tuple[Optional[Any], str]:
        logger.info(f"[VCJoiner] Public channel: @{username}")
        try:
            try:
                entity = await client.get_entity(username)
                logger.info(
                    f"[VCJoiner] Got: "
                    f"{getattr(entity, 'title', username)}"
                )
                return entity, ""
            except Exception:
                pass

            await client(JoinChannelRequest(username))
            await asyncio.sleep(2)
            entity = await client.get_entity(username)
            logger.info(f"[VCJoiner] Joined @{username}")
            return entity, ""

        except UserAlreadyParticipantError:
            try:
                return await client.get_entity(username), ""
            except Exception as e:
                return None, str(e)
        except FloodWaitError as e:
            await asyncio.sleep(min(e.seconds, 30))
            try:
                return await client.get_entity(username), ""
            except Exception as e2:
                return None, str(e2)
        except Exception as e:
            logger.error(
                f"[VCJoiner] get_entity_from_username: "
                f"{type(e).__name__}: {e}"
            )
            return None, f"{type(e).__name__}: {e}"

    @staticmethod
    async def get_active_call(
        client: TelegramClient, entity
    ) -> Tuple[Optional[Any], str]:
        try:
            title = getattr(entity, 'title', str(entity.id))
            logger.info(f"[VCJoiner] Getting call for: {title}")
            full = await client(GetFullChannelRequest(channel=entity))
            call = getattr(full.full_chat, 'call', None)
            if call:
                logger.info(f"[VCJoiner] Active call found: {call}")
                return call, ""
            return None, f"No active live stream in '{title}'"
        except Exception as e:
            logger.error(
                f"[VCJoiner] get_active_call: {type(e).__name__}: {e}"
            )
            return None, f"Could not get call: {type(e).__name__}: {e}"

    @staticmethod
    async def join_call(
        client: TelegramClient, entity, call
    ) -> Tuple[bool, str]:
        try:
            me = await client.get_me()
            logger.info(
                f"[VCJoiner] Joining as {me.first_name} ({me.id})"
            )
            params = {
                "ufrag": "".join(
                    random.choices(
                        "abcdefghijklmnopqrstuvwxyz0123456789", k=8
                    )
                ),
                "pwd": "".join(
                    random.choices(
                        "abcdefghijklmnopqrstuvwxyz0123456789", k=22
                    )
                ),
                "fingerprints": [{
                    "hash": "sha-256",
                    "setup": "active",
                    "fingerprint": ":".join(
                        [f"{random.randint(0, 255):02X}" for _ in range(32)]
                    )
                }],
                "ssrc": random.randint(100000000, 999999999)
            }
            join_as = await client.get_input_entity(me)
            await client(JoinGroupCallRequest(
                call=call,
                join_as=join_as,
                params=DataJSON(data=json.dumps(params)),
                muted=True,
                video_stopped=True,
                invite_hash=None,
            ))
            logger.info("[VCJoiner] Joined call successfully!")
            return True, ""
        except UserAlreadyParticipantError:
            logger.info("[VCJoiner] Already in call")
            return True, ""
        except FloodWaitError as e:
            return False, f"FloodWait {e.seconds}s"
        except Exception as e:
            logger.error(
                f"[VCJoiner] join_call: {type(e).__name__}: {e}"
            )
            return False, f"{type(e).__name__}: {e}"


# ─── Userbot Manager ──────────────────────────────────────────────────────────
class UserbotManager:
    def __init__(self, db: DatabaseManager):
        self.db = db
        self.clients: Dict[str, TelegramClient] = {}
        self.pending_logins: Dict[int, Dict] = {}
        self.vc_keepalive_tasks: Dict[str, asyncio.Task] = {}
        self.behavior_tasks: Dict[str, asyncio.Task] = {}
        self.vc_join_info: Dict[str, Dict] = {}
        self.last_errors: Dict[str, str] = {}
        os.makedirs(SESSIONS_DIR, exist_ok=True)

    async def create_client(self, phone: str) -> TelegramClient:
        device = random.choice(DEVICE_TYPES)
        session_file = os.path.join(SESSIONS_DIR, phone.replace('+', ''))
        return TelegramClient(
            session_file, API_ID, API_HASH,
            device_model=device['device'],
            system_version=device['system'],
            app_version=device['app'],
            lang_code='en',
            system_lang_code='en-US',
            connection_retries=999,
            retry_delay=3,
            auto_reconnect=True,
        )

    # ── Login ─────────────────────────────────────────────────────────────────
    async def start_login(self, phone: str, user_id: int) -> Dict:
        try:
            client = await self.create_client(phone)
            await client.connect()
            if await client.is_user_authorized():
                self.clients[phone] = client
                self.db.add_userbot(
                    phone,
                    os.path.join(SESSIONS_DIR, phone.replace('+', '')),
                    user_id
                )
                await self._start_tasks(phone)
                return {'status': 'already_logged_in'}
            result = await client.send_code_request(phone)
            self.pending_logins[user_id] = {
                'phone': phone,
                'client': client,
                'phone_code_hash': result.phone_code_hash,
                'step': 'code'
            }
            return {'status': 'code_sent'}
        except Exception as e:
            logger.error(f"start_login {phone}: {e}")
            return {'status': 'error', 'message': str(e)}

    async def complete_login_code(self, user_id: int, code: str) -> Dict:
        if user_id not in self.pending_logins:
            return {'status': 'error', 'message': 'No pending login found.'}
        data = self.pending_logins[user_id]
        client, phone = data['client'], data['phone']
        try:
            await client.sign_in(
                phone=phone, code=code,
                phone_code_hash=data['phone_code_hash']
            )
            self.clients[phone] = client
            self.db.add_userbot(
                phone,
                os.path.join(SESSIONS_DIR, phone.replace('+', '')),
                user_id
            )
            del self.pending_logins[user_id]
            await self._start_tasks(phone)
            return {'status': 'success', 'phone': phone}
        except SessionPasswordNeededError:
            data['step'] = 'password'
            return {'status': 'need_password'}
        except (PhoneCodeInvalidError, PhoneCodeExpiredError) as e:
            return {'status': 'error', 'message': str(e)}
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    async def complete_login_password(
        self, user_id: int, password: str
    ) -> Dict:
        if user_id not in self.pending_logins:
            return {'status': 'error', 'message': 'No pending login found.'}
        data = self.pending_logins[user_id]
        client, phone = data['client'], data['phone']
        try:
            await client.sign_in(password=password)
            self.clients[phone] = client
            self.db.add_userbot(
                phone,
                os.path.join(SESSIONS_DIR, phone.replace('+', '')),
                user_id
            )
            del self.pending_logins[user_id]
            await self._start_tasks(phone)
            return {'status': 'success', 'phone': phone}
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    # ── Sessions ──────────────────────────────────────────────────────────────
    async def load_all_sessions(self) -> int:
        userbots = self.db.get_all_userbots(active_only=True)
        loaded = 0
        for ub in userbots:
            phone = ub['phone']
            try:
                client = await self.create_client(phone)
                await client.connect()
                if await client.is_user_authorized():
                    self.clients[phone] = client
                    self.db.update_last_active(phone)
                    await self._start_tasks(phone)
                    loaded += 1
                    logger.info(f"Session loaded: {phone}")
                else:
                    logger.warning(f"Session expired: {phone}")
            except Exception as e:
                logger.error(f"Load error {phone}: {e}")
        return loaded

    # ── Background tasks ──────────────────────────────────────────────────────
    async def _start_tasks(self, phone: str):
        await self._start_keepalive(phone)
        await self._start_behavior(phone)

    async def _start_keepalive(self, phone: str):
        if phone in self.vc_keepalive_tasks \
                and not self.vc_keepalive_tasks[phone].done():
            return
        self.vc_keepalive_tasks[phone] = asyncio.create_task(
            self._keepalive_loop(phone)
        )

    async def _keepalive_loop(self, phone: str):
        logger.info(f"Keepalive started: {phone}")
        last_online = 0.0
        last_vc_check = 0.0

        while True:
            try:
                if phone not in self.clients:
                    await asyncio.sleep(10)
                    continue

                client = self.clients[phone]
                now = time.time()

                if now - last_online >= 240:
                    try:
                        await client(
                            functions.account.UpdateStatusRequest(
                                offline=False
                            )
                        )
                        self.db.update_last_active(phone)
                        last_online = now
                    except Exception as e:
                        logger.debug(f"Online ping {phone}: {e}")

                if now - last_vc_check >= 20:
                    last_vc_check = now
                    avc = self.db.get_active_vc()
                    if avc and avc['is_active'] == 1:
                        if phone not in self.vc_join_info:
                            logger.info(f"Auto-rejoin: {phone}")
                            await self._do_join_vc(
                                client, phone,
                                avc['vc_link'],
                                avc['channel_invite']
                            )
                        else:
                            await self._vc_ping(client, phone)

            except Exception as e:
                logger.error(f"Keepalive error {phone}: {e}")

            await asyncio.sleep(5)

    async def _vc_ping(self, client: TelegramClient, phone: str):
        try:
            info = self.vc_join_info.get(phone)
            if not info or not info.get('call'):
                return
            await client(CheckGroupCallRequest(call=info['call']))
        except Exception:
            self.vc_join_info.pop(phone, None)
            logger.info(f"VC ping failed, will rejoin: {phone}")

    # ── Core join ─────────────────────────────────────────────────────────────
    async def _do_join_vc(
        self,
        client: TelegramClient,
        phone: str,
        vc_link: str,
        channel_invite: Optional[str] = None
    ) -> bool:
        self.last_errors.pop(phone, None)

        logger.info(f"JOIN | {phone} | VC={vc_link} | Inv={channel_invite}")

        entity = None

        if channel_invite and is_private_link(channel_invite):
            h = extract_invite_hash(channel_invite)
            entity, err = await VCJoiner.get_entity_from_invite(client, h)
            if entity is None:
                self.last_errors[phone] = err
                return False

        elif is_private_link(vc_link):
            h = extract_invite_hash(vc_link)
            entity, err = await VCJoiner.get_entity_from_invite(client, h)
            if entity is None:
                self.last_errors[phone] = err
                return False

        else:
            username = extract_username(vc_link)
            if not username:
                self.last_errors[phone] = f"Cannot parse: {vc_link}"
                return False
            entity, err = await VCJoiner.get_entity_from_username(
                client, username
            )
            if entity is None:
                self.last_errors[phone] = err
                return False

        logger.info(
            f"[{phone}] Entity: "
            f"{getattr(entity, 'title', 'N/A')} (id={entity.id})"
        )

        await asyncio.sleep(random.uniform(1, 2))

        call, err = await VCJoiner.get_active_call(client, entity)
        if call is None:
            self.last_errors[phone] = err
            return False

        ok, err = await VCJoiner.join_call(client, entity, call)
        if not ok:
            self.last_errors[phone] = err
            return False

        self.vc_join_info[phone] = {
            'call': call,
            'entity': entity,
            'entity_id': entity.id,
            'channel_name': getattr(entity, 'title', 'Unknown'),
            'vc_link': vc_link,
            'channel_invite': channel_invite,
            'joined_at': time.time(),
        }
        self.db.log_stat(
            'vc_join', phone,
            f"Joined: {getattr(entity, 'title', entity.id)}"
        )
        logger.info(f"[{phone}] LIVE STREAM JOINED!")
        return True

    async def join_vc_all(
        self, vc_link: str, channel_invite: Optional[str] = None
    ) -> Dict:
        results: Dict = {
            'success': [], 'failed': [],
            'errors': {}, 'total': 0
        }
        active_bots = self.db.get_all_userbots(active_only=True)
        results['total'] = len(active_bots)

        if not active_bots:
            return results

        for i, ub in enumerate(active_bots):
            phone = ub['phone']
            if phone not in self.clients:
                results['failed'].append(phone)
                results['errors'][phone] = "Not connected"
                continue

            if i > 0:
                await asyncio.sleep(random.uniform(2, 4))

            try:
                ok = await self._do_join_vc(
                    self.clients[phone], phone, vc_link, channel_invite
                )
                if ok:
                    results['success'].append(phone)
                else:
                    results['failed'].append(phone)
                    results['errors'][phone] = self.last_errors.get(
                        phone, 'Unknown'
                    )
            except Exception as e:
                logger.error(f"join_vc_all {phone}: {e}")
                results['failed'].append(phone)
                results['errors'][phone] = str(e)

        return results

    async def leave_vc_all(self) -> Dict:
        results: Dict = {'success': [], 'failed': []}
        for phone, client in list(self.clients.items()):
            try:
                info = self.vc_join_info.get(phone)
                if info and info.get('call'):
                    await client(
                        LeaveGroupCallRequest(call=info['call'], source=0)
                    )
                    results['success'].append(phone)
                self.vc_join_info.pop(phone, None)
            except Exception as e:
                logger.error(f"leave_vc {phone}: {e}")
                results['failed'].append(phone)
                self.vc_join_info.pop(phone, None)
        self.db.end_vc_session()
        return results

    # ── Human behavior ────────────────────────────────────────────────────────
    async def _start_behavior(self, phone: str):
        if phone in self.behavior_tasks \
                and not self.behavior_tasks[phone].done():
            return
        self.behavior_tasks[phone] = asyncio.create_task(
            self._behavior_loop(phone)
        )

    async def _behavior_loop(self, phone: str):
        while True:
            try:
                if phone not in self.clients:
                    await asyncio.sleep(60)
                    continue
                if self.db.get_setting('human_behavior') != '1':
                    await asyncio.sleep(60)
                    continue

                client = self.clients[phone]
                info = self.vc_join_info.get(phone)

                if info and info.get('entity'):
                    entity = info['entity']
                    if self.db.get_setting('auto_react') == '1':
                        await self._auto_react(client, phone, entity)
                    await asyncio.sleep(random.uniform(15, 45))
                    if self.db.get_setting('auto_watch') == '1':
                        await self._auto_watch(client, phone, entity)

            except Exception as e:
                logger.error(f"Behavior loop {phone}: {e}")

            await asyncio.sleep(random.uniform(60, 180))

    async def _auto_react(self, client, phone, entity):
        try:
            msgs = await client.get_messages(entity, limit=1)
            if msgs:
                await client(SendReactionRequest(
                    peer=entity,
                    msg_id=msgs[0].id,
                    reaction=[ReactionEmoji(
                        emoticon=random.choice(REACTION_EMOJIS)
                    )]
                ))
        except Exception as e:
            logger.debug(f"Auto react {phone}: {e}")

    async def _auto_watch(self, client, phone, entity):
        try:
            msgs = await client.get_messages(entity, limit=5)
            if msgs:
                ids = [m.id for m in msgs]
                await client(GetMessagesViewsRequest(
                    peer=entity, id=ids, increment=True
                ))
                await client(
                    ReadHistoryRequest(peer=entity, max_id=ids[0])
                )
        except Exception as e:
            logger.debug(f"Auto watch {phone}: {e}")

    def get_vc_status(self) -> Dict:
        status = {}
        for phone in self.clients:
            info = self.vc_join_info.get(phone)
            if info:
                elapsed = int(
                    time.time() - info.get('joined_at', time.time())
                )
                status[phone] = {
                    'in_vc': True,
                    'elapsed': elapsed,
                    'elapsed_str': format_duration(elapsed),
                    'channel': info.get('channel_name', 'Unknown'),
                }
            else:
                status[phone] = {
                    'in_vc': False,
                    'last_error': self.last_errors.get(phone, ''),
                }
        return status

    async def disconnect_all(self):
        for client in list(self.clients.values()):
            try:
                await client.disconnect()
            except Exception:
                pass
        self.clients.clear()


# ─── Telegram Bot ─────────────────────────────────────────────────────────────
class VCBot:
    def __init__(self):
        self.db = DatabaseManager()
        self.um = UserbotManager(self.db)
        self.app = Application.builder().token(BOT_TOKEN).build()
        self.user_states: Dict[int, Dict] = {}
        self._register_handlers()

    def _register_handlers(self):
        a = self.app
        cmds = [
            ("start", self.cmd_start),
            ("help", self.cmd_help),
            ("addbot", self.cmd_addbot),
            ("bots", self.cmd_bots),
            ("joinvc", self.cmd_joinvc),
            ("leavevc", self.cmd_leavevc),
            ("status", self.cmd_status),
            ("stats", self.cmd_stats),
            ("adduser", self.cmd_adduser),
            ("users", self.cmd_users),
            ("removeuser", self.cmd_removeuser),
            ("settings", self.cmd_settings),
            ("deletebot", self.cmd_deletebot),
            ("togglebot", self.cmd_togglebot),
            ("toggleall", self.cmd_toggleall),
        ]
        for name, handler in cmds:
            a.add_handler(CommandHandler(name, handler))
        a.add_handler(CallbackQueryHandler(self.handle_callback))
        a.add_handler(
            MessageHandler(
                filters.TEXT & ~filters.COMMAND, self.handle_text
            )
        )

    def _allowed(self, uid: int) -> bool:
        return self.db.is_allowed_user(uid)

    def _admin(self, uid: int) -> bool:
        return uid == ADMIN_ID

    async def _deny(self, update: Update):
        await update.message.reply_text(
            "*Access Denied*\n\n"
            "You are not authorized to use this bot.\n"
            "Contact the administrator for access.",
            parse_mode=ParseMode.MARKDOWN
        )

    # ── /start ────────────────────────────────────────────────────────────────
    async def cmd_start(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        if not self._allowed(update.effective_user.id):
            await self._deny(update)
            return

        stats = self.db.get_stats()
        in_vc = sum(
            1 for s in self.um.get_vc_status().values()
            if s.get('in_vc')
        )
        live_status = "Active" if stats['active_vc'] else "Inactive"

        text = (
            "*VC Bot - Live Stream Manager*\n"
            "================================\n\n"
            f"Hello, *{update.effective_user.first_name}*!\n\n"
            "*Quick Overview:*\n"
            f"  Bots: `{stats['active_userbots']}/{stats['total_userbots']}`\n"
            f"  Online: `{len(self.um.clients)}`\n"
            f"  In VC: `{in_vc}`\n"
            f"  Live Session: `{live_status}`\n\n"
            "================================\n"
            "Select an option below:"
        )

        kb = [
            [
                InlineKeyboardButton("Help", callback_data="help"),
                InlineKeyboardButton("Status", callback_data="status"),
            ],
            [
                InlineKeyboardButton("My Bots", callback_data="bots"),
                InlineKeyboardButton("Settings", callback_data="settings"),
            ],
            [
                InlineKeyboardButton(
                    "Statistics", callback_data="stats"
                ),
            ],
        ]
        await update.message.reply_text(
            text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(kb)
        )

    # ── /help ─────────────────────────────────────────────────────────────────
    async def cmd_help(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        if not self._allowed(update.effective_user.id):
            await self._deny(update)
            return
        kb = [[InlineKeyboardButton("Home", callback_data="home")]]
        await update.message.reply_text(
            self._help_text(),
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(kb)
        )

    def _help_text(self) -> str:
        return (
            "*Commands and Usage Guide*\n"
            "================================\n\n"
            "*Bot Management*\n"
            "`/addbot +923001234567`\n"
            "  Add a userbot account\n"
            "`/bots` - View all bots\n"
            "`/deletebot <phone>` - Remove bot\n"
            "`/togglebot <phone>` - Enable/Disable\n"
            "`/toggleall on|off` - Toggle all\n\n"
            "*Live Stream Control*\n"
            "`/joinvc <link>` - Join live\n"
            "`/joinvc <link> <invite>` - Private\n"
            "`/leavevc` - Leave all VCs\n"
            "`/status` - Current status\n\n"
            "*User Management (Admin)*\n"
            "`/adduser <id> [username]`\n"
            "`/removeuser <id>`\n"
            "`/users` - List users\n\n"
            "*Other*\n"
            "`/stats` - Statistics\n"
            "`/settings` - Configure bot\n\n"
            "================================\n"
            "*Private Channel Examples:*\n\n"
            "Same link (most common):\n"
            "`/joinvc https://t.me/+InviteHash`\n\n"
            "Separate channel and live:\n"
            "`/joinvc https://t.me/pubchan https://t.me/+Hash`\n\n"
            "Bot joins channel first, then live!"
        )

    # ── /addbot ───────────────────────────────────────────────────────────────
    async def cmd_addbot(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        if not self._allowed(update.effective_user.id):
            await self._deny(update)
            return

        if not context.args:
            await update.message.reply_text(
                "*Add Userbot Account*\n\n"
                "Usage: `/addbot <phone_number>`\n\n"
                "Example:\n"
                "`/addbot +923001234567`\n\n"
                "International format required (with country code)",
                parse_mode=ParseMode.MARKDOWN
            )
            return

        phone = context.args[0].strip()
        if not phone.startswith('+'):
            phone = '+' + phone

        msg = await update.message.reply_text(
            f"*Connecting Account*\n\n"
            f"Phone: `{phone}`\n"
            f"Status: Sending OTP...",
            parse_mode=ParseMode.MARKDOWN
        )

        result = await self.um.start_login(phone, update.effective_user.id)

        if result['status'] == 'code_sent':
            self.user_states[update.effective_user.id] = {
                'action': 'login_code', 'phone': phone
            }
            await msg.edit_text(
                f"*OTP Sent!*\n\n"
                f"Phone: `{phone}`\n\n"
                f"Check your Telegram for the verification code\n"
                f"and send it here:\n\n"
                f"Format: `12345` or `1 2 3 4 5`",
                parse_mode=ParseMode.MARKDOWN
            )
        elif result['status'] == 'already_logged_in':
            await msg.edit_text(
                f"*Already Active!*\n\n"
                f"`{phone}` is already logged in and running.",
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await msg.edit_text(
                f"*Login Failed*\n\n"
                f"Phone: `{phone}`\n"
                f"Error: `{result.get('message', 'Unknown error')}`",
                parse_mode=ParseMode.MARKDOWN
            )

    # ── /bots ─────────────────────────────────────────────────────────────────
    async def cmd_bots(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        if not self._allowed(update.effective_user.id):
            await self._deny(update)
            return

        userbots = self.db.get_all_userbots()
        kb = [
            [
                InlineKeyboardButton("Refresh", callback_data="bots"),
                InlineKeyboardButton("Home", callback_data="home"),
            ]
        ]
        await update.message.reply_text(
            self._build_bots_text(userbots),
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(kb)
        )

    def _build_bots_text(self, userbots) -> str:
        if not userbots:
            return (
                "*No Bots Added Yet*\n\n"
                "Add your first bot:\n"
                "`/addbot +923001234567`"
            )

        total = len(userbots)
        active = sum(1 for u in userbots if u['is_active'])
        online = sum(
            1 for u in userbots if u['phone'] in self.um.clients
        )
        in_vc = sum(
            1 for u in userbots if u['phone'] in self.um.vc_join_info
        )

        bar = progress_bar(in_vc, total or 1, 12)

        lines = [
            "*Userbot Accounts*",
            "================================",
            "",
            f"[{bar}] {in_vc}/{total} in VC",
            f"Total: `{total}` | Active: `{active}` | "
            f"Online: `{online}` | In VC: `{in_vc}`",
            "",
        ]

        for i, ub in enumerate(userbots, 1):
            phone = ub['phone']
            is_active = ub['is_active']
            is_online = phone in self.um.clients
            is_in_vc = phone in self.um.vc_join_info

            act = "ON " if is_active else "OFF"
            net = "Online " if is_online else "Offline"
            vc = "In-VC" if is_in_vc else "Idle "

            vc_info = self.um.vc_join_info.get(phone)
            vc_str = ""
            if vc_info:
                elapsed = int(
                    time.time() - vc_info.get('joined_at', time.time())
                )
                vc_str = f" [{format_duration(elapsed)}]"

            last = str(ub['last_active'] or 'Never')[:16]
            lines.append(
                f"`{i:02d}.` [{act}][{net}][{vc}]"
                f"{vc_str}\n"
                f"      `{phone}`\n"
                f"      Last: `{last}`\n"
            )

        lines.append("================================")
        lines.append(
            "ON/OFF=Active/Disabled | "
            "Online/Offline=Connection | "
            "In-VC/Idle=VC Status"
        )
        return "\n".join(lines)

    # ── /joinvc ───────────────────────────────────────────────────────────────
    async def cmd_joinvc(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        if not self._allowed(update.effective_user.id):
            await self._deny(update)
            return

        if not context.args:
            await update.message.reply_text(
                "*Join Live Stream*\n\n"
                "*Public Channel:*\n"
                "`/joinvc https://t.me/channelname`\n\n"
                "*Private Channel:*\n"
                "`/joinvc https://t.me/+InviteHash`\n\n"
                "*Separate channel and live:*\n"
                "`/joinvc https://t.me/pub https://t.me/+Hash`\n\n"
                "For private channels, just send the invite link!\n"
                "Bot joins channel first, then the live stream.",
                parse_mode=ParseMode.MARKDOWN
            )
            return

        vc_link = context.args[0].strip()
        channel_invite = (
            context.args[1].strip() if len(context.args) > 1 else None
        )

        if is_private_link(vc_link) and channel_invite is None:
            channel_invite = vc_link

        if not vc_link.startswith('http'):
            await update.message.reply_text(
                "Invalid link. Must start with `https://t.me/`",
                parse_mode=ParseMode.MARKDOWN
            )
            return

        active_bots = self.db.get_all_userbots(active_only=True)
        connected = [
            ub['phone'] for ub in active_bots
            if ub['phone'] in self.um.clients
        ]

        if not connected:
            await update.message.reply_text(
                "*No Connected Bots!*\n\n"
                "All bots are offline. Add one with:\n"
                "`/addbot +phone`",
                parse_mode=ParseMode.MARKDOWN
            )
            return

        is_private = channel_invite and is_private_link(channel_invite)
        link_type = "Private Channel" if is_private else "Public Channel"

        vc_display = (
            vc_link[:50] + '...' if len(vc_link) > 50 else vc_link
        )

        msg = await update.message.reply_text(
            f"*Joining Live Stream*\n\n"
            f"Type: {link_type}\n"
            f"Link: `{vc_display}`\n"
            f"Accounts: `{len(connected)}`\n\n"
            f"{'Joining private channel first...' if is_private else ''}\n"
            f"Please wait...",
            parse_mode=ParseMode.MARKDOWN
        )

        self.db.save_vc_session(
            vc_link, None, update.effective_user.id, channel_invite
        )

        results = await self.um.join_vc_all(vc_link, channel_invite)

        s = len(results['success'])
        f = len(results['failed'])
        t = results['total']
        bar = progress_bar(s, t, 14)

        out = [
            "*Join Results*",
            "================================",
            "",
            f"[{bar}]",
            f"Joined : `{s}/{t}`",
            f"Failed : `{f}/{t}`",
            "",
        ]

        if results['success']:
            out.append("*Joined Successfully:*")
            for ph in results['success']:
                info = self.um.vc_join_info.get(ph, {})
                ch = info.get('channel_name', '')
                ch_str = f" -> {ch}" if ch else ""
                out.append(f"  `{ph}`{ch_str}")

        if results['failed']:
            out.append("\n*Failed:*")
            for ph in results['failed']:
                err = results['errors'].get(ph, 'Unknown error')
                out.append(f"  `{ph}`")
                out.append(f"  Reason: _{err[:70]}_")

        out.append("\n================================")

        if s > 0:
            out.append(f"*{s} account(s) are now live!*")
            out.append("Auto-rejoin is active - they wont leave!")
        else:
            out.append("*All attempts failed.*")
            out.append("Possible reasons:")
            out.append("- Live stream not currently active")
            out.append("- Invite link expired or invalid")
            out.append("- Check vcbot.log for details")

        kb = [
            [
                InlineKeyboardButton(
                    "Status", callback_data="status"
                ),
                InlineKeyboardButton(
                    "Leave All", callback_data="leave_vc"
                ),
            ],
            [InlineKeyboardButton("Home", callback_data="home")],
        ]

        await msg.edit_text(
            "\n".join(out),
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(kb)
        )

    # ── /leavevc ──────────────────────────────────────────────────────────────
    async def cmd_leavevc(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        if not self._allowed(update.effective_user.id):
            await self._deny(update)
            return

        msg = await update.message.reply_text(
            "*Leaving all voice chats...*",
            parse_mode=ParseMode.MARKDOWN
        )
        r = await self.um.leave_vc_all()
        await msg.edit_text(
            f"*Left Voice Chats*\n\n"
            f"Successfully left: `{len(r['success'])}`\n"
            f"Failed: `{len(r['failed'])}`",
            parse_mode=ParseMode.MARKDOWN
        )

    # ── /status ───────────────────────────────────────────────────────────────
    async def cmd_status(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        if not self._allowed(update.effective_user.id):
            await self._deny(update)
            return

        kb = [
            [
                InlineKeyboardButton(
                    "Refresh", callback_data="status"
                ),
                InlineKeyboardButton(
                    "Leave All", callback_data="leave_vc"
                ),
            ],
            [InlineKeyboardButton("Home", callback_data="home")],
        ]
        await update.message.reply_text(
            self._build_status_text(),
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(kb)
        )

    def _build_status_text(self) -> str:
        avc = self.db.get_active_vc()
        vs = self.um.get_vc_status()
        all_bots = self.db.get_all_userbots()

        in_vc_count = sum(1 for s in vs.values() if s.get('in_vc'))
        online_count = len(self.um.clients)
        total = len(all_bots)

        bar = progress_bar(in_vc_count, total or 1, 12)

        lines = [
            "*Live Status Dashboard*",
            "================================",
            "",
        ]

        if avc and avc['is_active']:
            vc_link = avc['vc_link']
            started = str(avc['started_at'])[:16]
            vc_disp = (
                vc_link[:45] + '...' if len(vc_link) > 45 else vc_link
            )
            lines.append(">> LIVE SESSION ACTIVE <<")
            lines.append(f"Link: `{vc_disp}`")
            if avc.get('channel_invite') and \
                    avc['channel_invite'] != vc_link:
                ci = avc['channel_invite']
                ci_disp = ci[:45] + '...' if len(ci) > 45 else ci
                lines.append(f"Channel: `{ci_disp}`")
            lines.append(f"Started: `{started}`")
        else:
            lines.append(">> NO ACTIVE SESSION <<")

        lines.append("")
        lines.append(f"[{bar}] `{in_vc_count}/{total}` in VC")
        lines.append("")
        lines.append(
            f"Total: `{total}` | Online: `{online_count}` | "
            f"In VC: `{in_vc_count}`"
        )
        lines.append("")

        if vs:
            lines.append("*Account Details:*")
            lines.append("--------------------------------")
            for phone, s in vs.items():
                if s.get('in_vc'):
                    ch = s.get('channel', '')
                    t = s.get('elapsed_str', '-')
                    ch_str = f" | {ch}" if ch else ""
                    lines.append(f"[LIVE] `{phone}`")
                    lines.append(f"  Time: `{t}`{ch_str}")
                else:
                    err = s.get('last_error', '')
                    lines.append(f"[IDLE] `{phone}`")
                    if err:
                        lines.append(f"  Note: _{err[:60]}_")

        lines.append("")
        lines.append("================================")
        lines.append(
            f"Updated: `{datetime.now().strftime('%H:%M:%S')}`"
        )
        return "\n".join(lines)

    # ── /stats ────────────────────────────────────────────────────────────────
    async def cmd_stats(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        if not self._allowed(update.effective_user.id):
            await self._deny(update)
            return

        s = self.db.get_stats()
        in_vc = sum(
            1 for st in self.um.get_vc_status().values()
            if st.get('in_vc')
        )
        bot_bar = progress_bar(
            s['active_userbots'], s['total_userbots'] or 1, 14
        )
        vc_bar = progress_bar(in_vc, s['total_userbots'] or 1, 14)

        text = (
            "*Bot Statistics*\n"
            "================================\n\n"
            "*Userbot Accounts*\n"
            f"[{bot_bar}]\n"
            f"Total  : `{s['total_userbots']}`\n"
            f"Active : `{s['active_userbots']}`\n"
            f"Online : `{len(self.um.clients)}`\n\n"
            "*Voice Chat*\n"
            f"[{vc_bar}]\n"
            f"In VC   : `{in_vc}`\n"
            f"Session : "
            f"`{'Active' if s['active_vc'] else 'Inactive'}`\n\n"
            "*Usage*\n"
            f"Users   : `{s['total_users']}`\n"
            f"Actions : `{s['total_actions']}`\n\n"
            "================================\n"
            f"`{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`"
        )
        kb = [[InlineKeyboardButton("Home", callback_data="home")]]
        await update.message.reply_text(
            text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(kb)
        )

    # ── /adduser ──────────────────────────────────────────────────────────────
    async def cmd_adduser(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        if not self._admin(update.effective_user.id):
            await update.message.reply_text(
                "*Admin Only Command*",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        if not context.args:
            await update.message.reply_text(
                "Usage: `/adduser <user_id> [username]`",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        try:
            uid = int(context.args[0])
            uname = context.args[1] if len(context.args) > 1 else ''
            self.db.add_allowed_user(uid, uname, update.effective_user.id)
            await update.message.reply_text(
                f"*User Added*\n\n"
                f"ID: `{uid}`\n"
                f"Username: `{uname or 'Not set'}`",
                parse_mode=ParseMode.MARKDOWN
            )
        except ValueError:
            await update.message.reply_text(
                "Invalid user ID. Must be a number."
            )

    # ── /users ────────────────────────────────────────────────────────────────
    async def cmd_users(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        if not self._admin(update.effective_user.id):
            await update.message.reply_text(
                "*Admin Only Command*",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        users = self.db.get_allowed_users()
        if not users:
            await update.message.reply_text(
                "*No Allowed Users*\n\nAdd one with:\n`/adduser <id>`",
                parse_mode=ParseMode.MARKDOWN
            )
            return

        lines = [
            f"*Allowed Users ({len(users)})*",
            "================================",
            "",
        ]
        for i, u in enumerate(users, 1):
            uname = f"@{u['username']}" if u['username'] else "No username"
            added = str(u['added_at'])[:10]
            lines.append(
                f"`{i:02d}.` ID: `{u['user_id']}`\n"
                f"      User: {uname}\n"
                f"      Added: `{added}`\n"
            )
        await update.message.reply_text(
            "\n".join(lines), parse_mode=ParseMode.MARKDOWN
        )

    # ── /removeuser ───────────────────────────────────────────────────────────
    async def cmd_removeuser(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        if not self._admin(update.effective_user.id):
            await update.message.reply_text(
                "*Admin Only*", parse_mode=ParseMode.MARKDOWN
            )
            return
        if not context.args:
            await update.message.reply_text(
                "Usage: `/removeuser <user_id>`",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        try:
            uid = int(context.args[0])
            self.db.remove_allowed_user(uid)
            await update.message.reply_text(
                f"User `{uid}` removed.", parse_mode=ParseMode.MARKDOWN
            )
        except ValueError:
            await update.message.reply_text("Invalid user ID.")

    # ── /deletebot ────────────────────────────────────────────────────────────
    async def cmd_deletebot(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        if not self._allowed(update.effective_user.id):
            await self._deny(update)
            return
        if not context.args:
            await update.message.reply_text(
                "Usage: `/deletebot <phone>`",
                parse_mode=ParseMode.MARKDOWN
            )
            return

        phone = context.args[0].strip()
        if not phone.startswith('+'):
            phone = '+' + phone

        if phone in self.um.clients:
            try:
                await self.um.clients[phone].disconnect()
            except Exception:
                pass
            del self.um.clients[phone]

        for d in [self.um.vc_join_info, self.um.last_errors]:
            d.pop(phone, None)

        for td in [self.um.vc_keepalive_tasks, self.um.behavior_tasks]:
            if phone in td:
                td[phone].cancel()
                del td[phone]

        self.db.delete_userbot(phone)

        sf = os.path.join(
            SESSIONS_DIR, phone.replace('+', '') + '.session'
        )
        try:
            if os.path.exists(sf):
                os.remove(sf)
        except Exception:
            pass

        await update.message.reply_text(
            f"*Bot Deleted*\n\n`{phone}` removed successfully.",
            parse_mode=ParseMode.MARKDOWN
        )

    # ── /togglebot ────────────────────────────────────────────────────────────
    async def cmd_togglebot(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        if not self._allowed(update.effective_user.id):
            await self._deny(update)
            return
        if not context.args:
            await update.message.reply_text(
                "Usage: `/togglebot <phone>`",
                parse_mode=ParseMode.MARKDOWN
            )
            return

        phone = context.args[0].strip()
        if not phone.startswith('+'):
            phone = '+' + phone

        ub = self.db.get_userbot(phone)
        if not ub:
            await update.message.reply_text(
                f"Bot `{phone}` not found.", parse_mode=ParseMode.MARKDOWN
            )
            return

        new = 0 if ub['is_active'] else 1
        self.db.toggle_userbot(phone, new)
        state = "Enabled" if new else "Disabled"
        await update.message.reply_text(
            f"{state} `{phone}`", parse_mode=ParseMode.MARKDOWN
        )

    # ── /toggleall ────────────────────────────────────────────────────────────
    async def cmd_toggleall(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        if not self._allowed(update.effective_user.id):
            await self._deny(update)
            return
        if not context.args or \
                context.args[0].lower() not in ('on', 'off'):
            await update.message.reply_text(
                "Usage: `/toggleall on|off`",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        active = context.args[0].lower() == 'on'
        self.db.toggle_all_userbots(active)
        await update.message.reply_text(
            f"{'All bots enabled' if active else 'All bots disabled'}"
        )

    # ── /settings ─────────────────────────────────────────────────────────────
    async def cmd_settings(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        if not self._allowed(update.effective_user.id):
            await self._deny(update)
            return
        t, kb = self._settings_content()
        await update.message.reply_text(
            t,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(kb)
        )

    def _settings_content(self):
        ar = self.db.get_setting('auto_react') == '1'
        aw = self.db.get_setting('auto_watch') == '1'
        hb = self.db.get_setting('human_behavior') == '1'

        on = "ON "
        off = "OFF"

        text = (
            "*Bot Settings*\n"
            "================================\n\n"
            "Configure bot behavior in live streams:\n\n"
            f"[{on if ar else off}] *Auto React*\n"
            f"   Automatically react to messages\n\n"
            f"[{on if aw else off}] *Auto Watch*\n"
            f"   Mark messages as viewed\n\n"
            f"[{on if hb else off}] *Human Behavior*\n"
            f"   Simulate natural human activity\n\n"
            "================================\n"
            "Tap buttons to toggle:"
        )
        kb = [
            [InlineKeyboardButton(
                f"[{on if ar else off}] Auto React",
                callback_data="toggle_auto_react"
            )],
            [InlineKeyboardButton(
                f"[{on if aw else off}] Auto Watch",
                callback_data="toggle_auto_watch"
            )],
            [InlineKeyboardButton(
                f"[{on if hb else off}] Human Behavior",
                callback_data="toggle_human_behavior"
            )],
            [InlineKeyboardButton("Home", callback_data="home")],
        ]
        return text, kb

    # ── Callbacks ─────────────────────────────────────────────────────────────
    async def handle_callback(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        q = update.callback_query
        await q.answer()
        uid = q.from_user.id

        if not self._allowed(uid):
            await q.edit_message_text("Not authorized.")
            return

        d = q.data

        try:
            if d == "home":
                stats = self.db.get_stats()
                in_vc = sum(
                    1 for s in self.um.get_vc_status().values()
                    if s.get('in_vc')
                )
                live_status = "Active" if stats['active_vc'] else "Inactive"
                text = (
                    "*VC Bot - Live Stream Manager*\n"
                    "================================\n\n"
                    "*Quick Overview:*\n"
                    f"  Bots: `{stats['active_userbots']}"
                    f"/{stats['total_userbots']}`\n"
                    f"  Online: `{len(self.um.clients)}`\n"
                    f"  In VC: `{in_vc}`\n"
                    f"  Live Session: `{live_status}`\n\n"
                    "Select an option:"
                )
                kb = [
                    [
                        InlineKeyboardButton(
                            "Help", callback_data="help"
                        ),
                        InlineKeyboardButton(
                            "Status", callback_data="status"
                        ),
                    ],
                    [
                        InlineKeyboardButton(
                            "My Bots", callback_data="bots"
                        ),
                        InlineKeyboardButton(
                            "Settings", callback_data="settings"
                        ),
                    ],
                    [
                        InlineKeyboardButton(
                            "Statistics", callback_data="stats"
                        )
                    ],
                ]
                await q.edit_message_text(
                    text,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup(kb)
                )

            elif d == "help":
                kb = [[
                    InlineKeyboardButton("Home", callback_data="home")
                ]]
                await q.edit_message_text(
                    self._help_text(),
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup(kb)
                )

            elif d == "status":
                kb = [
                    [
                        InlineKeyboardButton(
                            "Refresh", callback_data="status"
                        ),
                        InlineKeyboardButton(
                            "Leave All", callback_data="leave_vc"
                        ),
                    ],
                    [
                        InlineKeyboardButton("Home", callback_data="home")
                    ],
                ]
                await q.edit_message_text(
                    self._build_status_text(),
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup(kb)
                )

            elif d == "bots":
                userbots = self.db.get_all_userbots()
                kb = [
                    [
                        InlineKeyboardButton(
                            "Refresh", callback_data="bots"
                        ),
                        InlineKeyboardButton(
                            "Home", callback_data="home"
                        ),
                    ]
                ]
                await q.edit_message_text(
                    self._build_bots_text(userbots),
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup(kb)
                )

            elif d == "settings":
                t, kb = self._settings_content()
                await q.edit_message_text(
                    t,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup(kb)
                )

            elif d == "stats":
                s = self.db.get_stats()
                in_vc = sum(
                    1 for st in self.um.get_vc_status().values()
                    if st.get('in_vc')
                )
                bot_bar = progress_bar(
                    s['active_userbots'], s['total_userbots'] or 1, 14
                )
                vc_bar = progress_bar(
                    in_vc, s['total_userbots'] or 1, 14
                )
                text = (
                    "*Bot Statistics*\n"
                    "================================\n\n"
                    "*Userbot Accounts*\n"
                    f"[{bot_bar}]\n"
                    f"Total  : `{s['total_userbots']}`\n"
                    f"Active : `{s['active_userbots']}`\n"
                    f"Online : `{len(self.um.clients)}`\n\n"
                    "*Voice Chat*\n"
                    f"[{vc_bar}]\n"
                    f"In VC   : `{in_vc}`\n"
                    f"Session : "
                    f"`{'Active' if s['active_vc'] else 'Inactive'}`\n\n"
                    "*Usage*\n"
                    f"Users   : `{s['total_users']}`\n"
                    f"Actions : `{s['total_actions']}`\n\n"
                    "================================\n"
                    f"`{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`"
                )
                kb = [[
                    InlineKeyboardButton("Home", callback_data="home")
                ]]
                await q.edit_message_text(
                    text,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup(kb)
                )

            elif d == "leave_vc":
                await q.edit_message_text(
                    "*Leaving all voice chats...*",
                    parse_mode=ParseMode.MARKDOWN
                )
                r = await self.um.leave_vc_all()
                kb = [[
                    InlineKeyboardButton("Home", callback_data="home")
                ]]
                await q.edit_message_text(
                    f"*Done!*\n\n"
                    f"Left: `{len(r['success'])}`\n"
                    f"Failed: `{len(r['failed'])}`",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup(kb)
                )

            elif d.startswith("toggle_"):
                key = d.replace("toggle_", "")
                cur = self.db.get_setting(key)
                self.db.set_setting(key, '0' if cur == '1' else '1')
                t, kb = self._settings_content()
                await q.edit_message_text(
                    t,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup(kb)
                )

        except Exception as e:
            logger.error(f"Callback error [{d}]: {e}")
            try:
                await q.edit_message_text(
                    f"An error occurred:\n`{str(e)[:100]}`",
                    parse_mode=ParseMode.MARKDOWN
                )
            except Exception:
                pass

    # ── Text handler ──────────────────────────────────────────────────────────
    async def handle_text(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ):
        uid = update.effective_user.id
        text = update.message.text.strip()

        if not self._allowed(uid):
            return

        state = self.user_states.get(uid)
        if not state:
            return

        action = state.get('action')

        if action == 'login_code':
            code = re.sub(r'\D', '', text)
            if len(code) < 4:
                await update.message.reply_text(
                    "Invalid code. Please send the 5-digit OTP code."
                )
                return

            msg = await update.message.reply_text(
                f"*Verifying OTP*\n\nCode: `{code}`\nPlease wait...",
                parse_mode=ParseMode.MARKDOWN
            )
            result = await self.um.complete_login_code(uid, code)

            if result['status'] == 'success':
                del self.user_states[uid]
                await msg.edit_text(
                    f"*Login Successful!*\n\n"
                    f"Phone: `{result['phone']}`\n"
                    f"Status: Online and Active\n\n"
                    f"Your bot is ready to join live streams!",
                    parse_mode=ParseMode.MARKDOWN
                )
            elif result['status'] == 'need_password':
                self.user_states[uid] = {
                    'action': 'login_password',
                    'phone': state['phone']
                }
                await msg.edit_text(
                    f"*2FA Required*\n\n"
                    f"Your account has two-factor authentication.\n"
                    f"Please send your 2FA password:",
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                await msg.edit_text(
                    f"*Verification Failed*\n\n"
                    f"Error: `{result.get('message', 'Unknown error')}`\n\n"
                    f"Try again with `/addbot {state['phone']}`",
                    parse_mode=ParseMode.MARKDOWN
                )
                del self.user_states[uid]

        elif action == 'login_password':
            msg = await update.message.reply_text(
                "*Verifying 2FA Password...*",
                parse_mode=ParseMode.MARKDOWN
            )
            result = await self.um.complete_login_password(uid, text)
            if result['status'] == 'success':
                del self.user_states[uid]
                await msg.edit_text(
                    f"*Login Successful!*\n\n"
                    f"Phone: `{result['phone']}`\n"
                    f"2FA: Verified\n"
                    f"Status: Online and Active",
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                await msg.edit_text(
                    f"*Wrong Password*\n\n"
                    f"Error: `{result.get('message', 'Incorrect password')}`",
                    parse_mode=ParseMode.MARKDOWN
                )
                del self.user_states[uid]

    # ── Run ───────────────────────────────────────────────────────────────────
    async def run(self):
        logger.info("=" * 50)
        logger.info("VC Bot Starting...")
        logger.info("=" * 50)

        loaded = await self.um.load_all_sessions()
        logger.info(f"Sessions loaded: {loaded}")

        avc = self.db.get_active_vc()
        if avc and avc['is_active']:
            logger.info("Found active VC session - will auto-rejoin")

        await self.app.initialize()
        await self.app.start()
        await self.app.updater.start_polling(drop_pending_updates=True)

        logger.info("Bot is running! Press Ctrl+C to stop.")
        logger.info("=" * 50)

        try:
            await asyncio.Event().wait()
        except (KeyboardInterrupt, SystemExit):
            pass
        finally:
            logger.info("Shutting down...")
            await self.um.disconnect_all()
            await self.app.updater.stop()
            await self.app.stop()
            await self.app.shutdown()


# ─── Entry Point ──────────────────────────────────────────────────────────────
async def main():
    await VCBot().run()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nBot stopped.")
