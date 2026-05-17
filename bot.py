import os
import sys
import json
import asyncio
import logging
import time
import random
from datetime import datetime
from pathlib import Path
from typing import Optional

from telethon import TelegramClient, events, Button, errors
from telethon.sessions import StringSession
from telethon.tl.functions.messages import (
    GetChatInviteImportersRequest,
    GetHistoryRequest,
    ForwardMessagesRequest,
)
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.functions.account import UpdateStatusRequest
from telethon.tl.types import (
    UpdateBotChatInviteRequester,
    PeerChannel,
    InputUserEmpty,
    InputPeerSelf,
)

# ═══════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════

BOT_TOKEN = "8746503686:AAEBiFjdK-QnwXxulQVeKjIB17rOuK9NDcA"
API_ID    = 39052980
API_HASH  = "5b0b6f9aedd2113a4a591dbcde61be43"
ADMIN_ID  = 7353041224

CONFIG_FILE   = "config.json"
PROGRESS_FILE = "progress.json"
SENT_FILE     = "sent_users.json"
MEDIA_DIR     = "saved_media"
LOG_FILE      = "userbot.log"

Path(MEDIA_DIR).mkdir(exist_ok=True)

# ═══════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("UBM")


# ═══════════════════════════════════════════════════
# SENT STORE — only used when user manually enables skip
# ═══════════════════════════════════════════════════

class SentStore:
    def __init__(self):
        self._ids: set = set()
        self._load()

    def _load(self):
        try:
            if Path(SENT_FILE).exists():
                with open(SENT_FILE) as f:
                    data = json.load(f)
                self._ids = set(data.get("ids", []))
                logger.info(f"SentStore loaded: {len(self._ids)} users")
        except Exception as e:
            logger.error(f"SentStore load error: {e}")
            self._ids = set()

    def save(self):
        try:
            with open(SENT_FILE, "w") as f:
                json.dump({
                    "ids": list(self._ids),
                    "count": len(self._ids),
                    "ts": str(datetime.now())
                }, f)
        except Exception as e:
            logger.error(f"SentStore save error: {e}")

    def add(self, uid: int):
        self._ids.add(uid)

    def has(self, uid: int) -> bool:
        return uid in self._ids

    def count(self) -> int:
        return len(self._ids)

    def clear(self):
        self._ids.clear()
        self.save()
        logger.info("SentStore cleared")

    def get_all(self) -> list:
        return list(self._ids)


sent_store = SentStore()


# ═══════════════════════════════════════════════════
# STATE
# ═══════════════════════════════════════════════════

class State:
    def __init__(self):
        # Auth
        self.authorized_users: list = []
        self.phone_number           = None
        self.session_string         = None
        self.accounts: dict         = {}
        self.active_account         = None

        # Message
        self.auto_message = (
            "👋 Hello! Thanks for requesting to join.\n"
            "We'll review your request shortly!"
        )
        self.media_file_id   = None
        self.media_type      = None
        self.media_caption   = None
        self.send_media_only = False
        self.send_both       = False

        # Forward mode
        self.use_saved_forward = False
        self.forward_count     = 2
        self.forward_with_tag  = True
        self._saved_msg_ids: list = []

        # Timing — safe defaults
        self.random_delay      = True
        self.min_delay         = 5
        self.max_delay         = 9
        self.batch_size        = 35
        self.batch_delay       = 90
        self.peer_flood_pause  = 900
        self.flood_wait_extra  = 15
        self.max_flood_retries = 2
        self.max_peer_retries  = 1

        # KEY FIX: skip_already_sent DEFAULT = FALSE
        # so ALL members get messaged, not just 199
        self.skip_already_sent = False

        self.auto_approve    = False
        self.online_status   = False
        self.typing_sim      = False
        self.userbot_running = False

        # Stats
        self.stats = {
            "sent": 0, "failed": 0,
            "total_requests": 0, "approved": 0,
            "flood_waits": 0, "peer_floods": 0,
            "session_start": str(datetime.now()),
            "last_reset": str(datetime.now()),
        }

        # Channel / user lists
        self.monitored_channels: list = []
        self.channel_names: dict      = {}
        self.blacklisted_users: list  = []
        self.whitelisted_users: list  = []

        # Runtime — not saved
        self.login_state      = None
        self.phone_code_hash  = None
        self.pending_phone    = None
        self._temp_client     = None

        self._load()
        self._normalize_settings()

    def save(self):
        skip = {"login_state", "phone_code_hash", "pending_phone"}
        data = {
            k: v for k, v in self.__dict__.items()
            if not k.startswith("_") and k not in skip
        }
        try:
            with open(CONFIG_FILE, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Config save error: {e}")

    def _load(self):
        if not Path(CONFIG_FILE).exists():
            return
        try:
            with open(CONFIG_FILE) as f:
                d = json.load(f)
            for k, v in d.items():
                if hasattr(self, k) and not k.startswith("_"):
                    setattr(self, k, v)
            logger.info("Config loaded ✅")
        except Exception as e:
            logger.error(f"Config load error: {e}")

    def _normalize_settings(self):
        old_timing = (
            self.min_delay == 10
            and self.max_delay == 18
            and self.batch_size == 20
            and self.batch_delay == 180
        )
        if old_timing:
            self.min_delay = 5
            self.max_delay = 9
            self.batch_size = 35
            self.batch_delay = 90
            logger.info("Timing upgraded to faster defaults")
        if self.max_delay < self.min_delay:
            self.max_delay = self.min_delay

    def reset_stats(self):
        self.stats = {
            "sent": 0, "failed": 0,
            "total_requests": 0, "approved": 0,
            "flood_waits": 0, "peer_floods": 0,
            "session_start": str(datetime.now()),
            "last_reset": str(datetime.now()),
        }
        self.save()

    def is_allowed(self, uid: int) -> bool:
        if uid in self.blacklisted_users:
            return False
        if self.whitelisted_users and uid not in self.whitelisted_users:
            return False
        return True

    def get_delay(self) -> float:
        if self.max_delay < self.min_delay:
            self.max_delay = self.min_delay
        if self.random_delay:
            return random.uniform(self.min_delay, self.max_delay)
        return float(self.min_delay)

    def save_progress(self, done: list, pending: list, failed: list):
        try:
            with open(PROGRESS_FILE, "w") as f:
                json.dump({
                    "done":    done,
                    "pending": pending,
                    "failed":  failed,
                    "ts":      str(datetime.now()),
                }, f)
        except Exception as e:
            logger.error(f"Progress save error: {e}")

    def load_progress(self) -> dict:
        if not Path(PROGRESS_FILE).exists():
            return {}
        try:
            with open(PROGRESS_FILE) as f:
                return json.load(f)
        except Exception:
            return {}

    def clear_progress(self):
        if Path(PROGRESS_FILE).exists():
            try:
                os.remove(PROGRESS_FILE)
            except Exception:
                pass


st  = State()
bot = TelegramClient("bot_session", API_ID, API_HASH)
userbot: Optional[TelegramClient] = None


# ═══════════════════════════════════════════════════
# ACCESS
# ═══════════════════════════════════════════════════

def is_auth(uid: int) -> bool:
    return uid == ADMIN_ID or uid in st.authorized_users

def is_admin(uid: int) -> bool:
    return uid == ADMIN_ID


# ═══════════════════════════════════════════════════
# CAMPAIGN ENGINE
# ═══════════════════════════════════════════════════

class Campaign:
    def __init__(self):
        self._stop_ev = asyncio.Event()
        self._task: Optional[asyncio.Task] = None
        self.active       = False
        self.paused       = False
        self.pause_reason = ""
        self.sent         = 0
        self.failed       = 0
        self.skipped      = 0
        self.total        = 0
        self.idx          = 0
        self.t_start      = 0.0
        self.label        = ""

    def is_running(self) -> bool:
        return self.active

    def request_stop(self):
        self._stop_ev.set()
        async def _force():
            await asyncio.sleep(60)
            if self._task and not self._task.done():
                self._task.cancel()
        asyncio.create_task(_force())

    def should_stop(self) -> bool:
        return self._stop_ev.is_set()

    def reset(self):
        self._stop_ev.clear()
        self.active       = False
        self.paused       = False
        self.pause_reason = ""
        self.sent         = 0
        self.failed       = 0
        self.skipped      = 0
        self.total        = 0
        self.idx          = 0
        self.t_start      = 0.0
        self.label        = ""

    async def sleep(self, secs: float):
        """Interruptible sleep."""
        deadline = time.time() + secs
        while time.time() < deadline:
            if self._stop_ev.is_set():
                return
            await asyncio.sleep(min(1.0, deadline - time.time()))

    def eta(self) -> str:
        elapsed = time.time() - self.t_start
        if self.idx == 0 or elapsed == 0:
            return "Calculating…"
        rate = self.idx / elapsed
        rem  = (self.total - self.idx) / rate if rate > 0 else 0
        h, r = divmod(int(rem), 3600)
        m, s = divmod(r, 60)
        return f"{h}h {m}m" if h else f"{m}m {s}s"

    def speed(self) -> int:
        elapsed = time.time() - self.t_start
        return round(self.sent / elapsed * 3600) if elapsed > 0 and self.sent > 0 else 0

    def pbar(self, w=18) -> str:
        pct = self.idx / self.total * 100 if self.total else 0
        f   = int(pct / 100 * w)
        return "█" * f + "░" * (w - f)

    def progress_text(self) -> str:
        pct = self.idx / self.total * 100 if self.total else 0
        pl  = f"\n⏸️ **{self.pause_reason}**" if self.paused else ""
        return (
            f"🚀 **{self.label}**{pl}\n\n"
            f"`{self.pbar()}` **{pct:.1f}%**\n\n"
            f"✅ Sent:     **{self.sent}**\n"
            f"❌ Failed:   **{self.failed}**\n"
            f"⏭️ Skipped:  **{self.skipped}**\n"
            f"📊 Progress: **{self.idx}/{self.total}**\n"
            f"⚡ Speed:    **{self.speed()}/hr**\n"
            f"⏱️ ETA:      **{self.eta()}**\n\n"
            f"_/stop to abort_"
        )

    def launch(self, chat_id: int, mode: str):
        if self._task and not self._task.done():
            self._task.cancel()
        self.reset()
        self._task = asyncio.create_task(_run(self, chat_id, mode))
        return self._task


camp = Campaign()


# ═══════════════════════════════════════════════════
# USERBOT MANAGEMENT
# ═══════════════════════════════════════════════════

async def ensure_connected() -> bool:
    global userbot
    if not userbot:
        return False
    try:
        if not userbot.is_connected():
            await userbot.connect()
        return await userbot.is_user_authorized()
    except Exception as e:
        logger.error(f"ensure_connected: {e}")
        return False


async def reconnect() -> bool:
    global userbot
    for i in range(5):
        try:
            if userbot:
                try:
                    await userbot.disconnect()
                except Exception:
                    pass
                await asyncio.sleep(5)
                await userbot.connect()
                if await userbot.is_user_authorized():
                    logger.info("✅ Reconnected")
                    return True
        except Exception as e:
            logger.error(f"Reconnect {i+1}: {e}")
            await asyncio.sleep(10)
    return False


async def start_userbot() -> bool:
    global userbot
    if not st.session_string:
        return False
    try:
        if userbot and userbot.is_connected():
            return True
        userbot = TelegramClient(
            StringSession(st.session_string),
            API_ID, API_HASH,
            connection_retries=20,
            retry_delay=3,
            auto_reconnect=True,
        )
        await userbot.connect()
        if not await userbot.is_user_authorized():
            return False
        me = await userbot.get_me()
        logger.info(f"Userbot: {me.first_name} ({me.id})")
        if st.online_status:
            try:
                await userbot(UpdateStatusRequest(offline=False))
            except Exception:
                pass
        if st.use_saved_forward:
            st._saved_msg_ids = await fetch_saved_ids()

        @userbot.on(events.Raw(UpdateBotChatInviteRequester))
        async def _on_join(ev):
            await handle_join_request(ev)

        st.userbot_running = True
        st.stats["session_start"] = str(datetime.now())
        st.save()
        return True
    except Exception as e:
        logger.error(f"start_userbot: {e}")
        return False


async def stop_userbot():
    global userbot
    st.userbot_running = False
    st._saved_msg_ids  = []
    if userbot:
        try:
            if st.online_status:
                await userbot(UpdateStatusRequest(offline=True))
        except Exception:
            pass
        try:
            await userbot.disconnect()
        except Exception:
            pass
    userbot = None
    st.save()


async def fetch_saved_ids(count: int = None) -> list:
    c = count or st.forward_count
    if not await ensure_connected():
        return []
    try:
        res = await userbot(GetHistoryRequest(
            peer=InputPeerSelf(), offset_id=0, offset_date=None,
            add_offset=0, limit=c, max_id=0, min_id=0, hash=0,
        ))
        ids = [m.id for m in res.messages if hasattr(m, "id")]
        logger.info(f"Saved IDs: {ids}")
        return ids
    except Exception as e:
        logger.error(f"fetch_saved_ids: {e}")
        return []


# ═══════════════════════════════════════════════════
# DM SENDER
# ═══════════════════════════════════════════════════

async def send_one(uid: int, attempt: int = 0, stop_check=None) -> str:
    """
    Returns: ok | skip | dead | fail | rate_limited
    FloodWait is retried for the same user. PeerFlood is bounded so a
    campaign can save progress instead of getting stuck forever.
    """
    net_retry = 0
    flood_retries = 0
    peer_retries = 0
    should_stop = stop_check or (lambda: False)

    while True:
        if should_stop():
            return "fail"

        if not await ensure_connected():
            ok = await reconnect()
            if not ok:
                return "dead"

        try:
            if st.use_saved_forward and st._saved_msg_ids:
                await userbot(ForwardMessagesRequest(
                    from_peer=InputPeerSelf(),
                    id=st._saved_msg_ids,
                    to_peer=uid,
                    drop_author=not st.forward_with_tag,
                    silent=False,
                ))
            elif st.media_file_id and st.send_media_only:
                await userbot.send_file(
                    uid, st.media_file_id,
                    caption=st.media_caption or st.auto_message,
                )
            elif st.media_file_id and st.send_both:
                await userbot.send_message(uid, st.auto_message)
                await asyncio.sleep(0.2)
                await userbot.send_file(
                    uid, st.media_file_id,
                    caption=st.media_caption
                )
            else:
                await userbot.send_message(uid, st.auto_message)

            st.stats["sent"] += 1
            sent_store.add(uid)
            logger.info(f"✅ Sent → {uid}")
            return "ok"

        except errors.FloodWaitError as e:
            wait = e.seconds + st.flood_wait_extra
            st.stats["flood_waits"] += 1
            flood_retries += 1
            logger.warning(f"⚠️ FloodWait {e.seconds}s, waiting {wait}s (uid={uid})")
            if flood_retries > getattr(st, "max_flood_retries", 2):
                logger.warning(
                    f"FloodWait retry limit hit for {uid}; saving for resume"
                )
                return "rate_limited"
            try:
                await bot.send_message(
                    ADMIN_ID,
                    f"⏳ **FloodWait {e.seconds}s**\n"
                    f"Sleeping {wait}s then retrying same user.\n"
                    f"UID: `{uid}` | Sent: {camp.sent}"
                )
            except Exception:
                pass
            for _ in range(wait):
                if should_stop():
                    return "fail"
                await asyncio.sleep(1)
            continue

        except errors.PeerFloodError:
            st.stats["peer_floods"] += 1
            peer_retries += 1
            pause = st.peer_flood_pause
            logger.error(f"🚨 PeerFlood! Pausing {pause}s, retry same user {uid}")
            if peer_retries > getattr(st, "max_peer_retries", 1):
                logger.warning(
                    f"PeerFlood retry limit hit for {uid}; saving for resume"
                )
                return "rate_limited"
            try:
                await bot.send_message(
                    ADMIN_ID,
                    f"🚨 **PeerFloodError**\n\n"
                    f"Sent: **{camp.sent}** | Progress: {camp.idx}/{camp.total}\n"
                    f"UID: `{uid}`\n\n"
                    f"Pausing **{pause // 60}min** then retrying same user."
                )
            except Exception:
                pass
            for _ in range(pause):
                if should_stop():
                    return "fail"
                await asyncio.sleep(1)
            continue

        except (
            errors.UserPrivacyRestrictedError,
            errors.UserIsBlockedError,
            errors.InputUserDeactivatedError,
            errors.UserBannedInChannelError,
            errors.ChatWriteForbiddenError,
            errors.UserNotMutualContactError,
        ):
            if uid not in st.blacklisted_users:
                st.blacklisted_users.append(uid)
            st.stats["failed"] += 1
            return "skip"

        except (
            errors.AuthKeyUnregisteredError,
            errors.AuthKeyDuplicatedError,
            errors.SessionRevokedError,
        ) as e:
            logger.error(f"Session dead: {e}")
            try:
                await bot.send_message(
                    ADMIN_ID,
                    "🔑 **Session Expired!**\n\n"
                    "Please /logout and login again."
                )
            except Exception:
                pass
            return "dead"

        except (ConnectionError, OSError) as e:
            net_retry += 1
            logger.warning(f"Connection error {uid}: {e} (retry {net_retry})")
            for _ in range(min(10 * net_retry, 60)):
                if should_stop():
                    return "fail"
                await asyncio.sleep(1)
            if net_retry >= 8:
                st.stats["failed"] += 1
                return "fail"
            continue

        except Exception as e:
            s = str(e).lower()
            logger.error(f"send_one {uid}: {type(e).__name__}: {e}")
            # Unknown flood-like text errors: wait and retry same user.
            if "flood" in s or "too many" in s or "wait" in s:
                for _ in range(60):
                    if should_stop():
                        return "fail"
                    await asyncio.sleep(1)
                continue
            if attempt < 2:
                await asyncio.sleep(5)
                attempt += 1
                continue
            st.stats["failed"] += 1
            return "fail"


# ═══════════════════════════════════════════════════
# FETCH ALL JOIN REQUESTS — full pagination
# ═══════════════════════════════════════════════════

async def fetch_all_requests(
    chat_id: int,
    skip_set: set,
    prog_msg
) -> list:
    """
    Fetch ALL pending join requests.
    skip_set: only skip if user manually enabled skip_already_sent
    """
    all_uids = []

    for ci, ch_id in enumerate(st.monitored_channels, 1):
        ch_name = st.channel_names.get(str(ch_id), str(ch_id))

        if camp.should_stop():
            break

        if not await ensure_connected():
            await bot.send_message(chat_id, "❌ Userbot disconnected!")
            break

        try:
            entity = await userbot.get_entity(ch_id)
        except Exception as e:
            await bot.send_message(
                chat_id,
                f"⚠️ Cannot get `{ch_name}`: {e}"
            )
            continue

        page         = 0
        ch_count     = 0
        off_date     = 0
        off_user     = InputUserEmpty()
        err_streak   = 0
        stall_pages  = 0
        last_cursor  = None

        while True:
            if camp.should_stop():
                return all_uids

            page += 1
            try:
                try:
                    res = await userbot(GetChatInviteImportersRequest(
                        peer=entity, link=None, q="",
                        offset_date=off_date,
                        offset_user=off_user,
                        limit=100, requested=True,
                        subscription_expired=False,
                    ))
                except TypeError:
                    res = await userbot(GetChatInviteImportersRequest(
                        peer=entity, link=None, q="",
                        offset_date=off_date,
                        offset_user=off_user,
                        limit=100, requested=True,
                    ))

                err_streak = 0

                if not res.importers:
                    logger.info(
                        f"[{ch_name}] done — "
                        f"{ch_count} records, {len(all_uids)} new total"
                    )
                    break

                for imp in res.importers:
                    u = imp.user_id
                    ch_count += 1
                    # Only skip if in skip_set AND skip is enabled
                    if u in skip_set:
                        continue
                    if not st.is_allowed(u):
                        continue
                    if u not in all_uids:
                        all_uids.append(u)

                logger.info(
                    f"[{ch_name}] page {page}: "
                    f"{len(res.importers)} records | "
                    f"total collected: {len(all_uids)}"
                )

                # Update progress msg every 5 pages
                if page % 5 == 0:
                    try:
                        await prog_msg.edit(
                            f"⏳ **Collecting requests…**\n\n"
                            f"📡 **{ch_name}** "
                            f"({ci}/{len(st.monitored_channels)})\n"
                            f"📋 Records from this channel: **{ch_count}**\n"
                            f"👥 Total to message: **{len(all_uids)}**\n"
                            f"📄 Page: **{page}**\n\n"
                            f"_Please wait…_"
                        )
                    except Exception:
                        pass

                last = res.importers[-1]
                next_off_date = (
                    int(last.date.timestamp())
                    if hasattr(last.date, "timestamp")
                    else int(last.date)
                )
                next_user_id = last.user_id

                # Cursor anti-stall: if Telegram returns same tail item repeatedly,
                # stop this channel loop to avoid infinite paging.
                cursor = (next_off_date, next_user_id)
                if cursor == last_cursor:
                    stall_pages += 1
                else:
                    stall_pages = 0
                last_cursor = cursor

                if stall_pages >= 3:
                    logger.warning(
                        f"[{ch_name}] pagination stalled at page {page}, stopping safely"
                    )
                    break

                off_date = next_off_date

                # IMPORTANT: old users may not be resolvable via get_input_entity.
                # Fall back to InputUserEmpty instead of breaking pagination.
                try:
                    off_user = await userbot.get_input_entity(next_user_id)
                except Exception:
                    off_user = InputUserEmpty()

                # Keep fetching until Telegram returns empty page.
                # Do NOT stop just because page size < 100.

                await asyncio.sleep(0.2)

            except errors.FloodWaitError as e:
                logger.warning(f"Fetch FloodWait {e.seconds}s")
                await asyncio.sleep(e.seconds + 5)
                err_streak += 1

            except errors.ChatAdminRequiredError:
                await bot.send_message(
                    chat_id,
                    f"⚠️ No admin rights in **{ch_name}**!\n"
                    f"Give userbot 'Manage Members' permission."
                )
                break

            except Exception as e:
                err_streak += 1
                logger.error(f"Fetch {ch_name} page {page}: {e}")
                if err_streak >= 5:
                    await bot.send_message(
                        chat_id,
                        f"⚠️ Too many errors for `{ch_name}`, skipping.\n{e}"
                    )
                    break
                await asyncio.sleep(5 * err_streak)

    logger.info(f"Total users to message: {len(all_uids)}")
    return all_uids


# ═══════════════════════════════════════════════════
# CAMPAIGN RUNNER
# ═══════════════════════════════════════════════════

async def _run(c: Campaign, chat_id: int, mode: str):
    c.active  = True
    c.t_start = time.time()
    c.label   = {
        "fresh":  "🚀 Fresh Campaign",
        "resume": "⏩ Resume Campaign",
        "retry":  "🔁 Retry Failed",
    }.get(mode, "Campaign")

    done_uids:    list = []
    failed_uids:  list = []
    targets:      list = []

    try:
        # ── Pre-flight ──────────────────────────────────────────────
        if not await ensure_connected():
            await bot.send_message(chat_id, "❌ Userbot not connected!")
            return

        if not st.monitored_channels:
            await bot.send_message(chat_id, "❌ No channels monitored!")
            return

        if st.use_saved_forward:
            st._saved_msg_ids = await fetch_saved_ids()
            if not st._saved_msg_ids:
                await bot.send_message(
                    chat_id,
                    "❌ No Saved Messages found!\n"
                    "Add messages to your Saved Messages first."
                )
                return

        mode_str = (
            f"📨 Forward {st.forward_count} saved msg(s)"
            if st.use_saved_forward else "💬 Text / Media DM"
        )

        # ── Build target list ───────────────────────────────────────
        prog_msg = await bot.send_message(
            chat_id,
            f"🔍 **{c.label}**\n\n{mode_str}\n\nCollecting requests…"
        )

        # Build skip set
        # ONLY skip if user explicitly enabled skip_already_sent
        skip_set: set = set()
        if st.skip_already_sent:
            skip_set = set(sent_store._ids)
            logger.info(
                f"Skip mode ON: will skip {len(skip_set)} already-sent users"
            )
        else:
            logger.info(
                "Skip mode OFF: will message ALL pending users "
                "(including previously messaged)"
            )

        if mode == "fresh":
            targets = await fetch_all_requests(
                chat_id, skip_set, prog_msg
            )
        elif mode == "resume":
            prog    = st.load_progress()
            targets = prog.get("pending", [])
            if skip_set:
                targets = [u for u in targets if u not in skip_set]
            logger.info(f"Resume: {len(targets)} pending users")
        elif mode == "retry":
            prog    = st.load_progress()
            targets = prog.get("failed", [])
            logger.info(f"Retry: {len(targets)} failed users")

        if c.should_stop():
            await prog_msg.edit("🛑 Stopped during collection.")
            return

        c.total = len(targets)

        if c.total == 0:
            skipped_info = (
                f"\n\n⚠️ **{len(skip_set)} users were skipped** "
                f"because 'Skip Already Sent' is ON.\n"
                f"Turn it OFF to message everyone."
                if skip_set else ""
            )
            await prog_msg.edit(
                f"ℹ️ **No users to message!**\n\n"
                f"Possible reasons:\n"
                f"• No pending join requests\n"
                f"• All users already in skip list"
                f"{skipped_info}"
            )
            return

        await prog_msg.edit(
            f"📋 **{c.label} — Ready!**\n\n"
            f"Mode: {mode_str}\n\n"
            f"👥 Total to message: **{c.total}**\n"
            f"⏭️ Skipped (already sent): **{len(skip_set)}**\n\n"
            f"⚙️ Settings:\n"
            f"  ⏱️ Delay: {st.min_delay}–{st.max_delay}s\n"
            f"  📦 Batch: {st.batch_size} msgs\n"
            f"  ⏳ Batch break: {st.batch_delay}s\n"
            f"  🚨 PeerFlood pause: {st.peer_flood_pause}s "
            f"(auto-continues!)\n\n"
            f"▶️ Starting in 3s…\n"
            f"_/stop to abort_"
        )
        await c.sleep(3)

        if c.should_stop():
            await prog_msg.edit("🛑 Stopped.")
            return

        # ── Send loop ───────────────────────────────────────────────
        t_last_update = time.time()
        t_last_save   = time.time()
        batch_count   = 0

        for idx, uid in enumerate(targets):

            if c.should_stop():
                logger.info("Stop signal received")
                break

            # ── SEND ───────────────────────────────────────────────
            result = await send_one(uid, stop_check=c.should_stop)

            # ── Process result ─────────────────────────────────────
            if result == "ok":
                c.sent += 1
                done_uids.append(uid)

            elif result == "dead":
                # Session expired — must stop
                await prog_msg.edit(
                    f"⛔ **Session Expired — Stopped**\n\n"
                    f"✅ Sent: {c.sent} | ❌ Failed: {c.failed}\n\n"
                    f"Progress saved. Login again then Resume."
                )
                remaining = targets[idx:]
                st.save_progress(done_uids, remaining, failed_uids)
                sent_store.save()
                st.save()
                return

            elif result == "rate_limited":
                c.paused = True
                c.pause_reason = "Rate limited - saved for resume"
                remaining = targets[idx:]
                st.save_progress(done_uids, remaining, failed_uids)
                sent_store.save()
                st.save()
                await prog_msg.edit(
                    f"⏸️ **Rate limit hit — paused safely**\n\n"
                    f"✅ Sent: **{c.sent}** | ❌ Failed: **{c.failed}**\n"
                    f"📊 Progress: **{idx}/{c.total}**\n\n"
                    f"UID `{uid}` was not skipped. It is saved as the next "
                    f"pending user.\n\n"
                    f"Wait a while, then use **Resume**."
                )
                return

            elif result in ("skip", "fail"):
                c.failed += 1
                failed_uids.append(uid)

            # Track processed count (1-based) for accurate ETA/progress.
            c.idx = idx + 1

            # ── Progress update every 15s ──────────────────────────
            now = time.time()
            if (now - t_last_update) >= 15 or idx == c.total - 1:
                try:
                    await prog_msg.edit(c.progress_text())
                except Exception:
                    pass
                t_last_update = now

            # ── Save progress every 25 users or 45s ───────────────
            if (idx + 1) % 25 == 0 or (now - t_last_save) >= 45:
                remaining = targets[idx + 1:]
                st.save_progress(done_uids, remaining, failed_uids)
                sent_store.save()
                st.save()
                t_last_save = now

            # ── Per-message delay ──────────────────────────────────
            if idx < c.total - 1 and not c.should_stop():
                await c.sleep(st.get_delay())

            # ── Batch break ────────────────────────────────────────
            actual = c.sent + c.failed
            if (
                actual > 0
                and actual % st.batch_size == 0
                and idx < c.total - 1
                and not c.should_stop()
            ):
                batch_count += 1
                c.paused      = True
                c.pause_reason = (
                    f"Batch #{batch_count} done — "
                    f"resting {st.batch_delay}s"
                )
                logger.info(
                    f"Batch #{batch_count}: "
                    f"sent={c.sent} failed={c.failed}. "
                    f"Sleeping {st.batch_delay}s…"
                )
                try:
                    await prog_msg.edit(
                        f"😴 **Batch #{batch_count} — Resting**\n\n"
                        f"✅ Sent: **{c.sent}** | "
                        f"❌ Failed: **{c.failed}**\n"
                        f"📊 {idx + 1}/{c.total}\n\n"
                        f"⏳ Break: **{st.batch_delay}s** "
                        f"({st.batch_delay // 60}m "
                        f"{st.batch_delay % 60}s)\n\n"
                        f"_Continuing automatically…_\n"
                        f"_/stop to abort_"
                    )
                except Exception:
                    pass

                await c.sleep(st.batch_delay)
                c.paused = False

        # ── Final report ────────────────────────────────────────────
        stopped = c.should_stop()
        elapsed = time.time() - c.t_start
        h, r    = divmod(int(elapsed), 3600)
        m, s    = divmod(r, 60)
        t_str   = f"{h}h {m}m {s}s" if h else f"{m}m {s}s"
        total_p = c.sent + c.failed
        rate    = round(c.sent / total_p * 100, 1) if total_p else 0

        remaining = targets[c.idx:] if stopped else []
        st.save_progress(done_uids, remaining, failed_uids)
        sent_store.save()
        st.save()

        if not stopped:
            st.clear_progress()

        emoji  = "🛑" if stopped else "✅"
        status = "Stopped" if stopped else "Complete!"

        report = (
            f"{emoji} **Campaign {status}**\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"✅ Sent:    **{c.sent}**\n"
            f"❌ Failed:  **{c.failed}**\n"
            f"⏭️ Skipped: **{c.skipped}**\n"
            f"📊 Total:   **{c.total}**\n"
            f"📈 Rate:    **{rate}%**\n"
            f"⏱️ Time:    **{t_str}**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🌐 FloodWaits:  {st.stats.get('flood_waits', 0)}\n"
            f"🚨 PeerFloods:  {st.stats.get('peer_floods', 0)}\n"
            f"💾 Total ever:  {sent_store.count()}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━"
        )
        if stopped:
            report += "\n\n_Progress saved — use Resume to continue._"
        if failed_uids:
            report += (
                f"\n_Use **Retry Failed** "
                f"for {len(failed_uids)} failed users._"
            )
        try:
            await prog_msg.edit(report)
        except Exception:
            await bot.send_message(chat_id, report)

    except asyncio.CancelledError:
        logger.info("Campaign cancelled")
        if targets:
            st.save_progress(
                done_uids, targets[c.idx:], failed_uids
            )
        sent_store.save()
        st.save()
        try:
            await bot.send_message(
                chat_id,
                f"🛑 Campaign cancelled.\n"
                f"Sent: {c.sent} | Saved for Resume."
            )
        except Exception:
            pass

    except Exception as e:
        logger.error(
            f"Campaign crash: {type(e).__name__}: {e}",
            exc_info=True
        )
        if targets:
            st.save_progress(
                done_uids, targets[c.idx:], failed_uids
            )
        sent_store.save()
        st.save()
        try:
            await bot.send_message(
                chat_id,
                f"❌ Campaign crashed: {type(e).__name__}\n{e}\n\n"
                f"Sent: {c.sent} | Progress saved. Use Resume."
            )
        except Exception:
            pass

    finally:
        c.active  = False
        c.paused  = False
        logger.info(
            f"Campaign ended — sent={c.sent} "
            f"failed={c.failed} skipped={c.skipped}"
        )


# ═══════════════════════════════════════════════════
# LIVE JOIN REQUEST HANDLER
# ═══════════════════════════════════════════════════

async def handle_join_request(event):
    try:
        st.stats["total_requests"] += 1
        ch_id = (
            event.peer.channel_id
            if isinstance(event.peer, PeerChannel) else None
        )
        if st.monitored_channels and ch_id:
            if not any(
                abs(c) == ch_id or str(abs(c)).endswith(str(ch_id))
                for c in st.monitored_channels
            ):
                return

        uid = event.user_id
        if not st.is_allowed(uid):
            return

        if st.skip_already_sent and sent_store.has(uid):
            logger.info(f"⏭️ Skip live {uid} (already sent)")
            return

        if st.auto_approve:
            try:
                from telethon.tl.functions.messages import (
                    HideChatJoinRequestRequest,
                )
                await userbot(HideChatJoinRequestRequest(
                    peer=event.peer, user_id=uid, approved=True
                ))
                st.stats["approved"] += 1
            except Exception as e:
                logger.error(f"auto-approve: {e}")

        await asyncio.sleep(st.get_delay())
        if st.use_saved_forward and not st._saved_msg_ids:
            st._saved_msg_ids = await fetch_saved_ids()

        await send_one(uid)
    except Exception as e:
        logger.error(f"join_handler: {e}")


# ═══════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════

def uptime() -> str:
    start = datetime.fromisoformat(
        st.stats.get("session_start", str(datetime.now()))
    )
    d = datetime.now() - start
    h, r = divmod(int(d.total_seconds()), 3600)
    m, s = divmod(r, 60)
    return f"{h}h {m}m {s}s"


# ═══════════════════════════════════════════════════
# MENUS
# ═══════════════════════════════════════════════════

def main_menu(uid: int = ADMIN_ID) -> list:
    logged  = st.session_string is not None
    running = st.userbot_running
    btns    = []
    if not logged:
        btns.append([Button.inline("🔑 Login", b"login")])
    else:
        ico = "🟢" if running else "🔴"
        act = b"stop_ub" if running else b"start_ub"
        btns.append([Button.inline(
            f"{ico} {'Stop' if running else 'Start'} Userbot", act
        )])
        btns.append([
            Button.inline("💬 Message",  b"msg_settings"),
            Button.inline("📊 Stats",    b"stats"),
        ])
        btns.append([
            Button.inline("📡 Channels", b"channels"),
            Button.inline("⚙️ Settings", b"settings"),
        ])
        btns.append([
            Button.inline("🛡️ Anti-Ban", b"antiban"),
            Button.inline("👤 Filters",  b"filters"),
        ])
        btns.append([
            Button.inline("📨 Campaign", b"campaign"),
            Button.inline("📋 Logs",     b"logs"),
        ])
        btns.append([
            Button.inline("👥 Accounts", b"accounts"),
            Button.inline("🔓 Logout",   b"logout"),
        ])
        if is_admin(uid):
            btns.append([Button.inline("🔐 Access", b"access")])
    return btns


def status_text(uid: int = ADMIN_ID) -> str:
    lg  = "✅ Logged In" if st.session_string else "❌ Not logged"
    ub  = "🟢 Running"   if st.userbot_running  else "🔴 Stopped"
    cpn = "⚡ ACTIVE"    if camp.is_running()    else "💤 Idle"

    skip_status = (
        f"⏭️ Skip sent: ✅ ON ({sent_store.count()} users)"
        if st.skip_already_sent
        else f"⏭️ Skip sent: ❌ OFF (messages ALL users)"
    )

    mmode = (
        f"📨 Fwd {st.forward_count}" if st.use_saved_forward
        else f"📷{'+ Text' if st.send_both else ''}" if st.media_file_id
        else "📝 Text"
    )

    prog  = st.load_progress()
    pend  = len(prog.get("pending", []))
    fail  = len(prog.get("failed",  []))

    pl = f"\n⏸️ {camp.pause_reason}" if camp.paused else ""

    return (
        f"🤖 **Userbot Manager v6.0**"
        f"{'  👑' if uid == ADMIN_ID else ''}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 {lg}\n"
        f"📱 Phone: `{st.phone_number or 'N/A'}`\n"
        f"🔄 {ub} | 🚀 Campaign: {cpn}{pl}\n"
        f"📡 Channels: {len(st.monitored_channels)}\n"
        f"💬 Mode: {mmode}\n"
        f"{skip_status}\n"
        f"💾 Pending: {pend} | Failed: {fail}\n"
        f"⏱️ Uptime: {uptime()}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📤 Sent: **{st.stats['sent']}** | "
        f"❌ Failed: **{st.stats['failed']}**\n"
        f"🌐 Floods: {st.stats.get('flood_waits', 0)} | "
        f"PeerFlood: {st.stats.get('peer_floods', 0)}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━"
    )


def camp_btns() -> list:
    prog = st.load_progress()
    pend = len(prog.get("pending", []))
    fail = len(prog.get("failed",  []))
    si   = "✅ ON" if st.skip_already_sent else "❌ OFF"
    btns = []

    if camp.is_running():
        pct = camp.idx / camp.total * 100 if camp.total else 0
        btns.append([Button.inline(
            f"📊 {camp.sent}✅ {camp.failed}❌ "
            f"{camp.idx}/{camp.total} ({pct:.0f}%)",
            b"camp_live"
        )])
        btns.append([Button.inline("🛑 STOP Campaign", b"camp_stop")])
    else:
        btns.append([Button.inline("🚀 Fresh Campaign", b"camp_fresh")])
        if pend:
            btns.append([Button.inline(
                f"⏩ Resume ({pend} pending)", b"camp_resume"
            )])
        if fail:
            btns.append([Button.inline(
                f"🔁 Retry Failed ({fail})", b"camp_retry"
            )])

    btns.append([Button.inline(
        f"⏭️ Skip Already Sent [{si}]", b"tog_skip"
    )])
    btns.append([Button.inline(
        f"📋 Sent Log ({sent_store.count()})", b"view_sent"
    )])
    btns.append([Button.inline("🗑️ Clear Sent History", b"clear_sent")])
    btns.append([Button.inline("🔙 Back", b"back")])
    return btns


def antiban_btns() -> list:
    ri = "✅" if st.random_delay else "❌"
    ai = "✅" if st.auto_approve else "❌"
    oi = "✅" if st.online_status else "❌"
    return [
        [Button.inline(f"🎲 Random Delay [{ri}]", b"tog_rand"),
         Button.inline("ℹ️ Info", b"ab_info")],
        [Button.inline("⏱️ Min Delay",    b"s_min"),
         Button.inline("⏱️ Max Delay",    b"s_max")],
        [Button.inline("📦 Batch Size",   b"s_batch"),
         Button.inline("⏳ Batch Break",  b"s_break")],
        [Button.inline("🚨 PeerFlood Pause", b"s_pfp"),
         Button.inline(f"✅ Auto-Approve [{ai}]", b"tog_approve")],
        [Button.inline(f"🟢 Online [{oi}]", b"tog_online")],
        [Button.inline("🔙 Back", b"back")],
    ]


def msg_btns() -> list:
    mi = "✅" if st.media_file_id else "❌"
    bi = "✅" if st.send_both else "❌"
    fi = "✅" if st.use_saved_forward else "❌"
    return [
        [Button.inline("✏️ Edit Msg",      b"edit_msg"),
         Button.inline("👁️ View Msg",      b"view_msg")],
        [Button.inline(f"📷 Media [{mi}]", b"set_media"),
         Button.inline("🗑️ Clear Media",   b"clr_media")],
        [Button.inline(f"📝+📷 Both [{bi}]", b"tog_both"),
         Button.inline("📋 Mode",           b"media_mode")],
        [Button.inline(f"📨 Saved Fwd [{fi}]", b"saved_fwd")],
        [Button.inline("🔙 Back", b"back")],
    ]


def sfwd_btns() -> list:
    ti = "✅" if st.forward_with_tag else "❌"
    return [
        [Button.inline(
            f"{'✅ Disable' if st.use_saved_forward else '❌ Enable'} Mode",
            b"tog_sfwd"
        )],
        [Button.inline(f"🏷️ Tag [{ti}]",                b"tog_ftag"),
         Button.inline(f"🔢 Count: {st.forward_count}", b"s_fcount")],
        [Button.inline("🔄 Refresh IDs", b"refresh_saved"),
         Button.inline("👁️ Preview",     b"prev_saved")],
        [Button.inline("🔙 Back", b"msg_settings")],
    ]


def ch_btns() -> list:
    return [
        [Button.inline("➕ Add",    b"add_ch"),
         Button.inline("➖ Remove", b"rm_ch")],
        [Button.inline("📡 List",  b"list_ch"),
         Button.inline("ℹ️ Info",  b"ch_info")],
        [Button.inline("🔙 Back", b"back")],
    ]


def filter_btns() -> list:
    return [
        [Button.inline("🚫 BL Add",  b"bl_add"),
         Button.inline("✅ WL Add",  b"wl_add")],
        [Button.inline("👁️ View BL", b"view_bl"),
         Button.inline("👁️ View WL", b"view_wl")],
        [Button.inline("🗑️ Clear BL", b"clr_bl"),
         Button.inline("🗑️ Clear WL", b"clr_wl")],
        [Button.inline("🔙 Back", b"back")],
    ]


# ═══════════════════════════════════════════════════
# BOT COMMANDS
# ═══════════════════════════════════════════════════

@bot.on(events.NewMessage(pattern="/start"))
async def c_start(ev):
    if not is_auth(ev.sender_id):
        return await ev.reply("⛔ Not authorized.")
    await ev.reply(
        status_text(ev.sender_id), buttons=main_menu(ev.sender_id)
    )


@bot.on(events.NewMessage(pattern="/panel"))
async def c_panel(ev):
    if not is_auth(ev.sender_id):
        return
    await ev.reply(
        status_text(ev.sender_id), buttons=main_menu(ev.sender_id)
    )


@bot.on(events.NewMessage(pattern="/stop"))
async def c_stop(ev):
    if not is_auth(ev.sender_id):
        return
    if camp.is_running():
        camp.request_stop()
        await ev.reply(
            "🛑 **Stop signal sent!**\n\n"
            "Stopping after current message.\n"
            "Progress saved — use Resume to continue."
        )
    else:
        camp.active = False
        camp.paused = False
        await ev.reply(
            "ℹ️ No campaign running.\n(State forcefully reset)"
        )


@bot.on(events.NewMessage(pattern="/status"))
async def c_status(ev):
    if not is_auth(ev.sender_id):
        return
    prog = st.load_progress()
    if camp.is_running():
        pct = camp.idx / camp.total * 100 if camp.total else 0
        txt = (
            f"🚀 **Campaign ACTIVE**\n\n"
            f"✅ Sent:    {camp.sent}\n"
            f"❌ Failed:  {camp.failed}\n"
            f"⏭️ Skipped: {camp.skipped}\n"
            f"📊 {camp.idx}/{camp.total} ({pct:.1f}%)\n"
            f"⚡ {camp.speed()}/hr | ⏱️ {camp.eta()}\n"
            f"⏸️ Paused: {camp.paused}"
            + (f"\n   {camp.pause_reason}" if camp.paused else "")
            + "\n\n/stop to stop"
        )
    else:
        txt = (
            f"💤 **No campaign running**\n\n"
            f"Ever sent:  {sent_store.count()}\n"
            f"Pending:    {len(prog.get('pending', []))}\n"
            f"Failed:     {len(prog.get('failed', []))}\n\n"
            f"Skip sent:  {'✅ ON' if st.skip_already_sent else '❌ OFF'}"
        )
    await ev.reply(txt)


@bot.on(events.NewMessage(pattern="/reset"))
async def c_reset(ev):
    if not is_auth(ev.sender_id):
        return
    if camp._task and not camp._task.done():
        camp._task.cancel()
    camp.reset()
    await ev.reply(
        "🔄 **Campaign state reset!**\n\n"
        "Use /start to open panel."
    )


@bot.on(events.NewMessage(pattern="/logs"))
async def c_logs(ev):
    if not is_auth(ev.sender_id):
        return
    await send_logs(ev.chat_id)


@bot.on(events.NewMessage(pattern="/clearskip"))
async def c_clearskip(ev):
    """Clear sent store so all users get messaged again."""
    if not is_auth(ev.sender_id):
        return
    count = sent_store.count()
    sent_store.clear()
    await ev.reply(
        f"✅ **Sent history cleared!**\n\n"
        f"Removed {count} users from skip list.\n"
        f"All users will be messaged in next campaign."
    )


# ═══════════════════════════════════════════════════
# CALLBACKS
# ═══════════════════════════════════════════════════

@bot.on(events.CallbackQuery())
async def on_cb(ev):
    if not is_auth(ev.sender_id):
        return await ev.answer("⛔ Not authorized!", alert=True)
    d = ev.data.decode()

    # Navigation
    if d == "back":
        st.login_state = None
        await ev.answer()
        await ev.edit(
            status_text(ev.sender_id), buttons=main_menu(ev.sender_id)
        )

    # Login
    elif d == "login":
        if st.session_string:
            return await ev.answer("Already logged in!", alert=True)
        st.login_state = "phone"
        await ev.answer()
        await ev.respond(
            "📱 Send phone with country code:\n"
            "`+1234567890`\n\n/cancel"
        )

    # Userbot
    elif d == "start_ub":
        if not st.session_string:
            return await ev.answer("Login first!", alert=True)
        await ev.answer("Starting…")
        msg = await ev.respond("⏳ Starting…")
        ok  = await start_userbot()
        await msg.edit(
            "✅ Userbot started!" if ok
            else "❌ Failed — session expired?"
        )
        await ev.respond(
            status_text(ev.sender_id), buttons=main_menu(ev.sender_id)
        )

    elif d == "stop_ub":
        await ev.answer("Stopping…")
        await stop_userbot()
        await ev.edit(
            "🔴 Userbot stopped.",
            buttons=[
                [Button.inline("🟢 Start", b"start_ub"),
                 Button.inline("🏠 Menu",  b"back")]
            ]
        )

    # Campaign
    elif d == "campaign":
        await ev.answer()
        prog  = st.load_progress()
        pend  = len(prog.get("pending", []))
        fail  = len(prog.get("failed",  []))
        si    = "✅ ON" if st.skip_already_sent else "❌ OFF (messages ALL)"

        avg   = (st.min_delay + st.max_delay) / 2
        cycle = st.batch_size * avg + st.batch_delay
        mph   = round(st.batch_size / cycle * 3600) if cycle > 0 else 0

        cpn_line = ""
        if camp.is_running():
            pct = camp.idx / camp.total * 100 if camp.total else 0
            cpn_line = (
                f"\n\n⚡ **ACTIVE**: "
                f"{camp.sent}✅ {camp.failed}❌ "
                f"{camp.idx}/{camp.total} ({pct:.0f}%)"
            )
            if camp.paused:
                cpn_line += f"\n⏸️ {camp.pause_reason}"

        # BIG WARNING if skip is on
        skip_warn = ""
        if st.skip_already_sent and sent_store.count() > 0:
            skip_warn = (
                f"\n\n⚠️ **SKIP IS ON** — "
                f"{sent_store.count()} users will be skipped!\n"
                f"Turn OFF to message ALL users."
            )

        await ev.edit(
            f"📨 **DM Campaign**\n\n"
            f"Mode: **{'📨 Saved Fwd' if st.use_saved_forward else '💬 Text/Media'}**\n"
            f"⏱️ {st.min_delay}–{st.max_delay}s | "
            f"Batch: {st.batch_size} | Break: {st.batch_delay}s\n"
            f"🚨 PeerFlood: pause {st.peer_flood_pause}s → auto-continues\n"
            f"📈 ~{mph} msgs/hr\n\n"
            f"⏭️ Skip sent: **{si}**\n"
            f"💾 Pending: **{pend}** | Failed: **{fail}**"
            f"{skip_warn}{cpn_line}",
            buttons=camp_btns()
        )

    elif d == "camp_live":
        await ev.answer()
        try:
            await ev.edit(
                camp.progress_text(),
                buttons=[
                    [Button.inline("🛑 STOP", b"camp_stop"),
                     Button.inline("🔙 Back", b"campaign")]
                ]
            )
        except Exception:
            pass

    elif d == "camp_fresh":
        if camp.is_running():
            return await ev.answer("Already running!", alert=True)
        st.clear_progress()
        await ev.answer()

        # Warn if skip is on
        warn = ""
        if st.skip_already_sent and sent_store.count() > 0:
            warn = (
                f"\n\n⚠️ Will skip **{sent_store.count()}** "
                f"already-sent users!\nUse /clearskip to reset."
            )

        await ev.edit(
            f"🚀 **Fresh Campaign starting…**\n\n"
            f"Will message ALL pending join requests.{warn}\n\n"
            f"_/stop to abort_",
            buttons=[[Button.inline("🛑 Stop", b"camp_stop")]]
        )
        camp.launch(ev.chat_id, "fresh")

    elif d == "camp_resume":
        if camp.is_running():
            return await ev.answer("Already running!", alert=True)
        await ev.answer()
        await ev.edit(
            "⏩ **Resuming…**\n_/stop to abort_",
            buttons=[[Button.inline("🛑 Stop", b"camp_stop")]]
        )
        camp.launch(ev.chat_id, "resume")

    elif d == "camp_retry":
        if camp.is_running():
            return await ev.answer("Already running!", alert=True)
        await ev.answer()
        await ev.edit(
            "🔁 **Retrying failed…**\n_/stop to abort_",
            buttons=[[Button.inline("🛑 Stop", b"camp_stop")]]
        )
        camp.launch(ev.chat_id, "retry")

    elif d == "camp_stop":
        if not camp.is_running():
            camp.active = False
            camp.paused = False
            await ev.answer("Not running.", alert=True)
            await ev.edit(
                status_text(ev.sender_id),
                buttons=main_menu(ev.sender_id)
            )
            return
        camp.request_stop()
        await ev.answer("🛑 Stop sent!")
        await ev.edit(
            "🛑 **Stopping…**\n\n"
            "Will stop after current message.\n"
            "Progress saved.",
            buttons=[
                [Button.inline("📊 Status", b"campaign"),
                 Button.inline("🏠 Menu",   b"back")]
            ]
        )

    elif d == "tog_skip":
        st.skip_already_sent = not st.skip_already_sent
        st.save()
        icon = "✅ ON" if st.skip_already_sent else "❌ OFF"
        msg  = (
            f"Skip sent: {icon}\n\n"
            + (
                f"⚠️ {sent_store.count()} users will be skipped!"
                if st.skip_already_sent
                else "All pending users will be messaged."
            )
        )
        await ev.answer(msg, alert=True)
        await ev.edit("📨 **Campaign**", buttons=camp_btns())

    elif d == "view_sent":
        await ev.answer()
        n  = sent_store.count()
        sp = "\n".join(f"• `{u}`" for u in sent_store.get_all()[:20])
        more = f"\n_…+{n-20} more_" if n > 20 else ""
        await ev.respond(
            f"📋 **Sent Users Log**\n\n"
            f"Total: **{n}**\n\n{sp}{more}\n\n"
            f"_These are skipped when 'Skip Sent' is ON_",
            buttons=[[Button.inline("🔙 Back", b"campaign")]]
        )

    elif d == "clear_sent":
        await ev.answer()
        await ev.edit(
            f"🗑️ Clear **{sent_store.count()}** sent users?\n\n"
            f"After clearing, ALL users will be messaged next campaign.",
            buttons=[
                [Button.inline("✅ Yes, Clear All", b"do_clr_sent"),
                 Button.inline("🔙 Cancel",          b"campaign")]
            ]
        )

    elif d == "do_clr_sent":
        n = sent_store.count()
        sent_store.clear()
        await ev.answer(f"✅ Cleared {n} users!")
        await ev.edit("📨 **Campaign**", buttons=camp_btns())

    # Message settings
    elif d == "msg_settings":
        await ev.answer()
        await ev.edit("💬 **Message Settings**", buttons=msg_btns())

    elif d == "edit_msg":
        st.login_state = "message"
        await ev.answer()
        await ev.respond(
            f"✏️ Current:\n───\n{st.auto_message}\n───\n\n"
            f"Send new message:\n\n/cancel"
        )

    elif d == "view_msg":
        await ev.answer()
        extra = ""
        if st.media_file_id:
            extra += f"\n📷 Media: {st.media_type}"
        if st.use_saved_forward:
            extra += f"\n📨 Forward {st.forward_count} saved msgs"
        await ev.respond(
            f"📋 **Current Message:**\n───\n{st.auto_message}\n───{extra}"
        )

    elif d == "set_media":
        st.login_state = "media"
        await ev.answer()
        await ev.respond("📷 Send photo/video/GIF/doc:\n\n/cancel")

    elif d == "clr_media":
        st.media_file_id = st.media_type = st.media_caption = None
        st.send_media_only = st.send_both = False
        st.save()
        await ev.answer("✅ Cleared!")
        await ev.edit("💬 **Message Settings**", buttons=msg_btns())

    elif d == "tog_both":
        st.send_both = not st.send_both
        if st.send_both:
            st.send_media_only = False
        st.save()
        await ev.answer(f"Both: {'✅' if st.send_both else '❌'}")
        await ev.edit("💬 **Message Settings**", buttons=msg_btns())

    elif d == "media_mode":
        await ev.answer()
        to = "✅" if not st.send_media_only and not st.send_both else "❌"
        mo = "✅" if st.send_media_only else "❌"
        bo = "✅" if st.send_both else "❌"
        await ev.edit("📋 **Media Mode**", buttons=[
            [Button.inline(f"📝 Text [{to}]",    b"m_text")],
            [Button.inline(f"📷 Media [{mo}]",   b"m_media")],
            [Button.inline(f"📝+📷 Both [{bo}]", b"m_both")],
            [Button.inline("🔙 Back", b"msg_settings")],
        ])

    elif d == "m_text":
        st.send_media_only = st.send_both = False; st.save()
        await ev.answer("✅")
        await ev.edit("💬", buttons=msg_btns())

    elif d == "m_media":
        st.send_media_only = True; st.send_both = False; st.save()
        await ev.answer("✅")
        await ev.edit("💬", buttons=msg_btns())

    elif d == "m_both":
        st.send_both = True; st.send_media_only = False; st.save()
        await ev.answer("✅")
        await ev.edit("💬", buttons=msg_btns())

    elif d == "saved_fwd":
        await ev.answer()
        ids = ", ".join(str(i) for i in st._saved_msg_ids) or "Not fetched"
        await ev.edit(
            f"📨 **Saved Forward**\n\n"
            f"Count: {st.forward_count} | "
            f"Tag: {'On' if st.forward_with_tag else 'Off'}\n"
            f"IDs: `{ids}`",
            buttons=sfwd_btns()
        )

    elif d == "tog_sfwd":
        st.use_saved_forward = not st.use_saved_forward
        if st.use_saved_forward:
            st.send_media_only = st.send_both = False
        st.save()
        await ev.answer(f"{'✅ Enabled' if st.use_saved_forward else '❌ Disabled'}")
        await ev.edit("📨 **Saved Forward**", buttons=sfwd_btns())

    elif d == "tog_ftag":
        st.forward_with_tag = not st.forward_with_tag; st.save()
        await ev.answer(f"{'✅' if st.forward_with_tag else '❌'}")
        await ev.edit("📨 **Saved Forward**", buttons=sfwd_btns())

    elif d == "s_fcount":
        st.login_state = "fwd_count"
        await ev.answer()
        await ev.respond(
            f"🔢 Current: {st.forward_count}\nSend 1–10:\n\n/cancel"
        )

    elif d == "refresh_saved":
        await ev.answer("Fetching…")
        if not await ensure_connected():
            await ev.respond("❌ Userbot not connected!")
        else:
            st._saved_msg_ids = await fetch_saved_ids()
            ids = ", ".join(str(i) for i in st._saved_msg_ids) or "None"
            await ev.respond(f"✅ IDs: `{ids}`")
        await ev.edit("📨 **Saved Forward**", buttons=sfwd_btns())

    elif d == "prev_saved":
        await ev.answer()
        if not await ensure_connected():
            return await ev.respond("❌ Not connected!")
        try:
            res = await userbot(GetHistoryRequest(
                peer=InputPeerSelf(), offset_id=0,
                offset_date=None, add_offset=0,
                limit=st.forward_count, max_id=0, min_id=0, hash=0,
            ))
            txt = f"👁️ Last {st.forward_count} saved:\n\n"
            for i, m in enumerate(res.messages, 1):
                t = getattr(m, "message", "") or "[media]"
                txt += f"#{i} (ID:{m.id})\n{t[:200]}\n\n"
            await ev.respond(txt[:4000])
        except Exception as e:
            await ev.respond(f"❌ {e}")

    # Channels
    elif d == "channels":
        await ev.answer()
        await ev.edit(
            f"📡 **Channels** — {len(st.monitored_channels)} monitored",
            buttons=ch_btns()
        )

    elif d == "add_ch":
        st.login_state = "add_ch"
        await ev.answer()
        await ev.respond("➕ Send channel ID or @username:\n\n/cancel")

    elif d == "rm_ch":
        if not st.monitored_channels:
            return await ev.answer("No channels!", alert=True)
        btns = [
            [Button.inline(
                f"❌ {st.channel_names.get(str(c), str(c))}",
                f"do_rm_{c}".encode()
            )]
            for c in st.monitored_channels
        ]
        btns.append([Button.inline("🔙 Back", b"channels")])
        await ev.answer()
        await ev.edit("➖ Remove channel:", buttons=btns)

    elif d.startswith("do_rm_"):
        raw = d[6:]
        try:
            cid = int(raw)
        except ValueError:
            cid = raw
        if cid in st.monitored_channels:
            st.monitored_channels.remove(cid)
            st.channel_names.pop(str(cid), None)
            st.save()
            await ev.answer("✅ Removed!")
        await ev.edit(
            f"📡 **Channels** — {len(st.monitored_channels)}",
            buttons=ch_btns()
        )

    elif d == "list_ch":
        await ev.answer()
        txt = (
            "📡 No channels added."
            if not st.monitored_channels
            else "📡 **Channels:**\n\n" + "\n".join(
                f"{i}. **{st.channel_names.get(str(c),'Unknown')}** `{c}`"
                for i, c in enumerate(st.monitored_channels, 1)
            )
        )
        await ev.respond(
            txt, buttons=[[Button.inline("🔙 Back", b"channels")]]
        )

    elif d == "ch_info":
        if not st.monitored_channels or not await ensure_connected():
            return await ev.answer(
                "No channels or not connected!", alert=True
            )
        await ev.answer("Fetching…")
        txt = "📊 **Channel Info:**\n\n"
        for cid in st.monitored_channels:
            try:
                full = await userbot(GetFullChannelRequest(channel=cid))
                ch   = full.chats[0]
                txt += (
                    f"📡 **{ch.title}**\n"
                    f"├ ID: `{cid}`\n"
                    f"├ Members: {full.full_chat.participants_count:,}\n"
                    f"└ Pending: "
                    f"{getattr(full.full_chat,'requests_pending','N/A')}\n\n"
                )
            except Exception as e:
                txt += f"❌ `{cid}`: {e}\n\n"
        await ev.respond(
            txt, buttons=[[Button.inline("🔙 Back", b"channels")]]
        )

    # Anti-ban
    elif d == "antiban":
        await ev.answer()
        avg   = (st.min_delay + st.max_delay) / 2
        cycle = st.batch_size * avg + st.batch_delay
        mph   = round(st.batch_size / cycle * 3600) if cycle > 0 else 0
        await ev.edit(
            f"🛡️ **Anti-Ban Settings**\n\n"
            f"⏱️ Delay: {st.min_delay}–{st.max_delay}s\n"
            f"📦 Batch: {st.batch_size} | Break: {st.batch_delay}s\n"
            f"🚨 PeerFlood pause: {st.peer_flood_pause}s "
            f"(auto-continues after!)\n"
            f"📈 ~{mph} msgs/hr",
            buttons=antiban_btns()
        )

    elif d == "ab_info":
        await ev.answer()
        avg   = (st.min_delay + st.max_delay) / 2
        cycle = st.batch_size * avg + st.batch_delay
        mph   = round(st.batch_size / cycle * 3600) if cycle > 0 else 0
        await ev.respond(
            f"📊 **Anti-Ban Info**\n\n"
            f"Min Delay:     **{st.min_delay}s**\n"
            f"Max Delay:     **{st.max_delay}s**\n"
            f"Batch Size:    **{st.batch_size}**\n"
            f"Batch Break:   **{st.batch_delay}s** "
            f"({st.batch_delay//60}m)\n"
            f"PeerFlood Pause: **{st.peer_flood_pause}s**\n\n"
            f"📈 ~**{mph} msgs/hr**\n\n"
            f"**Why stops at ~200?**\n"
            f"Telegram PeerFloodError after bulk DMs.\n"
            f"Bot now pauses {st.peer_flood_pause//60}min then CONTINUES.\n\n"
            f"💡 Tips:\n"
            f"• Account must be >3 months old\n"
            f"• Keep delay ≥10s\n"
            f"• Batch break ≥120s"
        )

    elif d == "tog_rand":
        st.random_delay = not st.random_delay; st.save()
        await ev.answer(f"{'✅' if st.random_delay else '❌'}")
        await ev.edit("🛡️", buttons=antiban_btns())

    elif d == "tog_approve":
        st.auto_approve = not st.auto_approve; st.save()
        await ev.answer(f"{'✅' if st.auto_approve else '❌'}")
        await ev.edit("🛡️", buttons=antiban_btns())

    elif d == "tog_online":
        st.online_status = not st.online_status; st.save()
        if userbot and userbot.is_connected():
            try:
                await userbot(UpdateStatusRequest(offline=not st.online_status))
            except Exception:
                pass
        await ev.answer(f"{'✅' if st.online_status else '❌'}")
        await ev.edit("🛡️", buttons=antiban_btns())

    elif d in ("s_min","s_max","s_batch","s_break","s_pfp"):
        mp = {
            "s_min":   ("min_delay",       1,    3600, "Min Delay (s)"),
            "s_max":   ("max_delay",       1,    3600, "Max Delay (s)"),
            "s_batch": ("batch_size",      1,    500,  "Batch Size"),
            "s_break": ("batch_delay",     0,    7200, "Batch Break (s)"),
            "s_pfp":   ("peer_flood_pause",60,   7200, "PeerFlood Pause (s)"),
        }
        attr, mn, mx, label = mp[d]
        st.login_state = f"num_{attr}_{mn}_{mx}"
        await ev.answer()
        await ev.respond(
            f"⚙️ **{label}**\n\n"
            f"Current: **{getattr(st, attr)}**\n\n"
            f"Send value ({mn}–{mx}):\n\n/cancel"
        )

    # Filters
    elif d == "filters":
        await ev.answer()
        await ev.edit(
            f"👤 **Filters**\n"
            f"🚫 BL: {len(st.blacklisted_users)} | "
            f"✅ WL: {len(st.whitelisted_users)}",
            buttons=filter_btns()
        )

    elif d == "bl_add":
        st.login_state = "bl"
        await ev.answer()
        await ev.respond("🚫 Send user ID to blacklist:\n\n/cancel")

    elif d == "wl_add":
        st.login_state = "wl"
        await ev.answer()
        await ev.respond("✅ Send user ID to whitelist:\n\n/cancel")

    elif d == "view_bl":
        await ev.answer()
        txt = (
            f"🚫 **BL ({len(st.blacklisted_users)}):**\n"
            + "\n".join(f"• `{u}`" for u in st.blacklisted_users[:50])
            if st.blacklisted_users else "🚫 Empty"
        )
        await ev.respond(txt, buttons=[[Button.inline("🔙", b"filters")]])

    elif d == "view_wl":
        await ev.answer()
        txt = (
            f"✅ **WL ({len(st.whitelisted_users)}):**\n"
            + "\n".join(f"• `{u}`" for u in st.whitelisted_users[:50])
            if st.whitelisted_users else "✅ Empty (all allowed)"
        )
        await ev.respond(txt, buttons=[[Button.inline("🔙", b"filters")]])

    elif d == "clr_bl":
        st.blacklisted_users = []; st.save()
        await ev.answer("✅ Cleared!")
        await ev.edit("👤 **Filters**", buttons=filter_btns())

    elif d == "clr_wl":
        st.whitelisted_users = []; st.save()
        await ev.answer("✅ Cleared!")
        await ev.edit("👤 **Filters**", buttons=filter_btns())

    # Access
    elif d == "access":
        if not is_admin(ev.sender_id):
            return await ev.answer("⛔", alert=True)
        await ev.answer()
        users = (
            "\n".join(f"• `{u}`" for u in st.authorized_users)
            or "_None_"
        )
        await ev.edit(
            f"🔐 **Access**\n\nAdmin: `{ADMIN_ID}`\n\n{users}",
            buttons=[
                [Button.inline("➕ Add",    b"acc_add"),
                 Button.inline("➖ Remove", b"acc_rm")],
                [Button.inline("🔙 Back", b"back")],
            ]
        )

    elif d == "acc_add":
        if not is_admin(ev.sender_id):
            return await ev.answer("⛔", alert=True)
        st.login_state = "acc_add"
        await ev.answer()
        await ev.respond("➕ Send Telegram user ID:\n\n/cancel")

    elif d == "acc_rm":
        if not is_admin(ev.sender_id):
            return await ev.answer("⛔", alert=True)
        if not st.authorized_users:
            return await ev.answer("None!", alert=True)
        btns = [
            [Button.inline(f"❌ {u}", f"acc_del_{u}".encode())]
            for u in st.authorized_users
        ]
        btns.append([Button.inline("🔙", b"access")])
        await ev.answer()
        await ev.edit("➖ Remove:", buttons=btns)

    elif d.startswith("acc_del_"):
        if not is_admin(ev.sender_id):
            return await ev.answer("⛔", alert=True)
        try:
            uid = int(d[8:])
            if uid in st.authorized_users:
                st.authorized_users.remove(uid)
                st.save()
            await ev.answer(f"✅ Removed {uid}")
        except Exception as e:
            await ev.answer(str(e), alert=True)
        await ev.edit("🔐 **Access**", buttons=[
            [Button.inline("➕ Add",    b"acc_add"),
             Button.inline("➖ Remove", b"acc_rm")],
            [Button.inline("🔙", b"back")],
        ])

    # Settings
    elif d == "settings":
        await ev.answer()
        await ev.edit("⚙️ **Settings**", buttons=[
            [Button.inline("🔄 Restart UB",     b"restart_ub"),
             Button.inline("💾 Backup",          b"backup")],
            [Button.inline("📋 Logs",            b"logs"),
             Button.inline("🗑️ Reset Stats",     b"reset_stats")],
            [Button.inline("🗑️ Clear Progress",  b"clr_prog"),
             Button.inline("👥 Accounts",        b"accounts")],
            [Button.inline("🔙 Back", b"back")],
        ])

    elif d == "restart_ub":
        await ev.answer("Restarting…")
        await stop_userbot()
        await asyncio.sleep(2)
        ok = await start_userbot()
        await ev.respond(
            "✅ Restarted!" if ok else "❌ Failed!",
            buttons=main_menu(ev.sender_id)
        )

    elif d == "clr_prog":
        st.clear_progress()
        await ev.answer("✅ Progress cleared!")
        await ev.edit("⚙️ **Settings**", buttons=[
            [Button.inline("🔄 Restart UB",     b"restart_ub"),
             Button.inline("💾 Backup",          b"backup")],
            [Button.inline("📋 Logs",            b"logs"),
             Button.inline("🗑️ Reset Stats",     b"reset_stats")],
            [Button.inline("🗑️ Clear Progress",  b"clr_prog"),
             Button.inline("👥 Accounts",        b"accounts")],
            [Button.inline("🔙 Back", b"back")],
        ])

    elif d == "reset_stats":
        st.reset_stats()
        await ev.answer("✅ Reset!")
        await ev.edit(
            status_text(ev.sender_id), buttons=main_menu(ev.sender_id)
        )

    elif d == "stats":
        await ev.answer()
        tot  = st.stats["sent"] + st.stats["failed"]
        rate = round(st.stats["sent"] / tot * 100, 1) if tot else 0
        prog = st.load_progress()
        await ev.edit(
            f"📊 **Statistics**\n\n"
            f"📤 Sent:      **{st.stats['sent']}**\n"
            f"❌ Failed:    **{st.stats['failed']}**\n"
            f"📈 Rate:      **{rate}%**\n"
            f"📩 Requests:  **{st.stats['total_requests']}**\n"
            f"✅ Approved:  **{st.stats['approved']}**\n\n"
            f"🌐 FloodWaits:  **{st.stats.get('flood_waits',0)}**\n"
            f"🚨 PeerFloods:  **{st.stats.get('peer_floods',0)}**\n\n"
            f"💾 Sent store:  **{sent_store.count()}** users\n"
            f"⏳ Pending:     **{len(prog.get('pending',[]))}**\n"
            f"⏱️ Uptime:      {uptime()}",
            buttons=[
                [Button.inline("🔄 Reset", b"reset_stats"),
                 Button.inline("🔙 Back",  b"back")]
            ]
        )

    elif d == "logs":
        await ev.answer()
        await send_logs(ev.chat_id)

    elif d == "backup":
        await ev.answer("Creating…")
        try:
            import shutil
            name = f"backup_{datetime.now():%Y%m%d_%H%M%S}.json"
            shutil.copy(CONFIG_FILE, name)
            await bot.send_file(
                ev.chat_id, name,
                caption=f"💾 Backup {datetime.now():%Y-%m-%d %H:%M}"
            )
            os.remove(name)
        except Exception as e:
            await ev.respond(f"❌ {e}")

    elif d == "accounts":
        await ev.answer()
        accts = "\n".join(
            f"{'🟢' if p == st.active_account else '⚪'} `{p}`"
            for p in st.accounts
        ) or "_None_"
        await ev.edit(
            f"👥 **Accounts ({len(st.accounts)}):**\n{accts}\n\n"
            f"Active: `{st.phone_number or 'None'}`",
            buttons=[
                [Button.inline("➕ Add Account", b"login")],
                [Button.inline("🔙 Back",        b"back")],
            ]
        )

    elif d == "logout":
        await ev.answer()
        await ev.edit(
            "🔓 **Logout?** Will stop userbot & clear session.",
            buttons=[
                [Button.inline("✅ Yes", b"do_logout"),
                 Button.inline("🔙 No",  b"back")]
            ]
        )

    elif d == "do_logout":
        await stop_userbot()
        if st.phone_number and st.session_string:
            st.accounts[st.phone_number] = st.session_string
        st.session_string = st.phone_number = st.active_account = None
        st.save()
        await ev.answer("✅ Logged out!")
        await ev.edit(
            status_text(ev.sender_id), buttons=main_menu(ev.sender_id)
        )


# ═══════════════════════════════════════════════════
# TEXT HANDLER
# ═══════════════════════════════════════════════════

@bot.on(events.NewMessage())
async def text_handler(ev):
    if not is_auth(ev.sender_id):
        return

    if st.login_state == "media" and ev.media:
        await handle_media(ev)
        return

    if not st.login_state:
        return

    text = (ev.text or "").strip()

    if text == "/cancel":
        st.login_state = st.phone_code_hash = st.pending_phone = None
        if st._temp_client:
            try:
                await st._temp_client.disconnect()
            except Exception:
                pass
            st._temp_client = None
        await ev.reply("❌ Cancelled.", buttons=main_menu(ev.sender_id))
        return

    if not text:
        return

    ls = st.login_state

    if ls == "phone":
        phone = text if text.startswith("+") else f"+{text}"
        await ev.reply(f"📱 Sending OTP to `{phone}`…")
        try:
            tmp = TelegramClient(StringSession(), API_ID, API_HASH)
            await tmp.connect()
            res = await tmp.send_code_request(phone)
            st.phone_code_hash = res.phone_code_hash
            st.pending_phone   = phone
            st._temp_client    = tmp
            st.login_state     = "otp"
            await ev.reply(
                "✅ OTP sent!\n\n"
                "Enter code with spaces:\n`1 2 3 4 5`\n\n/cancel"
            )
        except errors.PhoneNumberInvalidError:
            st.login_state = None
            await ev.reply("❌ Invalid phone number.")
        except errors.FloodWaitError as e:
            st.login_state = None
            await ev.reply(f"⚠️ Please wait {e.seconds}s.")
        except Exception as e:
            st.login_state = None
            await ev.reply(f"❌ Error: {e}")

    elif ls == "otp":
        code = text.replace(" ", "").replace("-", "")
        try:
            await st._temp_client.sign_in(
                phone=st.pending_phone,
                code=code,
                phone_code_hash=st.phone_code_hash
            )
            await finalize_login(ev, st._temp_client)
        except errors.SessionPasswordNeededError:
            st.login_state = "2fa"
            await ev.reply("🔐 2FA required. Send password:\n\n/cancel")
        except errors.PhoneCodeInvalidError:
            await ev.reply("❌ Wrong code. Try again.")
        except errors.PhoneCodeExpiredError:
            st.login_state = None
            await ev.reply("❌ Code expired. Start over.")
        except Exception as e:
            st.login_state = None
            await ev.reply(f"❌ {e}")

    elif ls == "2fa":
        try:
            await st._temp_client.sign_in(password=text)
            try:
                await ev.delete()
            except Exception:
                pass
            await finalize_login(ev, st._temp_client)
        except errors.PasswordHashInvalidError:
            await ev.reply("❌ Wrong password.")
        except Exception as e:
            st.login_state = None
            await ev.reply(f"❌ {e}")

    elif ls == "message":
        st.auto_message = ev.text
        st.login_state  = None
        st.save()
        await ev.reply(
            f"✅ Message updated!\n───\n{st.auto_message}\n───",
            buttons=main_menu(ev.sender_id)
        )

    elif ls == "add_ch":
        st.login_state = None
        inp = text
        try:
            cid  = int(inp)
            name = str(cid)
        except ValueError:
            if not await ensure_connected():
                await ev.reply(
                    "❌ Start userbot first.",
                    buttons=main_menu(ev.sender_id)
                )
                return
            try:
                ent  = await userbot.get_entity(inp)
                raw  = ent.id
                cid  = raw if str(raw).startswith("-100") else int(f"-100{raw}")
                name = ent.title
            except Exception as e:
                await ev.reply(f"❌ {e}", buttons=main_menu(ev.sender_id))
                return
        if cid in st.monitored_channels:
            await ev.reply("⚠️ Already added!", buttons=main_menu(ev.sender_id))
            return
        st.monitored_channels.append(cid)
        st.channel_names[str(cid)] = name
        st.save()
        await ev.reply(
            f"✅ Added **{name}** (`{cid}`)",
            buttons=main_menu(ev.sender_id)
        )

    elif ls == "acc_add":
        if not is_admin(ev.sender_id):
            st.login_state = None
            return
        try:
            uid = int(text)
        except ValueError:
            await ev.reply("❌ Send a number.")
            return
        if uid == ADMIN_ID:
            await ev.reply("ℹ️ That's admin!", buttons=main_menu(ev.sender_id))
        elif uid in st.authorized_users:
            await ev.reply("⚠️ Already!", buttons=main_menu(ev.sender_id))
        else:
            st.authorized_users.append(uid)
            st.save()
            await ev.reply(
                f"✅ `{uid}` authorized!", buttons=main_menu(ev.sender_id)
            )
        st.login_state = None

    elif ls in ("bl", "wl"):
        try:
            uid = int(text)
        except ValueError:
            await ev.reply("❌ Send numeric ID.")
            return
        lst   = st.blacklisted_users if ls == "bl" else st.whitelisted_users
        label = "Blacklisted" if ls == "bl" else "Whitelisted"
        if uid not in lst:
            lst.append(uid)
            st.save()
            await ev.reply(
                f"✅ `{uid}` {label}!", buttons=main_menu(ev.sender_id)
            )
        else:
            await ev.reply(
                f"Already {label.lower()}!", buttons=main_menu(ev.sender_id)
            )
        st.login_state = None

    elif ls == "fwd_count":
        try:
            v = int(text)
            if not (1 <= v <= 10):
                raise ValueError
            st.forward_count = v
            st.login_state   = None
            st.save()
            await ev.reply(
                f"✅ Count → {v}", buttons=main_menu(ev.sender_id)
            )
        except ValueError:
            await ev.reply("❌ Send 1–10.")

    elif ls and ls.startswith("num_"):
        # Robust parser for states like:
        # num_min_delay_1_3600
        # num_peer_flood_pause_60_7200
        raw = ls[4:]  # strip "num_"
        try:
            attr, mn_s, mx_s = raw.rsplit("_", 2)
            mn, mx = int(mn_s), int(mx_s)
            v = int(text)
            if not (mn <= v <= mx):
                raise ValueError(f"Must be {mn}–{mx}")
            setattr(st, attr, v)
            st.login_state = None
            st.save()
            await ev.reply(
                f"✅ **{attr.replace('_',' ').title()}** → {v}",
                buttons=main_menu(ev.sender_id)
            )
        except ValueError as e:
            await ev.reply(f"❌ {e}")
        except Exception:
            st.login_state = None
            await ev.reply(
                "❌ Invalid setting state. Please open settings again.",
                buttons=main_menu(ev.sender_id)
            )


# ═══════════════════════════════════════════════════
# MEDIA
# ═══════════════════════════════════════════════════

async def handle_media(ev):
    try:
        if ev.photo:          mtype = "photo"
        elif ev.video:        mtype = "video"
        elif ev.gif:          mtype = "gif"
        elif ev.audio:        mtype = "audio"
        elif ev.document:     mtype = "document"
        elif ev.sticker:      mtype = "sticker"
        else:
            await ev.reply("❌ Unsupported media!")
            return
        await ev.reply("⏳ Saving…")
        path = await ev.download_media(
            file=f"{MEDIA_DIR}/media_{int(time.time())}"
        )
        caption = ev.message.message or None
        st.media_file_id  = path
        st.media_type     = mtype
        st.media_caption  = caption
        st.login_state    = None
        st.save()
        icons = {
            "photo":"📷","video":"🎥","gif":"🎞️",
            "audio":"🎵","document":"📄","sticker":"🎭"
        }
        await ev.reply(
            f"✅ {icons.get(mtype,'📎')} **{mtype.title()} saved!**\n"
            f"Caption: _{caption or 'None'}_",
            buttons=[
                [Button.inline("💬 Settings", b"msg_settings"),
                 Button.inline("🏠 Menu",     b"back")]
            ]
        )
    except Exception as e:
        logger.error(f"handle_media: {e}")
        await ev.reply(f"❌ {e}")
        st.login_state = None


# ═══════════════════════════════════════════════════
# LOGIN
# ═══════════════════════════════════════════════════

async def finalize_login(ev, tmp: TelegramClient):
    st.session_string = StringSession.save(tmp.session)
    st.phone_number   = st.pending_phone
    st.active_account = st.pending_phone
    st.accounts[st.pending_phone] = st.session_string
    me = await tmp.get_me()
    await tmp.disconnect()
    st._temp_client = None
    st.login_state  = None
    st.save()
    await ev.reply(
        f"✅ **Logged in as {me.first_name}!**\n"
        f"ID: `{me.id}` | Phone: `{st.phone_number}`",
        buttons=main_menu(ev.sender_id)
    )


# ═══════════════════════════════════════════════════
# LOGS
# ═══════════════════════════════════════════════════

async def send_logs(chat_id: int):
    try:
        if not Path(LOG_FILE).exists():
            return await bot.send_message(chat_id, "📋 No logs.")
        with open(LOG_FILE, encoding="utf-8") as f:
            lines = f.readlines()
        last = "".join(lines[-150:])
        if len(last) > 3800:
            await bot.send_file(chat_id, LOG_FILE, caption="📋 Full logs")
        else:
            await bot.send_message(
                chat_id, f"📋 **Logs:**\n\n```\n{last[-3500:]}\n```"
            )
    except Exception as e:
        await bot.send_message(chat_id, f"❌ Log error: {e}")


# ═══════════════════════════════════════════════════
# AUTO START
# ═══════════════════════════════════════════════════

async def auto_start():
    if st.session_string and st.userbot_running:
        ok = await start_userbot()
        try:
            msg = (
                "✅ Userbot auto-started!"
                if ok else
                "❌ Auto-start failed — session expired?"
            )
            await bot.send_message(
                ADMIN_ID, f"🔄 **Bot Restarted**\n{msg}",
                buttons=main_menu(ADMIN_ID)
            )
        except Exception:
            pass
        if not ok:
            st.userbot_running = False
            st.save()


# ═══════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════

async def main():
    logger.info("=" * 50)
    logger.info("Userbot Manager v6.0")
    logger.info(
        f"SentStore: {sent_store.count()} users | "
        f"Skip mode: {'ON' if st.skip_already_sent else 'OFF'}"
    )
    logger.info("=" * 50)
    await bot.start(bot_token=BOT_TOKEN)
    logger.info("Bot online ✅")
    await auto_start()
    logger.info("Ready ✅")
    await bot.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
