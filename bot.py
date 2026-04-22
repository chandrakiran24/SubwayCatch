#!/usr/bin/env python3
"""Production-ready Telegram bot for NYC MTA real-time subway arrivals."""

import logging
import os
import threading
import time
import asyncio
from dataclasses import dataclass
from collections import OrderedDict
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, List, Optional, Set, Tuple

import aiohttp
import pytz
from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
    Update,
)
from telegram.error import Conflict, NetworkError, TimedOut
from html import escape
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes
from thefuzz import process

from station_metadata import IMPORTANT_STATIONS, STATION_ALIAS_TO_STOP_PREFIXES

# Configure logger format for cloud/runtime observability.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("subway_bot")

POLLING_CONFLICT_DETECTED = False
NYC_TZ = pytz.timezone("America/New_York")
_MAX_TRACKED_CHATS = 2_000
_FEED_LATENCY_TOLERANCE_MINUTES = -1
_FEED_CACHE_TTL_SECONDS = 30

# Official MTA GTFS-RT feeds by line.
TRAIN_FEEDS: Dict[str, str] = {
    "A": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace",
    "C": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace",
    "E": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace",
    "B": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-bdfm",
    "D": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-bdfm",
    "F": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-bdfm",
    "M": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-bdfm",
    "G": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-g",
    "J": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-jz",
    "Z": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-jz",
    "N": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-nqrw",
    "Q": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-nqrw",
    "R": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-nqrw",
    "W": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-nqrw",
    "1": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",
    "2": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",
    "3": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",
    "4": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",
    "5": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",
    "6": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",
    "7": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",
    "S": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",
}

STATION_CODE_TO_NAME: Dict[str, str] = {
    code: name
    for group in IMPORTANT_STATIONS.values()
    for code, name in group.items()
}


class _LRUDict(OrderedDict):
    """OrderedDict that evicts the oldest entry once the cap is reached."""

    def __init__(self, maxsize: int) -> None:
        super().__init__()
        self._maxsize = maxsize

    def __setitem__(self, key: int, value: Tuple[str, str]) -> None:
        super().__setitem__(key, value)
        self.move_to_end(key)
        if len(self) > self._maxsize:
            self.popitem(last=False)


LAST_REQUEST_BY_CHAT: _LRUDict = _LRUDict(_MAX_TRACKED_CHATS)

VALID_TRAINS_TEXT = "A,B,C,D,E,F,G,J,S,Z,N,Q,R,W,1,2,3,4,5,6,7"

@dataclass
class FeedCacheEntry:
    fetched_at: float
    data: Dict[str, bytes]


class FeedBatchCache:
    """Async cache for raw feed payloads with request coalescing."""

    def __init__(self, ttl_seconds: int) -> None:
        self._ttl_seconds = ttl_seconds
        self._entries: Dict[Tuple[str, ...], FeedCacheEntry] = {}
        self._in_flight: Dict[Tuple[str, ...], asyncio.Task[Dict[str, bytes]]] = {}
        self._lock = asyncio.Lock()

    async def get_or_fetch(self, session: aiohttp.ClientSession, feed_urls: List[str], api_key: str) -> Dict[str, bytes]:
        key = tuple(sorted(feed_urls))
        now = time.monotonic()
        async with self._lock:
            cached = self._entries.get(key)
            if cached and now - cached.fetched_at <= self._ttl_seconds:
                logger.info("Feed cache hit key=%s feeds=%s", key, len(key))
                return dict(cached.data)

            task = self._in_flight.get(key)
            if task is None:
                logger.info("Feed cache miss key=%s", key)
                task = asyncio.create_task(self._fetch_batch(session, list(key), api_key))
                self._in_flight[key] = task
            else:
                logger.info("Feed cache join in-flight key=%s", key)

        try:
            data = await task
        finally:
            async with self._lock:
                if self._in_flight.get(key) is task:
                    self._in_flight.pop(key, None)

        async with self._lock:
            self._entries[key] = FeedCacheEntry(fetched_at=time.monotonic(), data=dict(data))
            self._entries = {
                existing_key: entry
                for existing_key, entry in self._entries.items()
                if time.monotonic() - entry.fetched_at <= self._ttl_seconds
            }
        return dict(data)

    async def _fetch_batch(self, session: aiohttp.ClientSession, feed_urls: List[str], api_key: str) -> Dict[str, bytes]:
        raw_results = await asyncio.gather(
            *[_fetch_feed(session, feed_url, api_key) for feed_url in feed_urls],
            return_exceptions=True,
        )
        payloads: Dict[str, bytes] = {}
        for item in raw_results:
            if isinstance(item, BaseException):
                logger.error("Unexpected exception during feed gather: %s", item, exc_info=item)
                continue
            feed_url, content, per_feed_fetch_ms = item
            logger.info("mta_metrics feed_url=%s feed_fetch_ms=%.2f", feed_url, per_feed_fetch_ms)
            if content is not None:
                payloads[feed_url] = content
        return payloads


FEED_BATCH_CACHE = FeedBatchCache(ttl_seconds=_FEED_CACHE_TTL_SECONDS)


def direction_from_stop_id(stop_id: str) -> str:
    """Infer train direction from stop suffix."""
    if stop_id.endswith("N"):
        return "Uptown"
    if stop_id.endswith("S"):
        return "Downtown"
    return "Unknown"


class HealthHandler(BaseHTTPRequestHandler):
    """Simple health endpoint for platforms that require an open HTTP port."""

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, format: str, *args) -> None:
        # Silence default HTTP request logs; main logger already provides runtime visibility.
        del format, args


def start_health_server() -> None:
    """Start a tiny HTTP server when PORT is provided (e.g., Render web services)."""
    port_value = os.getenv("PORT")
    if not port_value:
        logger.info("PORT not set; skipping health server startup.")
        return

    try:
        port = int(port_value)
    except ValueError:
        logger.warning("Invalid PORT value %s; skipping health server startup.", port_value)
        return

    def run_server() -> None:
        server = ThreadingHTTPServer(("0.0.0.0", port), HealthHandler)
        logger.info("Health server listening on 0.0.0.0:%s", port)
        server.serve_forever()

    thread = threading.Thread(target=run_server, name="health-server", daemon=True)
    thread.start()


async def _fetch_feed(
    session: aiohttp.ClientSession,
    feed_url: str,
    api_key: str,
) -> Tuple[str, Optional[bytes], float]:
    """Fetch one GTFS feed and return URL, content, and fetch latency in milliseconds."""
    fetch_started = time.perf_counter()
    try:
        async with session.get(feed_url, headers={"x-api-key": api_key}) as response:
            if response.status == 429:
                retry_after = response.headers.get("Retry-After", "unknown")
                logger.error(
                    "MTA rate limit hit feed=%s status=429 retry_after=%s",
                    feed_url,
                    retry_after,
                )
                return feed_url, None, (time.perf_counter() - fetch_started) * 1000
            response.raise_for_status()
            content = await response.read()
            return feed_url, content, (time.perf_counter() - fetch_started) * 1000
    except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
        logger.warning("MTA feed request failed feed=%s: %s", feed_url, exc)
        return feed_url, None, (time.perf_counter() - fetch_started) * 1000
    except Exception as exc:  # noqa: BLE001 - avoid bubbling to gather cancellation
        logger.exception("Unexpected error fetching feed=%s", feed_url, exc_info=exc)
        return feed_url, None, (time.perf_counter() - fetch_started) * 1000


async def fetch_mta_updates(station_code: str, train_filter: str = "") -> Dict[str, object]:
    """Fetch upcoming arrivals for a station, optionally filtered to one train line."""
    station_code = station_code.upper().strip()
    train_filter = train_filter.upper().strip()

    if station_code not in STATION_ALIAS_TO_STOP_PREFIXES:
        return {
            "ok": False,
            "error": "Invalid station code. Use /stationid to see supported codes.",
        }

    if train_filter and train_filter not in TRAIN_FEEDS:
        return {
            "ok": False,
            "error": f"Invalid train line '{train_filter}'. Valid trains: {VALID_TRAINS_TEXT}",
        }

    api_key = os.getenv("MTA_API_KEY")
    if not api_key:
        return {"ok": False, "error": "Server misconfiguration: MTA_API_KEY is missing."}

    stop_id_prefixes = STATION_ALIAS_TO_STOP_PREFIXES[station_code]
    if train_filter:
        feeds_for_request = {TRAIN_FEEDS[train_filter]}
    else:
        feeds_for_request = set(TRAIN_FEEDS.values())
    feed_urls: List[str] = sorted(feeds_for_request)

    logger.info("Fetching MTA feeds for station_code=%s train_filter=%s", station_code, train_filter or "ALL")

    # Data flow: line -> feed URL -> protobuf bytes -> parsed trip/alert entities.
    lines_in_scope = sorted({train_filter} if train_filter else {line for line, feed in TRAIN_FEEDS.items() if feed in feeds_for_request})
    logger.info(
        "MTA data path station=%s lines=%s feeds=%s",
        station_code,
        ",".join(lines_in_scope) if lines_in_scope else "ALL",
        len(feed_urls),
    )

    now = datetime.now(tz=NYC_TZ)
    arrivals: List[Tuple[int, str, str, str]] = []
    alert_set: Set[str] = set()
    timeout = aiohttp.ClientTimeout(total=15)

    fetch_batch_started = time.perf_counter()
    async with aiohttp.ClientSession(timeout=timeout) as session:
        raw_payloads = await FEED_BATCH_CACHE.get_or_fetch(session, feed_urls, api_key)
    fetch_batch_ms = (time.perf_counter() - fetch_batch_started) * 1000

    parse_started = time.perf_counter()
    for feed_url in feed_urls:
        content = raw_payloads.get(feed_url)
        if not content:
            continue
        feed = gtfs_realtime_pb2.FeedMessage()
        try:
            feed.ParseFromString(content)
        except Exception as exc:
            logger.exception("Failed to parse protobuf feed for feed=%s: %s", feed_url, exc)
            continue

        for entity in feed.entity:
            if not entity.HasField("trip_update"):
                continue

            trip_update = entity.trip_update
            train = trip_update.trip.route_id.upper().strip() if trip_update.trip.route_id else ""
            if train not in TRAIN_FEEDS:
                continue
            if train_filter and train != train_filter:
                continue

            for stu in trip_update.stop_time_update:
                stop_id = stu.stop_id.upper() if stu.stop_id else ""
                if not any(stop_id.startswith(prefix) for prefix in stop_id_prefixes):
                    continue

                direction = direction_from_stop_id(stop_id)
                if direction == "Unknown":
                    continue

                if not stu.HasField("arrival") or stu.arrival.time <= 0:
                    continue

                arrival_dt = datetime.fromtimestamp(stu.arrival.time, tz=NYC_TZ)
                minutes = int((arrival_dt - now).total_seconds() // 60)
                if minutes < _FEED_LATENCY_TOLERANCE_MINUTES:
                    continue
                display_minutes = max(0, minutes)

                arrivals.append((display_minutes, direction, train, arrival_dt.strftime("%I:%M %p")))

        for entity in feed.entity:
            if not entity.HasField("alert"):
                continue
            if entity.alert.header_text.translation:
                text = entity.alert.header_text.translation[0].text.strip()
                if text:
                    alert_set.add(text)
    parse_ms = (time.perf_counter() - parse_started) * 1000
    logger.info(
        "mta_metrics station_code=%s train_filter=%s feed_fetch_ms=%.2f parse_ms=%.2f",
        station_code,
        train_filter or "ALL",
        fetch_batch_ms,
        parse_ms,
    )

    if not arrivals:
        return {
            "ok": False,
            "error": "No upcoming arrivals found for this request right now.",
        }

    station_name = STATION_CODE_TO_NAME.get(station_code, station_code)

    arrivals.sort(key=lambda row: (row[2], row[0]))

    uptown_by_train: Dict[str, List[Tuple[int, str]]] = {}
    downtown_by_train: Dict[str, List[Tuple[int, str]]] = {}

    for minutes, direction, train, local_time in arrivals:
        if direction == "Uptown":
            uptown_by_train.setdefault(train, [])
            if len(uptown_by_train[train]) < 2:
                uptown_by_train[train].append((minutes, local_time))
        elif direction == "Downtown":
            downtown_by_train.setdefault(train, [])
            if len(downtown_by_train[train]) < 2:
                downtown_by_train[train].append((minutes, local_time))

    return {
        "ok": True,
        "station_name": station_name,
        "station_code": station_code,
        "train_filter": train_filter,
        # Backward-compatible key for older render codepaths that still expect direction_filter.
        "direction_filter": "both",
        "uptown_by_train": dict(sorted(uptown_by_train.items())),
        "downtown_by_train": dict(sorted(downtown_by_train.items())),
        "alerts": sorted(alert_set)[:3],
    }


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send welcome and quick-start guidance."""
    del context
    message = (
        "<b>NYC Subway Arrival Bot</b>\n\n"
        "<b>Commands</b>\n"
        "• /next &lt;station_code&gt;\n"
        "• /next &lt;train&gt; &lt;station_code&gt;\n"
        "• /stationid\n"
        "• /refresh\n"
        "• /help\n\n"
        "<b>Render free-tier note</b>\n"
        "If the bot was idle, the first request can take up to 60 seconds due to a cold start while Render wakes the server.\n\n"
        "<b>Examples</b>\n"
        "/next HS34\n"
        "/next D HS34"
    )
    await update.message.reply_text(message, parse_mode="HTML")


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Explain command usage and station-code model."""
    del context
    message = (
        "<b>How this bot works</b>\n\n"
        "Commands:\n"
        "• /start - welcome message\n"
        "• /help - usage guide\n"
        "• /stationid - supported station codes\n"
        "• /refresh - rerun your last /next query\n"
        "• /next &lt;station_code&gt; - all trains at station, both directions\n"
        "• /next &lt;train&gt; &lt;station_code&gt; - one train at station, both directions\n\n"
        "Train examples: A, D, Q, 2, 7\n"
        "Station codes are short aliases such as HS34, TS42, GC42.\n"
        "Use /stationid to browse all supported station codes."
    )
    await update.message.reply_text(message, parse_mode="HTML")


def _borough_keyboard() -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    current_row: List[InlineKeyboardButton] = []
    for borough in IMPORTANT_STATIONS:
        current_row.append(InlineKeyboardButton(borough, callback_data=f"stationid:{borough}"))
        if len(current_row) == 2:
            rows.append(current_row)
            current_row = []
    if current_row:
        rows.append(current_row)
    return InlineKeyboardMarkup(rows)


async def stationid_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show borough menu for station aliases."""
    del context
    await update.message.reply_text(
        "Choose a borough to browse station aliases:",
        reply_markup=_borough_keyboard(),
    )


async def stationid_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle borough menu callbacks for /stationid."""
    del context
    query = update.callback_query
    if not query or not query.data:
        return
    await query.answer()
    borough = query.data.split(":", maxsplit=1)[-1]
    stations = IMPORTANT_STATIONS.get(borough)
    if not stations:
        await query.edit_message_text("Borough menu expired. Please run /stationid again.")
        return

    lines = [f"<b>{escape(borough)} station aliases</b>", ""]
    for code, name in stations.items():
        lines.append(f"• {escape(code)} → {escape(name)}")
    await query.edit_message_text(
        "\n".join(lines),
        parse_mode="HTML",
        reply_markup=_borough_keyboard(),
    )


def refresh_reply_markup() -> ReplyKeyboardMarkup:
    """Return a compact one-tap keyboard with /refresh."""
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton("/refresh")]],
        resize_keyboard=True,
        one_time_keyboard=False,
    )


def build_arrival_message(result: Dict[str, object], train_scope: str = "") -> str:
    """Build a Telegram HTML message for arrivals output."""
    title = "Station Arrivals" if not train_scope else f"{train_scope} Train Arrival"
    lines = [
        f"<b>{escape(title)}</b>",
        "",
        f"<b>Station:</b> {escape(str(result['station_name']))}",
        f"<b>Direction:</b> {escape(str(result['direction_filter']).title())}",
        "",
    ]

    lines.append("<b>Uptown (next 2 per train)</b>")
    if result["uptown_by_train"]:
        for train, arrivals in result["uptown_by_train"].items():
            formatted = ", ".join(
                f"{int(minutes)}m ({escape(local_time)})" for minutes, local_time in arrivals
            )
            lines.append(f"• <b>{escape(train)}</b>: {formatted}")
    else:
        lines.append("• No upcoming uptown trains")

    lines.append("")
    lines.append("<b>Downtown (next 2 per train)</b>")
    if result["downtown_by_train"]:
        for train, arrivals in result["downtown_by_train"].items():
            formatted = ", ".join(
                f"{int(minutes)}m ({escape(local_time)})" for minutes, local_time in arrivals
            )
            lines.append(f"• <b>{escape(train)}</b>: {formatted}")
    else:
        lines.append("• No upcoming downtown trains")

    lines.append("")
    lines.append("<b>Service Status</b>")
    if result["alerts"]:
        for alert in result["alerts"]:
            lines.append(f"• {escape(alert)}")
    else:
        lines.append("• No delays reported")

    return "\n".join(lines)


def build_station_suggestions(input_alias: str, max_results: int = 5) -> List[str]:
    """Return closest known station aliases using fuzzy matching."""
    choices = list(STATION_ALIAS_TO_STOP_PREFIXES.keys())
    if not choices:
        return []
    ranked = process.extract(input_alias.upper().strip(), choices, limit=max_results)
    return [alias for alias, score in ranked if score >= 60]


async def refresh_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Repeat the most recent /next query for this chat."""
    del context

    if not update.effective_chat:
        return

    chat_id = update.effective_chat.id
    last_request = LAST_REQUEST_BY_CHAT.get(chat_id)
    if not last_request:
        if update.effective_message:
            await update.effective_message.reply_text(
                "No previous /next request found. Run /next <station_code> first."
            )
        return

    train_filter, station_code = last_request
    logger.info("User requested /refresh train=%s station_code=%s", train_filter or "ALL", station_code)

    result = await fetch_mta_updates(station_code, train_filter)
    if not result["ok"]:
        if update.effective_message:
            await update.effective_message.reply_text(str(result["error"]))
        return

    message = build_arrival_message(result, train_filter)
    if update.effective_message:
        await update.effective_message.reply_text(message, parse_mode="HTML", reply_markup=refresh_reply_markup())


async def next_train(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /next <station_code> or /next <train> <station_code> requests."""
    if not context.args:
        await update.message.reply_text(
            "Missing parameters. Usage:\n"
            "/next <station_code>\n"
            "/next <train> <station_code>\n"
            "Examples: /next HS34  or  /next D HS34"
        )
        return

    train_filter = ""
    station_code = ""

    if len(context.args) == 1:
        station_code = context.args[0]
    elif len(context.args) == 2:
        train_filter = context.args[0]
        station_code = context.args[1]
    else:
        await update.message.reply_text(
            "Invalid parameters. Usage:\n"
            "/next <station_code>\n"
            "/next <train> <station_code>"
        )
        return

    logger.info("User requested /next train=%s station_code=%s", train_filter or "ALL", station_code)

    normalized_station = station_code.upper().strip()
    if normalized_station not in STATION_ALIAS_TO_STOP_PREFIXES:
        suggestions = build_station_suggestions(normalized_station)
        suggestion_text = f" Try: {', '.join(suggestions)}." if suggestions else ""
        await update.message.reply_text(
            f"Invalid station code '{normalized_station}'. Use /stationid to browse aliases.{suggestion_text}"
        )
        return

    if update.effective_chat:
        LAST_REQUEST_BY_CHAT[update.effective_chat.id] = (train_filter, normalized_station)

    result = await fetch_mta_updates(station_code, train_filter)
    if not result["ok"]:
        await update.message.reply_text(str(result["error"]))
        return

    train_scope = str(result.get("train_filter", "")).upper()
    message = build_arrival_message(result, train_scope)
    await update.message.reply_text(message, parse_mode="HTML", reply_markup=refresh_reply_markup())



async def handle_application_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle uncaught handler/runtime errors from python-telegram-bot."""
    global POLLING_CONFLICT_DETECTED

    if isinstance(context.error, Conflict):
        POLLING_CONFLICT_DETECTED = True
        logger.error(
            "Telegram polling conflict detected (409). "
            "Only one active bot instance can poll this token. Stopping current instance."
        )
        await context.application.stop()
        return

    logger.exception("Unhandled exception while processing update", exc_info=context.error)

    if isinstance(update, Update) and update.effective_message:
        try:
            await update.effective_message.reply_text(
                "Sorry, something went wrong while processing your request. Please try again."
            )
        except Exception:
            logger.exception("Failed to send Telegram error reply to user.")

def main() -> None:
    """Initialize and run the Telegram bot."""
    load_dotenv()
    token = os.getenv("TELEGRAM_BOT_TOKEN")

    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set.")

    webhook_base_url = os.getenv("TELEGRAM_WEBHOOK_BASE_URL", "").strip().rstrip("/")
    if not webhook_base_url:
        webhook_base_url = os.getenv("RENDER_EXTERNAL_URL", "").strip().rstrip("/")
    webhook_path = os.getenv("TELEGRAM_WEBHOOK_PATH", token)
    drop_pending_updates = os.getenv("DROP_PENDING_UPDATES", "false").strip().lower() == "true"

    if not webhook_base_url:
        start_health_server()

    retry_delay_seconds = int(os.getenv("BOT_STARTUP_RETRY_DELAY_SECONDS", "10"))
    max_retries = int(os.getenv("BOT_STARTUP_MAX_RETRIES", "0"))
    # BOT_STARTUP_MAX_RETRIES=0 means retry forever.

    attempt = 0
    global POLLING_CONFLICT_DETECTED

    while True:
        attempt += 1
        POLLING_CONFLICT_DETECTED = False
        logger.info("Starting NYC Subway Arrival Bot (attempt %s)", attempt)

        app = Application.builder().token(token).build()
        app.add_handler(CommandHandler("start", start_command))
        app.add_handler(CommandHandler("help", help_command))
        app.add_handler(CommandHandler("stationid", stationid_command))
        app.add_handler(CallbackQueryHandler(stationid_callback, pattern=r"^stationid:"))
        app.add_handler(CommandHandler("next", next_train))
        app.add_handler(CommandHandler("refresh", refresh_command))
        app.add_error_handler(handle_application_error)

        try:
            if webhook_base_url:
                port = int(os.getenv("PORT", "10000"))
                webhook_url = f"{webhook_base_url}/{webhook_path}"
                logger.info("Starting webhook mode on port=%s webhook_url=%s", port, webhook_url)
                try:
                    app.run_webhook(
                        listen="0.0.0.0",
                        port=port,
                        url_path=webhook_path,
                        webhook_url=webhook_url,
                        drop_pending_updates=drop_pending_updates,
                    )
                except RuntimeError as exc:
                    if "python-telegram-bot[webhooks]" not in str(exc):
                        raise
                    logger.warning(
                        "Webhook dependencies are missing; falling back to polling mode. "
                        "Install with: pip install 'python-telegram-bot[webhooks]'."
                    )
                    app.run_polling(drop_pending_updates=drop_pending_updates)
            else:
                app.run_polling(drop_pending_updates=drop_pending_updates)
            if POLLING_CONFLICT_DETECTED:
                logger.warning(
                    "Polling stopped due to Telegram conflict; retrying in %s seconds.",
                    retry_delay_seconds,
                )
                time.sleep(retry_delay_seconds)
                continue
            logger.info("Bot polling stopped gracefully.")
            break
        except (TimedOut, NetworkError, Conflict):
            logger.exception(
                "Telegram API was temporarily unreachable/conflicted during startup/polling. "
                "Retrying in %s seconds.",
                retry_delay_seconds,
            )
            if max_retries > 0 and attempt >= max_retries:
                logger.error("Reached BOT_STARTUP_MAX_RETRIES=%s. Exiting.", max_retries)
                raise
            time.sleep(retry_delay_seconds)
        finally:
            try:
                asyncio.run(app.shutdown())
            except RuntimeError:
                logger.debug("Skipping app.shutdown(); an event loop is already running.")
            except Exception:
                logger.exception("Best-effort app.shutdown() failed during restart cleanup.")


if __name__ == "__main__":
    main()
