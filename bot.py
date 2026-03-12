#!/usr/bin/env python3
"""Production-ready Telegram bot for NYC MTA real-time subway arrivals."""

import logging
import os
import threading
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, List, Set, Tuple

import pytz
import requests
from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2
from telegram import Update
from telegram.error import NetworkError, TimedOut
from html import escape
from telegram.ext import Application, CommandHandler, ContextTypes

# Configure logger format for cloud/runtime observability.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("subway_bot")

NYC_TZ = pytz.timezone("America/New_York")

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
}

# User-friendly station codes requested by product requirements.
IMPORTANT_STATIONS: Dict[str, Dict[str, str]] = {
    "Manhattan": {
        "TS42": "Times Square 42 St",
        "HS34": "Herald Square 34 St",
        "US14": "Union Square 14 St",
        "GC42": "Grand Central 42 St",
        "CS59": "Columbus Circle 59 St",
        "LP66": "Lincoln Center 66 St",
        "CP72": "Central Park West 72 St",
        "FS": "Fulton Street",
        "WS4": "West 4 St",
        "ASTOR": "Astor Place",
        "CANAL": "Canal Street",
        "CHAM": "Chambers Street",
        "BATPK": "Battery Park",
        "8NYU": "8 St NYU",
        "WH": "Whitehall Street",
        "WTC": "WTC Cortlandt",
    },
    "Brooklyn": {
        "AABC": "Atlantic Ave Barclays Center",
        "JAY": "Jay St MetroTech",
        "DEK": "DeKalb Ave",
        "BRA": "Bay Ridge Ave",
        "BDWY": "Broadway Junction",
        "MYRT": "Myrtle Ave",
        "BED": "Bedford Ave",
        "WKOS": "Wilson Ave",
        "FLAT": "Flatbush Ave Brooklyn College",
        "PROS": "Prospect Park",
        "7AVB": "7 Ave Brooklyn",
        "9STB": "9 St Brooklyn",
        "CHCH": "Church Ave",
        "KNGS": "Kings Highway",
        "UTIC": "Utica Ave",
        "FRKL": "Franklin Ave",
        "CI": "Coney Island Stillwell Av",
    },
}

# Station alias -> MTA stop prefix. Prefix matching handles direction suffixes (N/S).
STATION_ALIAS_TO_STOP_ID: Dict[str, str] = {
    "HS34": "D17",
    "TS42": "R16",
    "US14": "R20",
    "GC42": "631",
    "CS59": "A24",
    "LP66": "127",
    "CP72": "A22",
    "FS": "A38",
    "WS4": "A32",
    "ASTOR": "635",
    "CANAL": "R31",
    "CHAM": "A36",
    "BATPK": "R27",
    "8NYU": "R21",
    "WH": "R26",
    "WTC": "A55",
    "AABC": "D24",
    "JAY": "A41",
    "DEK": "R30",
    "BRA": "R41",
    "BDWY": "A51",
    "MYRT": "M11",
    "BED": "L05",
    "WKOS": "L01",
    "FLAT": "247",
    "PROS": "B16",
    "7AVB": "D25",
    "9STB": "F21",
    "CHCH": "D28",
    "KNGS": "D35",
    "UTIC": "A65",
    "FRKL": "S03",
    "CI": "D43",
}

VALID_DIRECTION_TEXT = "uptown,downtown,both"


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


def fetch_mta_updates(station_code: str, direction_filter: str = "both") -> Dict[str, object]:
    """Fetch upcoming arrivals for all trains at a station, optionally filtered by direction."""
    station_code = station_code.upper().strip()
    direction_filter = direction_filter.lower().strip()

    if direction_filter not in {"uptown", "downtown", "both"}:
        return {
            "ok": False,
            "error": f"Invalid direction. Use {VALID_DIRECTION_TEXT}.",
        }

    if station_code not in STATION_ALIAS_TO_STOP_ID:
        return {
            "ok": False,
            "error": "Invalid station code. Use /stationid to see supported codes.",
        }

    api_key = os.getenv("MTA_API_KEY")
    if not api_key:
        return {"ok": False, "error": "Server misconfiguration: MTA_API_KEY is missing."}

    stop_id_prefix = STATION_ALIAS_TO_STOP_ID[station_code]
    feed_urls: List[str] = sorted(set(TRAIN_FEEDS.values()))
    logger.info("Fetching MTA feeds for station_code=%s direction=%s", station_code, direction_filter)

    now = datetime.now(tz=NYC_TZ)
    arrivals: List[Tuple[int, str, str, str]] = []
    alert_set: Set[str] = set()

    for feed_url in feed_urls:
        try:
            response = requests.get(feed_url, headers={"x-api-key": api_key}, timeout=15)
            response.raise_for_status()
        except requests.Timeout:
            logger.exception("MTA API timeout for feed=%s", feed_url)
            continue
        except requests.RequestException as exc:
            logger.exception("MTA API request failed for feed=%s: %s", feed_url, exc)
            continue

        feed = gtfs_realtime_pb2.FeedMessage()
        try:
            feed.ParseFromString(response.content)
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

            for stu in trip_update.stop_time_update:
                stop_id = stu.stop_id.upper() if stu.stop_id else ""
                if not stop_id.startswith(stop_id_prefix):
                    continue

                direction = direction_from_stop_id(stop_id)
                if direction == "Unknown":
                    continue
                if direction_filter != "both" and direction.lower() != direction_filter:
                    continue

                if not stu.HasField("arrival") or stu.arrival.time <= 0:
                    continue

                arrival_dt = datetime.fromtimestamp(stu.arrival.time, tz=NYC_TZ)
                minutes = int((arrival_dt - now).total_seconds() // 60)
                if minutes < 0:
                    continue

                arrivals.append((minutes, direction, train, arrival_dt.strftime("%I:%M %p")))

        for entity in feed.entity:
            if not entity.HasField("alert"):
                continue
            if entity.alert.header_text.translation:
                text = entity.alert.header_text.translation[0].text.strip()
                if text:
                    alert_set.add(text)

    if not arrivals:
        return {
            "ok": False,
            "error": "No upcoming arrivals found for this station right now.",
        }

    station_name = next(
        (name for group in IMPORTANT_STATIONS.values() for code, name in group.items() if code == station_code),
        station_code,
    )

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
        "direction_filter": direction_filter,
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
        "• /next &lt;station_code&gt; [uptown|downtown|both]\n"
        "• /stationid\n"
        "• /help\n\n"
        "<b>Examples</b>\n"
        "/next HS34\n"
        "/next CI uptown"
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
        "• /next &lt;station_code&gt; [uptown|downtown|both] - next arrivals\n\n"
        "Direction is optional and defaults to both.\n"
        "Station codes are short aliases such as HS34, TS42, GC42.\n"
        "Use /stationid to browse all supported station codes."
    )
    await update.message.reply_text(message, parse_mode="HTML")


async def stationid_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Display important station codes grouped by borough."""
    del context
    lines = ["<b>Important Stations</b>", ""]
    for borough, stations in IMPORTANT_STATIONS.items():
        lines.append(f"<b>{escape(borough)}</b>")
        for code, name in stations.items():
            lines.append(f"• {escape(code)} → {escape(name)}")
        lines.append("")

    await update.message.reply_text("\n".join(lines).strip(), parse_mode="HTML")


async def next_train(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /next <station_code> [direction] requests."""
    if not context.args:
        await update.message.reply_text(
            "Missing parameters. Usage: /next <station_code> [uptown|downtown|both]\n"
            "Examples: /next HS34  or  /next CI uptown"
        )
        return

    station_code = context.args[0]
    direction_filter = context.args[1] if len(context.args) > 1 else "both"
    logger.info("User requested /next station_code=%s direction=%s", station_code, direction_filter)

    result = fetch_mta_updates(station_code, direction_filter)
    if not result["ok"]:
        await update.message.reply_text(str(result["error"]))
        return

    lines = [
        "<b>Station Arrivals</b>",
        "",
        f"<b>Station:</b> {escape(str(result['station_name']))}",
        f"<b>Direction:</b> {escape(str(result['direction_filter']).title())}",
        "",
    ]

    show_uptown = result["direction_filter"] in {"uptown", "both"}
    show_downtown = result["direction_filter"] in {"downtown", "both"}

    if show_uptown:
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

    if show_downtown:
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

    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


def main() -> None:
    """Initialize and run the Telegram bot."""
    load_dotenv()
    token = os.getenv("TELEGRAM_BOT_TOKEN")

    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set.")

    start_health_server()

    retry_delay_seconds = int(os.getenv("BOT_STARTUP_RETRY_DELAY_SECONDS", "10"))
    max_retries = int(os.getenv("BOT_STARTUP_MAX_RETRIES", "0"))
    # BOT_STARTUP_MAX_RETRIES=0 means retry forever.

    attempt = 0
    while True:
        attempt += 1
        logger.info("Starting NYC Subway Arrival Bot (attempt %s)", attempt)

        app = Application.builder().token(token).build()
        app.add_handler(CommandHandler("start", start_command))
        app.add_handler(CommandHandler("help", help_command))
        app.add_handler(CommandHandler("stationid", stationid_command))
        app.add_handler(CommandHandler("next", next_train))

        try:
            app.run_polling(drop_pending_updates=True)
            logger.info("Bot polling stopped gracefully.")
            break
        except (TimedOut, NetworkError):
            logger.exception(
                "Telegram API was temporarily unreachable during startup/polling. "
                "Retrying in %s seconds.",
                retry_delay_seconds,
            )
            if max_retries > 0 and attempt >= max_retries:
                logger.error("Reached BOT_STARTUP_MAX_RETRIES=%s. Exiting.", max_retries)
                raise
            time.sleep(retry_delay_seconds)


if __name__ == "__main__":
    main()
