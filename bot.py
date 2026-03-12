#!/usr/bin/env python3
"""Production-ready Telegram bot for NYC MTA real-time subway arrivals."""

import logging
import os
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
from datetime import datetime
from typing import Dict, List, Tuple

import pytz
import requests
from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2
from telegram import Update
from telegram.error import Conflict, NetworkError, TimedOut
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
        "CI": "Coney Island",
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
    "PROS": "D43",
    "7AVB": "D25",
    "9STB": "F21",
    "CHCH": "D28",
    "KNGS": "D35",
    "UTIC": "A65",
    "FRKL": "S03",
    "CI": "D43",
}

VALID_TRAINS_TEXT = "A,B,C,D,E,F,G,J,Z,N,Q,R,W,1,2,3,4,5,6,7"


def direction_from_stop_id(stop_id: str) -> str:
    """Infer train direction from stop suffix."""
    if stop_id.endswith("N"):
        return "Uptown"
    if stop_id.endswith("S"):
        return "Downtown"
    return "Unknown"


def fetch_mta_updates(train: str, station_code: str) -> Dict[str, object]:
    """
    Fetch upcoming arrivals for a train and user-friendly station code.

    Responsibilities:
      - validate inputs
      - call MTA GTFS-RT feed
      - parse protobuf TripUpdates/StopTimeUpdates
      - return next arrivals and optional alerts
    """
    train = train.upper().strip()
    station_code = station_code.upper().strip()

    # Validate train first so users get immediate feedback.
    if train not in TRAIN_FEEDS:
        return {
            "ok": False,
            "error": f"Invalid train line. Use {VALID_TRAINS_TEXT}.",
        }

    # Validate station code and map to stop_id prefix.
    if station_code not in STATION_ALIAS_TO_STOP_ID:
        return {
            "ok": False,
            "error": "Invalid station code. Use /stationid to see supported codes.",
        }

    api_key = os.getenv("MTA_API_KEY")
    if not api_key:
        return {"ok": False, "error": "Server misconfiguration: MTA_API_KEY is missing."}

    stop_id_prefix = STATION_ALIAS_TO_STOP_ID[station_code]
    feed_url = TRAIN_FEEDS[train]
    logger.info("Fetching MTA feed for train=%s station_code=%s url=%s", train, station_code, feed_url)

    try:
        response = requests.get(feed_url, headers={"x-api-key": api_key}, timeout=15)
        response.raise_for_status()
    except requests.Timeout:
        logger.exception("MTA API timeout for train=%s", train)
        return {"ok": False, "error": "MTA API timeout. Please try again in a moment."}
    except requests.RequestException as exc:
        logger.exception("MTA API request failed: %s", exc)
        return {"ok": False, "error": "MTA API request failed. Please try again later."}

    feed = gtfs_realtime_pb2.FeedMessage()
    try:
        feed.ParseFromString(response.content)
    except Exception as exc:
        logger.exception("Failed to parse protobuf feed: %s", exc)
        return {"ok": False, "error": "Could not parse MTA feed response."}

    now = datetime.now(tz=NYC_TZ)
    arrivals: List[Tuple[int, str, str]] = []

    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue

        trip_update = entity.trip_update
        if not trip_update.trip.route_id or trip_update.trip.route_id.upper() != train:
            continue

        for stu in trip_update.stop_time_update:
            stop_id = stu.stop_id.upper() if stu.stop_id else ""
            if not stop_id.startswith(stop_id_prefix):
                continue

            if not stu.HasField("arrival") or stu.arrival.time <= 0:
                continue

            arrival_dt = datetime.fromtimestamp(stu.arrival.time, tz=NYC_TZ)
            minutes = int((arrival_dt - now).total_seconds() // 60)
            if minutes < 0:
                continue

            arrivals.append((minutes, direction_from_stop_id(stop_id), arrival_dt.strftime("%I:%M %p")))

    arrivals.sort(key=lambda row: row[0])

    # Best-effort alert extraction from same feed when alert entities are present.
    alerts: List[str] = []
    for entity in feed.entity:
        if not entity.HasField("alert"):
            continue
        if entity.alert.header_text.translation:
            text = entity.alert.header_text.translation[0].text.strip()
            if text:
                alerts.append(text)

    if not arrivals:
        return {
            "ok": False,
            "error": "No upcoming arrivals found for that train at this station right now.",
        }

    station_name = next(
        (name for group in IMPORTANT_STATIONS.values() for code, name in group.items() if code == station_code),
        station_code,
    )

    return {
        "ok": True,
        "station_name": station_name,
        "arrivals": arrivals[:2],  # Return next 2 arrivals as requested.
        "alerts": alerts[:2],
    }


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send welcome and quick-start guidance."""
    del context
    message = (
        "<b>NYC Subway Arrival Bot</b>\n\n"
        "<b>Commands</b>\n"
        "• /next &lt;train&gt; &lt;station_code&gt;\n"
        "• /stationid\n"
        "• /help\n\n"
        "<b>Example</b>\n"
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
        "• /next &lt;train&gt; &lt;station_code&gt; - next arrivals\n\n"
        "Train examples: A, D, Q, 2, 7\n"
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
    """Handle /next <train> <station_code> requests."""
    if not context.args or len(context.args) < 2:
        await update.message.reply_text(
            "Missing parameters. Usage: /next <train> <station_code>\nExample: /next D HS34"
        )
        return

    train = context.args[0]
    station_code = context.args[1]
    logger.info("User requested /next train=%s station_code=%s", train, station_code)

    result = fetch_mta_updates(train, station_code)
    if not result["ok"]:
        await update.message.reply_text(str(result["error"]))
        return

    lines = [
        f"<b>{escape(train.upper())} Train Arrival</b>",
        "",
        f"<b>Station:</b> {escape(str(result['station_name']))}",
        "",
        "<b>Next Trains</b>",
    ]

    for minutes, direction, local_time in result["arrivals"]:
        lines.append(
            f"• {escape(direction)} → {int(minutes)} minutes ({escape(local_time)})"
        )

    lines.append("")
    lines.append("<b>Service Status</b>")

    if result["alerts"]:
        for alert in result["alerts"]:
            lines.append(f"• {escape(alert)}")
    else:
        lines.append("• No delays reported")

    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


async def log_application_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Log unexpected framework-level errors for easier production debugging."""
    logger.exception("Unhandled telegram application error. Update=%s", update, exc_info=context.error)


def main() -> None:
    """Initialize and run the Telegram bot."""
    load_dotenv()
    token = os.getenv("TELEGRAM_BOT_TOKEN")

    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set.")

    retry_delay_seconds = int(os.getenv("BOT_STARTUP_RETRY_DELAY_SECONDS", "10"))
    max_retries = int(os.getenv("BOT_STARTUP_MAX_RETRIES", "0"))
    # BOT_STARTUP_MAX_RETRIES=0 means retry forever.

    # Render web services expect an open port. For long-polling bots, expose a tiny health server
    # when PORT is present so the process is considered healthy without changing bot behavior.
    render_port = os.getenv("PORT")
    if render_port:
        class _HealthHandler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802 (stdlib naming)
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.end_headers()
                self.wfile.write(b"ok")

            def log_message(self, format: str, *args: object) -> None:  # noqa: A003
                return

        def _run_health_server() -> None:
            try:
                server = HTTPServer(("0.0.0.0", int(render_port)), _HealthHandler)
                logger.info("Health server listening on 0.0.0.0:%s", render_port)
                server.serve_forever()
            except Exception:
                logger.exception("Failed to start health server on PORT=%s", render_port)

        Thread(target=_run_health_server, daemon=True).start()

    attempt = 0
    while True:
        attempt += 1
        logger.info("Starting NYC Subway Arrival Bot (attempt %s)", attempt)

        app = Application.builder().token(token).build()
        app.add_handler(CommandHandler("start", start_command))
        app.add_handler(CommandHandler("help", help_command))
        app.add_handler(CommandHandler("stationid", stationid_command))
        app.add_handler(CommandHandler("next", next_train))
        app.add_error_handler(log_application_error)

        try:
            app.run_polling(drop_pending_updates=True)
            logger.info("Bot polling stopped gracefully.")
            break
        except Conflict:
            logger.exception(
                "Telegram conflict detected: another getUpdates consumer is active. "
                "Retrying in %s seconds. Ensure only one polling instance is running.",
                retry_delay_seconds,
            )
            if max_retries > 0 and attempt >= max_retries:
                logger.error("Reached BOT_STARTUP_MAX_RETRIES=%s. Exiting.", max_retries)
                raise
            time.sleep(retry_delay_seconds)
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
