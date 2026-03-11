#!/usr/bin/env python3
"""
Telegram bot for NYC MTA real-time subway arrivals.

Install dependencies:
    pip install -r requirements.txt

Environment variables required:
    TELEGRAM_BOT_TOKEN
    MTA_API_KEY
"""

import logging
import os
from datetime import datetime
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# NYC local timezone for human-readable timestamps.
NYC_TZ = ZoneInfo("America/New_York")

# MTA feed mapping by train line.
TRAIN_FEED_MAP: Dict[str, str] = {
    "A": "gtfs-ace",
    "C": "gtfs-ace",
    "E": "gtfs-ace",
    "B": "gtfs-bdfm",
    "D": "gtfs-bdfm",
    "F": "gtfs-bdfm",
    "M": "gtfs-bdfm",
    "G": "gtfs-g",
    "J": "gtfs-jz",
    "Z": "gtfs-jz",
    "N": "gtfs-nqrw",
    "Q": "gtfs-nqrw",
    "R": "gtfs-nqrw",
    "W": "gtfs-nqrw",
    "L": "gtfs-l",
    "1": "gtfs",
    "2": "gtfs",
    "3": "gtfs",
    "4": "gtfs",
    "5": "gtfs",
    "6": "gtfs",
    "7": "gtfs-7",
    "S": "gtfs-si",
}

MTA_FEED_BASE_URL = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2F"


def direction_from_stop_id(stop_id: str) -> str:
    """Convert stop suffix to user-friendly direction."""
    if stop_id.endswith("N"):
        return "Uptown"
    if stop_id.endswith("S"):
        return "Downtown"
    return "Unknown"


def parse_service_alerts(feed: gtfs_realtime_pb2.FeedMessage, train: str, station_id: str) -> List[str]:
    """Extract alert text that appears relevant to the selected train/station."""
    alerts: List[str] = []
    for entity in feed.entity:
        if not entity.HasField("alert"):
            continue

        informed = entity.alert.informed_entity
        relevant = False
        if not informed:
            relevant = True
        else:
            for informed_entity in informed:
                route_match = (
                    not informed_entity.route_id
                    or informed_entity.route_id.upper() == train
                )
                stop_match = (
                    not informed_entity.stop_id
                    or informed_entity.stop_id.startswith(station_id)
                )
                if route_match and stop_match:
                    relevant = True
                    break

        if not relevant:
            continue

        if entity.alert.header_text.translation:
            text = entity.alert.header_text.translation[0].text
            if text:
                alerts.append(text)

    return alerts


def fetch_mta_updates(train: str, station_id: str) -> Dict[str, object]:
    """
    Fetch and parse MTA GTFS-RT updates for a train/station.

    Returns a dictionary with either:
      - {"ok": True, "arrivals": [...], "alerts": [...]}
      - {"ok": False, "error": "..."}
    """
    normalized_train = train.upper()
    normalized_station = station_id.strip().upper()

    if normalized_train not in TRAIN_FEED_MAP:
        valid = ", ".join(sorted(TRAIN_FEED_MAP.keys()))
        return {"ok": False, "error": f"Invalid train line '{train}'. Valid options: {valid}"}

    api_key = os.getenv("MTA_API_KEY")
    if not api_key:
        return {"ok": False, "error": "MTA_API_KEY environment variable is not set."}

    feed_name = TRAIN_FEED_MAP[normalized_train]
    feed_url = f"{MTA_FEED_BASE_URL}{feed_name}"

    try:
        response = requests.get(feed_url, headers={"x-api-key": api_key}, timeout=20)
        response.raise_for_status()
    except requests.RequestException as exc:
        return {"ok": False, "error": f"Error calling MTA API: {exc}"}

    feed = gtfs_realtime_pb2.FeedMessage()
    try:
        feed.ParseFromString(response.content)
    except Exception as exc:  # protobuf parsing error
        return {"ok": False, "error": f"Could not parse GTFS-RT feed: {exc}"}

    now_utc = datetime.now(tz=ZoneInfo("UTC"))
    arrivals: List[Dict[str, str]] = []
    found_any_stop_prefix = False

    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue

        trip_update = entity.trip_update
        route_id = trip_update.trip.route_id.upper() if trip_update.trip.route_id else ""
        if route_id != normalized_train:
            continue

        for stop_time_update in trip_update.stop_time_update:
            stop_id = stop_time_update.stop_id.upper()
            if stop_id.startswith(normalized_station):
                found_any_stop_prefix = True
            if not stop_id.startswith(normalized_station):
                continue

            if not stop_time_update.HasField("arrival"):
                continue

            arrival_ts = stop_time_update.arrival.time
            if arrival_ts <= 0:
                continue

            arrival_dt = datetime.fromtimestamp(arrival_ts, tz=ZoneInfo("UTC")).astimezone(NYC_TZ)
            minutes_away = int((arrival_dt.astimezone(ZoneInfo("UTC")) - now_utc).total_seconds() // 60)
            if minutes_away < 0:
                continue

            delay_seconds: Optional[int] = None
            if stop_time_update.arrival.HasField("delay"):
                delay_seconds = stop_time_update.arrival.delay
            elif trip_update.HasField("delay"):
                delay_seconds = trip_update.delay

            arrivals.append(
                {
                    "stop_id": stop_id,
                    "direction": direction_from_stop_id(stop_id),
                    "minutes": str(minutes_away),
                    "arrival_time": arrival_dt.strftime("%I:%M %p"),
                    "delay": str(delay_seconds) if delay_seconds is not None else "0",
                }
            )

    arrivals.sort(key=lambda item: int(item["minutes"]))
    alerts = parse_service_alerts(feed, normalized_train, normalized_station)

    if not arrivals:
        if not found_any_stop_prefix:
            return {
                "ok": False,
                "error": (
                    f"No matching stop updates found for station '{station_id}'. "
                    "Please verify the station ID."
                ),
            }
        return {
            "ok": False,
            "error": "No upcoming arrivals currently available for that train/station.",
        }

    return {"ok": True, "arrivals": arrivals[:3], "alerts": alerts[:3]}


async def next_train(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /next <train> <station_id> command."""
    if not context.args or len(context.args) < 2:
        await update.message.reply_text(
            "Usage: /next <train> <station_id>\nExample: /next D 635"
        )
        return

    train = context.args[0]
    station_id = context.args[1]
    logging.info("/next requested: train=%s station_id=%s", train, station_id)

    result = fetch_mta_updates(train, station_id)
    if not result["ok"]:
        await update.message.reply_text(f"⚠️ {result['error']}")
        return

    arrivals = result["arrivals"]
    alerts = result["alerts"]

    lines = [
        "<b>🚇 NYC Subway Arrival Update</b>",
        f"<b>Train:</b> {train.upper()}",
        f"<b>Station ID:</b> {station_id.upper()}",
        "",
        "<b>Next Arrivals</b>",
    ]

    for arrival in arrivals:
        delay_text = "On time"
        delay_seconds = int(arrival["delay"])
        if delay_seconds > 0:
            delay_text = f"Delayed by {delay_seconds // 60} min"

        lines.extend(
            [
                f"• {arrival['minutes']} min ({arrival['arrival_time']})",
                f"  - Direction: {arrival['direction']}",
                f"  - Status: {delay_text}",
            ]
        )

    if alerts:
        lines.append("")
        lines.append("<b>Service Alerts</b>")
        for alert in alerts:
            lines.append(f"• {alert}")

    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


def main() -> None:
    """Start the Telegram bot application."""
    load_dotenv()

    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN environment variable is not set.")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    logging.info("Starting SubwayCatch bot...")

    app = Application.builder().token(token).build()
    app.add_handler(CommandHandler("next", next_train))

    logging.info("Bot started. Waiting for /next commands...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
