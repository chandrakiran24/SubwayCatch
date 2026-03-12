# NYC Subway Arrival Telegram Bot

A production-ready Telegram bot that returns real-time NYC subway arrivals using MTA GTFS-Realtime feeds.

## Features
- `/start` welcome message
- `/help` usage instructions
- `/stationid` station-code directory (grouped by borough)
- `/next <train> <station_code>` for next 2 arrivals
- Graceful errors for invalid train lines, station codes, missing params, API issues, and timeouts

## Requirements
- Python 3.10+
- Telegram bot token
- MTA API key

## Installation
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Environment Variables
Set these in your shell or `.env` file:

```bash
export TELEGRAM_BOT_TOKEN="your_token"
export MTA_API_KEY="your_mta_api_key"
```

## Run Locally
```bash
python3 bot.py
```

## Deploy to Cloud
The bot is stateless and uses polling, so it can run on platforms like Render, Railway, Fly.io, or a VM.

1. Provision a Python service/container.
2. Install dependencies with `pip install -r requirements.txt`.
3. Set `TELEGRAM_BOT_TOKEN` and `MTA_API_KEY` in environment settings.
4. Start command: `python3 bot.py`.

### Render-specific notes
- Prefer a **Background Worker** service for polling bots.
- If you deploy as a **Web Service**, this project opens a minimal health endpoint on `PORT` (returns `ok`) so Render sees an open port.
- If you see `telegram.error.Conflict: terminated by other getUpdates request`, another process is using the same bot token in polling mode. Stop the duplicate instance (local machine/another deploy) so only one poller remains.

### Startup reliability on cloud platforms
If Telegram is temporarily unreachable during deploy/startup, the bot now retries startup automatically instead of crashing.

Optional env vars:

- `BOT_STARTUP_RETRY_DELAY_SECONDS` (default: `10`)
- `BOT_STARTUP_MAX_RETRIES` (default: `0`, meaning infinite retries)

## Example Usage
- `/next D HS34`
- `/next A WTC`
- `/stationid`
