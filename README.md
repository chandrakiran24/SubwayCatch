# NYC Subway Arrival Telegram Bot

A production-ready Telegram bot that returns real-time NYC subway arrivals using MTA GTFS-Realtime feeds.

## Features
- `/start` welcome message
- `/help` usage instructions
- `/stationid` station-code directory (grouped by borough)
- `/next <station_code> [uptown|downtown|both]` for all trains at a station (next 2 arrivals per train)
- Graceful errors for invalid directions, station codes, missing params, API issues, and timeouts

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

### Startup reliability on cloud platforms
If Telegram is temporarily unreachable during deploy/startup, the bot now retries startup automatically instead of crashing.

Optional env vars:

- `BOT_STARTUP_RETRY_DELAY_SECONDS` (default: `10`)
- `BOT_STARTUP_MAX_RETRIES` (default: `0`, meaning infinite retries)

## Example Usage
- `/next HS34`
- `/next CI uptown`
- `/stationid`

### Render deployment note
If you deploy this bot as a **Render Web Service**, Render requires the process to bind an HTTP port.
The bot now starts a lightweight health server automatically when `PORT` is set, so `python3 bot.py` works on Render web services while polling Telegram updates.

If you prefer not to expose an HTTP port at all, deploy as a **Background Worker** instead.
