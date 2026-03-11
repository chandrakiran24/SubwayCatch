# SubwayCatch Telegram Bot

Telegram bot that returns real-time NYC MTA subway arrivals via:

```text
/next <train> <station_id>
```

Example:

```text
/next D 635
```

## 1) Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Add values for:
- `TELEGRAM_BOT_TOKEN`
- `MTA_API_KEY`

## 2) Run

```bash
python3 bot.py
```

## Notes
- Train lines are validated dynamically from a mapping in `bot.py`.
- Station IDs are user-provided and not hardcoded.
- GTFS-RT protobuf parsing uses `gtfs-realtime-bindings`.
