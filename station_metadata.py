#!/usr/bin/env python3
"""Static station metadata loading and lookup helpers."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, TypedDict

logger = logging.getLogger("subway_bot.metadata")


class StationInfo(TypedDict):
    """Minimal station metadata shape."""

    name: str
    routes: List[str]
    north: str
    south: str


DATA_PATH = Path(__file__).resolve().parent / "data" / "stations.json"


def _load_station_metadata(path: Path = DATA_PATH) -> Dict[str, StationInfo]:
    """Load station metadata once at startup for O(1) lookups."""
    with path.open("r", encoding="utf-8") as f:
        raw = json.load(f)

    if not isinstance(raw, dict):
        raise ValueError(f"Expected station metadata object keyed by stop ID; got {type(raw)!r}")

    metadata: Dict[str, StationInfo] = {}
    for gtfs_stop_id, info in raw.items():
        metadata[str(gtfs_stop_id)] = {
            "name": str(info.get("name", "")).strip(),
            "routes": [str(route).strip().upper() for route in info.get("routes", []) if str(route).strip()],
            "north": str(info.get("north", "")).strip(),
            "south": str(info.get("south", "")).strip(),
        }

    logger.info("Loaded station metadata records=%s from %s", len(metadata), path)
    return metadata


def _build_route_index(metadata: Dict[str, StationInfo]) -> Dict[str, List[str]]:
    """Build a reverse index route -> list of gtfs_stop_ids."""
    route_to_stations: Dict[str, List[str]] = {}
    for gtfs_stop_id, info in metadata.items():
        for route in info["routes"]:
            route_to_stations.setdefault(route, []).append(gtfs_stop_id)

    for route in route_to_stations:
        route_to_stations[route].sort()

    return route_to_stations


STATION_METADATA = _load_station_metadata()
ROUTE_TO_STATIONS = _build_route_index(STATION_METADATA)


def get_station_info(gtfs_stop_id: str) -> Optional[StationInfo]:
    """Get station metadata by GTFS stop id."""
    return STATION_METADATA.get(str(gtfs_stop_id).strip())


def get_station_routes(gtfs_stop_id: str) -> List[str]:
    """Get route list for a station."""
    info = get_station_info(gtfs_stop_id)
    return info["routes"] if info else []


def get_direction_labels(gtfs_stop_id: str) -> Optional[Dict[str, str]]:
    """Get north/south direction labels for a station."""
    info = get_station_info(gtfs_stop_id)
    if not info:
        return None
    return {"north": info["north"], "south": info["south"]}


def get_stations_for_route(route: str) -> List[str]:
    """Get GTFS stop IDs served by a route from reverse index."""
    return ROUTE_TO_STATIONS.get(route.strip().upper(), [])
