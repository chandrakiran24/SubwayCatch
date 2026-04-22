#!/usr/bin/env python3
"""Static station metadata loading and lookup helpers."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, TypedDict, cast

logger = logging.getLogger("subway_bot.metadata")


class StationInfo(TypedDict):
    """Minimal station metadata shape."""

    name: str
    routes: List[str]
    north: str
    south: str


DATA_PATH = Path(__file__).resolve().parent / "data" / "stations.json"
STATION_CONFIG_PATH = Path(__file__).resolve().parent / "data" / "station_config.json"


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


def _validate_config(metadata: Dict[str, StationInfo], aliases: Dict[str, List[str]]) -> None:
    """Validate alias mapping against known metadata stop IDs."""
    unknown: List[str] = []
    ambiguous: List[str] = []

    for alias, prefixes in aliases.items():
        station_names = {
            metadata[prefix]["name"].strip().lower()
            for prefix in prefixes
            if prefix in metadata
        }
        for prefix in prefixes:
            if prefix not in metadata:
                unknown.append(f"{alias}->{prefix}")
        if len(station_names) > 1:
            ambiguous.append(alias)

    if unknown:
        logger.warning(
            "station_alias_to_stop_prefixes references %d unknown stop IDs (sample=%s)",
            len(unknown),
            unknown[:20],
        )
    if ambiguous:
        logger.warning(
            "station_alias_to_stop_prefixes has %d aliases mapped to multiple station names (sample=%s)",
            len(ambiguous),
            ambiguous[:20],
        )


STATION_METADATA = _load_station_metadata()
ROUTE_TO_STATIONS = _build_route_index(STATION_METADATA)


def _load_station_config(path: Path = STATION_CONFIG_PATH) -> Dict[str, Dict[str, object]]:
    """Load alias/station presentation config."""
    with path.open("r", encoding="utf-8") as f:
        raw = json.load(f)

    if not isinstance(raw, dict):
        raise ValueError(f"Expected station config object; got {type(raw)!r}")

    important = raw.get("important_stations", {})
    aliases = raw.get("station_alias_to_stop_prefixes", {})
    if not isinstance(important, dict) or not isinstance(aliases, dict):
        raise ValueError("station_config.json must contain important_stations and station_alias_to_stop_prefixes objects")

    normalized_important: Dict[str, Dict[str, str]] = {}
    for borough, stations in important.items():
        if not isinstance(stations, dict):
            continue
        normalized_important[str(borough)] = {
            str(code).upper().strip(): str(name).strip()
            for code, name in stations.items()
            if str(code).strip() and str(name).strip()
        }

    normalized_aliases: Dict[str, List[str]] = {}
    for alias, prefixes in aliases.items():
        if not isinstance(prefixes, list):
            continue
        normalized_aliases[str(alias).upper().strip()] = sorted(
            {str(prefix).upper().strip() for prefix in prefixes if str(prefix).strip()}
        )

    logger.info(
        "Loaded station config boroughs=%s aliases=%s from %s",
        len(normalized_important),
        len(normalized_aliases),
        path,
    )

    return {
        "important_stations": normalized_important,
        "station_alias_to_stop_prefixes": normalized_aliases,
    }


STATION_CONFIG = _load_station_config()
IMPORTANT_STATIONS = cast(Dict[str, Dict[str, str]], STATION_CONFIG["important_stations"])
STATION_ALIAS_TO_STOP_PREFIXES = cast(Dict[str, List[str]], STATION_CONFIG["station_alias_to_stop_prefixes"])
_validate_config(STATION_METADATA, STATION_ALIAS_TO_STOP_PREFIXES)


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
