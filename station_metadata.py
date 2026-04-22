#!/usr/bin/env python3
"""Static station metadata loading and lookup helpers."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Set, TypedDict, cast

logger = logging.getLogger("subway_bot.metadata")


class StationInfo(TypedDict):
    """Minimal station metadata shape."""

    name: str
    routes: List[str]
    north: str
    south: str


class ComplexInfo(TypedDict):
    """Transfer complex metadata."""

    name: str
    lines: List[str]
    note: str


DATA_PATH = Path(__file__).resolve().parent / "data" / "stations.json"
STATION_CONFIG_PATH = Path(__file__).resolve().parent / "data" / "station_config.json"


# ---------------------------------------------------------------------------
# stations.json loader
# ---------------------------------------------------------------------------


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
            "routes": [
                str(route).strip().upper()
                for route in info.get("routes", [])
                if str(route).strip()
            ],
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


# ---------------------------------------------------------------------------
# station_config.json loader
# ---------------------------------------------------------------------------


def _load_station_config(path: Path = STATION_CONFIG_PATH) -> Dict[str, object]:
    """Load alias/station presentation config, complexes, and geographic order."""
    with path.open("r", encoding="utf-8") as f:
        raw = json.load(f)

    if not isinstance(raw, dict):
        raise ValueError(f"Expected station config object; got {type(raw)!r}")

    # --- important_stations ---
    important = raw.get("important_stations", {})
    if not isinstance(important, dict):
        raise ValueError("station_config.json must contain important_stations object")

    normalized_important: Dict[str, Dict[str, str]] = {}
    for borough, stations in important.items():
        if not isinstance(stations, dict):
            continue
        normalized_important[str(borough)] = {
            str(code).upper().strip(): str(name).strip()
            for code, name in stations.items()
            if str(code).strip() and str(name).strip()
        }

    # --- station_alias_to_stop_prefixes ---
    aliases_raw = raw.get("station_alias_to_stop_prefixes", {})
    if not isinstance(aliases_raw, dict):
        raise ValueError("station_config.json must contain station_alias_to_stop_prefixes object")

    normalized_aliases: Dict[str, List[str]] = {}
    for alias, prefixes in aliases_raw.items():
        if not isinstance(prefixes, list):
            continue
        normalized_aliases[str(alias).upper().strip()] = sorted(
            {str(p).upper().strip() for p in prefixes if str(p).strip()}
        )

    # --- station_complexes ---
    complexes_raw = raw.get("station_complexes", {})
    normalized_complexes: Dict[str, ComplexInfo] = {}
    if isinstance(complexes_raw, dict):
        for cid, cdata in complexes_raw.items():
            if not isinstance(cdata, dict):
                continue
            normalized_complexes[str(cid).strip()] = {
                "name": str(cdata.get("name", "")).strip(),
                "lines": [
                    str(l).strip().upper()
                    for l in cdata.get("lines", [])
                    if str(l).strip()
                ],
                "note": str(cdata.get("note", "")).strip(),
            }

    # --- alias_to_complex ---
    a2c_raw = raw.get("alias_to_complex", {})
    normalized_a2c: Dict[str, str] = {}
    if isinstance(a2c_raw, dict):
        for alias, cid in a2c_raw.items():
            a = str(alias).upper().strip()
            c = str(cid).strip()
            if a and c:
                normalized_a2c[a] = c

    # --- borough_station_order ---
    bso_raw = raw.get("borough_station_order", {})
    normalized_bso: Dict[str, List[str]] = {}
    if isinstance(bso_raw, dict):
        for borough, order_list in bso_raw.items():
            if not isinstance(order_list, list):
                continue
            normalized_bso[str(borough)] = [
                str(a).upper().strip()
                for a in order_list
                if str(a).strip()
            ]

    logger.info(
        "Loaded station config boroughs=%s aliases=%s complexes=%s from %s",
        len(normalized_important),
        len(normalized_aliases),
        len(normalized_complexes),
        path,
    )

    return {
        "important_stations": normalized_important,
        "station_alias_to_stop_prefixes": normalized_aliases,
        "station_complexes": normalized_complexes,
        "alias_to_complex": normalized_a2c,
        "borough_station_order": normalized_bso,
    }


# ---------------------------------------------------------------------------
# Pre-computed alias -> line index
# ---------------------------------------------------------------------------


def _build_alias_line_index(
    aliases: Dict[str, List[str]],
    metadata: Dict[str, StationInfo],
) -> Dict[str, List[str]]:
    """
    Pre-compute alias -> sorted line list at startup so /stationid and
    build_arrival_message never pay the scan cost at request time.

    Sort order: numeric trains (1-7 ascending) then alphabetic (A, B, C...).
    """
    index: Dict[str, List[str]] = {}
    for alias, prefixes in aliases.items():
        all_lines: Set[str] = set()
        for prefix in prefixes:
            info = metadata.get(prefix)
            if info:
                all_lines.update(info["routes"])
        numeric = sorted((l for l in all_lines if l.isdigit()), key=int)
        alpha = sorted(l for l in all_lines if not l.isdigit())
        index[alias] = numeric + alpha
    return index


def _validate_config(
    metadata: Dict[str, StationInfo],
    aliases: Dict[str, List[str]],
    complexes: Dict[str, ComplexInfo],
    a2c: Dict[str, str],
) -> None:
    """Warn at startup about data integrity issues; never raises."""
    unknown_prefixes: List[str] = []
    for alias, prefixes in aliases.items():
        for prefix in prefixes:
            if prefix not in metadata:
                unknown_prefixes.append(f"{alias}->{prefix}")
    if unknown_prefixes:
        logger.warning(
            "station_alias_to_stop_prefixes references %d unknown stop IDs "
            "(first 20): %s",
            len(unknown_prefixes),
            unknown_prefixes[:20],
        )

    unknown_complexes: List[str] = [
        alias for alias, cid in a2c.items() if cid not in complexes
    ]
    if unknown_complexes:
        logger.warning(
            "alias_to_complex references unknown complex IDs for aliases: %s",
            unknown_complexes,
        )


# ---------------------------------------------------------------------------
# Module-level singletons (loaded once at import time)
# ---------------------------------------------------------------------------

STATION_METADATA: Dict[str, StationInfo] = _load_station_metadata()
ROUTE_TO_STATIONS: Dict[str, List[str]] = _build_route_index(STATION_METADATA)

_config = _load_station_config()

IMPORTANT_STATIONS = cast(Dict[str, Dict[str, str]], _config["important_stations"])
STATION_ALIAS_TO_STOP_PREFIXES = cast(Dict[str, List[str]], _config["station_alias_to_stop_prefixes"])
STATION_COMPLEXES = cast(Dict[str, ComplexInfo], _config["station_complexes"])
ALIAS_TO_COMPLEX = cast(Dict[str, str], _config["alias_to_complex"])
BOROUGH_STATION_ORDER = cast(Dict[str, List[str]], _config["borough_station_order"])

# Pre-computed at import; O(1) access everywhere downstream.
ALIAS_TO_LINES: Dict[str, List[str]] = _build_alias_line_index(
    STATION_ALIAS_TO_STOP_PREFIXES, STATION_METADATA
)

_validate_config(STATION_METADATA, STATION_ALIAS_TO_STOP_PREFIXES, STATION_COMPLEXES, ALIAS_TO_COMPLEX)


# ---------------------------------------------------------------------------
# Public helper API
# ---------------------------------------------------------------------------


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

# NEW — pass all four required arguments
_validate_config(STATION_METADATA, STATION_ALIAS_TO_STOP_PREFIXES, STATION_COMPLEXES, ALIAS_TO_COMPLEX)


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


def get_complex_for_alias(alias: str) -> Optional[ComplexInfo]:
    """
    Return the ComplexInfo for this alias if it belongs to a named transfer
    complex, or None otherwise. O(1).
    """
    complex_id = ALIAS_TO_COMPLEX.get(alias.upper().strip())
    if complex_id:
        return STATION_COMPLEXES.get(complex_id)
    return None


def get_lines_for_alias(alias: str) -> List[str]:
    """
    Return all train lines served at this alias (pre-computed at startup).
    Returns an empty list for unknown aliases. O(1).
    """
    return ALIAS_TO_LINES.get(alias.upper().strip(), [])
