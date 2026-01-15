# airflow/ingestion/velib_parser.py
from datetime import datetime, timezone
from typing import List, Dict
import uuid


def ts_from_epoch(epoch: int) -> datetime:
    return datetime.fromtimestamp(epoch, tz=timezone.utc)


def parse_snapshot_metadata(payload: dict) -> dict:
    return {
        "snapshot_id": str(uuid.uuid4()),
        "snapshot_ts": ts_from_epoch(payload["lastUpdatedOther"]),
        "raw_payload": payload,
    }


def parse_stations(payload: dict, snapshot_id: str) -> List[Dict]:
    rows = []
    for s in payload["data"]["stations"]:
        rows.append(
            {
                "snapshot_id": snapshot_id,
                "station_id": int(s["station_id"]),
                "station_code": s.get("stationCode"),
                "name": s.get("name"),
                "capacity": s.get("capacity"),
                "lat": s["lat"],
                "lon": s["lon"],
            }
        )
    return rows


def parse_station_status(payload: dict, snapshot_id: str) -> List[Dict]:
    rows = []
    for s in payload["data"]["stations"]:
        rows.append(
            {
                "snapshot_id": snapshot_id,
                "station_id": int(s["station_id"]),
                "bikes_available": s.get("num_bikes_available"),
                "ebikes_available": s["num_bikes_available_types"][1]["count"],
                "mechanical_available": s["num_bikes_available_types"][0]["count"],
                "docks_available": s.get("num_docks_available"),
                "is_installed": s.get("is_installed") == 1,
                "is_renting": s.get("is_renting") == 1,
                "is_returning": s.get("is_returning") == 1,
                "station_last_reported_ts": ts_from_epoch(s["last_reported"]),
            }
        )
    return rows
