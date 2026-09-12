#!/usr/bin/env python3
"""Validate targeted local-ingestion artifacts before Parquet export."""

from __future__ import annotations

import csv
import json
import re
import sys
import urllib.parse
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
from opendoor.local_ingest import (  # noqa: E402
    COUNTIES,
    completion_manifest_valid,
)

DEFAULT_COUNTIES = ("baker", "st-lucie", "highlands", "citrus")


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.is_file():
        return []
    with path.open() as handle:
        return list(csv.DictReader(handle))


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text())
    if not isinstance(payload, dict):
        raise ValueError(f"{path.name} is not an object")
    return payload


def _normalized_identifier(value: Any) -> str:
    return re.sub(r"[^A-Z0-9]+", "", str(value).upper())


def _request_url(request: Any) -> str:
    if not isinstance(request, dict) or not request.get("url"):
        return ""
    url = str(request["url"])
    query = request.get("multiValueQueryString")
    if not isinstance(query, dict):
        return url
    pairs = [
        (key, value)
        for key, values in query.items()
        for value in (values if isinstance(values, list) else [values])
    ]
    return f"{url}?{urllib.parse.urlencode(pairs)}"


def _validate_row(
    slug: str,
    row: dict[str, str],
    parcel_dir: Path,
) -> list[str]:
    errors: list[str] = []
    cfg = COUNTIES[slug]
    try:
        property_record = _load_json(parcel_dir / "data" / "property.json")
        identity = _load_json(parcel_dir / "identity.json")
    except (FileNotFoundError, json.JSONDecodeError, ValueError) as error:
        return [str(error)]
    try:
        source_payload = _load_json(parcel_dir / "source_payload.json")
    except (FileNotFoundError, json.JSONDecodeError, ValueError):
        source_payload = {}
    supported_partial = source_payload.get("availability") == "supported_partial"

    expected = {
        "request_identifier": row["parcel_id"],
        "county_name": cfg["display"],
        "elephant_uuid": row["elephant_uuid"],
        "elephant_token": row["elephant_token"],
    }
    for field, value in expected.items():
        if property_record.get(field) != value:
            errors.append(f"property.{field} disagrees with seed")
    property_identifier_field = cfg.get(
        "property_identifier_field",
        "parcel_id",
    )
    if _normalized_identifier(property_record.get("parcel_identifier")) != (
        _normalized_identifier(row[property_identifier_field])
    ):
        errors.append("property.parcel_identifier disagrees with seed")
    if not property_record.get("property_type") and not supported_partial:
        errors.append("property.property_type is empty")
    if not property_record.get("property_usage_type") and not supported_partial:
        errors.append("property.property_usage_type is empty")
    if _request_url(property_record.get("source_http_request")) != row["url"]:
        errors.append("property.source_http_request disagrees with seed")
    if identity.get("elephant_uuid") != row["elephant_uuid"]:
        errors.append("identity.elephant_uuid disagrees with seed")
    if identity.get("elephant_token") != row["elephant_token"]:
        errors.append("identity.elephant_token disagrees with seed")
    if identity.get("mint_agrees") is not True:
        errors.append("identity canonical mint does not agree")
    if not completion_manifest_valid(parcel_dir, row, cfg):
        errors.append("completion manifest or artifact hashes are invalid")
    return errors


def reconcile_county(slug: str) -> dict[str, Any]:
    seed_path = ROOT / "data" / "seeds" / f"{slug}.csv"
    unmatched_path = (
        ROOT / "data" / "artifacts" / f"{slug}-opendoor" / "unmatched.csv"
    )
    ingest_dir = ROOT / "downloads" / slug / "local-ingest"
    rows = _read_csv(seed_path)
    unmatched = _read_csv(unmatched_path)
    invalid = []
    subdivision_non_null = 0
    for row in rows:
        parcel_dir = ingest_dir / row["parcel_id"]
        errors = _validate_row(slug, row, parcel_dir)
        if errors:
            invalid.append({"parcel_id": row["parcel_id"], "errors": errors})
            continue
        property_record = _load_json(parcel_dir / "data" / "property.json")
        subdivision_non_null += bool(property_record.get("subdivision"))

    request_ids = [row["parcel_id"] for row in rows]
    uuids = [row["elephant_uuid"] for row in rows]
    tokens = [row["elephant_token"] for row in rows]
    duplicate_identity = (
        len(set(request_ids)) != len(rows)
        or len(set(uuids)) != len(rows)
        or len(set(tokens)) != len(rows)
    )
    status = "PASS" if not invalid and not duplicate_identity else "FAIL"
    return {
        "status": status,
        "selectedSourceCount": len(rows) + len(unmatched),
        "seedCount": len(rows),
        "unmatchedCount": len(unmatched),
        "validCount": len(rows) - len(invalid),
        "invalidCount": len(invalid),
        "distinctRequestIdentifiers": len(set(request_ids)),
        "distinctElephantUuids": len(set(uuids)),
        "distinctElephantTokens": len(set(tokens)),
        "subdivisionNonNull": subdivision_non_null,
        "unmatched": unmatched,
        "invalid": invalid,
    }


def main() -> None:
    counties = tuple(sys.argv[1:]) or DEFAULT_COUNTIES
    unknown = [slug for slug in counties if slug not in COUNTIES]
    if unknown:
        raise SystemExit(f"unknown county: {', '.join(unknown)}")
    county_reports = {slug: reconcile_county(slug) for slug in counties}
    report = {
        "status": (
            "PASS"
            if all(report["status"] == "PASS" for report in county_reports.values())
            else "FAIL"
        ),
        "counties": county_reports,
        "totals": {
            "selectedSourceCount": sum(
                report["selectedSourceCount"] for report in county_reports.values()
            ),
            "seedCount": sum(report["seedCount"] for report in county_reports.values()),
            "unmatchedCount": sum(
                report["unmatchedCount"] for report in county_reports.values()
            ),
            "validCount": sum(report["validCount"] for report in county_reports.values()),
            "invalidCount": sum(
                report["invalidCount"] for report in county_reports.values()
            ),
        },
    }
    output = ROOT / "data" / "artifacts" / "opendoor-wave3-ingest-reconciliation.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))
    if report["status"] != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
