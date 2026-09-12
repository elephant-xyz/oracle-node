#!/usr/bin/env python3
"""Stamp canonical elephant_uuid / elephant_token onto already-ingested parcels."""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
from opendoor.identity import canonicalize_token, canonicalize_uuid, identity_from_opendoor, stamp_parcel_identity  # noqa: E402


def backfill_county(seed_path: Path, out_dir: Path) -> dict:
    rows = list(csv.DictReader(seed_path.open()))
    stamped = 0
    missing = 0
    mint_mismatch = 0
    rewritten = []
    for row in rows:
        identity = identity_from_opendoor(row)
        if not identity["mint_agrees"]:
            mint_mismatch += 1
        row["elephant_uuid"] = identity["elephant_uuid"]
        row["elephant_token"] = identity["elephant_token"]
        rewritten.append(row)
        parcel_dir = out_dir / row["parcel_id"]
        if not (parcel_dir / "data" / "property.json").exists():
            missing += 1
            continue
        stamp_parcel_identity(parcel_dir, identity)
        stamped += 1
    if rewritten:
        with seed_path.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rewritten[0].keys()))
            writer.writeheader()
            writer.writerows(rewritten)
    return {
        "seed": str(seed_path),
        "seed_rows": len(rows),
        "stamped": stamped,
        "missing_property_json": missing,
        "mint_mismatch": mint_mismatch,
    }


def main() -> None:
    reports = []
    reports.append(
        backfill_county(
            ROOT / "data/seeds/sarasota.csv",
            ROOT / "downloads/sarasota/local-ingest",
        )
    )
    print(json.dumps(reports, indent=2))


if __name__ == "__main__":
    main()
