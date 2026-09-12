#!/usr/bin/env python3
"""Match OpenDoor Wave 1 addresses to official county GIS parcel ids."""

from __future__ import annotations

import csv
import json
import re
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
from opendoor.identity import identity_from_opendoor  # noqa: E402

SOURCE = ROOT.parent / "opendoor-unmatched-unavailable-counties.csv"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
SUFFIXES = {
    "st": "st",
    "street": "st",
    "dr": "dr",
    "drive": "dr",
    "ct": "ct",
    "court": "ct",
    "cir": "cir",
    "circle": "cir",
    "pl": "pl",
    "place": "pl",
    "ave": "ave",
    "avenue": "ave",
    "ln": "ln",
    "lane": "ln",
    "blvd": "blvd",
    "boulevard": "blvd",
    "rd": "rd",
    "road": "rd",
    "ter": "ter",
    "terr": "ter",
    "terrace": "ter",
    "way": "way",
    "pkwy": "pkwy",
    "parkway": "pkwy",
    "hwy": "hwy",
    "highway": "hwy",
    "trl": "trl",
    "trail": "trl",
    "pt": "pt",
    "point": "pt",
    "run": "run",
    "loop": "loop",
    "pass": "pass",
    "xing": "xing",
    "crossing": "xing",
    "mnr": "manor",
    "manor": "manor",
    "ter": "ter",
    "terr": "ter",
    "terrace": "ter",
    "trce": "trce",
    "trace": "trce",
    "cv": "cove",
    "cove": "cove",
    "lndg": "landing",
    "landing": "landing",
    "cir": "cir",
    "circle": "cir",
    "pl": "pl",
    "place": "pl",
    "ct": "ct",
    "court": "ct",
    "dr": "dr",
    "drive": "dr",
    "ave": "ave",
    "avenue": "ave",
    "ln": "ln",
    "lane": "ln",
    "blvd": "blvd",
    "boulevard": "blvd",
    "rd": "rd",
    "road": "rd",
    "hwy": "hwy",
    "highway": "hwy",
    "pkwy": "pkwy",
    "parkway": "pkwy",
    "way": "way",
    "loop": "loop",
}
DIRECTIONS = {"n", "s", "e", "w", "ne", "nw", "se", "sw", "north", "south", "east", "west"}
DIR_CANON = {
    "north": "n",
    "south": "s",
    "east": "e",
    "west": "w",
}

def _pasco_url(row: dict) -> str:
    return extract_href(row.get("URL2")).replace("http://", "https://")


def _clay_street(row: dict) -> str:
    parts = [row.get("HOUSE_NO"), row.get("ST_DIR"), row.get("STREET"), row.get("ST_MD")]
    return " ".join(str(part).strip() for part in parts if part)


def _clay_url(row: dict) -> str:
    key = row.get("PIN_DSP") or row.get("PIN") or ""
    return (
        "https://qpublic.schneidercorp.com/Application.aspx"
        f"?AppID=830&LayerID=15008&PageTypeID=4&PageID=6754&KeyValue={key}"
    )


def _osceola_street(row: dict) -> str:
    parts = [
        row.get("StreetNumb"),
        row.get("StreetPfx"),
        row.get("StreetName"),
        row.get("StreetSfx"),
        row.get("CondoUnit"),
    ]
    return " ".join(str(part).strip() for part in parts if part and str(part).strip())


def _osceola_url(row: dict) -> str:
    key = row.get("Dsp_strap") or row.get("PIN") or ""
    return f"https://www.property-appraiser.org/PropertySearch?parcel={key}" if key else ""


COUNTIES = {
    "pasco": {
        "resolved_county": "Pasco County",
        "fips": "12101",
        "gis_query": "https://maps.pascopa.com/arcgis/rest/services/Parcels/MapServer/3/query",
        "out_fields": "ParcelID,PHYS_STREET,PHYS_CITY,PHYS_ZIP,URL2",
        "primary_where": lambda house, zip5, name: (
            f"PHYS_ZIP='{zip5}' AND PHYS_STREET LIKE '{house}%'"
        ),
        "fallback_where": lambda house, zip5, name: (
            f"UPPER(PHYS_STREET) LIKE '%{house}%{name.replace(chr(39), chr(39)*2).upper()}%'"
        ),
        "street_from": lambda row: (row.get("PHYS_STREET") or "").strip(),
        "parcel_from": lambda row: row.get("ParcelID") or "",
        "url_from": _pasco_url,
    },
    "volusia": {
        "resolved_county": "Volusia County",
        "fips": "12127",
        "gis_query": "https://maps2.vcgov.org/arcgis/rest/services/Pictometry_Parcels/MapServer/0/query",
        "out_fields": "ALTKEY,PID,ADRNO,ADRDIR,ADRSTR,ADRSUF,UNITNO,CITYNAME,ADDRFULL,ZIP1,SUBDIVISION",
        "primary_where": lambda house, zip5, name: f"ZIP1='{zip5}' AND ADRNO={house}",
        "fallback_where": lambda house, zip5, name: (
            f"ADRNO={house} AND UPPER(ADRSTR) LIKE '%{name.replace(chr(39), chr(39)*2).upper()}%'"
        ),
        "street_from": lambda row: (row.get("ADDRFULL") or "").strip(),
        "parcel_from": lambda row: str(int(row["ALTKEY"])) if row.get("ALTKEY") is not None else "",
        "url_from": lambda row: (
            f"https://paproapp.vcgov.org/search/real-property/{int(row['ALTKEY'])}"
            if row.get("ALTKEY") is not None
            else ""
        ),
    },
    "clay": {
        "resolved_county": "Clay County",
        "fips": "12019",
        "gis_query": "https://maps.clayutility.org/server/rest/services/Base_LGIM/MapServer/29/query",
        "out_fields": "PIN,PIN_DSP,HOUSE_NO,STREET,ST_MD,ST_DIR,ST_UNIT,ST_CITY,ST_ZIP5,LEGL1,OWNER_NAME",
        "primary_where": lambda house, zip5, name: (
            f"ST_ZIP5='{zip5}' AND HOUSE_NO='{house}'"
        ),
        "fallback_where": lambda house, zip5, name: (
            f"HOUSE_NO='{house}' AND UPPER(STREET) LIKE '%{name.replace(chr(39), chr(39)*2).upper()}%'"
        ),
        "street_from": _clay_street,
        "parcel_from": lambda row: row.get("PIN_DSP") or row.get("PIN") or "",
        "url_from": _clay_url,
    },
    "lake": {
        "resolved_county": "Lake County",
        "fips": "12069",
        "gis_query": "https://maps.floridahealth.gov/server/rest/services/EHWATER/Parcels/MapServer/33/query",
        "out_fields": "PARCEL_ID,ALT_KEY,PHY_ADDR1,PHY_CITY,PHY_ZIPCD,S_LEGAL",
        "primary_where": lambda house, zip5, name: _health_where(house, zip5, name),
        "fallback_where": lambda house, zip5, name: _health_fallback(house, zip5, name),
        "street_from": lambda row: " ".join((row.get("PHY_ADDR1") or "").split()),
        "parcel_from": lambda row: str(row.get("ALT_KEY") or row.get("PARCEL_ID") or ""),
        "url_from": lambda row: (
            f"https://www.lakecopropappr.com/property-details.aspx?AltKey={row.get('ALT_KEY')}"
            if row.get("ALT_KEY")
            else ""
        ),
    },
    "osceola": {
        "resolved_county": "Osceola County",
        "fips": "12097",
        "gis_query": "https://gis.osceola.org/hosting/rest/services/Parcels/MapServer/3/query",
        "out_fields": "PIN,Dsp_strap,StreetNumb,StreetPfx,StreetName,StreetSfx,LocZip,CondoUnit,SubName",
        "primary_where": lambda house, zip5, name: (
            f"LocZip LIKE '{zip5}%' AND StreetNumb='{house}'"
        ),
        "fallback_where": lambda house, zip5, name: (
            f"StreetNumb='{house}' AND UPPER(StreetName) LIKE '%{_name_token(name)}%'"
            if _name_token(name)
            else f"StreetNumb='{house}'"
        ),
        "street_from": _osceola_street,
        "parcel_from": lambda row: row.get("Dsp_strap") or row.get("PIN") or "",
        "url_from": _osceola_url,
    },
    "st-johns": {
        "resolved_county": "St. Johns County",
        "fips": "12109",
        "gis_query": "https://maps.floridahealth.gov/server/rest/services/EHWATER/Parcels/MapServer/54/query",
        "out_fields": "PARCEL_ID,ALT_KEY,PHY_ADDR1,PHY_CITY,PHY_ZIPCD,S_LEGAL",
        "primary_where": lambda house, zip5, name: _health_where(house, zip5, name),
        "fallback_where": lambda house, zip5, name: _health_fallback(house, zip5, name),
        "street_from": lambda row: " ".join((row.get("PHY_ADDR1") or "").split()),
        "parcel_from": lambda row: str(row.get("PARCEL_ID") or row.get("ALT_KEY") or "").strip(),
        "url_from": lambda row: (
            "https://qpublic.schneidercorp.com/Application.aspx"
            f"?AppID=1207&LayerID=23272&PageTypeID=4&PageID=9728&KeyValue={row.get('PARCEL_ID')}"
            if row.get("PARCEL_ID")
            else ""
        ),
    },
    "marion": {
        "resolved_county": "Marion County",
        "fips": "12083",
        "gis_query": "https://maps.floridahealth.gov/server/rest/services/EHWATER/Parcels/MapServer/40/query",
        "out_fields": "PARCEL_ID,ALT_KEY,PHY_ADDR1,PHY_CITY,PHY_ZIPCD,S_LEGAL",
        "primary_where": lambda house, zip5, name: _health_where(house, zip5, name),
        "fallback_where": lambda house, zip5, name: _health_fallback(house, zip5, name),
        "street_from": lambda row: " ".join((row.get("PHY_ADDR1") or "").split()),
        "parcel_from": lambda row: str(row.get("ALT_KEY") or row.get("PARCEL_ID") or "").strip(),
        "url_from": lambda row: (
            f"https://www.pa.marionfl.org/Property/Details?AltKey={row.get('ALT_KEY')}"
            if row.get("ALT_KEY")
            else ""
        ),
    },
    "hernando": {
        "resolved_county": "Hernando County",
        "fips": "12053",
        "gis_query": "https://maps.floridahealth.gov/server/rest/services/EHWATER/Parcels/MapServer/25/query",
        "out_fields": "PARCEL_ID,ALT_KEY,PHY_ADDR1,PHY_CITY,PHY_ZIPCD,S_LEGAL",
        "primary_where": lambda house, zip5, name: _health_where(house, zip5, name),
        "fallback_where": lambda house, zip5, name: _health_fallback(house, zip5, name),
        "street_from": lambda row: " ".join((row.get("PHY_ADDR1") or "").split()),
        "parcel_from": lambda row: str(row.get("ALT_KEY") or row.get("PARCEL_ID") or "").strip(),
        "url_from": lambda row: (
            f"https://www.hernandopa-fl.us/propertysearch?parid={row.get('PARCEL_ID')}"
            if row.get("PARCEL_ID")
            else ""
        ),
    },
    "manatee": {
        "resolved_county": "Manatee County",
        "fips": "12081",
        "gis_query": "https://maps.floridahealth.gov/server/rest/services/EHWATER/Parcels/MapServer/39/query",
        "out_fields": "PARCEL_ID,ALT_KEY,PHY_ADDR1,PHY_CITY,PHY_ZIPCD,S_LEGAL",
        "primary_where": lambda house, zip5, name: _health_where(house, zip5, name),
        "fallback_where": lambda house, zip5, name: _health_fallback(house, zip5, name),
        "street_from": lambda row: " ".join((row.get("PHY_ADDR1") or "").split()),
        "parcel_from": lambda row: str(row.get("PARCEL_ID") or row.get("ALT_KEY") or "").strip(),
        "url_from": lambda row: (
            f"https://www.manateepao.gov/parcel/?parid={row.get('PARCEL_ID')}"
            if row.get("PARCEL_ID")
            else ""
        ),
    },
}


def _name_token(name: str) -> str:
    tokens = [part for part in name.split() if len(part) >= 3]
    if not tokens:
        tokens = name.split()
    if not tokens:
        return ""
    return max(tokens, key=len).replace("'", "''").upper()


def _health_where(house: str, zip5: str, name: str) -> str:
    # Zip + house only. A name-token AND drops spaced/compact variants
    # (DESOTA vs DE SOTA, MCCLENDON vs MC CLENDON).
    return f"PHY_ZIPCD={zip5} AND PHY_ADDR1 LIKE '{house}%'"


def _health_fallback(house: str, zip5: str, name: str) -> str:
    token = _name_token(name)
    if token:
        return f"PHY_ADDR1 LIKE '{house}%' AND UPPER(PHY_ADDR1) LIKE '%{token}%'"
    return f"PHY_ADDR1 LIKE '{house}%'"


def normalize_text(value: str | None) -> str:
    if not value:
        return ""
    text = value.lower().replace(".", " ").replace(",", " ")
    text = re.sub(r"[^a-z0-9 ]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def parse_street(street: str) -> tuple[str, str, str, str]:
    tokens = normalize_text(street).split()
    house = ""
    if tokens and tokens[0].isdigit():
        house = tokens[0]
        tokens = tokens[1:]
    suffix = ""
    if tokens and tokens[-1] in SUFFIXES:
        suffix = SUFFIXES[tokens[-1]]
        tokens = tokens[:-1]
    direction = ""
    if tokens and tokens[0] in DIRECTIONS:
        direction = DIR_CANON.get(tokens[0], tokens[0])
        tokens = tokens[1:]
    if tokens and tokens[-1] in DIRECTIONS:
        # keep trailing direction as part of the name (e.g. 11th)
        pass
    name = " ".join(tokens)
    return house, direction, name, suffix


def street_key(street: str) -> str:
    house, direction, name, suffix = parse_street(street)
    return " ".join(part for part in (house, direction, name, suffix) if part)


def gis_query(url: str, where: str, out_fields: str) -> list[dict]:
    params = {
        "where": where,
        "outFields": out_fields,
        "returnGeometry": "false",
        "f": "json",
        "resultRecordCount": "200",
    }
    full = url + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(full, headers={"User-Agent": USER_AGENT})
    context = ssl._create_unverified_context() if "gis.osceola.org" in url else None
    last_error: Exception | str | None = None
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=45, context=context) as resp:
                data = json.loads(resp.read().decode())
            if data.get("error"):
                last_error = data["error"]
                time.sleep(0.6 * (attempt + 1))
                continue
            return [feature["attributes"] for feature in data.get("features", [])]
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as error:
            last_error = error
            time.sleep(0.6 * (attempt + 1))
    raise RuntimeError(last_error)


def extract_href(html: str | None) -> str:
    if not html:
        return ""
    match = re.search(r"href=['\"]([^'\"]+)['\"]", html)
    return match.group(1) if match else ""


def _canon_name(name: str, suffix: str) -> str:
    tokens = [SUFFIXES.get(token, token) for token in name.split() if token]
    if suffix:
        tokens.append(SUFFIXES.get(suffix, suffix))
    return " ".join(tokens)


def _edit_distance(left: str, right: str) -> int:
    if left == right:
        return 0
    if abs(len(left) - len(right)) > 2:
        return 99
    prev = list(range(len(right) + 1))
    for i, lch in enumerate(left, start=1):
        curr = [i]
        for j, rch in enumerate(right, start=1):
            curr.append(min(prev[j] + 1, curr[j - 1] + 1, prev[j - 1] + (lch != rch)))
        prev = curr
    return prev[-1]


def score_candidate(gis_street: str, want_street: str, unit: str) -> int:
    want_house, _, want_name, want_suffix = parse_street(want_street)
    got_house, _, got_name, got_suffix = parse_street(gis_street)
    if not want_house or want_house != got_house:
        return 0
    want_canon = _canon_name(want_name, want_suffix)
    got_canon = _canon_name(got_name, got_suffix)
    if not want_canon or not got_canon:
        return 0
    want_compact = want_canon.replace(" ", "")
    got_compact = got_canon.replace(" ", "")
    if want_canon == got_canon or want_compact == got_compact:
        return 100
    if min(len(want_compact), len(got_compact)) >= 5 and _edit_distance(want_compact, got_compact) <= 1:
        return 70
    want_tokens = want_canon.split()
    got_tokens = got_canon.split()
    overlap = [token for token in want_tokens if token in got_tokens or token in got_compact]
    if not overlap:
        return 0
    score = 40
    if all(token in got_tokens or token in got_compact for token in want_tokens):
        score += 40
    elif want_canon in got_canon or got_canon in want_canon:
        score += 20
    elif len(overlap) >= max(1, len(want_tokens) - 1):
        score += 15
    else:
        return 0
    if want_suffix and SUFFIXES.get(want_suffix, want_suffix) == SUFFIXES.get(got_suffix, got_suffix):
        score += 10
    if unit and normalize_text(unit) in normalize_text(gis_street):
        score += 10
    return score


def pick_match(candidates: list[dict], street_from, want_street: str, unit: str) -> tuple[dict | None, str]:
    scored = []
    for row in candidates:
        score = score_candidate(street_from(row), want_street, unit)
        if score > 0:
            scored.append((score, row))
    scored.sort(key=lambda item: item[0], reverse=True)
    if not scored:
        return None, "no_street_match"
    best_score = scored[0][0]
    best = [row for score, row in scored if score == best_score]
    if len(best) != 1:
        return None, "ambiguous"
    if best_score < 50:
        return None, "low_confidence"
    return best[0], "address_match"


def match_county(slug: str, limit: int | None = None, rematch: bool = False) -> dict:
    cfg = COUNTIES[slug]
    rows = [
        row
        for row in csv.DictReader(SOURCE.open())
        if row["resolved_county"] == cfg["resolved_county"]
    ]
    seed_path = ROOT / "data/seeds" / f"{slug}.csv"
    artifact_dir = ROOT / "data/artifacts" / f"{slug}-opendoor"
    existing_seed: list[dict[str, str]] = []
    seen: dict[str, str] = {}
    if rematch:
        unmatched_path = artifact_dir / "unmatched.csv"
        retry_ids = {
            row["elephant_uuid"]
            for row in csv.DictReader(unmatched_path.open())
        }
        rows = [row for row in rows if row["elephant_uuid"] in retry_ids]
        if seed_path.exists():
            existing_seed = list(csv.DictReader(seed_path.open()))
            seen = {row["parcel_id"]: row["elephant_uuid"] for row in existing_seed}
    if limit is not None:
        rows = rows[:limit]
    matches = []
    unmatched = []
    outcomes: Counter[str] = Counter()
    for index, row in enumerate(rows, start=1):
        identity = identity_from_opendoor(row)
        zip5 = (row["postal_code"] or "")[:5]
        house, _, name, _ = parse_street(row["street"])
        candidates: list[dict] = []
        try:
            if zip5 and house:
                candidates = gis_query(
                    cfg["gis_query"],
                    cfg["primary_where"](house, zip5, name),
                    cfg["out_fields"],
                )
            match, reason = pick_match(candidates, cfg["street_from"], row["street"], row["unit"])
            if match is None and house and name:
                candidates = gis_query(
                    cfg["gis_query"],
                    cfg["fallback_where"](house, zip5, name),
                    cfg["out_fields"],
                )
                match, reason = pick_match(candidates, cfg["street_from"], row["street"], row["unit"])
                if match is not None:
                    reason = "address_match_no_zip"
        except Exception as error:  # noqa: BLE001
            match, reason = None, f"gis_error:{error}"
        outcomes[reason] += 1
        parcel_id = cfg["parcel_from"](match) if match else ""
        record = {
            "elephant_uuid": identity["elephant_uuid"],
            "elephant_token": identity["elephant_token"],
            "mint_agrees": identity["mint_agrees"],
            "address_full": row["address_full"],
            "street": row["street"],
            "unit": row["unit"],
            "postal_code": zip5,
            "reason": reason,
            "parcel_id": parcel_id,
            "situs_address": cfg["street_from"](match) if match else "",
            "url": cfg["url_from"](match) if match else "",
        }
        if match:
            if parcel_id in seen:
                record["reason"] = "shared_account"
                outcomes[reason] -= 1
                outcomes["shared_account"] += 1
                unmatched.append(record)
            else:
                seen[parcel_id] = identity["elephant_uuid"]
                matches.append(record)
        else:
            unmatched.append(record)
        if index % 25 == 0:
            print(f"{slug} {index}/{len(rows)} {dict(outcomes)}", flush=True)
        time.sleep(0.04)
    if rematch:
        existing_by_uuid = {row["elephant_uuid"]: row for row in existing_seed}
        for match in matches:
            existing_by_uuid[match["elephant_uuid"]] = {
                "parcel_id": match["parcel_id"],
                "source_identifier": match["parcel_id"],
                "situs_address": match["situs_address"],
                "method": "GET",
                "url": match["url"],
                "county": slug,
                "county_fips": cfg["fips"],
                "elephant_uuid": match["elephant_uuid"],
                "elephant_token": match["elephant_token"],
                "opendoor_address": match["address_full"],
            }
        merged = list(existing_by_uuid.values())
    else:
        merged = [
            {
                "parcel_id": match["parcel_id"],
                "source_identifier": match["parcel_id"],
                "situs_address": match["situs_address"],
                "method": "GET",
                "url": match["url"],
                "county": slug,
                "county_fips": cfg["fips"],
                "elephant_uuid": match["elephant_uuid"],
                "elephant_token": match["elephant_token"],
                "opendoor_address": match["address_full"],
            }
            for match in matches
        ]
    seed_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_dir.mkdir(parents=True, exist_ok=True)
    with seed_path.open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "parcel_id",
                "source_identifier",
                "situs_address",
                "method",
                "url",
                "county",
                "county_fips",
                "elephant_uuid",
                "elephant_token",
                "opendoor_address",
            ],
        )
        writer.writeheader()
        writer.writerows(merged)
    unmatched_path = artifact_dir / "unmatched.csv"
    with unmatched_path.open("w", newline="") as handle:
        fieldnames = list(unmatched[0].keys()) if unmatched else ["reason"]
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(unmatched)
    report = {
        "county": slug,
        "rematch": rematch,
        "source_rows": len(rows),
        "matched": len(merged),
        "unmatched": len(unmatched),
        "unique_parcels": len({row["parcel_id"] for row in merged}),
        "outcomes": dict(outcomes),
        "mint_mismatch": sum(1 for row in matches + unmatched if not row["mint_agrees"]),
        "seed_path": str(seed_path),
    }
    (artifact_dir / "match-report.json").write_text(json.dumps(report, indent=2) + "\n")
    return report


def main() -> None:
    slugs = []
    limit = None
    rematch = False
    for arg in sys.argv[1:]:
        if arg.startswith("--limit="):
            limit = int(arg.split("=", 1)[1])
        elif arg == "--rematch":
            rematch = True
        else:
            slugs.append(arg)
    slugs = slugs or ["pasco"]
    reports = [match_county(slug, limit, rematch) for slug in slugs]
    print(json.dumps(reports, indent=2))


if __name__ == "__main__":
    main()
