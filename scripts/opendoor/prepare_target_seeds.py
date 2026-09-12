#!/usr/bin/env python3
"""Resolve reviewed OpenDoor targets through Florida's statewide cadastral layer."""

from __future__ import annotations

import argparse
import csv
import http.cookiejar
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[2]
WORKSPACE = ROOT.parent
sys.path.insert(0, str(ROOT / "scripts"))

from opendoor.identity import identity_from_opendoor  # noqa: E402

DOR_QUERY_URL = (
    "https://services9.arcgis.com/Gh9awoU677aKree0/ArcGIS/rest/services/"
    "Florida_Statewide_Cadastral/FeatureServer/0/query"
)
ST_LUCIE_QUERY_URL = (
    "https://map.paslc.gov/arcgis/rest/services/PROD/"
    "SLCPA_PublicParcels/MapServer/0/query"
)
NASSAU_QUERY_URL = (
    "https://maps.ncpafl.com/ncflpa_arcgis/rest/services/nassau/"
    "NassauCountyPublicTaxMap/MapServer/144/query"
)
BRADFORD_GIS_URL = "https://www.bradfordappraiser.com/gis/"
DEFAULT_SOURCE = WORKSPACE / "opendoor-unmatched-unavailable-counties.csv"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0 Safari/537.36"
)
DIRECTIONS = {"n", "s", "e", "w", "ne", "nw", "se", "sw"}
SUFFIXES = {
    "aly",
    "ave",
    "blvd",
    "cir",
    "ct",
    "dr",
    "gdn",
    "hwy",
    "ln",
    "loop",
    "pkwy",
    "pl",
    "pt",
    "rd",
    "run",
    "st",
    "ter",
    "trl",
    "way",
}
SUFFIX_ALIASES = {
    "avenue": "ave",
    "boulevard": "blvd",
    "circle": "cir",
    "court": "ct",
    "drive": "dr",
    "garden": "gdn",
    "gardens": "gdn",
    "gdns": "gdn",
    "highway": "hwy",
    "lane": "ln",
    "parkway": "pkwy",
    "place": "pl",
    "point": "pt",
    "road": "rd",
    "street": "st",
    "terrace": "ter",
    "trail": "trl",
    "oaks": "oak",
    "thirteenth": "13th",
}


def _highlands_url(attributes: dict[str, Any]) -> str:
    parcel_id = str(attributes.get("PARCEL_ID") or "").strip().upper()
    match = re.fullmatch(r"([A-Z])(\d{2})(\d{2})(\d{2})([A-Z0-9]+)", parcel_id)
    if not match:
        return ""
    suffix, section, township, range_number, remainder = match.groups()
    cama_id = f"{range_number}{township}{section}{remainder}{suffix}"
    return f"https://www.hcpao.org/Search/Parcel/{cama_id}"


def _citrus_url(attributes: dict[str, Any]) -> str:
    alt_key = str(attributes.get("ALT_KEY") or "").strip()
    assessment_year = str(attributes.get("ASMNT_YR") or "2025").strip()
    if not alt_key:
        return ""
    query = urllib.parse.urlencode(
        {
            "LMparent": "20",
            "UseSearch": "no",
            "jur": "19",
            "mode": "profileall",
            "pin": alt_key,
            "taxyr": assessment_year,
        }
    )
    return f"https://www.citruspa.org/_Web/datalets/datalet.aspx?{query}"


def _st_lucie_url(attributes: dict[str, Any]) -> str:
    account = str(attributes.get("ALT_KEY") or "").strip()
    return f"https://apps.paslc.gov/rerecordcard/{account}" if account else ""


def _baker_url(attributes: dict[str, Any]) -> str:
    parcel_id = str(attributes.get("PARCEL_ID") or "").strip()
    if not parcel_id:
        return ""
    return (
        "https://bakerpa.com/propertydetails.php?"
        + urllib.parse.urlencode({"parcel": parcel_id})
    )


def _pin_url(base: str, attributes: dict[str, Any]) -> str:
    parcel_id = str(attributes.get("PARCEL_ID") or "").strip()
    return f"{base}?{urllib.parse.urlencode({'pin': parcel_id})}" if parcel_id else ""


def _format_nassau_parcel(attributes: dict[str, Any]) -> str:
    parcel_id = re.sub(
        r"[^A-Z0-9]",
        "",
        str(attributes.get("PARCEL_ID") or "").upper(),
    )
    if len(parcel_id) != 18:
        return ""
    widths = (2, 2, 2, 4, 4, 4)
    groups = []
    offset = 0
    for width in widths:
        groups.append(parcel_id[offset : offset + width])
        offset += width
    return "-".join(groups)


def _arcgis_query_url(
    endpoint: str,
    identifier_field: str,
    out_fields: list[str],
    parcel_id: str,
) -> str:
    query = urllib.parse.urlencode(
        {
            "where": f"{identifier_field}='{parcel_id}'",
            "outFields": ",".join(out_fields),
            "returnGeometry": "false",
            "f": "json",
        }
    )
    return f"{endpoint}?{query}"


def _nassau_url(attributes: dict[str, Any]) -> str:
    parcel_id = _format_nassau_parcel(attributes)
    if not parcel_id:
        return ""
    return _arcgis_query_url(
        (
            "https://maps.ncpafl.com/ncflpa_arcgis/rest/services/nassau/"
            "NassauCountyPublicTaxMap/MapServer/144/query"
        ),
        "PIN",
        ["PIN", "Situs_full", "subdiv_grp"],
        parcel_id,
    )


def _putnam_url(attributes: dict[str, Any]) -> str:
    parcel_id = str(attributes.get("PARCEL_ID") or "").strip()
    if not parcel_id:
        return ""
    return _arcgis_query_url(
        (
            "https://pamap.putnam-fl.gov/server/rest/services/"
            "CadastralData/FeatureServer/2/query"
        ),
        "PARCELID",
        [
            "PARCELID",
            "SITEADDRESS",
            "PRPRTYDSCRP",
            "CNVYNAME",
            "USECD",
            "USEDSCRP",
            "RESYRBLT",
            "BLDGAREA",
            "LNDVALUE",
            "CNTASSDVAL",
            "CNTTXBLVAL",
            "LEGAL",
            "PA_URL",
            "LASTUPDATE",
        ],
        parcel_id,
    )


def _charlotte_url(attributes: dict[str, Any]) -> str:
    parcel_id = str(attributes.get("PARCEL_ID") or "").strip()
    if not parcel_id:
        return ""
    query = urllib.parse.urlencode(
        {
            "acct": parcel_id,
            "bld": "T",
            "gen": "T",
            "leg": "T",
            "lnd": "T",
            "oth": "T",
            "sal": "T",
            "tax": "T",
        }
    )
    return f"https://www.ccappraiser.com/Show_parcel.asp?{query}"


def _escambia_url(attributes: dict[str, Any]) -> str:
    parcel_id = str(attributes.get("PARCEL_ID") or "").strip()
    return (
        "https://escpa.org/cama/Detail_a.aspx?"
        + urllib.parse.urlencode({"s": parcel_id})
        if parcel_id
        else ""
    )


def _santa_rosa_url(attributes: dict[str, Any]) -> str:
    parcel_id = str(attributes.get("PARCEL_ID") or "").strip()
    return (
        "https://parcelview.srcpa.gov/?"
        + urllib.parse.urlencode({"parcel": parcel_id})
        if parcel_id
        else ""
    )


def _martin_url(attributes: dict[str, Any]) -> str:
    account = str(attributes.get("ALT_KEY") or "").strip()
    return (
        f"https://www.pamartinfl.gov/app/search/view/{account}"
        if account
        else ""
    )


def _nassau_match_supported(
    _attributes: dict[str, Any],
    row: dict[str, str],
) -> bool:
    # Layer 144 exposes a parent PIN for condominium units, not a unit PIN.
    return not bool(row.get("unit"))


CountyConfig = dict[str, str | int | Callable[[dict[str, Any]], str]]
COUNTIES: dict[str, CountyConfig] = {
    "baker": {
        "display": "Baker",
        "resolved_county": "Baker County",
        "fips": "12003",
        "co_no": 12,
        "outcome": "zip_not_in_published_tables",
        "expected_rows": 47,
        "url_from": _baker_url,
    },
    "st-lucie": {
        "display": "St. Lucie",
        "resolved_county": "St. Lucie County",
        "fips": "12111",
        "co_no": 66,
        "outcome": "address_not_matched",
        "expected_rows": 141,
        "url_from": _st_lucie_url,
    },
    "highlands": {
        "display": "Highlands",
        "resolved_county": "Highlands County",
        "fips": "12055",
        "co_no": 38,
        "outcome": "address_not_matched",
        "expected_rows": 18,
        "url_from": _highlands_url,
    },
    "citrus": {
        "display": "Citrus",
        "resolved_county": "Citrus County",
        "fips": "12017",
        "co_no": 19,
        "outcome": "address_not_matched",
        "expected_rows": 11,
        "url_from": _citrus_url,
    },
    "bradford": {
        "display": "Bradford",
        "resolved_county": "Bradford County",
        "fips": "12007",
        "co_no": 14,
        "outcomes": ("address_not_matched", "zip_not_in_published_tables"),
        "expected_rows": 15,
        "url_from": lambda attributes: _pin_url(
            "https://www.bradfordappraiser.com/gis/", attributes
        ),
    },
    "columbia": {
        "display": "Columbia",
        "resolved_county": "Columbia County",
        "fips": "12023",
        "co_no": 22,
        "outcomes": ("address_not_matched", "zip_not_in_published_tables"),
        "expected_rows": 1,
        "url_from": lambda attributes: _pin_url(
            "https://columbia.floridapa.com/gis/", attributes
        ),
    },
    "desoto": {
        "display": "DeSoto",
        "resolved_county": "DeSoto County",
        "fips": "12027",
        "co_no": 24,
        "outcomes": ("address_not_matched", "zip_not_in_published_tables"),
        "expected_rows": 4,
        "url_from": lambda attributes: _pin_url(
            "https://www.desotopa.com/gis/", attributes
        ),
    },
    "nassau": {
        "display": "Nassau",
        "resolved_county": "Nassau County",
        "fips": "12089",
        "co_no": 55,
        "outcomes": (
            "address_not_matched",
            "zip_not_in_published_tables",
            "source_key_conflict",
        ),
        "expected_rows": 156,
        "url_from": _nassau_url,
        "parcel_from": _format_nassau_parcel,
        "match_supported": _nassau_match_supported,
    },
    "putnam": {
        "display": "Putnam",
        "resolved_county": "Putnam County",
        "fips": "12107",
        "co_no": 64,
        "outcomes": ("address_not_matched", "zip_not_in_published_tables"),
        "expected_rows": 6,
        "url_from": _putnam_url,
    },
    "charlotte": {
        "display": "Charlotte",
        "resolved_county": "Charlotte County",
        "fips": "12015",
        "co_no": 18,
        "outcomes": ("address_not_matched", "zip_not_in_published_tables"),
        "expected_rows": 64,
        "url_from": _charlotte_url,
    },
    "escambia": {
        "display": "Escambia",
        "resolved_county": "Escambia County",
        "fips": "12033",
        "co_no": 27,
        "outcomes": ("address_not_matched", "zip_not_in_published_tables"),
        "expected_rows": 11,
        "url_from": _escambia_url,
    },
    "santa-rosa": {
        "display": "Santa Rosa",
        "resolved_county": "Santa Rosa County",
        "fips": "12113",
        "co_no": 57,
        "outcomes": ("address_not_matched", "zip_not_in_published_tables"),
        "expected_rows": 5,
        "url_from": _santa_rosa_url,
    },
    "martin": {
        "display": "Martin",
        "resolved_county": "Martin County",
        "fips": "12085",
        "co_no": 43,
        "outcomes": ("address_not_matched", "zip_not_in_published_tables"),
        "expected_rows": 22,
        "url_from": _martin_url,
    },
}
SEED_FIELDS = [
    "parcel_id",
    "request_identifier",
    "source_identifier",
    "card_key",
    "situs_address",
    "method",
    "url",
    "county",
    "county_fips",
    "elephant_uuid",
    "elephant_token",
    "opendoor_address",
    "street",
    "unit",
    "state",
    "postal_code",
    "latitude",
    "longitude",
    "address_signature",
    "fdor_co_no",
    "fdor_parcelno",
    "fdor_alt_key",
    "fdor_assessment_year",
]


def _tokens(value: str) -> list[str]:
    normalized = value.lower()
    normalized = re.sub(r"\bcounty\s+(?:road|rd)\b", "cr", normalized)
    normalized = re.sub(r"\bstate\s+(?:road|rd)\b", "sr", normalized)
    normalized = re.sub(r"[^a-z0-9]+", " ", normalized).strip()
    return [SUFFIX_ALIASES.get(token, token) for token in normalized.split()]


def address_score(candidate: str, target: str) -> int:
    candidate_tokens = _tokens(candidate)
    target_tokens = _tokens(target)
    if not candidate_tokens or not target_tokens:
        return 0
    if candidate_tokens[0] != target_tokens[0]:
        return 0

    def core(tokens: list[str]) -> list[str]:
        return [
            token
            for token in tokens[1:]
            if token not in DIRECTIONS and token not in SUFFIXES
        ]

    candidate_core = core(candidate_tokens)
    target_core = core(target_tokens)
    if not candidate_core or not target_core:
        return 0
    if candidate_core == target_core:
        return 100
    if "".join(candidate_core) == "".join(target_core):
        return 95
    overlap = set(candidate_core) & set(target_core)
    if overlap and len(overlap) == min(len(candidate_core), len(target_core)):
        return 80
    return 0


def query_dor_point(longitude: str, latitude: str) -> list[dict[str, Any]]:
    params = {
        "geometry": f"{longitude},{latitude}",
        "geometryType": "esriGeometryPoint",
        "inSR": "4326",
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": (
            "CO_NO,PARCEL_ID,PARCELNO,ALT_KEY,PHY_ADDR1,PHY_ADDR2,"
            "PHY_CITY,PHY_ZIPCD,ASMNT_YR"
        ),
        "returnGeometry": "false",
        "f": "json",
    }
    url = f"{DOR_QUERY_URL}?{urllib.parse.urlencode(params)}"
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            with urllib.request.urlopen(request, timeout=45) as response:
                payload = json.loads(response.read().decode("utf-8"))
            if payload.get("error"):
                raise RuntimeError(payload["error"])
            return [
                feature["attributes"]
                for feature in payload.get("features", [])
                if isinstance(feature.get("attributes"), dict)
            ]
        except (
            TimeoutError,
            urllib.error.URLError,
            json.JSONDecodeError,
            RuntimeError,
        ) as error:
            last_error = error
            time.sleep(0.75 * (attempt + 1))
    raise RuntimeError(f"DOR point query failed: {last_error}")


def query_dor_address(
    street: str,
    county_number: int,
) -> list[dict[str, Any]]:
    house_number = re.match(r"\d+", street.strip())
    if not house_number:
        return []
    params = {
        "where": (
            f"CO_NO = {county_number} AND "
            f"PHY_ADDR1 LIKE '{house_number.group()}%'"
        ),
        "outFields": (
            "CO_NO,PARCEL_ID,PARCELNO,ALT_KEY,PHY_ADDR1,PHY_ADDR2,"
            "PHY_CITY,PHY_ZIPCD,ASMNT_YR"
        ),
        "returnGeometry": "false",
        "f": "json",
    }
    url = f"{DOR_QUERY_URL}?{urllib.parse.urlencode(params)}"
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=45) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if payload.get("error"):
        raise RuntimeError(payload["error"])
    return [
        feature["attributes"]
        for feature in payload.get("features", [])
        if isinstance(feature.get("attributes"), dict)
    ]


def query_st_lucie_address(street: str) -> list[dict[str, Any]]:
    escaped = street.upper().replace("'", "''")
    params = {
        "where": f"UPPER(SiteAddress) LIKE '{escaped}%'",
        "outFields": "PARCELNO,AccountNumber,ParcelID,PropertyID,SiteAddress",
        "returnGeometry": "false",
        "f": "json",
    }
    url = f"{ST_LUCIE_QUERY_URL}?{urllib.parse.urlencode(params)}"
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=45) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if payload.get("error"):
        raise RuntimeError(payload["error"])
    return [
        {
            "CO_NO": 66,
            "PARCEL_ID": attributes.get("ParcelID"),
            "PARCELNO": attributes.get("PARCELNO"),
            "ALT_KEY": attributes.get("AccountNumber"),
            "PHY_ADDR1": attributes.get("SiteAddress"),
            "PHY_CITY": "",
            "PHY_ZIPCD": "",
        }
        for feature in payload.get("features", [])
        if isinstance((attributes := feature.get("attributes")), dict)
    ]


def query_nassau_address(street: str) -> list[dict[str, Any]]:
    house_number = re.match(r"\d+", street.strip())
    if not house_number:
        return []
    params = {
        "where": f"Situs_full LIKE '{house_number.group()}%'",
        "outFields": "PIN,Situs_full,subdiv_grp",
        "returnGeometry": "false",
        "f": "json",
    }
    request = urllib.request.Request(
        f"{NASSAU_QUERY_URL}?{urllib.parse.urlencode(params)}",
        headers={"User-Agent": USER_AGENT},
    )
    with urllib.request.urlopen(request, timeout=45) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if payload.get("error"):
        raise RuntimeError(payload["error"])
    return [
        {
            "CO_NO": 55,
            "PARCEL_ID": attributes.get("PIN"),
            "PARCELNO": attributes.get("PIN"),
            "ALT_KEY": "",
            "PHY_ADDR1": attributes.get("Situs_full"),
            "PHY_CITY": "",
            "PHY_ZIPCD": "",
        }
        for feature in payload.get("features", [])
        if isinstance((attributes := feature.get("attributes")), dict)
    ]


def query_bradford_address(street: str) -> list[dict[str, Any]]:
    cookies = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(
        urllib.request.HTTPCookieProcessor(cookies)
    )
    headers = {"User-Agent": USER_AGENT}
    with opener.open(
        urllib.request.Request(BRADFORD_GIS_URL, headers=headers),
        timeout=45,
    ):
        pass
    initialization = urllib.parse.urlencode(
        {
            "clientWidth": "1280",
            "clientHeight": "900",
            "clientOrientation": "0",
            "bHandoff": "-1",
            "bHandoff_PIN": "",
            "bHandoff_saleBook": "",
            "bHandoff_salePage": "",
            "requestQueryString": "",
        }
    ).encode()
    with opener.open(
        urllib.request.Request(
            BRADFORD_GIS_URL,
            data=initialization,
            headers={
                **headers,
                "Content-Type": "application/x-www-form-urlencoded",
            },
            method="POST",
        ),
        timeout=45,
    ):
        pass
    search = urllib.parse.urlencode(
        {
            "SearchBy": "STREET",
            "searchInput": street,
            "butSearch": "GO",
        }
    ).encode()
    with opener.open(
        urllib.request.Request(
            urllib.parse.urljoin(
                BRADFORD_GIS_URL,
                "gisSideMenu_2_Search/quickSearch/",
            ),
            data=search,
            headers={
                **headers,
                "Content-Type": "application/x-www-form-urlencoded",
            },
            method="POST",
        ),
        timeout=45,
    ) as response:
        html = response.read().decode("utf-8", "replace")
    parcel_ids = list(
        dict.fromkeys(
            re.findall(r"pSel\(\s*'1'\s*,\s*'([^']+)'\s*\)", html)
        )
    )
    return [
        {
            "CO_NO": 14,
            "PARCEL_ID": parcel_id,
            "PARCELNO": parcel_id,
            "ALT_KEY": "",
            "PHY_ADDR1": street,
            "PHY_CITY": "",
            "PHY_ZIPCD": "",
        }
        for parcel_id in parcel_ids
    ]


def query_santa_rosa_address(street: str) -> list[dict[str, Any]]:
    search_street = re.sub(
        r"\bGDN\b",
        "GARDEN",
        street,
        flags=re.IGNORECASE,
    )
    body = urllib.parse.urlencode(
        {
            "action": "query",
            "parameterMatch": "intersect",
            "street": search_street,
        }
    ).encode()
    request = urllib.request.Request(
        (
            "https://search.srcpa.gov/property/query?"
            "_data=routes%2F%24category.query"
        ),
        data=body,
        headers={
            "User-Agent": USER_AGENT,
            "Content-Type": "application/x-www-form-urlencoded",
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=45) as response:
        data = json.load(response)
    return [
        {
            "CO_NO": 57,
            "PARCEL_ID": item.get("parcelNumber", ""),
            "PARCELNO": item.get("parcelNumber", ""),
            "ALT_KEY": str(item.get("parcelKey") or ""),
            "PHY_ADDR1": item.get("situsAddress", ""),
            "PHY_CITY": "",
            "PHY_ZIPCD": "",
        }
        for item in data.get("results", [])
        if item.get("parcelNumber")
    ]


def query_martin_address(street: str) -> list[dict[str, Any]]:
    def search(query: str) -> dict[str, Any]:
        params = urllib.parse.urlencode(
            {
                "format": "json",
                "search": query,
                "orderBy": "pin",
                "direction": "asc",
                "limit": "20",
                "offset": "0",
                "searchField": "address",
                "exact": "false",
            }
        )
        request = urllib.request.Request(
            f"https://www.pamartinfl.gov/app/search/real-property?{params}",
            headers={"User-Agent": USER_AGENT},
        )
        with urllib.request.urlopen(request, timeout=45) as response:
            return json.load(response)

    data = search(street)
    if not data.get("records"):
        tokens = _tokens(street)
        relaxed = " ".join(
            [tokens[0]]
            + [
                token
                for token in tokens[1:]
                if token not in DIRECTIONS and token not in SUFFIXES
            ]
        )
        if relaxed and relaxed.lower() != street.lower():
            data = search(relaxed)
    return [
        {
            "CO_NO": 43,
            "PARCEL_ID": str(item.get("AIN") or ""),
            "PARCELNO": item.get("PIN") or item.get("PCNLink") or "",
            "ALT_KEY": str(item.get("AIN") or ""),
            "PHY_ADDR1": item.get("SitusAddress") or "",
            "PHY_CITY": item.get("SitusCity") or "",
            "PHY_ZIPCD": "",
            "ASMNT_YR": item.get("TaxYear"),
        }
        for item in data.get("records", [])
        if item.get("AIN")
    ]


def pick_feature(
    features: list[dict[str, Any]],
    row: dict[str, str],
    county_number: int,
) -> tuple[dict[str, Any] | None, str]:
    candidates = [
        feature
        for feature in features
        if int(feature.get("CO_NO") or -1) == county_number
    ]
    unit_key = re.sub(
        r"[^a-z0-9]",
        "",
        re.sub(r"^(?:apt|unit)\s*", "", row.get("unit", "").lower()),
    )
    if unit_key:
        unit_candidates = [
            feature
            for feature in candidates
            if re.sub(
                r"[^a-z0-9]",
                "",
                str(feature.get("PHY_ADDR1") or "").lower(),
            ).endswith(unit_key)
        ]
        if unit_candidates:
            candidates = unit_candidates
    scored = [
        (address_score(str(feature.get("PHY_ADDR1") or ""), row["street"]), feature)
        for feature in candidates
    ]
    scored = [(score, feature) for score, feature in scored if score >= 80]
    if not scored:
        return None, "no_exact_address_match"
    best_score = max(score for score, _ in scored)
    best = [feature for score, feature in scored if score == best_score]
    if len(best) != 1:
        return None, "ambiguous_address_match"
    return best[0], "address_match"


def _situs_address(attributes: dict[str, Any]) -> str:
    street = " ".join(
        str(attributes.get(field) or "").strip()
        for field in ("PHY_ADDR1", "PHY_ADDR2")
        if str(attributes.get(field) or "").strip()
    )
    city = str(attributes.get("PHY_CITY") or "").strip()
    zip_value = attributes.get("PHY_ZIPCD")
    if isinstance(zip_value, float):
        zip_code = str(int(zip_value))
    else:
        zip_code = str(zip_value or "").strip()
    return ", ".join(part for part in (street, city, "FL", zip_code) if part)


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def prepare_county(
    slug: str,
    source: Path = DEFAULT_SOURCE,
    limit: int | None = None,
    delay: float = 0.15,
    rematch: bool = False,
) -> dict[str, Any]:
    cfg = COUNTIES[slug]
    with source.open() as handle:
        selected_outcomes = set(
            cfg.get("outcomes") or (cfg["outcome"],)
        )
        source_rows = [
            row
            for row in csv.DictReader(handle)
            if row["resolved_county"] == cfg["resolved_county"]
            and row["outcome"] in selected_outcomes
        ]
    source_by_uuid = {row["elephant_uuid"]: row for row in source_rows}
    seed_path = ROOT / "data" / "seeds" / f"{slug}.csv"
    artifact_dir = ROOT / "data" / "artifacts" / f"{slug}-opendoor"
    existing_matches: list[dict[str, Any]] = []
    if rematch:
        unmatched_path = artifact_dir / "unmatched.csv"
        if not unmatched_path.is_file():
            raise RuntimeError(f"{slug}: no unmatched.csv available for rematch")
        with unmatched_path.open() as handle:
            retry_uuids = {
                row["elephant_uuid"]
                for row in csv.DictReader(handle)
                if row.get("elephant_uuid")
            }
        source_rows = [
            row for row in source_rows if row["elephant_uuid"] in retry_uuids
        ]
        if seed_path.is_file():
            with seed_path.open() as handle:
                existing_matches = list(csv.DictReader(handle))
    elif limit is None and len(source_rows) != cfg["expected_rows"]:
        raise RuntimeError(
            f"{slug}: selected {len(source_rows)} rows, expected {cfg['expected_rows']}"
        )
    rows = source_rows[:limit] if limit is not None else source_rows
    matches: list[dict[str, Any]] = []
    unmatched: list[dict[str, Any]] = []
    seen_parcels: set[str] = set()
    outcomes: Counter[str] = Counter()
    url_from = cfg["url_from"]
    if not callable(url_from):
        raise TypeError(f"{slug}: url_from must be callable")

    for index, row in enumerate(rows, start=1):
        identity = identity_from_opendoor(row)
        try:
            if slug == "martin":
                features = query_martin_address(row["street"])
                feature, reason = pick_feature(
                    features,
                    row,
                    int(cfg["co_no"]),
                )
            elif slug == "santa-rosa":
                features = query_santa_rosa_address(row["street"])
                feature, reason = pick_feature(
                    features,
                    row,
                    int(cfg["co_no"]),
                )
            elif rematch and slug == "nassau" and not row.get("unit"):
                features = query_nassau_address(row["street"])
                feature, reason = pick_feature(
                    features,
                    row,
                    int(cfg["co_no"]),
                )
            elif rematch and slug == "bradford":
                features = query_bradford_address(row["street"])
                feature, reason = pick_feature(
                    features,
                    row,
                    int(cfg["co_no"]),
                )
            else:
                features = query_dor_point(row["longitude"], row["latitude"])
                feature, reason = pick_feature(
                    features,
                    row,
                    int(cfg["co_no"]),
                )
                if feature is None:
                    features = query_dor_address(
                        row["street"],
                        int(cfg["co_no"]),
                    )
                    feature, reason = pick_feature(
                        features,
                        row,
                        int(cfg["co_no"]),
                    )
            if feature is None and slug == "nassau":
                features = query_nassau_address(row["street"])
                feature, reason = pick_feature(features, row, int(cfg["co_no"]))
            if feature is None and slug == "bradford":
                features = query_bradford_address(row["street"])
                feature, reason = pick_feature(features, row, int(cfg["co_no"]))
            if feature is None and slug == "st-lucie":
                features = query_st_lucie_address(row["street"])
                feature, reason = pick_feature(features, row, int(cfg["co_no"]))
        except Exception as error:  # noqa: BLE001
            feature, reason = None, f"dor_error:{error}"

        if feature is None:
            outcomes[reason] += 1
            unmatched.append(
                {
                    "elephant_uuid": identity["elephant_uuid"],
                    "opendoor_address": row["address_full"],
                    "reason": reason,
                }
            )
        else:
            parcel_from = cfg.get("parcel_from")
            parcel_id = (
                parcel_from(feature)
                if callable(parcel_from)
                else str(
                    feature.get("PARCEL_ID") or feature.get("PARCELNO") or ""
                ).strip()
            )
            match_supported = cfg.get("match_supported")
            if callable(match_supported) and not match_supported(feature, row):
                url = ""
                reason = "county_source_has_no_unique_unit_identifier"
            else:
                url = url_from(feature)
            if reason == "county_source_has_no_unique_unit_identifier":
                pass
            elif not parcel_id or not url:
                reason = "missing_source_identifier_or_url"
            elif parcel_id in seen_parcels:
                reason = "shared_parcel"
            else:
                reason = "address_match"
                seen_parcels.add(parcel_id)
                matches.append(
                    {
                        "parcel_id": parcel_id,
                        "request_identifier": parcel_id,
                        "source_identifier": parcel_id,
                        "card_key": str(
                            feature.get("ALT_KEY")
                            or feature.get("PARCEL_ID")
                            or ""
                        ).strip(),
                        "situs_address": _situs_address(feature),
                        "method": "GET",
                        "url": url,
                        "county": slug,
                        "county_fips": cfg["fips"],
                        "elephant_uuid": identity["elephant_uuid"],
                        "elephant_token": identity["elephant_token"],
                        "opendoor_address": row["address_full"],
                        "street": row["street"],
                        "unit": row["unit"],
                        "state": row["state"],
                        "postal_code": row["postal_code"],
                        "latitude": row["latitude"],
                        "longitude": row["longitude"],
                        "address_signature": row["address_signature"],
                        "fdor_co_no": feature.get("CO_NO"),
                        "fdor_parcelno": feature.get("PARCELNO"),
                        "fdor_alt_key": feature.get("ALT_KEY"),
                        "fdor_assessment_year": feature.get("ASMNT_YR"),
                    }
                )
            outcomes[reason] += 1
            if reason != "address_match":
                unmatched.append(
                    {
                        "elephant_uuid": identity["elephant_uuid"],
                        "opendoor_address": row["address_full"],
                        "reason": reason,
                    }
                )
        print(f"[{slug} {index}/{len(rows)}] {reason}", flush=True)
        if delay > 0:
            time.sleep(delay)

    if rematch:
        merged_by_uuid = {
            str(row["elephant_uuid"]): row for row in existing_matches
        }
        for row in matches:
            merged_by_uuid[str(row["elephant_uuid"])] = row
        matches = list(merged_by_uuid.values())
    for match in matches:
        source_row = source_by_uuid.get(str(match["elephant_uuid"]), {})
        match.setdefault("request_identifier", match["parcel_id"])
        for field in (
            "street",
            "unit",
            "state",
            "postal_code",
            "latitude",
            "longitude",
            "address_signature",
        ):
            match.setdefault(field, source_row.get(field, ""))
    _write_csv(
        seed_path,
        matches,
        SEED_FIELDS,
    )
    _write_csv(
        artifact_dir / "unmatched.csv",
        unmatched,
        ["elephant_uuid", "opendoor_address", "reason"],
    )
    report = {
        "county": slug,
        "rematch": rematch,
        "selected": len(rows),
        "matched": len(matches),
        "unmatched": len(unmatched),
        "uniqueParcels": len({str(row["parcel_id"]) for row in matches}),
        "mintMismatch": sum(
            1 for row in rows if not identity_from_opendoor(row)["mint_agrees"]
        ),
        "outcomes": dict(outcomes),
        "seedPath": str(seed_path),
    }
    artifact_dir.mkdir(parents=True, exist_ok=True)
    (artifact_dir / "match-report.json").write_text(
        json.dumps(report, indent=2) + "\n"
    )
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("counties", nargs="*", choices=sorted(COUNTIES))
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--delay", type=float, default=0.15)
    parser.add_argument("--rematch", action="store_true")
    args = parser.parse_args()
    counties = args.counties or list(COUNTIES)
    reports = [
        prepare_county(
            county,
            source=args.source,
            limit=args.limit,
            delay=max(0.0, args.delay),
            rematch=args.rematch,
        )
        for county in counties
    ]
    print(json.dumps(reports, indent=2))


if __name__ == "__main__":
    main()
