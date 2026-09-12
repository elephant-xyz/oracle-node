#!/usr/bin/env python3
"""Fetch PAO HTML and run county transform scripts for OpenDoor-targeted seeds."""

from __future__ import annotations

import csv
import hashlib
import http.cookiejar
import json
import os
import re
import subprocess
import sys
import tempfile
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
from opendoor.identity import identity_from_opendoor, stamp_parcel_identity  # noqa: E402

NODE_PATH = os.environ.get(
    "OPENDOOR_NODE_PATH",
    str(ROOT.parent / "Counties-trasform-scripts" / "duval" / "scripts" / "node_modules"),
)
CHROME_PATH = os.environ.get(
    "CHROME_PATH",
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
)
TRANSFORM_SCRIPTS = [
    "ownerMapping.js",
    "structureMapping.js",
    "utilityMapping.js",
    "layoutMapping.js",
    "data_extractor.js",
]
MANIFEST_CONTRACT_VERSION = "3"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
COUNTIES = {
    "baker": {
        "display": "Baker",
        "html_must_include": "<title>Parcel Details</title>",
        "input_name": "input.html",
        "transform_dir": "baker",
    },
    "st-lucie": {
        "display": "St. Lucie",
        "fetch_mode": "chrome_dump_dom",
        "html_must_include": "<title>Property Card</title>",
        "input_name": "input.html",
        "transform_dir": "st. lucie",
    },
    "highlands": {
        "display": "Highlands",
        "fetch_mode": "curl",
        "html_must_include": "Highlands County Property Appraiser",
        "input_name": "input.html",
        "transform_dir": "highlands",
    },
    "citrus": {
        "display": "Citrus",
        "html_must_include": "Citrus County Property Appraiser",
        "input_name": "input.html",
        "transform_dir": "citrus",
    },
    "bradford": {
        "display": "Bradford",
        "fetch_mode": "grizzly_details",
        "html_must_include": "gisDetails_OwnerContent",
        "input_name": "input.html",
        "transform_dir": "bradford",
    },
    "columbia": {
        "display": "Columbia",
        "fetch_mode": "grizzly_details",
        "html_must_include": "gisDetails_OwnerContent",
        "input_name": "input.html",
        "transform_dir": "columbia",
    },
    "desoto": {
        "display": "DeSoto",
        "fetch_mode": "grizzly_details",
        "html_must_include": "gisDetails_OwnerContent",
        "input_name": "input.html",
        "transform_dir": "desoto",
    },
    "nassau": {
        "display": "Nassau",
        "fetch_mode": "structured_adapter",
        "input_name": "input.json",
        "transform_dir": "nassau",
        "transform_script": "structured_appraisal.js",
    },
    "putnam": {
        "display": "Putnam",
        "fetch_mode": "structured_adapter",
        "input_name": "input.json",
        "transform_dir": "putnam",
        "transform_script": "structured_appraisal.js",
    },
    "charlotte": {
        "display": "Charlotte",
        "html_must_include": "Property Record Information for",
        "input_name": "input.html",
        "transform_dir": "charlotte",
    },
    "escambia": {
        "display": "Escambia",
        "html_must_include": "General Information",
        "input_name": "input.html",
        "transform_dir": "escambia",
    },
    "santa-rosa": {
        "display": "Santa Rosa",
        "html_must_include": "__remixContext",
        "input_name": "input.html",
        "transform_dir": "santa rosa",
    },
    "martin": {
        "display": "Martin",
        "html_must_include": "Martin County Property Appraiser Data",
        "input_name": "input.html",
        "property_identifier_field": "fdor_parcelno",
        "transform_path": (
            ROOT.parent
            / "soofi-xyz-team-kit"
            / "skills"
            / "use-oracle"
            / "runtime"
            / "counties"
            / "martin"
            / "transforms"
        ),
    },
    "sarasota": {
        "display": "Sarasota",
        "html_must_include": "Property Use:",
        "html_must_not_include": "Access Denied",
        "input_name": "input.html",
        "transform_dir": "sarasota",
    },
    "pasco": {
        "display": "Pasco",
        "html_must_include": "Parcel ID",
        "html_must_not_include": "Access Denied",
        "input_name": "input.html",
        "last_card_param": "showcards",
        "last_card_uses_parcel_id": True,
        "transform_dir": "pasco",
    },
    "marion": {
        "display": "Marion",
        "html_must_include": "Property Information",
        "html_must_not_include": "Access Denied",
        "input_name": "input.html",
        "transform_dir": "marion",
        "url_template": (
            "https://www.pa.marion.fl.us/PRC.aspx"
            "?YR=2026&key={parcel_id}&mName=False&mSitus=False"
        ),
    },
    "osceola": {
        "display": "Osceola",
        "fetch_mode": "osceola_odata",
        "input_name": "input.json",
        "transform_dir": "osceola",
    },
}

OSCEOLA_API_BASE = "https://search.property-appraiser.org/api/v1"
OSCEOLA_RESOURCES = {
    "ParcelInformation": ("ParcelMarket", {"$top": "1"}),
    "Building": ("Bld", {"$orderby": "CardNumber"}),
    "BuildingSubAreas": (
        "BldSubAreas",
        {"$orderby": "CardNumber,SeqNumber"},
    ),
    "ExtraFeatures": ("xfob", {"$orderby": "ln_num"}),
    "StructureElements": ("struct_el", {"$orderby": "bld_num,ln_num"}),
    "Land": ("Lnd_c", {"$orderby": "num"}),
    "ValuesAndTax": ("PropertyValues", {"$orderby": "tax_yr desc"}),
    "SalesHistory": ("sales", {"$orderby": "dos desc"}),
}


def _transform_paths(cfg: dict[str, Any]) -> list[Path]:
    scripts = cfg.get("transform_path")
    if not isinstance(scripts, Path):
        scripts = (
            ROOT.parent
            / "Counties-trasform-scripts"
            / cfg["transform_dir"]
            / "scripts"
        )
    if cfg.get("transform_script"):
        return [scripts / cfg["transform_script"]]
    return [scripts / name for name in TRANSFORM_SCRIPTS]


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _row_sha256(row: dict[str, str]) -> str:
    payload = json.dumps(row, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


def _producer_hashes(cfg: dict[str, Any]) -> dict[str, str]:
    paths = [
        ROOT / "scripts" / "opendoor" / "identity.py",
        *_transform_paths(cfg),
    ]
    hashes = {
        str(path.relative_to(ROOT.parent)): _sha256(path)
        for path in paths
        if path.is_file()
    }
    hashes["local_ingest_manifest_contract"] = hashlib.sha256(
        MANIFEST_CONTRACT_VERSION.encode()
    ).hexdigest()
    return hashes


def _artifact_hashes(parcel_dir: Path) -> dict[str, str]:
    return {
        str(path.relative_to(parcel_dir)): _sha256(path)
        for path in sorted(parcel_dir.rglob("*"))
        if path.is_file() and path.name != "completion.json"
    }


def write_completion_manifest(
    parcel_dir: Path,
    row: dict[str, str],
    cfg: dict[str, Any],
) -> None:
    manifest = {
        "schema_version": 1,
        "status": "complete",
        "parcel_id": row["parcel_id"],
        "seed_row_sha256": _row_sha256(row),
        "producer_sha256": _producer_hashes(cfg),
        "artifact_sha256": _artifact_hashes(parcel_dir),
    }
    (parcel_dir / "completion.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    )


def completion_manifest_valid(
    parcel_dir: Path,
    row: dict[str, str],
    cfg: dict[str, Any],
) -> bool:
    path = parcel_dir / "completion.json"
    try:
        manifest = json.loads(path.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return False
    if (
        manifest.get("status") != "complete"
        or manifest.get("parcel_id") != row["parcel_id"]
        or manifest.get("seed_row_sha256") != _row_sha256(row)
        or manifest.get("producer_sha256") != _producer_hashes(cfg)
    ):
        return False
    expected = manifest.get("artifact_sha256")
    return isinstance(expected, dict) and expected == _artifact_hashes(parcel_dir)


def read_seed(path: Path) -> list[dict[str, str]]:
    with path.open() as handle:
        return list(csv.DictReader(handle))


def _normalized_identifier(value: str) -> str:
    return re.sub(r"[^A-Z0-9]+", "", value.upper())


def validate_capture(html: str, url: str, cfg: dict[str, Any], parcel_id: str) -> None:
    blocked_markers = (
        "Access Denied",
        "Just a moment",
        "cf-chl-",
        "challenge-platform",
    )
    if cfg["html_must_include"] not in html:
        raise RuntimeError(f"expected property-card marker missing for {url}")
    if any(marker.lower() in html.lower() for marker in blocked_markers):
        raise RuntimeError(f"challenge or access-denied page returned for {url}")
    excluded = cfg.get("html_must_not_include")
    if excluded and str(excluded).lower() in html.lower():
        raise RuntimeError(f"excluded page marker returned for {url}")
    normalized_parcel = _normalized_identifier(parcel_id)
    if normalized_parcel and normalized_parcel not in _normalized_identifier(html):
        raise RuntimeError(f"parcel identifier missing from property card for {url}")


def fetch_html(url: str, cfg: dict[str, Any], parcel_id: str) -> str:
    def fetch(target: str) -> str:
        req = urllib.request.Request(target, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=45) as resp:
            return resp.read().decode("utf-8", "replace")

    html = fetch(url)
    card_param = cfg.get("last_card_param")
    if card_param:
        current_match = re.search(r'id="lblCurrentCard"[^>]*>(\d+)<', html)
        total_match = re.search(r'id="lblTotalCards"[^>]*>(\d+)<', html)
        if current_match and total_match and int(current_match.group(1)) < int(total_match.group(1)):
            parsed = urllib.parse.urlsplit(url)
            query = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
            query = [(key, value) for key, value in query if key != card_param]
            if cfg.get("last_card_uses_parcel_id"):
                query = [
                    (key, parcel_id if key == "parcel" else value)
                    for key, value in query
                ]
            query.append((card_param, total_match.group(1)))
            url = urllib.parse.urlunsplit(
                (parsed.scheme, parsed.netloc, parsed.path, urllib.parse.urlencode(query), parsed.fragment)
            )
            html = fetch(url)
    validate_capture(html, url, cfg, parcel_id)
    return html


def fetch_browser_dom(url: str, cfg: dict[str, Any], parcel_id: str) -> str:
    chrome = Path(CHROME_PATH)
    if not chrome.is_file():
        raise RuntimeError(f"Chrome executable not found: {chrome}")
    with tempfile.TemporaryDirectory(prefix="opendoor-chrome-") as profile:
        command = [
            str(chrome),
            "--headless=new",
            "--disable-gpu",
            "--disable-dev-shm-usage",
            "--no-first-run",
            "--no-default-browser-check",
            f"--user-data-dir={profile}",
            f"--user-agent={USER_AGENT}",
            "--virtual-time-budget=12000",
            "--dump-dom",
            url,
        ]
        try:
            completed = subprocess.run(
                command,
                check=True,
                capture_output=True,
                text=True,
                timeout=60,
            )
            html = completed.stdout
        except subprocess.TimeoutExpired as error:
            html = error.stdout or ""
            if isinstance(html, bytes):
                html = html.decode("utf-8", "replace")
    validate_capture(html, url, cfg, parcel_id)
    return html


def fetch_with_curl(url: str, cfg: dict[str, Any], parcel_id: str) -> str:
    completed = subprocess.run(
        [
            "curl",
            "--fail",
            "--silent",
            "--show-error",
            "--location",
            "--max-redirs",
            "3",
            "--connect-timeout",
            "15",
            "--max-time",
            "45",
            "--user-agent",
            USER_AGENT,
            url,
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=55,
    )
    html = completed.stdout
    validate_capture(html, url, cfg, parcel_id)
    return html


class _ParentFormParser(HTMLParser):
    def __init__(self, form_id: str = "parentForm") -> None:
        super().__init__()
        self.form_id = form_id
        self.in_parent_form = False
        self.fields: dict[str, str] = {}
        self.action = ""

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        values = dict(attrs)
        if tag == "form" and values.get("id") == self.form_id:
            self.in_parent_form = True
            self.action = str(values.get("action") or "")
        elif self.in_parent_form and tag == "input" and values.get("name"):
            self.fields[str(values["name"])] = str(values.get("value") or "")

    def handle_endtag(self, tag: str) -> None:
        if tag == "form" and self.in_parent_form:
            self.in_parent_form = False


def fetch_grizzly_details(
    url: str,
    cfg: dict[str, Any],
    parcel_id: str,
) -> str:
    cookie_jar = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(
        urllib.request.HTTPCookieProcessor(cookie_jar)
    )
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with opener.open(request, timeout=45) as response:
        landing = response.read().decode("utf-8", "replace")
        final_url = response.geturl()
    parser = _ParentFormParser()
    parser.feed(landing)
    if not parser.fields:
        initialization = _ParentFormParser("TaxPAsearchForm")
        initialization.feed(landing)
        if not initialization.fields:
            raise RuntimeError(f"GIS initialization form missing for {url}")
        initialization.fields.update(
            {
                "clientWidth": "1280",
                "clientHeight": "900",
                "clientOrientation": "0",
            }
        )
        initialization_url = urllib.parse.urljoin(
            final_url,
            initialization.action,
        )
        initialization_request = urllib.request.Request(
            initialization_url,
            data=urllib.parse.urlencode(initialization.fields).encode(),
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "User-Agent": USER_AGENT,
            },
            method="POST",
        )
        with opener.open(initialization_request, timeout=45) as response:
            landing = response.read().decode("utf-8", "replace")
            final_url = response.geturl()
        parser = _ParentFormParser()
        parser.feed(landing)
        if not parser.fields:
            raise RuntimeError(f"parcel-detail form missing for {url}")
    parser.fields.update(
        {
            "currentTab": "3",
            "Show_Rec": "1",
            "tempPIN": parcel_id,
            "zoomPIN": "0",
            "bHandoff": "1",
            "bHandoff_PIN": parcel_id,
        }
    )
    detail_url = urllib.parse.urljoin(
        final_url,
        "gisSideMenu_3_Details/showDetails/",
    )
    detail_request = urllib.request.Request(
        detail_url,
        data=urllib.parse.urlencode(parser.fields).encode(),
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": USER_AGENT,
        },
        method="POST",
    )
    with opener.open(detail_request, timeout=45) as response:
        html = response.read().decode("utf-8", "replace")
    validate_capture(html, detail_url, cfg, parcel_id)
    return html


def fetch_osceola_input(parcel_id: str) -> str:
    normalized = parcel_id.replace("-", "")
    payload: dict[str, Any] = {}
    for alias, (resource, resource_params) in OSCEOLA_RESOURCES.items():
        params = {
            "$filter": f"strap eq '{normalized}'",
            **resource_params,
        }
        url = f"{OSCEOLA_API_BASE}/{resource}"
        query = urllib.parse.urlencode(params)
        request_url = f"{url}?{query}"
        req = urllib.request.Request(
            request_url,
            headers={"Accept": "application/json", "User-Agent": USER_AGENT},
        )
        with urllib.request.urlopen(req, timeout=45) as resp:
            response = json.loads(resp.read().decode("utf-8"))
        while response.get("@odata.nextLink"):
            next_req = urllib.request.Request(
                response["@odata.nextLink"],
                headers={"Accept": "application/json", "User-Agent": USER_AGENT},
            )
            with urllib.request.urlopen(next_req, timeout=45) as next_resp:
                next_page = json.loads(next_resp.read().decode("utf-8"))
            response.setdefault("value", []).extend(next_page.get("value", []))
            if next_page.get("@odata.nextLink"):
                response["@odata.nextLink"] = next_page["@odata.nextLink"]
            else:
                response.pop("@odata.nextLink", None)
        payload[alias] = {
            "source_http_request": {
                "method": "GET",
                "url": url,
                "multiValueQueryString": {
                    key: [value] for key, value in params.items()
                },
            },
            "response": response,
        }
    if not payload["ParcelInformation"]["response"].get("value"):
        raise RuntimeError(f"Osceola parcel not found: {parcel_id}")
    return json.dumps(payload, indent=2)


def fetch_structured_input(parcel_id: str, cfg: dict[str, Any]) -> str:
    script = _transform_paths(cfg)[0]
    with tempfile.TemporaryDirectory(prefix="opendoor-structured-") as directory:
        output = Path(directory) / "input.json"
        subprocess.run(
            ["node", str(script), "capture", parcel_id, str(output)],
            check=True,
            capture_output=True,
            text=True,
            timeout=45,
        )
        return output.read_text()


def write_inputs(parcel_dir: Path, row: dict[str, str], body: str, cfg: dict[str, Any], identity: dict) -> None:
    parcel_dir.mkdir(parents=True, exist_ok=True)
    (parcel_dir / "owners").mkdir(exist_ok=True)
    (parcel_dir / "data").mkdir(exist_ok=True)
    (parcel_dir / cfg["input_name"]).write_text(body)
    (parcel_dir / "property_seed.json").write_text(
        json.dumps(
            {
                "parcel_id": row["parcel_id"],
                "request_identifier": row["parcel_id"],
                "source_http_request": {"method": "GET", "url": row["url"]},
                "county_name": cfg["display"],
                "elephant_uuid": identity["elephant_uuid"],
                "elephant_token": identity["elephant_token"],
            },
            indent=2,
        )
        + "\n"
    )
    (parcel_dir / "unnormalized_address.json").write_text(
        json.dumps(
            {
                "full_address": row.get("situs_address") or row.get("opendoor_address") or "",
                "unnormalized_address": row.get("situs_address") or row.get("opendoor_address") or "",
                "county_jurisdiction": cfg["display"],
                "country_code": "US",
                "latitude": row.get("latitude") or None,
                "longitude": row.get("longitude") or None,
                "source_http_request": {"method": "GET", "url": row["url"]},
                "request_identifier": row["parcel_id"],
                "elephant_uuid": identity["elephant_uuid"],
                "elephant_token": identity["elephant_token"],
            },
            indent=2,
        )
        + "\n"
    )
    with (parcel_dir / "seed.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["parcel_id", "latitude", "longitude"])
        writer.writeheader()
        writer.writerow(
            {
                "parcel_id": row["parcel_id"],
                "latitude": row.get("latitude") or "",
                "longitude": row.get("longitude") or "",
            }
        )


def run_transforms(parcel_dir: Path, cfg: dict[str, Any]) -> None:
    env = os.environ.copy()
    env["NODE_PATH"] = NODE_PATH
    for script in _transform_paths(cfg):
        subprocess.run(
            ["node", str(script)],
            cwd=parcel_dir,
            env=env,
            check=True,
            capture_output=True,
            text=True,
        )


def normalize_property_artifact(
    parcel_dir: Path,
    row: dict[str, str],
    cfg: dict[str, Any],
    identity: dict[str, Any],
) -> None:
    property_path = parcel_dir / "data" / "property.json"
    if not property_path.is_file():
        raise RuntimeError("property.json missing")
    payload = json.loads(property_path.read_text())
    if not isinstance(payload, dict):
        raise RuntimeError("property.json is not an object")
    if not payload.get("request_identifier"):
        payload["request_identifier"] = row["parcel_id"]
    if not payload.get("parcel_identifier"):
        payload["parcel_identifier"] = row["parcel_id"]
    if not payload.get("county_name"):
        payload["county_name"] = cfg["display"]
    payload["source_http_request"] = {"method": "GET", "url": row["url"]}
    payload["elephant_uuid"] = identity["elephant_uuid"]
    payload["elephant_token"] = identity["elephant_token"]
    property_path.write_text(json.dumps(payload, indent=2) + "\n")


def ingest_one(slug: str, cfg: dict[str, Any], out: Path, index: int, total: int, row: dict[str, str]) -> dict:
    row = dict(row)
    if cfg.get("url_template"):
        row["url"] = cfg["url_template"].format(parcel_id=row["parcel_id"])
    parcel_id = row["parcel_id"]
    parcel_dir = out / parcel_id
    property_json = parcel_dir / "data" / "property.json"
    identity = identity_from_opendoor(row)
    try:
        if completion_manifest_valid(parcel_dir, row, cfg):
            print(f"[{slug} {index}/{total}] skip {parcel_id}", flush=True)
            return {"ok": True, "parcel_id": parcel_id}
        input_path = parcel_dir / cfg["input_name"]
        if input_path.is_file():
            body = input_path.read_text()
        else:
            if cfg.get("fetch_mode") == "osceola_odata":
                body = fetch_osceola_input(parcel_id)
            elif cfg.get("fetch_mode") == "structured_adapter":
                body = fetch_structured_input(parcel_id, cfg)
            elif cfg.get("fetch_mode") == "grizzly_details":
                body = fetch_grizzly_details(row["url"], cfg, parcel_id)
            elif cfg.get("fetch_mode") == "chrome_dump_dom":
                body = fetch_browser_dom(row["url"], cfg, parcel_id)
            elif cfg.get("fetch_mode") == "curl":
                body = fetch_with_curl(row["url"], cfg, parcel_id)
            else:
                body = fetch_html(row["url"], cfg, parcel_id)
        write_inputs(parcel_dir, row, body, cfg, identity)
        run_transforms(parcel_dir, cfg)
        normalize_property_artifact(parcel_dir, row, cfg, identity)
        stamp_parcel_identity(parcel_dir, identity)
        write_completion_manifest(parcel_dir, row, cfg)
        prop = json.loads(property_json.read_text())
        print(
            f"[{slug} {index}/{total}] ok {parcel_id} subdivision={prop.get('subdivision')!r}",
            flush=True,
        )
        return {"ok": True, "parcel_id": parcel_id}
    except subprocess.CalledProcessError as error:
        stderr = (error.stderr or "")[-400:]
        if "Unable to locate input.csv" in stderr and property_json.exists():
            stamp_parcel_identity(parcel_dir, identity)
            write_completion_manifest(parcel_dir, row, cfg)
            prop = json.loads(property_json.read_text())
            print(
                f"[{slug} {index}/{total}] ok {parcel_id} subdivision={prop.get('subdivision')!r} (geom skip)",
                flush=True,
            )
            return {"ok": True, "parcel_id": parcel_id}
        print(f"[{slug} {index}/{total}] FAIL {parcel_id}: {stderr}", flush=True)
        return {"ok": False, "parcel_id": parcel_id, "error": stderr[-500:]}
    except Exception as error:  # noqa: BLE001
        print(f"[{slug} {index}/{total}] FAIL {parcel_id}: {error}", flush=True)
        return {"ok": False, "parcel_id": parcel_id, "error": str(error)}


def ingest_county(slug: str, limit: int | None, workers: int) -> dict:
    cfg = COUNTIES[slug]
    seed = ROOT / "data/seeds" / f"{slug}.csv"
    out = ROOT / "downloads" / slug / "local-ingest"
    rows = read_seed(seed)
    if limit is not None:
        rows = rows[:limit]
    out.mkdir(parents=True, exist_ok=True)
    ok = 0
    failed = []
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = [
            pool.submit(ingest_one, slug, cfg, out, index, len(rows), row)
            for index, row in enumerate(rows, start=1)
        ]
        for future in as_completed(futures):
            result = future.result()
            if result["ok"]:
                ok += 1
            else:
                failed.append({"parcel_id": result["parcel_id"], "error": result.get("error", "")})
    summary = {"county": slug, "attempted": len(rows), "ok": ok, "failed": failed, "workers": workers}
    (out / "ingest-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    printable = {k: summary[k] if k != "failed" else len(failed) for k in summary}
    print(json.dumps(printable, indent=2), flush=True)
    if failed and limit is None:
        raise SystemExit(1)
    return printable


def main() -> None:
    args = sys.argv[1:]
    if not args:
        raise SystemExit("usage: local_ingest.py <county> [--limit=N] [--workers=N]")
    slug = args[0]
    limit = None
    workers = 3
    for arg in args[1:]:
        if arg.startswith("--limit="):
            limit = int(arg.split("=", 1)[1])
        elif arg.startswith("--workers="):
            workers = int(arg.split("=", 1)[1])
    ingest_county(slug, limit, workers)


if __name__ == "__main__":
    main()
