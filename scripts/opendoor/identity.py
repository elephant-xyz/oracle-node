"""Canonical OpenDoor / Elephant address:v1 identity for ingest artifacts.

OpenDoor already carries `elephant_uuid` and a token that may be prefixed
`address:v1:<sha256>`. Published query-table rows use the raw 64-char sha256
and the UUIDv5 of the same signature. This module:

- strips the prefix
- verifies the pair against `address:v1` of the OpenDoor street and unit
- stamps both onto ingest artifacts so they exist before query-table insert
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from pathlib import Path
from typing import Any

ADDRESS_SIGNATURE_VERSION = "v1"
ELEPHANT_ADDRESS_UUID_NAMESPACE = uuid.UUID("47541537-6230-5494-bf31-221c5f53ccd5")
TOKEN_PREFIX = "address:v1:"
TOKEN_RE = re.compile(r"^[0-9a-f]{64}$")
UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)


def canonicalize_token(token: str | None) -> str:
    if not token:
        raise ValueError("missing elephant_token")
    value = token.strip()
    if value.startswith(TOKEN_PREFIX):
        value = value[len(TOKEN_PREFIX) :]
    value = value.lower()
    if not TOKEN_RE.fullmatch(value):
        raise ValueError(f"elephant_token is not a 64-char sha256: {token[:24]!r}")
    return value


def canonicalize_uuid(value: str | None) -> str:
    if not value:
        raise ValueError("missing elephant_uuid")
    normalized = value.strip().lower()
    if not UUID_RE.fullmatch(normalized):
        raise ValueError(f"elephant_uuid is not a v5 UUID: {value!r}")
    return normalized


def _normalize_field(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value).strip()).lower()


def _normalize_postal(value: Any) -> str | None:
    digits = re.sub(r"\D", "", _normalize_field(value))
    return digits[:5] if len(digits) >= 5 else None


def _serialize_part(name: str, value: str) -> str:
    return f"{name}:{len(value.encode('utf-8'))}:{value}"


def mint_situs_identity(
    street: str,
    postal_code: str,
    state: str = "FL",
    unit: str = "",
) -> dict[str, str]:
    country = "us"
    state_n = _normalize_field(state)
    street_n = _normalize_field(street)
    unit_n = _normalize_field(unit)
    postal = _normalize_postal(postal_code)
    if not state_n or not street_n or postal is None:
        raise ValueError("cannot mint address:v1 without street, state, and ZIP5")
    signature = "|".join(
        [
            f"address:{ADDRESS_SIGNATURE_VERSION}",
            _serialize_part("country", country),
            _serialize_part("state", state_n),
            _serialize_part("postal_code", postal),
            _serialize_part("street", street_n),
            _serialize_part("unit", unit_n),
        ]
    )
    token = hashlib.sha256(signature.encode("utf-8")).hexdigest()
    elephant_uuid = str(uuid.uuid5(ELEPHANT_ADDRESS_UUID_NAMESPACE, signature))
    return {
        "signature": signature,
        "elephant_token": token,
        "elephant_uuid": elephant_uuid,
    }


def _opendoor_parts(row: dict[str, str]) -> tuple[str, str, str]:
    street = (row.get("street") or "").strip()
    unit = (row.get("unit") or "").strip()
    postal = (row.get("postal_code") or "").strip()
    full = row.get("opendoor_address") or row.get("address_full") or ""
    if not street:
        street = full.split(",")[0].strip()
    if not postal:
        match = re.search(r"(\d{5})(?:-\d{4})?\s*$", full)
        if match:
            postal = match.group(1)
    return street, postal, unit


def identity_from_opendoor(row: dict[str, str]) -> dict[str, Any]:
    elephant_uuid = canonicalize_uuid(row.get("elephant_uuid"))
    elephant_token = canonicalize_token(row.get("elephant_token"))
    street, postal, unit = _opendoor_parts(row)
    minted = None
    mint_ok = False
    if street and postal:
        minted = mint_situs_identity(
            street,
            postal,
            row.get("state") or "FL",
            unit,
        )
        mint_ok = (
            minted["elephant_uuid"] == elephant_uuid
            and minted["elephant_token"] == elephant_token
        )
    return {
        "elephant_uuid": elephant_uuid,
        "elephant_token": elephant_token,
        "signature": minted["signature"] if minted else None,
        "mint_agrees": mint_ok,
        "minted_uuid": minted["elephant_uuid"] if minted else None,
        "minted_token": minted["elephant_token"] if minted else None,
        "opendoor_street": street,
        "opendoor_unit": unit,
        "postal_code": _normalize_postal(postal),
    }


def _patch_json(path: Path, updates: dict[str, Any]) -> None:
    payload: dict[str, Any] = {}
    if path.exists():
        payload = json.loads(path.read_text())
        if not isinstance(payload, dict):
            payload = {}
    payload.update(updates)
    path.write_text(json.dumps(payload, indent=2) + "\n")


def stamp_parcel_identity(parcel_dir: Path, identity: dict[str, Any]) -> None:
    """Write uuid/token onto ingest artifacts. Safe to rerun."""
    parcel_dir.mkdir(parents=True, exist_ok=True)
    (parcel_dir / "data").mkdir(exist_ok=True)
    fields = {
        "elephant_uuid": identity["elephant_uuid"],
        "elephant_token": identity["elephant_token"],
    }
    (parcel_dir / "identity.json").write_text(json.dumps(identity, indent=2) + "\n")
    _patch_json(parcel_dir / "property_seed.json", fields)
    _patch_json(parcel_dir / "unnormalized_address.json", fields)
    address_path = parcel_dir / "data" / "address.json"
    if address_path.exists():
        _patch_json(address_path, fields)
    _patch_json(parcel_dir / "data" / "address_identity.json", fields)
