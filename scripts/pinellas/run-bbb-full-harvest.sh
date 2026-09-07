#!/usr/bin/env bash
# Sequential full-paginate Pinellas BBB harvest (Clearwater + St. Petersburg × roofing/HVAC/solar).
# Operator override 2026-09-07: multi-page scrape empirically unblocked; do not use probe dirs.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
exec node "${ROOT}/scripts/pinellas/run-bbb-full-harvest.mjs" "${ROOT}/downloads/pinellas/bbb-harvest" "$@"
