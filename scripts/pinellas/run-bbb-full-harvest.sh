#!/usr/bin/env bash
# Sequential full-paginate Pinellas BBB harvest (Clearwater + St. Petersburg × roofing/HVAC/solar).
# Operator override 2026-09-07: multi-page scrape empirically unblocked; do not use probe dirs.
set -euo pipefail

ROOT="/workspace"
OUTPUT_ROOT="${ROOT}/downloads/pinellas/bbb-harvest"
CHROME="/usr/local/bin/google-chrome"
LOG_DIR="${OUTPUT_ROOT}/logs"
MAX_PAGES=1000
PAGE_DELAY_MS=2000
PROFILE_DELAY_MS=1500

mkdir -p "${LOG_DIR}"

run_job() {
  local city="$1"
  local trade="$2"
  local category_url="$3"
  local out="${OUTPUT_ROOT}/${city}/${trade}"
  local log="${LOG_DIR}/${city}-${trade}.log"

  echo "=== START ${city}/${trade} $(date -u +%Y-%m-%dT%H:%M:%SZ) ===" | tee -a "${log}"
  cd "${ROOT}"
  CHROME_EXECUTABLE_PATH="${CHROME}" node scripts/harvest-bbb-category.mjs \
    --category-url "${category_url}" \
    --output-dir "${out}" \
    --chromium-executable-path "${CHROME}" \
    --headless true \
    --no-html \
    --profile-subpages none \
    --max-pages "${MAX_PAGES}" \
    --page-delay-ms "${PAGE_DELAY_MS}" \
    --profile-delay-ms "${PROFILE_DELAY_MS}" \
    2>&1 | tee -a "${log}"
  echo "=== END ${city}/${trade} $(date -u +%Y-%m-%dT%H:%M:%SZ) ===" | tee -a "${log}"
}

# Order: Clearwater (proven) then St. Petersburg; roofing → HVAC → solar within each city.
run_job clearwater roofing "https://www.bbb.org/us/fl/clearwater/category/roofing-contractors"
run_job clearwater hvac "https://www.bbb.org/us/fl/clearwater/category/heating-and-air-conditioning"
run_job clearwater solar "https://www.bbb.org/us/fl/clearwater/category/solar-energy-contractors"
run_job st-petersburg roofing "https://www.bbb.org/us/fl/st-petersburg/category/roofing-contractors"
run_job st-petersburg hvac "https://www.bbb.org/us/fl/st-petersburg/category/heating-and-air-conditioning"
run_job st-petersburg solar "https://www.bbb.org/us/fl/st-petersburg/category/solar-energy-contractors"

echo "=== ALL PINELLAS BBB FULL HARVEST JOBS COMPLETE $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
