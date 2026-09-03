/**
 * Pinellas County Accela Citizen Access (agency PINELLAS, module Building).
 *
 * Date-window harvest is the countywide Accela path: list search caps at ~100
 * rows, so dense spans split until a window is terminal. City portals
 * (Clearwater Accela, Largo EnerGov, etc.) are separate adapters.
 */

export const PINELLAS_ACCELA_AGENCY = "PINELLAS";
export const PINELLAS_ACCELA_MODULE = "Building";
export const PINELLAS_PORTAL_URL =
  "https://aca-prod.accela.com/PINELLAS/Cap/CapHome.aspx?TabName=Home&module=Building";
export const PINELLAS_DEFAULT_START_DATE = "1990-01-01";
export const PINELLAS_SPLIT_THRESHOLD = 100;

/**
 * Accela PINELLAS record numbers seen on the appraisal print page plus the
 * Lee-style `TYPEyyyy-nnnnn` form. Keep this looser than Lee's matcher so
 * `PER-H-CB-…` / `EBP-…` rows are not dropped.
 *
 * @type {RegExp}
 */
export const PINELLAS_RECORD_NUMBER_PATTERN =
  /\b(?:[A-Z]{2,8}\d{2,4}[-A-Z0-9]*|[A-Z]{2,8}(?:-[A-Z0-9]+)+)\b/i;

/**
 * @typedef {object} DateWindow
 * @property {string} startDate Inclusive ISO start date.
 * @property {string} endDate Inclusive ISO end date.
 */

/**
 * @param {string} value ISO calendar date.
 * @returns {number} UTC midnight epoch ms.
 */
export function isoDateToUtcMillis(value) {
  return Date.parse(`${value}T00:00:00Z`);
}

/**
 * @param {string} value ISO calendar date.
 * @param {number} days Whole days to add.
 * @returns {string} New ISO calendar date.
 */
export function addDays(value, days) {
  return new Date(isoDateToUtcMillis(value) + days * 86400000)
    .toISOString()
    .slice(0, 10);
}

/**
 * Inclusive day count for a closed date range.
 *
 * @param {string} startDate Inclusive ISO start.
 * @param {string} endDate Inclusive ISO end.
 * @returns {number} Number of calendar days in the span.
 */
export function inclusiveDaySpan(startDate, endDate) {
  return (
    Math.round(
      (isoDateToUtcMillis(endDate) - isoDateToUtcMillis(startDate)) / 86400000,
    ) + 1
  );
}

/**
 * Accela list windows that should binary-split instead of paginating.
 *
 * A window is terminal when its reported total is below the cap at any span,
 * or when the span is one day (cannot split further). A missing total is
 * treated as at-cap.
 *
 * @param {object} params Window decision inputs.
 * @param {string} params.startDate Inclusive ISO start.
 * @param {string} params.endDate Inclusive ISO end.
 * @param {number | null} params.reportedTotal Accela "Showing … of N" total.
 * @param {number} [params.splitThreshold=100] Accela list cap.
 * @returns {boolean} True when the window must be split.
 */
export function shouldSplitAccelaWindow({
  startDate,
  endDate,
  reportedTotal,
  splitThreshold = PINELLAS_SPLIT_THRESHOLD,
}) {
  const spanDays = inclusiveDaySpan(startDate, endDate);
  if (spanDays <= 1) return false;
  if (reportedTotal === null || reportedTotal === undefined) return true;
  return reportedTotal >= splitThreshold;
}

/**
 * Split one inclusive window into two contiguous halves.
 *
 * @param {string} startDate Inclusive ISO start.
 * @param {string} endDate Inclusive ISO end.
 * @returns {[DateWindow, DateWindow]} Earlier half, later half.
 */
export function splitAccelaWindow(startDate, endDate) {
  const spanDays = inclusiveDaySpan(startDate, endDate);
  if (spanDays < 2) {
    throw new Error(`Cannot split a ${spanDays}-day Accela window`);
  }
  const leftDays = Math.floor(spanDays / 2);
  const mid = addDays(startDate, leftDays - 1);
  return [
    { startDate, endDate: mid },
    { startDate: addDays(mid, 1), endDate },
  ];
}

/**
 * Tile an inclusive range into initial Accela list windows.
 *
 * @param {string} startDate Inclusive ISO start.
 * @param {string} endDate Inclusive ISO end.
 * @param {number} windowDays Maximum days per initial window.
 * @returns {DateWindow[]} Windows in ascending order.
 */
export function createAccelaDateWindows(startDate, endDate, windowDays) {
  if (isoDateToUtcMillis(endDate) < isoDateToUtcMillis(startDate)) {
    throw new Error("endDate must be greater than or equal to startDate");
  }
  if (!Number.isInteger(windowDays) || windowDays <= 0) {
    throw new Error("windowDays must be a positive integer");
  }
  /** @type {DateWindow[]} */
  const windows = [];
  let cursor = startDate;
  while (isoDateToUtcMillis(cursor) <= isoDateToUtcMillis(endDate)) {
    const candidateEnd = addDays(cursor, windowDays - 1);
    const actualEnd =
      isoDateToUtcMillis(candidateEnd) <= isoDateToUtcMillis(endDate)
        ? candidateEnd
        : endDate;
    windows.push({ startDate: cursor, endDate: actualEnd });
    cursor = addDays(actualEnd, 1);
  }
  return windows;
}

/**
 * Today's UTC calendar date.
 *
 * @returns {string} YYYY-MM-DD.
 */
export function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}
