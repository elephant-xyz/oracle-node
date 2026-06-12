import { PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
import * as crypto from "crypto";
import { mkdir, writeFile } from "fs/promises";
import * as path from "path";
import { pathToFileURL } from "url";
import { parseArgs } from "util";
import puppeteer from "puppeteer";

const DEFAULT_CATEGORY_URL = "https://www.bbb.org/us/category/data";
const DEFAULT_OUTPUT_SCHEMA_VERSION = "oracle-node.bbb-category-harvest.v1";
const DEFAULT_PAGE_DELAY_MS = 3_000;
const DEFAULT_PROFILE_DELAY_MS = 5_000;
const DEFAULT_CHALLENGE_ATTEMPTS = 5;
const DEFAULT_CHALLENGE_CHECK_INTERVAL_MS = 3_000;
const DEFAULT_CHALLENGE_CHECKS_PER_ATTEMPT = 12;
const DEFAULT_NAVIGATION_TIMEOUT_MS = 90_000;
const DEFAULT_PART_RECORD_LIMIT = 100;
const DEFAULT_VIEWPORT_WIDTH = 1365;
const DEFAULT_VIEWPORT_HEIGHT = 900;
const DEFAULT_USER_AGENT =
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36";
const DEFAULT_PROFILE_SUBPAGES = [
  "customer-reviews",
  "complaints",
  "more-info",
];

/**
 * @typedef {Record<string, unknown>} JsonObject
 */

/**
 * @typedef {object} S3UriParts
 * @property {string} bucket - S3 bucket name parsed from an `s3://` URI.
 * @property {string} key - S3 object key parsed from an `s3://` URI.
 */

/**
 * @typedef {object} LocalOutputLocation
 * @property {"local"} kind - Discriminator for local filesystem output.
 * @property {string} dir - Directory where JSONL and manifest artifacts are written.
 */

/**
 * @typedef {object} S3OutputLocation
 * @property {"s3"} kind - Discriminator for S3 output.
 * @property {string} bucket - Destination S3 bucket.
 * @property {string} keyPrefix - Destination S3 key prefix without a trailing slash.
 */

/**
 * @typedef {LocalOutputLocation | S3OutputLocation} OutputLocation
 */

/**
 * @typedef {object} PageLink
 * @property {string} text - Visible link text with whitespace collapsed.
 * @property {string} href - Absolute link URL.
 */

/**
 * @typedef {object} PageSnapshot
 * @property {string} url - Final browser URL after redirects/challenges.
 * @property {string} title - Document title.
 * @property {string} text - Full visible page text.
 * @property {readonly string[]} headings - Visible h1-h4 headings.
 * @property {readonly PageLink[]} links - Visible anchor links with absolute URLs.
 * @property {readonly string[]} jsonLd - Raw JSON-LD script contents.
 * @property {string | null} html - Full page HTML when enabled.
 */

/**
 * @typedef {object} CategoryProfileListing
 * @property {string} profileUrl - BBB profile URL discovered from a category page.
 * @property {string} linkText - Visible profile link text.
 * @property {number} pageNumber - One-based category result page where the link appeared.
 * @property {number} ordinalOnPage - One-based profile-link ordinal on that page.
 * @property {string} categoryUrl - Category page URL where the profile link was discovered.
 */

/**
 * @typedef {object} CategoryPageRecord
 * @property {"bbb_category_page"} recordKind - Record discriminator for category evidence artifacts.
 * @property {string} schemaVersion - Harvester schema version.
 * @property {string} source - Source system label.
 * @property {string} categoryUrl - Requested BBB category URL.
 * @property {number} pageNumber - One-based category page number.
 * @property {number | null} totalResults - Total result count parsed from the page, when available.
 * @property {number | null} pageCount - Total pagination count parsed from the page, when available.
 * @property {readonly CategoryProfileListing[]} profileListings - Profile links discovered on this page.
 * @property {PageSnapshot} rawPage - Lossless visible/HTML/JSON-LD page snapshot.
 */

/**
 * @typedef {object} ProfileSubpageResult
 * @property {string} kind - Logical subpage kind such as `customer-reviews` or `complaints`.
 * @property {string} url - Requested subpage URL.
 * @property {number | null} status - HTTP status returned by the browser navigation, when available.
 * @property {boolean} ok - True when the page loaded past Cloudflare challenge text.
 * @property {PageSnapshot | null} page - Snapshot for loaded pages.
 * @property {string | null} error - Error message for failed subpage attempts.
 */

/**
 * @typedef {object} BbbProfileUrlIdentity
 * @property {string | null} providerBbbId - Local BBB code embedded in the profile URL when available.
 * @property {string | null} providerBusinessId - BBB business identifier embedded in the profile URL when available.
 * @property {string | null} addressId - Branch address identifier embedded in the profile URL when available.
 * @property {string | null} slug - Business slug embedded in the profile URL when available.
 */

/**
 * @typedef {object} BbbBusinessProfileRecord
 * @property {"bbb_business_profile"} recordKind - Record discriminator for profile artifacts.
 * @property {string} schemaVersion - Harvester schema version.
 * @property {string} parserSource - Script and parser identifier.
 * @property {string} source - Source label for provenance.
 * @property {string} sourceRetrievedAt - ISO timestamp when the profile was harvested.
 * @property {string} profileUrl - Canonical BBB profile URL.
 * @property {string | null} providerProfileId - Stable profile key derived from BBB URL identifiers.
 * @property {string | null} providerBusinessId - BBB business id when present in the profile URL.
 * @property {string | null} providerBbbId - Local BBB id when present in the profile URL.
 * @property {string | null} profileSlug - Business slug from the profile URL.
 * @property {string | null} name - Business display name.
 * @property {string | null} legalName - Business legal name.
 * @property {string | null} description - Business overview text.
 * @property {string | null} phone - Primary phone number.
 * @property {string | null} websiteUrl - Primary website URL.
 * @property {string | null} emailUrl - BBB email-this-business URL when present.
 * @property {JsonObject | null} address - Structured address object suitable for the query-db BBB mapper.
 * @property {boolean | null} accredited - BBB accreditation flag.
 * @property {string | null} accreditationStatus - Human-readable accreditation status.
 * @property {string | null} accreditedSince - BBB accredited-since date text.
 * @property {string | null} bbbRating - BBB letter rating text.
 * @property {string | null} bbbFileOpenedDate - BBB file-opened date text.
 * @property {string | null} businessStarted - Business-started date text.
 * @property {string | null} businessIncorporated - Business-incorporated date text.
 * @property {number | null} yearsInBusiness - Years in business when parsed.
 * @property {string | null} entityType - Type of legal entity.
 * @property {string | null} localBbbName - Local BBB office name.
 * @property {string | null} localBbbUrl - Local BBB office URL.
 * @property {string | null} productsAndServices - Product/service description text.
 * @property {readonly JsonObject[]} alternateNames - Alternate business names parsed from the profile page.
 * @property {readonly JsonObject[]} businessManagement - Management/contact rows parsed from JSON-LD or visible text.
 * @property {readonly JsonObject[]} categories - BBB categories linked from the profile page.
 * @property {readonly JsonObject[]} locations - Branch/location profile links parsed from the profile page.
 * @property {readonly JsonObject[]} licenses - Licensing text parsed from the profile page.
 * @property {readonly JsonObject[]} serviceAreas - Service-area rows parsed from JSON-LD or visible text.
 * @property {readonly JsonObject[]} images - Image/media rows parsed from JSON-LD and page markup.
 * @property {readonly JsonObject[]} links - External business links from the profile page.
 * @property {readonly JsonObject[]} ratingReasons - BBB rating-reason rows parsed from profile subpages.
 * @property {readonly JsonObject[]} reviews - Individual customer review rows parsed from the reviews subpage.
 * @property {readonly JsonObject[]} complaints - Individual complaint rows and event timelines parsed from the complaints subpage.
 * @property {JsonObject} reviewsComplaintsSummary - Review/complaint counters parsed from subpages when present.
 * @property {JsonObject} bbbHarvest - Harvester provenance and raw page snapshots.
 */

/**
 * @typedef {object} FailedProfileRecord
 * @property {"bbb_profile_failure"} recordKind - Record discriminator for failed profile attempts.
 * @property {string} schemaVersion - Harvester schema version.
 * @property {string} profileUrl - BBB profile URL that failed to load.
 * @property {CategoryProfileListing | null} listing - Category listing evidence for the failed profile.
 * @property {string} error - Failure reason.
 */

/**
 * @typedef {object} NavigationResult
 * @property {boolean} ok - True when the requested page loaded past BBB's challenge page.
 * @property {number | null} status - HTTP status from the most recent navigation response.
 * @property {string} title - Final document title.
 * @property {string} previewText - First visible characters from the page body.
 */

/**
 * @typedef {object} BbbHarvestOptions
 * @property {string} categoryUrl - BBB category URL to harvest.
 * @property {OutputLocation} outputLocation - Local or S3 artifact destination.
 * @property {string | null} chromiumExecutablePath - Browser executable path, or null for Puppeteer's default.
 * @property {boolean} headless - Whether Puppeteer should run headless.
 * @property {number} startPage - First one-based category page to visit.
 * @property {number | null} maxPages - Optional category page cap.
 * @property {number | null} maxProfiles - Optional profile cap after category discovery.
 * @property {number} partRecordLimit - Maximum profile records per JSONL part.
 * @property {number} pageDelayMs - Delay between category page visits.
 * @property {number} profileDelayMs - Delay between profile/subpage visits.
 * @property {number} challengeAttempts - Number of navigation retries for Cloudflare challenge pages.
 * @property {number} challengeCheckIntervalMs - Delay between challenge checks.
 * @property {number} challengeChecksPerAttempt - Number of challenge checks per navigation attempt.
 * @property {number} navigationTimeoutMs - Puppeteer navigation timeout in milliseconds.
 * @property {boolean} includeHtml - Whether raw HTML snapshots should be stored.
 * @property {readonly string[]} profileSubpages - Profile subpages to visit after the main profile page.
 */

/**
 * @typedef {object} BbbBrowserSession
 * @property {import("puppeteer").Browser} browser - Chromium browser process holding BBB cookies and challenge clearance state.
 * @property {import("puppeteer").Page} page - Single configured tab reused for category/search pages, profiles, and subpages.
 */

/**
 * @typedef {object} HarvestSummary
 * @property {string} schemaVersion - Harvester schema version.
 * @property {string} categoryUrl - BBB category URL harvested.
 * @property {string} startedAt - ISO timestamp when harvesting started.
 * @property {string} finishedAt - ISO timestamp when harvesting finished.
 * @property {number} categoryPagesVisited - Count of category pages visited.
 * @property {number} profileUrlsDiscovered - Unique profile URLs discovered from category pages.
 * @property {number} profilesHarvested - Count of profile records written.
 * @property {number} profilesFailed - Count of failed profile attempts written.
 * @property {readonly string[]} outputArtifacts - Local paths or S3 URIs written by the harvester.
 */

/**
 * Harvest a BBB category and profile pages into JSONL artifacts that can be loaded
 * by the query-db BBB mapper.
 *
 * @param {BbbHarvestOptions} options - Browser, crawl, and output configuration.
 * @returns {Promise<HarvestSummary>} Summary of category pages, profile records, failures, and output artifacts.
 */
export async function harvestBbbCategory(options) {
  const session = await createBbbBrowserSession(options);
  try {
    return await harvestBbbCategoryInExistingPage(options, session.page);
  } finally {
    await session.browser.close();
  }
}

/**
 * Launch one Chromium browser and one configured BBB tab that can be reused
 * across multiple category or search harvests. Reusing the session preserves
 * cookies and Cloudflare clearance state instead of creating a new browser for
 * every seed.
 *
 * @param {BbbHarvestOptions} options - Browser executable, headless, and navigation timeout settings.
 * @returns {Promise<BbbBrowserSession>} Browser session with one reusable page.
 */
export async function createBbbBrowserSession(options) {
  /** @type {import("puppeteer").LaunchOptions} */
  const launchOptions = {
    headless: options.headless,
    args: [
      "--no-sandbox",
      "--disable-setuid-sandbox",
      "--disable-dev-shm-usage",
      "--disable-blink-features=AutomationControlled",
      `--window-size=${DEFAULT_VIEWPORT_WIDTH},${DEFAULT_VIEWPORT_HEIGHT}`,
    ],
    defaultViewport: {
      width: DEFAULT_VIEWPORT_WIDTH,
      height: DEFAULT_VIEWPORT_HEIGHT,
    },
  };
  if (options.chromiumExecutablePath !== null) {
    launchOptions.executablePath = options.chromiumExecutablePath;
  }

  const browser = await puppeteer.launch(launchOptions);
  try {
    const page = await newConfiguredPage(browser, options.navigationTimeoutMs);
    return { browser, page };
  } catch (caught) {
    await browser.close().catch(() => undefined);
    throw caught;
  }
}

/**
 * Harvest a BBB category/search page and linked profile pages using an existing
 * configured tab. Callers that process many categories can keep passing the same
 * page to avoid reopening Chromium and retriggering Cloudflare checks.
 *
 * @param {BbbHarvestOptions} options - Crawl, parsing, and output configuration for one category/search URL.
 * @param {import("puppeteer").Page} page - Reusable configured Puppeteer tab to navigate for category pages, profiles, and subpages.
 * @returns {Promise<HarvestSummary>} Summary of category pages, profile records, failures, and output artifacts.
 */
export async function harvestBbbCategoryInExistingPage(options, page) {
  const startedAt = new Date().toISOString();
  /** @type {readonly string[]} */
  const profileSubpages = options.profileSubpages;
  /** @type {CategoryPageRecord[]} */
  const categoryPageRecords = [];
  /** @type {Map<string, CategoryProfileListing>} */
  const profileListingByUrl = new Map();
  /** @type {BbbBusinessProfileRecord[]} */
  let pendingProfileRecords = [];
  /** @type {FailedProfileRecord[]} */
  const failedProfiles = [];
  /** @type {string[]} */
  const outputArtifacts = [];
  let profilePartNumber = 1;

  let parsedPageCount = options.maxPages;
  let pageNumber = options.startPage;
  while (
    parsedPageCount === null ||
    pageNumber < options.startPage + parsedPageCount
  ) {
    const pageUrl = buildCategoryPageUrl(options.categoryUrl, pageNumber);
    const navigation = await gotoAccessibleBbbPage(page, pageUrl, options);
    if (!navigation.ok) {
      throw new Error(
        `Could not load BBB category page ${pageUrl}: ${navigation.title}`,
      );
    }
    const snapshot = await snapshotPage(page, options.includeHtml);
    const categoryRecord = buildCategoryPageRecord({
      categoryUrl: options.categoryUrl,
      pageNumber,
      snapshot,
    });
    categoryPageRecords.push(categoryRecord);
    for (const listing of categoryRecord.profileListings) {
      if (!profileListingByUrl.has(listing.profileUrl)) {
        profileListingByUrl.set(listing.profileUrl, listing);
      }
    }
    if (parsedPageCount === null) {
      parsedPageCount = categoryRecord.pageCount ?? 1;
    }
    if (options.maxPages !== null) {
      parsedPageCount = Math.min(parsedPageCount, options.maxPages);
    }
    pageNumber += 1;
    if (
      parsedPageCount === null ||
      pageNumber < options.startPage + parsedPageCount
    ) {
      await sleep(options.pageDelayMs);
    }
  }

  outputArtifacts.push(
    await writeJsonlRecords(
      options.outputLocation,
      "category-pages/category-pages.jsonl",
      categoryPageRecords,
    ),
  );

  const selectedListings = [...profileListingByUrl.values()].slice(
    0,
    options.maxProfiles ?? undefined,
  );
  for (const listing of selectedListings) {
    await sleep(options.profileDelayMs);
    try {
      const record = await harvestProfileRecord({
        listing,
        page,
        options,
        profileSubpages,
      });
      pendingProfileRecords.push(record);
      if (pendingProfileRecords.length >= options.partRecordLimit) {
        outputArtifacts.push(
          await writeJsonlRecords(
            options.outputLocation,
            `profiles/profiles-part-${String(profilePartNumber).padStart(4, "0")}.jsonl`,
            pendingProfileRecords,
          ),
        );
        profilePartNumber += 1;
        pendingProfileRecords = [];
      }
    } catch (caught) {
      failedProfiles.push({
        recordKind: "bbb_profile_failure",
        schemaVersion: DEFAULT_OUTPUT_SCHEMA_VERSION,
        profileUrl: listing.profileUrl,
        listing,
        error: caught instanceof Error ? caught.message : String(caught),
      });
    }
  }

  if (pendingProfileRecords.length > 0) {
    outputArtifacts.push(
      await writeJsonlRecords(
        options.outputLocation,
        `profiles/profiles-part-${String(profilePartNumber).padStart(4, "0")}.jsonl`,
        pendingProfileRecords,
      ),
    );
  }
  if (failedProfiles.length > 0) {
    outputArtifacts.push(
      await writeJsonlRecords(
        options.outputLocation,
        "failures/failed-profiles.jsonl",
        failedProfiles,
      ),
    );
  }

  const summary = {
    schemaVersion: DEFAULT_OUTPUT_SCHEMA_VERSION,
    categoryUrl: options.categoryUrl,
    startedAt,
    finishedAt: new Date().toISOString(),
    categoryPagesVisited: categoryPageRecords.length,
    profileUrlsDiscovered: profileListingByUrl.size,
    profilesHarvested: selectedListings.length - failedProfiles.length,
    profilesFailed: failedProfiles.length,
    outputArtifacts,
  };
  outputArtifacts.push(
    await writeJsonObject(
      options.outputLocation,
      "manifest/summary.json",
      summary,
    ),
  );
  return { ...summary, outputArtifacts };
}

/**
 * Build a query-db-ready BBB profile record from page snapshots.
 *
 * The returned object intentionally keeps raw page snapshots under `bbbHarvest`
 * while promoting commonly queried fields to stable top-level names consumed by
 * `@elephant-xyz/query-db`'s BBB mapper.
 *
 * @param {object} params - Profile record inputs.
 * @param {string} params.profileUrl - Canonical BBB profile URL.
 * @param {CategoryProfileListing | null} params.listing - Category listing evidence, when available.
 * @param {PageSnapshot} params.mainPage - Main profile page snapshot.
 * @param {readonly ProfileSubpageResult[]} params.subpages - Optional review/complaint/more-info snapshots.
 * @param {string} params.retrievedAt - ISO timestamp when the profile was harvested.
 * @returns {BbbBusinessProfileRecord} Profile record suitable for query-db BBB loading.
 */
export function buildBbbBusinessProfileRecord({
  profileUrl,
  listing,
  mainPage,
  subpages,
  retrievedAt,
}) {
  const jsonLdObjects = flattenJsonLdObjects(mainPage.jsonLd);
  const localBusiness = findJsonLdObjectByType(jsonLdObjects, "LocalBusiness");
  const identity = parseBbbProfileUrlIdentity(profileUrl);
  const address = buildAddressRecord(localBusiness);
  const links = externalBusinessLinks(mainPage.links);
  const reviewsPage =
    subpages.find((subpage) => subpage.kind === "customer-reviews")?.page ??
    null;
  const complaintsPage =
    subpages.find((subpage) => subpage.kind === "complaints")?.page ?? null;
  const moreInfoPage =
    subpages.find((subpage) => subpage.kind === "more-info")?.page ?? null;
  const reviewsSummary =
    reviewsPage === null ? {} : parseReviewsSummary(reviewsPage.text);
  const complaintsSummary =
    complaintsPage === null ? {} : parseComplaintsSummary(complaintsPage.text);
  const ratingReasons =
    moreInfoPage === null ? [] : parseRatingReasons(moreInfoPage.text);
  const reviews = reviewsPage === null ? [] : parseReviewRows(reviewsPage.text);
  const complaints =
    complaintsPage === null ? [] : parseComplaintRows(complaintsPage.text);
  const name =
    readStringProperty(localBusiness, "name") ??
    listing?.linkText ??
    readProfileNameFromText(mainPage.text);

  return {
    recordKind: "bbb_business_profile",
    schemaVersion: DEFAULT_OUTPUT_SCHEMA_VERSION,
    parserSource: "scripts/harvest-bbb-category.mjs",
    source: "bbb-public-browser",
    sourceRetrievedAt: retrievedAt,
    profileUrl,
    providerProfileId: buildProviderProfileId(identity, profileUrl),
    providerBusinessId: identity.providerBusinessId,
    providerBbbId: identity.providerBbbId,
    profileSlug: identity.slug,
    name,
    legalName: readStringProperty(localBusiness, "legalName"),
    description:
      readStringProperty(localBusiness, "description") ??
      readSection(mainPage.text, "About This Business", "BBB Accredited Since"),
    phone:
      readStringProperty(localBusiness, "telephone") ??
      firstTelephoneLink(mainPage.links),
    websiteUrl: firstWebsiteUrl(mainPage.links),
    emailUrl: firstEmailUrl(mainPage.links),
    address,
    accredited: readAccreditation(mainPage.text),
    accreditationStatus:
      readAccreditation(mainPage.text) === true ? "BBB Accredited" : null,
    accreditedSince: readTextAfterLabel(mainPage.text, "BBB Accredited Since"),
    bbbRating: readBbbRating(mainPage.text),
    bbbFileOpenedDate: readTextAfterLabel(mainPage.text, "BBB File Opened"),
    businessStarted: readTextAfterLabel(mainPage.text, "Business Started"),
    businessIncorporated: readTextAfterLabel(
      mainPage.text,
      "Business Incorporated",
    ),
    yearsInBusiness: readIntegerAfterLabel(mainPage.text, "Years in Business"),
    entityType: readTextAfterLabel(mainPage.text, "Type of Entity"),
    localBbbName: readTextAfterLabel(mainPage.text, "Local BBB"),
    localBbbUrl: firstLinkAfterText(
      mainPage.links,
      readTextAfterLabel(mainPage.text, "Local BBB"),
    ),
    productsAndServices: readSection(
      mainPage.text,
      "Products and Services",
      "Photos and Videos",
    ),
    alternateNames: alternateNameRows(mainPage.text),
    businessManagement: businessManagementRows(localBusiness, mainPage.text),
    categories: categoryRows(mainPage.links),
    locations: locationRows(mainPage.links, profileUrl),
    licenses: licenseRows(mainPage.text),
    serviceAreas: serviceAreaRows(
      localBusiness,
      moreInfoPage?.text ?? mainPage.text,
    ),
    images: imageRows(localBusiness),
    links,
    ratingReasons,
    reviews,
    complaints,
    reviewsComplaintsSummary: {
      ...reviewsSummary,
      ...complaintsSummary,
    },
    bbbHarvest: {
      listing,
      mainPage,
      subpages,
      rawJsonLdObjects: jsonLdObjects,
    },
  };
}

/**
 * Parse stable identifiers embedded in a BBB profile URL.
 *
 * @param {string} profileUrl - BBB profile URL.
 * @returns {BbbProfileUrlIdentity} Parsed profile URL identity parts.
 */
export function parseBbbProfileUrlIdentity(profileUrl) {
  const parsed = new URL(profileUrl);
  const segments = parsed.pathname.split("/").filter(Boolean);
  const addressIndex = segments.indexOf("addressId");
  const addressId =
    addressIndex >= 0 ? (segments[addressIndex + 1] ?? null) : null;
  const profileSegment =
    addressIndex >= 0
      ? (segments[addressIndex - 1] ?? null)
      : (segments.at(-1) ?? null);
  if (profileSegment === null) {
    return {
      providerBbbId: null,
      providerBusinessId: null,
      addressId,
      slug: null,
    };
  }
  const match =
    /^(?<slug>.+)-(?<providerBbbId>\d{4})-(?<providerBusinessId>\d+)$/.exec(
      profileSegment,
    );
  if (match?.groups === undefined) {
    return {
      providerBbbId: null,
      providerBusinessId: null,
      addressId,
      slug: profileSegment,
    };
  }
  return {
    providerBbbId: match.groups.providerBbbId,
    providerBusinessId: match.groups.providerBusinessId,
    addressId,
    slug: match.groups.slug,
  };
}

/**
 * Parse total result count and page count from a BBB category page snapshot.
 *
 * @param {PageSnapshot} snapshot - Category page snapshot.
 * @returns {{ readonly totalResults: number | null, readonly pageCount: number | null }} Parsed count metadata.
 */
export function parseCategoryCounts(snapshot) {
  const totalMatch = /Showing:\s*([\d,]+)\s+results/i.exec(snapshot.text);
  const totalResults =
    totalMatch === null ? null : Number(totalMatch[1].replace(/,/g, ""));
  const pageNumbers = snapshot.links.flatMap((link) => {
    const textMatch = /^Page\s+(\d+)$/i.exec(link.text);
    const urlPage = readPageNumberFromUrl(link.href);
    return [textMatch === null ? null : Number(textMatch[1]), urlPage].filter(
      isNonNullNumber,
    );
  });
  const pageCount = pageNumbers.length === 0 ? null : Math.max(...pageNumbers);
  return { totalResults, pageCount };
}

/**
 * CLI entrypoint.
 *
 * @returns {Promise<void>} Resolves after the harvest finishes and summary is printed.
 */
export async function main() {
  const options = parseCliOptions(process.argv.slice(2));
  const summary = await harvestBbbCategory(options);
  console.log(JSON.stringify(summary, null, 2));
}

/**
 * Harvest one BBB business profile and its selected subpages.
 *
 * @param {object} params - Profile harvest inputs.
 * @param {CategoryProfileListing} params.listing - Category listing to harvest.
 * @param {import("puppeteer").Page} params.page - Reused Puppeteer page.
 * @param {BbbHarvestOptions} params.options - Navigation/snapshot options.
 * @param {readonly string[]} params.profileSubpages - Profile subpage kinds to visit.
 * @returns {Promise<BbbBusinessProfileRecord>} Query-db-ready profile record.
 */
async function harvestProfileRecord({
  listing,
  page,
  options,
  profileSubpages,
}) {
  const navigation = await gotoAccessibleBbbPage(
    page,
    listing.profileUrl,
    options,
  );
  if (!navigation.ok) {
    throw new Error(
      `Could not load BBB profile ${listing.profileUrl}: ${navigation.title}`,
    );
  }
  const mainPage = await snapshotPage(page, options.includeHtml);
  const subpageTargets = discoverProfileSubpages(
    mainPage.links,
    profileSubpages,
  );
  /** @type {ProfileSubpageResult[]} */
  const subpages = [];
  for (const target of subpageTargets) {
    await sleep(options.profileDelayMs);
    try {
      const subpageNavigation = await gotoAccessibleBbbPage(
        page,
        target.url,
        options,
      );
      subpages.push({
        kind: target.kind,
        url: target.url,
        status: subpageNavigation.status,
        ok: subpageNavigation.ok,
        page: subpageNavigation.ok
          ? await snapshotPage(page, options.includeHtml)
          : null,
        error: subpageNavigation.ok
          ? null
          : `Challenge page remained after navigation: ${subpageNavigation.title}`,
      });
    } catch (caught) {
      subpages.push({
        kind: target.kind,
        url: target.url,
        status: null,
        ok: false,
        page: null,
        error: caught instanceof Error ? caught.message : String(caught),
      });
    }
  }
  return buildBbbBusinessProfileRecord({
    profileUrl: listing.profileUrl,
    listing,
    mainPage,
    subpages,
    retrievedAt: new Date().toISOString(),
  });
}

/**
 * Build a category page evidence record from a browser snapshot.
 *
 * @param {object} params - Category page inputs.
 * @param {string} params.categoryUrl - Original category URL.
 * @param {number} params.pageNumber - One-based category page number.
 * @param {PageSnapshot} params.snapshot - Browser page snapshot.
 * @returns {CategoryPageRecord} Category page record with discovered profile links.
 */
function buildCategoryPageRecord({ categoryUrl, pageNumber, snapshot }) {
  const counts = parseCategoryCounts(snapshot);
  const profileListings = profileListingsFromSnapshot(
    snapshot,
    categoryUrl,
    pageNumber,
  );
  return {
    recordKind: "bbb_category_page",
    schemaVersion: DEFAULT_OUTPUT_SCHEMA_VERSION,
    source: "bbb-public-browser",
    categoryUrl,
    pageNumber,
    totalResults: counts.totalResults,
    pageCount: counts.pageCount,
    profileListings,
    rawPage: snapshot,
  };
}

/**
 * Extract profile links from a category page snapshot.
 *
 * @param {PageSnapshot} snapshot - Category page snapshot.
 * @param {string} categoryUrl - Category URL where links were discovered.
 * @param {number} pageNumber - One-based category page number.
 * @returns {readonly CategoryProfileListing[]} De-duplicated profile links in page order.
 */
function profileListingsFromSnapshot(snapshot, categoryUrl, pageNumber) {
  /** @type {CategoryProfileListing[]} */
  const listings = [];
  const seen = new Set();
  for (const link of snapshot.links) {
    if (!isBbbProfileUrl(link.href)) continue;
    const profileUrl = canonicalBbbProfileBaseUrl(link.href);
    if (seen.has(profileUrl)) continue;
    seen.add(profileUrl);
    listings.push({
      profileUrl,
      linkText: link.text,
      pageNumber,
      ordinalOnPage: listings.length + 1,
      categoryUrl,
    });
  }
  return listings;
}

/**
 * Create and configure a Puppeteer page for BBB navigation.
 *
 * @param {import("puppeteer").Browser} browser - Browser instance.
 * @param {number} navigationTimeoutMs - Default page navigation timeout.
 * @returns {Promise<import("puppeteer").Page>} Configured page.
 */
async function newConfiguredPage(browser, navigationTimeoutMs) {
  const page = await browser.newPage();
  page.setDefaultNavigationTimeout(navigationTimeoutMs);
  page.setDefaultTimeout(navigationTimeoutMs);
  await page.setCacheEnabled(false);
  await page.setViewport({
    width: DEFAULT_VIEWPORT_WIDTH,
    height: DEFAULT_VIEWPORT_HEIGHT,
  });
  await page.setUserAgent(DEFAULT_USER_AGENT);
  await page.evaluateOnNewDocument(() => {
    Object.defineProperty(navigator, "webdriver", { get: () => undefined });
  });
  return page;
}

/**
 * Navigate to a BBB page and wait/retry until Cloudflare challenge text is gone.
 *
 * @param {import("puppeteer").Page} page - Puppeteer page to navigate.
 * @param {string} url - URL to visit.
 * @param {BbbHarvestOptions} options - Navigation retry configuration.
 * @returns {Promise<NavigationResult>} Final navigation status and challenge state.
 */
async function gotoAccessibleBbbPage(page, url, options) {
  /** @type {number | null} */
  let status = null;
  /** @type {NavigationResult} */
  let lastResult = { ok: false, status: null, title: "", previewText: "" };
  for (let attempt = 1; attempt <= options.challengeAttempts; attempt += 1) {
    const response = await page
      .goto(url, {
        waitUntil: "domcontentloaded",
        timeout: options.navigationTimeoutMs,
      })
      .catch(() => null);
    status = response?.status() ?? null;
    for (let check = 0; check < options.challengeChecksPerAttempt; check += 1) {
      await sleep(options.challengeCheckIntervalMs);
      const state = await readPageChallengeState(page);
      lastResult = {
        ok:
          !isCloudflareChallenge(state.title, state.previewText) &&
          !isBbbErrorPage(state.title, state.previewText),
        status,
        ...state,
      };
      if (lastResult.ok) return lastResult;
    }
  }
  return lastResult;
}

/**
 * Read title and preview text for challenge detection.
 *
 * @param {import("puppeteer").Page} page - Puppeteer page.
 * @returns {Promise<{ readonly title: string, readonly previewText: string }>} Page title and body preview.
 */
async function readPageChallengeState(page) {
  const title = await page.title();
  const previewText = await page.evaluate(
    () => document.body?.innerText?.slice(0, 500) ?? "",
  );
  return { title, previewText };
}

/**
 * Snapshot visible and structured page data.
 *
 * @param {import("puppeteer").Page} page - Puppeteer page.
 * @param {boolean} includeHtml - Whether to include full HTML in the snapshot.
 * @returns {Promise<PageSnapshot>} Captured page snapshot.
 */
async function snapshotPage(page, includeHtml) {
  return page.evaluate((shouldIncludeHtml) => {
    const links = [...document.querySelectorAll("a[href]")]
      .map((anchor) => ({
        text: anchor.textContent?.trim().replace(/\s+/g, " ") ?? "",
        href: /** @type {HTMLAnchorElement} */ (anchor).href,
      }))
      .filter((link) => link.text.length > 0 || link.href.length > 0);
    const headings = [...document.querySelectorAll("h1,h2,h3,h4")]
      .map((heading) => heading.textContent?.trim().replace(/\s+/g, " ") ?? "")
      .filter((heading) => heading.length > 0);
    const jsonLd = [
      ...document.querySelectorAll('script[type="application/ld+json"]'),
    ]
      .map((script) => script.textContent ?? "")
      .filter((text) => text.trim().length > 0);
    return {
      url: location.href,
      title: document.title,
      text: document.body?.innerText ?? "",
      headings,
      links,
      jsonLd,
      html: shouldIncludeHtml ? document.documentElement.outerHTML : null,
    };
  }, includeHtml);
}

/**
 * Discover selected profile subpage URLs from main profile links.
 *
 * @param {readonly PageLink[]} links - Main profile page links.
 * @param {readonly string[]} selectedKinds - Subpage kinds requested by CLI options.
 * @returns {readonly { readonly kind: string, readonly url: string }[]} Selected unique subpage targets.
 */
function discoverProfileSubpages(links, selectedKinds) {
  /** @type {{ readonly kind: string, readonly url: string }[]} */
  const targets = [];
  /**
   * Add the first matching subpage link for a selected subpage kind.
   *
   * @param {string} kind - Logical subpage kind.
   * @param {(href: string) => boolean} predicate - Link predicate for the subpage URL.
   * @returns {void}
   */
  const add = (kind, predicate) => {
    if (!selectedKinds.includes(kind)) return;
    const url = links.find((link) => predicate(link.href))?.href;
    if (url !== undefined && !targets.some((target) => target.url === url)) {
      targets.push({ kind, url: canonicalBbbProfileUrl(url) });
    }
  };
  add("customer-reviews", (href) => href.includes("/customer-reviews"));
  add("complaints", (href) => href.includes("/complaints"));
  add("more-info", (href) => href.includes("/more-info"));
  add("details", (href) => href.includes("/details"));
  return targets;
}

/**
 * Flatten raw JSON-LD strings into object records, including arrays and `@graph` containers.
 *
 * @param {readonly string[]} rawJsonLd - Raw JSON-LD script contents.
 * @returns {readonly JsonObject[]} Flattened JSON-LD objects.
 */
function flattenJsonLdObjects(rawJsonLd) {
  /** @type {JsonObject[]} */
  const objects = [];
  /**
   * Visit a parsed JSON-LD value and collect JSON object nodes.
   *
   * @param {unknown} value - Parsed JSON-LD value.
   * @returns {void}
   */
  const visit = (value) => {
    if (Array.isArray(value)) {
      for (const entry of value) visit(entry);
      return;
    }
    if (!isJsonObject(value)) return;
    objects.push(value);
    const graph = value["@graph"];
    if (Array.isArray(graph)) visit(graph);
  };
  for (const raw of rawJsonLd) {
    try {
      visit(JSON.parse(raw));
    } catch {
      // Keep malformed JSON-LD in raw page evidence; skip typed promotion.
    }
  }
  return objects;
}

/**
 * Find a JSON-LD object by Schema.org type.
 *
 * @param {readonly JsonObject[]} objects - Flattened JSON-LD objects.
 * @param {string} typeName - Type to match, e.g. `LocalBusiness`.
 * @returns {JsonObject | null} First matching object.
 */
function findJsonLdObjectByType(objects, typeName) {
  return (
    objects.find((object) => jsonLdTypeMatches(object["@type"], typeName)) ??
    null
  );
}

/**
 * Build a structured address object from LocalBusiness JSON-LD.
 *
 * @param {JsonObject | null} localBusiness - LocalBusiness JSON-LD object.
 * @returns {JsonObject | null} Address object with geo folded in when available.
 */
function buildAddressRecord(localBusiness) {
  if (localBusiness === null) return null;
  const rawAddress = localBusiness.address;
  if (!isJsonObject(rawAddress)) return null;
  const geo = isJsonObject(localBusiness.geo) ? localBusiness.geo : null;
  return {
    ...rawAddress,
    ...(geo === null ? {} : { geo }),
  };
}

/**
 * Build management rows from LocalBusiness employees plus visible management lines.
 *
 * @param {JsonObject | null} localBusiness - LocalBusiness JSON-LD object.
 * @param {string} pageText - Visible profile text.
 * @returns {readonly JsonObject[]} Management/contact rows.
 */
function businessManagementRows(localBusiness, pageText) {
  /** @type {JsonObject[]} */
  const rows = [];
  /**
   * Add a contact row, avoiding duplicate name/title/role combinations.
   *
   * @param {JsonObject} row - Contact row payload.
   * @returns {void}
   */
  const addRow = (row) => {
    const name = readStringProperty(row, "name") ?? joinContactName(row);
    if (name === null || name === ":") return;
    const title = readStringProperty(row, "title");
    const role = readStringProperty(row, "role");
    const identity = [name, title, role]
      .map((part) => part ?? "")
      .join("|")
      .toLowerCase();
    if (
      rows.some((existing) => {
        const existingName =
          readStringProperty(existing, "name") ?? joinContactName(existing);
        const existingTitle = readStringProperty(existing, "title");
        const existingRole = readStringProperty(existing, "role");
        return (
          [existingName, existingTitle, existingRole]
            .map((part) => part ?? "")
            .join("|")
            .toLowerCase() === identity
        );
      })
    )
      return;
    rows.push({
      ...row,
      ...(readStringProperty(row, "name") === null ? { name } : {}),
    });
  };
  const employee = localBusiness?.employee;
  if (Array.isArray(employee)) {
    for (const entry of employee) {
      if (isJsonObject(entry)) addRow(entry);
    }
  } else if (isJsonObject(employee)) {
    addRow(employee);
  }
  const managementText = readSection(
    pageText,
    "Business Management",
    "Additional Contact Information",
  );
  if (managementText !== null) {
    for (const line of managementText
      .split(/\n/)
      .map((value) => value.trim())
      .filter(Boolean)) {
      const split = line.split(/,\s*/);
      addRow({
        name: split[0] ?? line,
        title: split.slice(1).join(", ") || null,
        source: "visible_text",
      });
    }
  }
  for (const row of contactRowsFromSection(
    pageText,
    "Principal Contacts",
    "Customer Contacts",
    "PRINCIPAL",
  ))
    addRow(row);
  for (const row of contactRowsFromSection(
    pageText,
    "Customer Contacts",
    [
      "Additional Email Addresses",
      "Additional Websites",
      "Social Media",
      "Additional Information",
      "Business Categories",
    ],
    "CUSTOMER",
  ))
    addRow(row);
  return rows;
}

/**
 * Parse alternate business-name rows from the visible Business Details section.
 *
 * @param {string} pageText - Visible profile text.
 * @returns {readonly JsonObject[]} Alternate-name rows.
 */
function alternateNameRows(pageText) {
  const section = readSection(
    pageText,
    "Alternate Names:",
    "Business Management:",
  );
  if (section === null) return [];
  return section
    .split(/\n/)
    .map((value) => value.trim())
    .filter((value) => value.length > 0)
    .map((name) => ({ name, source: "visible_text" }));
}

/**
 * Parse contact rows from a bounded visible text section.
 *
 * @param {string} pageText - Visible profile text.
 * @param {string} startLabel - Section start label.
 * @param {string | readonly string[]} endLabel - Section end label or labels.
 * @param {string} role - Contact role to assign.
 * @returns {readonly JsonObject[]} Contact rows.
 */
function contactRowsFromSection(pageText, startLabel, endLabel, role) {
  const endLabels = typeof endLabel === "string" ? [endLabel] : endLabel;
  const section = readSectionUntilAny(pageText, startLabel, endLabels);
  if (section === null) return [];
  return section
    .split(/\n/)
    .map((value) => value.trim())
    .filter((value) => value.length > 0)
    .map((line) => {
      const split = line.split(/,\s*/);
      return {
        name: split[0] ?? line,
        title: split.slice(1).join(", ") || null,
        role,
        source: "visible_text",
      };
    });
}

/**
 * Build category rows from BBB category links.
 *
 * @param {readonly PageLink[]} links - Profile page links.
 * @returns {readonly JsonObject[]} Unique category rows.
 */
function categoryRows(links) {
  /** @type {Map<string, JsonObject>} */
  const rowsByUrl = new Map();
  for (const link of links) {
    if (!/\/category\//.test(link.href) && !/\/category-/.test(link.href))
      continue;
    if (
      link.text.length === 0 ||
      (/^Home|USA|Missouri|Florida|Data$/i.test(link.text) &&
        !link.href.includes("/category/data"))
    )
      continue;
    rowsByUrl.set(canonicalBbbProfileUrl(link.href), {
      name: link.text,
      url: canonicalBbbProfileUrl(link.href),
    });
  }
  return [...rowsByUrl.values()];
}

/**
 * Build branch/location rows from BBB profile links that carry an `addressId`.
 *
 * @param {readonly PageLink[]} links - Profile page links.
 * @param {string} primaryProfileUrl - Canonical primary profile URL.
 * @returns {readonly JsonObject[]} Branch/location rows.
 */
function locationRows(links, primaryProfileUrl) {
  /** @type {Map<string, JsonObject>} */
  const rowsByUrl = new Map();
  for (const link of links) {
    if (!isBbbProfileUrl(link.href) || !link.href.includes("/addressId/"))
      continue;
    const profileUrl = canonicalBbbProfileUrl(link.href);
    if (profileUrl === primaryProfileUrl) continue;
    const identity = parseBbbProfileUrlIdentity(profileUrl);
    rowsByUrl.set(profileUrl, {
      relationshipType: "BRANCH",
      name: link.text || null,
      profileUrl,
      providerBusinessId: identity.providerBusinessId,
      providerBbbId: identity.providerBbbId,
      source: "profile_link",
    });
  }
  return [...rowsByUrl.values()];
}

/**
 * Build licensing rows from BBB's visible licensing section.
 *
 * @param {string} pageText - Visible profile text.
 * @returns {readonly JsonObject[]} Licensing rows.
 */
function licenseRows(pageText) {
  const section = readSection(
    pageText,
    "Licensing information",
    "Additional Information",
  );
  if (section === null) return [];
  return [{ rawText: section, source: "visible_text" }];
}

/**
 * Build service-area rows from JSON-LD `areaServed` and visible section text.
 *
 * @param {JsonObject | null} localBusiness - LocalBusiness JSON-LD object.
 * @param {string} pageText - Visible page text.
 * @returns {readonly JsonObject[]} Service-area rows.
 */
function serviceAreaRows(localBusiness, pageText) {
  /** @type {JsonObject[]} */
  const rows = [];
  const areaServed = localBusiness?.areaServed;
  /**
   * Add one JSON-LD service area value when it has a display name.
   *
   * @param {unknown} value - JSON-LD service-area value.
   * @returns {void}
   */
  const addArea = (value) => {
    const name =
      typeof value === "string"
        ? value
        : isJsonObject(value)
          ? readStringProperty(value, "name")
          : null;
    if (name !== null && !rows.some((row) => row.name === name))
      rows.push({ name, source: "json_ld" });
  };
  if (Array.isArray(areaServed)) areaServed.forEach(addArea);
  else addArea(areaServed);
  const section =
    readSection(pageText, "Service Area", "BBB Business Profiles") ??
    readSection(pageText, "Serving the following areas:", "BBB Accreditation");
  if (section !== null) {
    for (const line of section
      .split(/\n/)
      .map((value) => value.trim())
      .filter(Boolean)) {
      if (!rows.some((row) => row.name === line))
        rows.push({ name: line, source: "visible_text" });
    }
  }
  return rows;
}

/**
 * Build image/media rows from LocalBusiness JSON-LD.
 *
 * @param {JsonObject | null} localBusiness - LocalBusiness JSON-LD object.
 * @returns {readonly JsonObject[]} Media rows.
 */
function imageRows(localBusiness) {
  if (localBusiness === null) return [];
  /** @type {Map<string, JsonObject>} */
  const rowsByUrl = new Map();
  /**
   * Add one JSON-LD image/logo value to the media rows.
   *
   * @param {unknown} value - JSON-LD media value.
   * @param {string} mediaKind - Promoted media kind.
   * @returns {void}
   */
  const add = (value, mediaKind) => {
    const url =
      typeof value === "string"
        ? value
        : isJsonObject(value)
          ? (readStringProperty(value, "url") ??
            readStringProperty(value, "contentUrl"))
          : null;
    if (url !== null)
      rowsByUrl.set(url, { type: mediaKind, url, source: "json_ld" });
  };
  add(localBusiness.logo, "LOGO");
  const image = localBusiness.image;
  if (Array.isArray(image)) image.forEach((entry) => add(entry, "IMAGE"));
  else add(image, "IMAGE");
  return [...rowsByUrl.values()];
}

/**
 * Return external links that describe the business rather than BBB navigation.
 *
 * @param {readonly PageLink[]} links - Profile page links.
 * @returns {readonly JsonObject[]} External link rows.
 */
function externalBusinessLinks(links) {
  /** @type {Map<string, JsonObject>} */
  const rowsByUrl = new Map();
  for (const link of links) {
    if (!isExternalBusinessUrl(link.href)) continue;
    if (isBbbOwnedExternalLink(link)) continue;
    rowsByUrl.set(link.href, {
      kind: classifyExternalLink(link.href),
      url: link.href,
      label: link.text || null,
    });
  }
  return [...rowsByUrl.values()];
}

/**
 * Parse review counters from a BBB customer-reviews page.
 *
 * @param {string} text - Visible review page text.
 * @returns {JsonObject} Review summary fields.
 */
function parseReviewsSummary(text) {
  return stripNullish({
    reviewsTotal: readFirstIntegerMatch(text, [
      /This business has\s+(\d+)\s+reviews?/i,
      /(\d+)\s+Customer Reviews?/i,
    ]),
    averageOfReviewStarRatings: readFirstNumberMatch(text, [
      /Average of\s+([\d.]+)\s+(?:Customer\s+)?Reviews?/i,
      /([\d.]+)\s+stars?/i,
    ]),
  });
}

/**
 * Parse individual customer review rows from a BBB customer-reviews page.
 *
 * The visible BBB layout has changed over time, so this parser is conservative:
 * it only emits rows for blocks that have a recognizable review heading/date,
 * while the full raw page remains preserved under `bbbHarvest`.
 *
 * @param {string} text - Visible review page text.
 * @returns {readonly JsonObject[]} Review rows.
 */
function parseReviewRows(text) {
  if (/This business has\s+0\s+reviews?/i.test(text)) return [];
  const section = truncateBbbBoilerplate(
    readSection(text, "Reviews", "BBB Business Profiles") ?? text,
  );
  /** @type {JsonObject[]} */
  const rows = [];
  const starts = [
    ...section.matchAll(/(?:^|\n)(Review from[^\n]*|Customer Review)\s*\n/g),
  ].map((match) => match.index ?? 0);
  for (const [index, start] of starts.entries()) {
    const end = starts[index + 1] ?? section.length;
    const block = cleanVisibleTextBlock(section.slice(start, end));
    const date = readTextAfterLabel(block, "Date");
    const rating = readFirstNumberMatch(block, [
      /([\d.]+)\s*(?:out of 5\s*)?stars?/i,
    ]);
    const reviewer =
      /^Review from\s+([^\n]+)/i.exec(block)?.[1]?.trim() ?? null;
    const body = cleanVisibleTextBlock(
      block
        .replace(/^Review from[^\n]*\n?/i, "")
        .replace(/^Customer Review\n?/i, "")
        .replace(/Date:\s*[^\n]+/i, "")
        .replace(/[\d.]+\s*(?:out of 5\s*)?stars?/i, ""),
    );
    if (date === null && rating === null && body.length === 0) continue;
    rows.push(
      stripNullish({
        providerReviewId: `visible:${index + 1}:${hashString(block).slice(0, 16)}`,
        reviewDate: date,
        reviewRating: rating,
        reviewerDisplayName: reviewer,
        reviewText: body.length > 0 ? body : null,
        source: "visible_text",
      }),
    );
  }
  return rows;
}

/**
 * Parse complaint counters from a BBB complaints page.
 *
 * @param {string} text - Visible complaints page text.
 * @returns {JsonObject} Complaint summary fields.
 */
function parseComplaintsSummary(text) {
  return stripNullish({
    totalClosedComplaintsPastThreeYears: readFirstIntegerMatch(text, [
      /(\d+)\s+complaints? closed in (?:the )?last 3 years/i,
      /(\d+)\s+complaints? closed in (?:the )?last three years/i,
    ]),
    totalClosedComplaintsPastTwelveMonths: readFirstIntegerMatch(text, [
      /(\d+)\s+complaints? closed in (?:the )?last 12 months/i,
      /(\d+)\s+complaints? closed in (?:the )?last twelve months/i,
    ]),
    complaintsTotal: readFirstIntegerMatch(text, [
      /Customer Complaints\s*(\d+)/i,
      /This business has\s+(\d+)\s+complaints?/i,
      /(\d+)\s+complaints? in (?:the )?last 3 years/i,
      /(\d+)\s+complaints? in (?:the )?last three years/i,
    ]),
  });
}

/**
 * Parse individual complaint rows and event timelines from a BBB complaints page.
 *
 * @param {string} text - Visible complaints page text.
 * @returns {readonly JsonObject[]} Complaint rows.
 */
function parseComplaintRows(text) {
  const startIndex = text.indexOf("Filter and sort by");
  if (startIndex < 0) return [];
  const body = truncateBbbBoilerplate(text.slice(startIndex));
  const starts = [...body.matchAll(/(?:^|\n)Initial Complaint\s*\n/g)].map(
    (match) => match.index ?? 0,
  );
  /** @type {JsonObject[]} */
  const rows = [];
  for (const [index, start] of starts.entries()) {
    const end = starts[index + 1] ?? body.length;
    const block = cleanVisibleTextBlock(body.slice(start, end));
    const complaintDate = readTextAfterLabel(block, "Date");
    const complaintType = readTextAfterLabel(block, "Type");
    const complaintStatus = readTextAfterLabel(block, "Status");
    const moreInfoIndex = block.indexOf("More info");
    const eventStartIndex = findFirstComplaintEventIndex(
      block,
      moreInfoIndex < 0 ? 0 : moreInfoIndex,
    );
    const complaintText =
      moreInfoIndex < 0
        ? null
        : cleanVisibleTextBlock(
            block.slice(
              moreInfoIndex + "More info".length,
              eventStartIndex ?? undefined,
            ),
          );
    const events =
      eventStartIndex === null
        ? []
        : parseComplaintEventRows(block.slice(eventStartIndex));
    if (
      complaintDate === null &&
      complaintType === null &&
      complaintStatus === null &&
      complaintText === null
    )
      continue;
    const row = stripNullish({
      providerComplaintId: `visible:${index + 1}:${hashString(block).slice(0, 16)}`,
      complaintDate,
      complaintType,
      complaintStatus,
      complaintText,
      events,
      source: "visible_text",
    });
    rows.push(row);
  }
  return rows;
}

/**
 * Find the first known complaint event heading in a complaint block.
 *
 * @param {string} block - Complaint block text.
 * @param {number} fromIndex - Offset where event headings may start.
 * @returns {number | null} First event-heading index.
 */
function findFirstComplaintEventIndex(block, fromIndex) {
  const labels = [
    "Business Response",
    "Customer Answer",
    "Customer Response",
    "Consumer Response",
  ];
  const indexes = labels
    .map((label) => block.indexOf(label, fromIndex))
    .filter((index) => index >= 0);
  return indexes.length === 0 ? null : Math.min(...indexes);
}

/**
 * Parse complaint event timeline rows from a complaint block suffix.
 *
 * @param {string} text - Complaint block suffix beginning at the first event heading.
 * @returns {readonly JsonObject[]} Complaint event rows.
 */
function parseComplaintEventRows(text) {
  /** @type {JsonObject[]} */
  const events = [];
  const pattern =
    /(?:^|\n)(Business Response|Customer Answer|Customer Response|Consumer Response)\s*\n+Date:\s*([^\n]+)\s*\n+([\s\S]*?)(?=\n(?:Business Response|Customer Answer|Customer Response|Consumer Response)\s*\n+Date:|$)/g;
  for (const match of text.matchAll(pattern)) {
    const heading = match[1];
    const eventDate = match[2].trim();
    const eventText = cleanVisibleTextBlock(match[3]);
    if (eventText.length === 0) continue;
    events.push({
      type: heading.toUpperCase().replace(/\s+/g, "_"),
      actorRole: heading.toLowerCase().includes("business")
        ? "BUSINESS"
        : "CUSTOMER",
      date: eventDate,
      text: eventText,
      source: "visible_text",
    });
  }
  return events;
}

/**
 * Parse rating-reason text from a BBB more-info page.
 *
 * @param {string} text - Visible more-info page text.
 * @returns {readonly JsonObject[]} Rating reason rows.
 */
function parseRatingReasons(text) {
  const section =
    readSection(text, "Reasons for BBB Rating", "BBB Business Profiles") ??
    readSection(text, "Factors that affect", "BBB Business Profiles");
  if (section === null) return [];
  return section
    .split(/\n/)
    .map((line) => line.trim())
    .filter((line) => line.length > 0)
    .map((line, index) => ({ text: line, ordinal: index + 1 }));
}

/**
 * Parse CLI options into a full harvest configuration.
 *
 * @param {readonly string[]} args - Raw command-line arguments after the script name.
 * @returns {BbbHarvestOptions} Parsed options.
 */
function parseCliOptions(args) {
  const { values } = parseArgs({
    args,
    options: {
      "category-url": { type: "string" },
      "output-dir": { type: "string" },
      "output-s3-uri": { type: "string" },
      "chromium-executable-path": { type: "string" },
      headless: { type: "string" },
      "start-page": { type: "string" },
      "max-pages": { type: "string" },
      "max-profiles": { type: "string" },
      "part-record-limit": { type: "string" },
      "page-delay-ms": { type: "string" },
      "profile-delay-ms": { type: "string" },
      "challenge-attempts": { type: "string" },
      "challenge-check-interval-ms": { type: "string" },
      "challenge-checks-per-attempt": { type: "string" },
      "navigation-timeout-ms": { type: "string" },
      "profile-subpages": { type: "string" },
      "no-html": { type: "boolean" },
    },
    strict: true,
    allowPositionals: false,
  });
  return {
    categoryUrl:
      readStringOption(values, "category-url") ?? DEFAULT_CATEGORY_URL,
    outputLocation: parseOutputLocation(values),
    chromiumExecutablePath:
      readStringOption(values, "chromium-executable-path") ??
      process.env.CHROME_EXECUTABLE_PATH ??
      "/usr/bin/chromium",
    headless: parseBooleanOption(readStringOption(values, "headless"), false),
    startPage:
      parsePositiveIntegerOption(values["start-page"], "start-page") ?? 1,
    maxPages: parsePositiveIntegerOption(values["max-pages"], "max-pages"),
    maxProfiles: parsePositiveIntegerOption(
      values["max-profiles"],
      "max-profiles",
    ),
    partRecordLimit:
      parsePositiveIntegerOption(
        values["part-record-limit"],
        "part-record-limit",
      ) ?? DEFAULT_PART_RECORD_LIMIT,
    pageDelayMs:
      parseNonNegativeIntegerOption(values["page-delay-ms"], "page-delay-ms") ??
      DEFAULT_PAGE_DELAY_MS,
    profileDelayMs:
      parseNonNegativeIntegerOption(
        values["profile-delay-ms"],
        "profile-delay-ms",
      ) ?? DEFAULT_PROFILE_DELAY_MS,
    challengeAttempts:
      parsePositiveIntegerOption(
        values["challenge-attempts"],
        "challenge-attempts",
      ) ?? DEFAULT_CHALLENGE_ATTEMPTS,
    challengeCheckIntervalMs:
      parseNonNegativeIntegerOption(
        values["challenge-check-interval-ms"],
        "challenge-check-interval-ms",
      ) ?? DEFAULT_CHALLENGE_CHECK_INTERVAL_MS,
    challengeChecksPerAttempt:
      parsePositiveIntegerOption(
        values["challenge-checks-per-attempt"],
        "challenge-checks-per-attempt",
      ) ?? DEFAULT_CHALLENGE_CHECKS_PER_ATTEMPT,
    navigationTimeoutMs:
      parsePositiveIntegerOption(
        values["navigation-timeout-ms"],
        "navigation-timeout-ms",
      ) ?? DEFAULT_NAVIGATION_TIMEOUT_MS,
    includeHtml: values["no-html"] !== true,
    profileSubpages: parseProfileSubpages(
      readStringOption(values, "profile-subpages"),
    ),
  };
}

/**
 * Parse output destination options.
 *
 * @param {Record<string, string | boolean | undefined>} values - CLI option values.
 * @returns {OutputLocation} Parsed local or S3 output location.
 */
function parseOutputLocation(values) {
  const outputS3Uri = readStringOption(values, "output-s3-uri");
  const outputDir = readStringOption(values, "output-dir");
  if (outputS3Uri !== null && outputDir !== null) {
    throw new Error("Use only one of --output-s3-uri or --output-dir");
  }
  if (outputS3Uri !== null) {
    const parsed = parseS3Uri(outputS3Uri.replace(/\/$/, "/_placeholder"));
    return {
      kind: "s3",
      bucket: parsed.bucket,
      keyPrefix: parsed.key.replace(/\/_placeholder$/, "").replace(/\/$/, ""),
    };
  }
  return {
    kind: "local",
    dir:
      outputDir ??
      path.join(
        ".harvest-runs",
        `bbb-${new Date()
          .toISOString()
          .replace(/[-:]/g, "")
          .replace(/\.\d{3}Z$/, "Z")}`,
      ),
  };
}

/**
 * Write JSONL records to local disk or S3.
 *
 * @param {OutputLocation} outputLocation - Destination root.
 * @param {string} relativePath - Relative artifact path beneath the destination root.
 * @param {readonly JsonObject[]} records - JSON records to serialize, one per line.
 * @returns {Promise<string>} Local path or S3 URI written.
 */
async function writeJsonlRecords(outputLocation, relativePath, records) {
  const body = records.map((record) => `${JSON.stringify(record)}\n`).join("");
  return writeOutputObject(
    outputLocation,
    relativePath,
    body,
    "application/x-ndjson",
  );
}

/**
 * Write one JSON object to local disk or S3.
 *
 * @param {OutputLocation} outputLocation - Destination root.
 * @param {string} relativePath - Relative artifact path beneath the destination root.
 * @param {JsonObject} value - JSON object to serialize.
 * @returns {Promise<string>} Local path or S3 URI written.
 */
async function writeJsonObject(outputLocation, relativePath, value) {
  return writeOutputObject(
    outputLocation,
    relativePath,
    `${JSON.stringify(value, null, 2)}\n`,
    "application/json",
  );
}

/**
 * Write one text object to local disk or S3.
 *
 * @param {OutputLocation} outputLocation - Destination root.
 * @param {string} relativePath - Relative artifact path beneath the destination root.
 * @param {string} body - Text body.
 * @param {string} contentType - MIME content type.
 * @returns {Promise<string>} Local path or S3 URI written.
 */
async function writeOutputObject(
  outputLocation,
  relativePath,
  body,
  contentType,
) {
  if (outputLocation.kind === "local") {
    const filePath = path.join(outputLocation.dir, relativePath);
    await mkdir(path.dirname(filePath), { recursive: true });
    await writeFile(filePath, body, "utf8");
    return filePath;
  }
  const key = `${outputLocation.keyPrefix}/${relativePath}`.replace(/^\//, "");
  const s3 = new S3Client({});
  await s3.send(
    new PutObjectCommand({
      Bucket: outputLocation.bucket,
      Key: key,
      Body: body,
      ContentType: contentType,
    }),
  );
  return `s3://${outputLocation.bucket}/${key}`;
}

/**
 * Build a category page URL with a one-based `page` query parameter.
 *
 * @param {string} categoryUrl - Base category URL.
 * @param {number} pageNumber - One-based page number.
 * @returns {string} URL for the requested category page.
 */
function buildCategoryPageUrl(categoryUrl, pageNumber) {
  const parsed = new URL(categoryUrl);
  if (pageNumber <= 1) parsed.searchParams.delete("page");
  else parsed.searchParams.set("page", String(pageNumber));
  return parsed.toString();
}

/**
 * Parse an S3 URI.
 *
 * @param {string} uri - S3 URI.
 * @returns {S3UriParts} Bucket/key pair.
 */
function parseS3Uri(uri) {
  const parsed = new URL(uri);
  if (parsed.protocol !== "s3:")
    throw new Error(`Expected s3:// URI, received ${uri}`);
  if (parsed.hostname.length === 0)
    throw new Error(`S3 URI is missing bucket: ${uri}`);
  return {
    bucket: parsed.hostname,
    key: decodeURIComponent(parsed.pathname.replace(/^\//, "")),
  };
}

/**
 * Build a provider profile id from parsed URL identity.
 *
 * @param {BbbProfileUrlIdentity} identity - Parsed profile URL identity.
 * @param {string} profileUrl - Canonical profile URL fallback.
 * @returns {string} Stable provider profile id.
 */
function buildProviderProfileId(identity, profileUrl) {
  if (identity.providerBbbId !== null && identity.providerBusinessId !== null) {
    return [
      identity.providerBbbId,
      identity.providerBusinessId,
      identity.addressId,
    ]
      .filter(Boolean)
      .join(":");
  }
  return hashString(profileUrl).slice(0, 24);
}

/**
 * Determine whether title/text still represent BBB's Cloudflare challenge.
 *
 * @param {string} title - Page title.
 * @param {string} text - Visible body preview.
 * @returns {boolean} True when the page is still a challenge page.
 */
function isCloudflareChallenge(title, text) {
  return /Just a moment/i.test(title) || /Just a moment\.\.\./i.test(text);
}

/**
 * Determine whether title/text represent BBB's own transient error shell.
 *
 * @param {string} title - Page title.
 * @param {string} text - Visible body preview.
 * @returns {boolean} True when BBB rendered an error page instead of requested data.
 */
function isBbbErrorPage(title, text) {
  return (
    /Error \| Better Business Bureau/i.test(title) ||
    /OOPS! WE'LL BE RIGHT BACK/i.test(text) ||
    /Loading chunk \d+ failed/i.test(text)
  );
}

/**
 * Identify BBB profile URLs.
 *
 * @param {string} url - URL to classify.
 * @returns {boolean} True when the URL points at a BBB profile path.
 */
function isBbbProfileUrl(url) {
  try {
    const parsed = new URL(url);
    return (
      parsed.hostname.endsWith("bbb.org") &&
      parsed.pathname.includes("/profile/")
    );
  } catch {
    return false;
  }
}

/**
 * Canonicalize BBB URLs for deduplication while preserving meaningful subpaths.
 *
 * @param {string} url - Input URL.
 * @returns {string} URL without hash or tracking search parameters.
 */
function canonicalBbbProfileUrl(url) {
  const parsed = new URL(url);
  parsed.hash = "";
  parsed.search = "";
  return parsed.toString().replace(/\/$/, "");
}

/**
 * Canonicalize a BBB profile URL to the business/location page, dropping tab subpages.
 *
 * @param {string} url - Input BBB profile or profile-subpage URL.
 * @returns {string} Canonical profile URL without review/complaint/detail subpage suffixes.
 */
function canonicalBbbProfileBaseUrl(url) {
  const parsed = new URL(canonicalBbbProfileUrl(url));
  const segments = parsed.pathname.split("/").filter(Boolean);
  const knownSubpages = new Set([
    "complaints",
    "customer-reviews",
    "details",
    "email-this-business",
    "leave-a-review",
    "more-info",
  ]);
  const subpageIndex = segments.findIndex(
    (segment, index) => index > 0 && knownSubpages.has(segment),
  );
  if (subpageIndex >= 0) {
    parsed.pathname = `/${segments.slice(0, subpageIndex).join("/")}`;
  }
  return parsed.toString().replace(/\/$/, "");
}

/**
 * Return whether a URL is an external business/resource URL worth promoting.
 *
 * @param {string} url - Link URL.
 * @returns {boolean} True for non-BBB http(s) links, excluding share/mail links.
 */
function isExternalBusinessUrl(url) {
  try {
    const parsed = new URL(url);
    if (parsed.protocol !== "http:" && parsed.protocol !== "https:")
      return false;
    if (parsed.hostname.endsWith("bbb.org")) return false;
    if (
      /facebook\.com\/sharer|twitter\.com\/intent|linkedin\.com\/shareArticle/i.test(
        url,
      )
    )
      return false;
    return true;
  } catch {
    return false;
  }
}

/**
 * Identify external footer/affiliate links owned by BBB rather than the profiled business.
 *
 * @param {PageLink} link - Page link to classify.
 * @returns {boolean} True when the link is BBB-owned footer or affiliate navigation.
 */
function isBbbOwnedExternalLink(link) {
  try {
    const parsed = new URL(link.href);
    const hostname = parsed.hostname.toLowerCase();
    const pathname = parsed.pathname.toLowerCase();
    if (/^our\s+/i.test(link.text)) return true;
    if (
      /BBB Institute|BBB Wise Giving|BBB National Programs|Scam Survival Kit/i.test(
        link.text,
      )
    )
      return true;
    if (
      hostname.endsWith("bbbmarketplacetrust.org") ||
      hostname.endsWith("give.org") ||
      hostname.endsWith("bbbprograms.org")
    )
      return true;
    if (
      hostname.includes("facebook.com") &&
      pathname.includes("betterbusinessbureau")
    )
      return true;
    if (
      (hostname === "x.com" || hostname.includes("twitter.com")) &&
      pathname.includes("bbb_us")
    )
      return true;
    if (
      hostname.includes("linkedin.com") &&
      pathname.includes("better-business-bureau")
    )
      return true;
    if (
      hostname.includes("instagram.com") &&
      pathname.includes("betterbusinessbureau")
    )
      return true;
    return false;
  } catch {
    return false;
  }
}

/**
 * Classify an external link by destination hostname.
 *
 * @param {string} url - External URL.
 * @returns {string} Link kind for query-db external link rows.
 */
function classifyExternalLink(url) {
  const hostname = new URL(url).hostname.toLowerCase();
  if (hostname.includes("facebook")) return "FACEBOOK";
  if (hostname.includes("linkedin")) return "LINKEDIN";
  if (hostname.includes("instagram")) return "INSTAGRAM";
  if (hostname.includes("x.com") || hostname.includes("twitter"))
    return "TWITTER";
  return "WEBSITE";
}

/**
 * Read first website-like URL from page links.
 *
 * @param {readonly PageLink[]} links - Page links.
 * @returns {string | null} First external website URL.
 */
function firstWebsiteUrl(links) {
  const visitWebsite = links.find(
    (link) =>
      /^Visit Website$/i.test(link.text) &&
      isExternalBusinessUrl(link.href) &&
      !isBbbOwnedExternalLink(link),
  );
  if (visitWebsite !== undefined) return visitWebsite.href;
  const websiteRow =
    externalBusinessLinks(links).find((link) => link.kind === "WEBSITE") ??
    null;
  return readStringProperty(websiteRow, "url");
}

/**
 * Read BBB's email-this-business URL from page links.
 *
 * @param {readonly PageLink[]} links - Page links.
 * @returns {string | null} Email-this-business URL.
 */
function firstEmailUrl(links) {
  const link = links.find(
    (candidate) =>
      /email-this-business/i.test(candidate.href) ||
      /^Email (?:this )?Business$/i.test(candidate.text),
  );
  return link?.href ?? null;
}

/**
 * Read first telephone link from page links.
 *
 * @param {readonly PageLink[]} links - Page links.
 * @returns {string | null} First visible phone number.
 */
function firstTelephoneLink(links) {
  const link = links.find((candidate) => candidate.href.startsWith("tel:"));
  return link?.text ?? null;
}

/**
 * Find the href for a visible link text value.
 *
 * @param {readonly PageLink[]} links - Page links.
 * @param {string | null} text - Visible link text to find.
 * @returns {string | null} Link href.
 */
function firstLinkAfterText(links, text) {
  if (text === null) return null;
  return links.find((link) => link.text === text)?.href ?? null;
}

/**
 * Read a string property from a JSON object.
 *
 * @param {JsonObject | null} object - Source object.
 * @param {string} key - Property name.
 * @returns {string | null} Trimmed string value.
 */
function readStringProperty(object, key) {
  if (object === null) return null;
  const value = object[key];
  return typeof value === "string" && value.trim().length > 0
    ? value.trim()
    : null;
}

/**
 * Join Schema.org person-name components into a display name.
 *
 * @param {JsonObject | null} object - Person/contact object.
 * @returns {string | null} Joined contact name.
 */
function joinContactName(object) {
  if (object === null) return null;
  return joinText([
    readStringProperty(object, "honorificPrefix"),
    readStringProperty(object, "givenName"),
    readStringProperty(object, "additionalName"),
    readStringProperty(object, "familyName"),
    readStringProperty(object, "honorificSuffix"),
  ]);
}

/**
 * Join non-empty text parts with normalized single spaces.
 *
 * @param {readonly (string | null)[]} parts - Text parts to join.
 * @returns {string | null} Joined text, or null when all parts are empty.
 */
function joinText(parts) {
  const text = parts
    .filter((part) => part !== null)
    .join(" ")
    .replace(/\s+/g, " ")
    .trim();
  return text.length === 0 ? null : text;
}

/**
 * Read a label value from visible BBB text.
 *
 * @param {string} text - Visible page text.
 * @param {string} label - Label before a colon.
 * @returns {string | null} One-line value after the label.
 */
function readTextAfterLabel(text, label) {
  const match = new RegExp(`${escapeRegExp(label)}:\\s*([^\\n]+)`, "i").exec(
    text,
  );
  return match === null ? null : match[1].trim();
}

/**
 * Read an integer value after a visible label.
 *
 * @param {string} text - Visible page text.
 * @param {string} label - Label before a colon.
 * @returns {number | null} Parsed integer.
 */
function readIntegerAfterLabel(text, label) {
  const value = readTextAfterLabel(text, label);
  if (value === null) return null;
  const match = /\d+/.exec(value.replace(/,/g, ""));
  return match === null ? null : Number(match[0]);
}

/**
 * Read a BBB letter rating from visible page text.
 *
 * @param {string} text - Visible page text.
 * @returns {string | null} Rating such as `A+` or `F`.
 */
function readBbbRating(text) {
  const ratedMatch = /\n\s*([A-F][+-]?|NR)\s*\n\s*Rated by BBB/i.exec(text);
  if (ratedMatch !== null) return ratedMatch[1];
  const labelMatch = /BBB Rating\s*\n\s*([A-F][+-]?|NR)/i.exec(text);
  return labelMatch === null ? null : labelMatch[1];
}

/**
 * Parse BBB accreditation status from visible text.
 *
 * @param {string} text - Visible page text.
 * @returns {boolean | null} Accreditation flag when visible.
 */
function readAccreditation(text) {
  if (/BBB Accredited Business|is BBB Accredited/i.test(text)) return true;
  if (/Not BBB Accredited|is not BBB Accredited/i.test(text)) return false;
  return null;
}

/**
 * Read a visible text section bounded by headings or labels.
 *
 * @param {string} text - Visible page text.
 * @param {string} startLabel - Starting heading/label.
 * @param {string} endLabel - Ending heading/label.
 * @returns {string | null} Trimmed section body.
 */
function readSection(text, startLabel, endLabel) {
  const startIndex = text.indexOf(startLabel);
  if (startIndex < 0) return null;
  const bodyStart = startIndex + startLabel.length;
  const endIndex = text.indexOf(endLabel, bodyStart);
  const section = text
    .slice(bodyStart, endIndex < 0 ? undefined : endIndex)
    .trim();
  return section.length === 0 ? null : section;
}

/**
 * Read a visible text section that must terminate at one of several labels.
 *
 * @param {string} text - Visible page text.
 * @param {string} startLabel - Starting heading/label.
 * @param {readonly string[]} endLabels - Candidate ending headings/labels.
 * @returns {string | null} Trimmed section body, or null when no boundary is found.
 */
function readSectionUntilAny(text, startLabel, endLabels) {
  const startIndex = text.indexOf(startLabel);
  if (startIndex < 0) return null;
  const bodyStart = startIndex + startLabel.length;
  const endIndexes = endLabels
    .map((label) => text.indexOf(label, bodyStart))
    .filter((index) => index >= 0);
  if (endIndexes.length === 0) return null;
  const section = text.slice(bodyStart, Math.min(...endIndexes)).trim();
  return section.length === 0 ? null : section;
}

/**
 * Trim BBB footer/accreditation boilerplate from a visible-text extraction window.
 *
 * @param {string} text - Visible text block.
 * @returns {string} Text before common BBB boilerplate/footer sections.
 */
function truncateBbbBoilerplate(text) {
  const accreditationMatch =
    /\n[^\n]+is BBB Accredited\.\s*\n\s*This business has committed/i.exec(
      text,
    );
  const stopIndexes = [
    accreditationMatch?.index ?? -1,
    text.indexOf("BBB Business Profiles are provided"),
    text.indexOf("\nTM\nFor Consumers"),
  ].filter((index) => index >= 0);
  const truncated =
    stopIndexes.length === 0 ? text : text.slice(0, Math.min(...stopIndexes));
  return cleanVisibleTextBlock(truncated);
}

/**
 * Normalize whitespace in a visible text block without flattening paragraphs.
 *
 * @param {string} text - Visible text block.
 * @returns {string} Cleaned text block.
 */
function cleanVisibleTextBlock(text) {
  return text
    .replace(/\u00a0/g, " ")
    .replace(/[ \t]+\n/g, "\n")
    .replace(/\n[ \t]+/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

/**
 * Best-effort business name fallback from visible text.
 *
 * @param {string} text - Visible page text.
 * @returns {string | null} Business profile name fallback.
 */
function readProfileNameFromText(text) {
  const lines = text
    .split(/\n/)
    .map((line) => line.trim())
    .filter(Boolean);
  const profileIndex = lines.findIndex((line) => line === "BUSINESS PROFILE");
  return profileIndex >= 0 ? (lines[profileIndex + 2] ?? null) : null;
}

/**
 * Read first integer capture across regex candidates.
 *
 * @param {string} text - Source text.
 * @param {readonly RegExp[]} patterns - Regex patterns with first capture group.
 * @returns {number | null} Parsed integer.
 */
function readFirstIntegerMatch(text, patterns) {
  for (const pattern of patterns) {
    const match = pattern.exec(text);
    if (match !== null) return Number(match[1].replace(/,/g, ""));
  }
  return null;
}

/**
 * Read first number capture across regex candidates.
 *
 * @param {string} text - Source text.
 * @param {readonly RegExp[]} patterns - Regex patterns with first capture group.
 * @returns {number | null} Parsed number.
 */
function readFirstNumberMatch(text, patterns) {
  for (const pattern of patterns) {
    const match = pattern.exec(text);
    if (match !== null) return Number(match[1].replace(/,/g, ""));
  }
  return null;
}

/**
 * Read `page` query value from a URL.
 *
 * @param {string} url - URL to inspect.
 * @returns {number | null} Positive page number.
 */
function readPageNumberFromUrl(url) {
  try {
    const value = new URL(url).searchParams.get("page");
    if (value === null) return null;
    const parsed = Number(value);
    return Number.isInteger(parsed) && parsed > 0 ? parsed : null;
  } catch {
    return null;
  }
}

/**
 * Check whether a JSON-LD `@type` matches a specific Schema.org type.
 *
 * @param {unknown} value - JSON-LD `@type` value.
 * @param {string} typeName - Type name to match.
 * @returns {boolean} True when the type value contains the requested type.
 */
function jsonLdTypeMatches(value, typeName) {
  if (typeof value === "string") return value === typeName;
  if (Array.isArray(value)) return value.some((entry) => entry === typeName);
  return false;
}

/**
 * Return true for non-null numbers.
 *
 * @param {number | null} value - Candidate value.
 * @returns {value is number} True for numbers.
 */
function isNonNullNumber(value) {
  return value !== null;
}

/**
 * Narrow an unknown value to a JSON object.
 *
 * @param {unknown} value - Candidate value.
 * @returns {value is JsonObject} True for plain non-array objects.
 */
function isJsonObject(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Remove null and undefined values from an object.
 *
 * @param {JsonObject} value - Source object.
 * @returns {JsonObject} Object without nullish values.
 */
function stripNullish(value) {
  return Object.fromEntries(
    Object.entries(value).filter(
      ([, entry]) => entry !== null && entry !== undefined,
    ),
  );
}

/**
 * Escape text for use in a RegExp constructor.
 *
 * @param {string} value - Raw text.
 * @returns {string} Regex-escaped text.
 */
function escapeRegExp(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * Hash a string with SHA-256.
 *
 * @param {string} value - String to hash.
 * @returns {string} Hex SHA-256 hash.
 */
function hashString(value) {
  return crypto.createHash("sha256").update(value).digest("hex");
}

/**
 * Sleep for a fixed duration.
 *
 * @param {number} milliseconds - Duration to wait.
 * @returns {Promise<void>} Promise resolved after the duration.
 */
function sleep(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

/**
 * Read a string option from parseArgs output.
 *
 * @param {Record<string, string | boolean | undefined>} values - CLI option values.
 * @param {string} key - Option key.
 * @returns {string | null} String option value.
 */
function readStringOption(values, key) {
  const value = values[key];
  return typeof value === "string" && value.trim().length > 0 ? value : null;
}

/**
 * Parse a boolean-like string option.
 *
 * @param {string | null} value - Option text.
 * @param {boolean} fallback - Fallback when option is absent.
 * @returns {boolean} Parsed boolean.
 */
function parseBooleanOption(value, fallback) {
  if (value === null) return fallback;
  if (/^(1|true|yes)$/i.test(value)) return true;
  if (/^(0|false|no)$/i.test(value)) return false;
  throw new Error(`Invalid boolean option value: ${value}`);
}

/**
 * Parse an optional positive integer option.
 *
 * @param {string | boolean | undefined} value - Raw option value.
 * @param {string} optionName - Option name for errors.
 * @returns {number | null} Positive integer or null.
 */
function parsePositiveIntegerOption(value, optionName) {
  if (value === undefined) return null;
  if (typeof value !== "string")
    throw new Error(`--${optionName} expects a value`);
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0)
    throw new Error(`Invalid --${optionName}: ${value}`);
  return parsed;
}

/**
 * Parse an optional non-negative integer option.
 *
 * @param {string | boolean | undefined} value - Raw option value.
 * @param {string} optionName - Option name for errors.
 * @returns {number | null} Non-negative integer or null.
 */
function parseNonNegativeIntegerOption(value, optionName) {
  if (value === undefined) return null;
  if (typeof value !== "string")
    throw new Error(`--${optionName} expects a value`);
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 0)
    throw new Error(`Invalid --${optionName}: ${value}`);
  return parsed;
}

/**
 * Parse selected profile subpages.
 *
 * @param {string | null} value - Comma-separated subpage names.
 * @returns {readonly string[]} Selected subpage names.
 */
function parseProfileSubpages(value) {
  if (value === null || value.trim().length === 0)
    return DEFAULT_PROFILE_SUBPAGES;
  if (value === "none") return [];
  if (value === "all")
    return ["customer-reviews", "complaints", "more-info", "details"];
  const selected = value
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);
  const allowed = new Set([
    "customer-reviews",
    "complaints",
    "more-info",
    "details",
  ]);
  for (const entry of selected) {
    if (!allowed.has(entry))
      throw new Error(`Unsupported profile subpage: ${entry}`);
  }
  return selected;
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
