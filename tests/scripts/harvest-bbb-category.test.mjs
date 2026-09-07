import { mkdtemp, readFile, readdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import * as path from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import {
  buildBbbBusinessProfileRecord,
  harvestBbbCategoryInExistingPage,
  parseBbbProfileUrlIdentity,
  parseCategoryCounts,
  readPageChallengeState,
  shouldStopCategoryPagination,
  snapshotPage,
} from "../../scripts/harvest-bbb-category.mjs";

/**
 * @typedef {import("../../scripts/harvest-bbb-category.mjs").PageSnapshot} PageSnapshotForDocumentation
 */

/** @type {string[]} */
const temporaryDirectories = [];

afterEach(async () => {
  await Promise.all(
    temporaryDirectories
      .splice(0)
      .map((directory) => rm(directory, { recursive: true, force: true })),
  );
});

describe("BBB category harvester", () => {
  it("parses stable BBB profile URL identity parts", () => {
    expect(
      parseBbbProfileUrlIdentity(
        "https://www.bbb.org/us/il/south-holland/profile/utility-contractors/cypher-logic-inc-0654-88239819/addressId/106122/customer-reviews",
      ),
    ).toEqual({
      providerBbbId: "0654",
      providerBusinessId: "88239819",
      addressId: "106122",
      slug: "cypher-logic-inc",
    });
  });

  it("parses category result counts from visible pagination evidence", () => {
    const snapshot = {
      url: "https://www.bbb.org/us/category/data",
      title: "Data in USA | Better Business Bureau",
      text: "Showing: 556 results for Data near USA",
      headings: [],
      links: [
        { text: "Page 2", href: "https://www.bbb.org/us/category/data?page=2" },
        {
          text: "Page 15",
          href: "https://www.bbb.org/us/category/data?page=15",
        },
      ],
      jsonLd: [],
      html: null,
    };

    expect(parseCategoryCounts(snapshot)).toEqual({
      totalResults: 556,
      pageCount: 15,
    });
  });

  it("stops category pagination after consecutive empty listing pages", async () => {
    const outputDirectory = await mkdtemp(
      path.join(tmpdir(), "bbb-empty-page-stop-"),
    );
    temporaryDirectories.push(outputDirectory);
    const categoryUrl = "https://www.bbb.org/us/fl/example/category/hvac";
    const profileUrl =
      "https://www.bbb.org/us/fl/example/profile/hvac/example-hvac-0633-12345678";
    let currentUrl = categoryUrl;
    let requestedPageNumber = 0;
    /** @type {import("puppeteer").Page} */
    const page = {
      browser: () => ({
        newPage: async () => page,
      }),
      close: async () => undefined,
      setDefaultNavigationTimeout: () => undefined,
      setDefaultTimeout: () => undefined,
      setCacheEnabled: async () => undefined,
      setViewport: async () => undefined,
      setUserAgent: async () => undefined,
      evaluateOnNewDocument: async () => undefined,
      goto: async (url) => {
        currentUrl = url;
        requestedPageNumber = Number(
          new URL(url).searchParams.get("page") ?? 1,
        );
        return { status: () => 200 };
      },
      title: async () => "Accessible BBB page",
      evaluate: async (_callback, ...args) => {
        if (args.length === 0) return "";
        const pageNumber = requestedPageNumber;
        if (pageNumber === 1) {
          return {
            url: `${categoryUrl}?page=1`,
            title: "HVAC near Example",
            text: "Showing: 1 result for HVAC near Example",
            headings: [],
            links: [{ text: "Example HVAC", href: profileUrl }],
            jsonLd: [],
            html: null,
          };
        }
        return {
          url: `${categoryUrl}?page=${pageNumber}`,
          title: "HVAC near Example",
          text: "Showing: 0 results for HVAC near Example",
          headings: [],
          links: [
            {
              text: "Page 1000",
              href: `${categoryUrl}?page=1000`,
            },
          ],
          jsonLd: [],
          html: null,
        };
      },
    };

    const summary = await harvestBbbCategoryInExistingPage(
      {
        categoryUrl,
        outputLocation: { kind: "local", dir: outputDirectory },
        chromiumExecutablePath: null,
        headless: true,
        startPage: 1,
        maxPages: 1000,
        maxProfiles: null,
        partRecordLimit: 100,
        pageDelayMs: 0,
        profileDelayMs: 0,
        profileAttempts: 1,
        challengeAttempts: 1,
        challengeCheckIntervalMs: 0,
        challengeChecksPerAttempt: 1,
        navigationTimeoutMs: 1_000,
        pageReadTimeoutMs: 50,
        snapshotTimeoutMs: 1_000,
        consecutiveEmptyCategoryPagesLimit: 3,
        includeHtml: false,
        profileSubpages: [],
      },
      page,
    );

    expect(summary.categoryPagesVisited).toBe(4);
    expect(summary.profileUrlsDiscovered).toBe(1);
    expect(shouldStopCategoryPagination(3, 3)).toBe(true);
    expect(shouldStopCategoryPagination(2, 3)).toBe(false);
  });

  it("continues category pagination when page.title() hits a ProtocolError timeout", async () => {
    const outputDirectory = await mkdtemp(
      path.join(tmpdir(), "bbb-title-timeout-"),
    );
    temporaryDirectories.push(outputDirectory);
    const categoryUrl = "https://www.bbb.org/us/fl/example/category/hvac";
    const profileUrl =
      "https://www.bbb.org/us/fl/example/profile/hvac/example-hvac-0633-12345678";
    let currentUrl = categoryUrl;
    let titleCallCount = 0;
    /** @type {import("puppeteer").Page} */
    const page = {
      browser: () => ({
        newPage: async () => page,
      }),
      close: async () => undefined,
      setDefaultNavigationTimeout: () => undefined,
      setDefaultTimeout: () => undefined,
      setCacheEnabled: async () => undefined,
      setViewport: async () => undefined,
      setUserAgent: async () => undefined,
      evaluateOnNewDocument: async () => undefined,
      goto: async (url) => {
        currentUrl = url;
        return { status: () => 200 };
      },
      title: async () => {
        titleCallCount += 1;
        if (titleCallCount === 1) {
          throw new Error(
            "ProtocolError: Runtime.callFunctionOn timed out after 90000ms",
          );
        }
        return "Accessible BBB page";
      },
      evaluate: async (_callback, ...args) => {
        if (args.length === 0) {
          return "Visible HVAC listings";
        }
        if (currentUrl === categoryUrl) {
          return {
            url: categoryUrl,
            title: "HVAC near Example",
            text: "Showing: 1 result for HVAC near Example",
            headings: [],
            links: [{ text: "Example HVAC", href: profileUrl }],
            jsonLd: [],
            html: null,
          };
        }
        return {
          url: profileUrl,
          title: "Example HVAC | BBB Business Profile",
          text: "Example HVAC LLC",
          headings: ["Example HVAC LLC"],
          links: [],
          jsonLd: [
            JSON.stringify({
              "@context": "https://schema.org",
              "@type": "LocalBusiness",
              name: "Example HVAC LLC",
            }),
          ],
          html: null,
        };
      },
    };

    const challengeState = await readPageChallengeState(page, 50);
    expect(challengeState).toEqual({
      title: "",
      previewText: "Visible HVAC listings",
      readFailed: true,
    });

    const summary = await harvestBbbCategoryInExistingPage(
      {
        categoryUrl,
        outputLocation: { kind: "local", dir: outputDirectory },
        chromiumExecutablePath: null,
        headless: true,
        startPage: 1,
        maxPages: 1,
        maxProfiles: 1,
        partRecordLimit: 100,
        pageDelayMs: 0,
        profileDelayMs: 0,
        profileAttempts: 1,
        challengeAttempts: 1,
        challengeCheckIntervalMs: 0,
        challengeChecksPerAttempt: 1,
        navigationTimeoutMs: 1_000,
        pageReadTimeoutMs: 50,
        snapshotTimeoutMs: 1_000,
        consecutiveEmptyCategoryPagesLimit: 3,
        includeHtml: false,
        profileSubpages: [],
      },
      page,
    );

    expect(summary).toMatchObject({
      categoryPagesVisited: 1,
      profilesHarvested: 1,
      profilesFailed: 0,
    });
  });

  it("retries a transient profile failure before recording a failed profile", async () => {
    const outputDirectory = await mkdtemp(
      path.join(tmpdir(), "bbb-profile-retry-"),
    );
    temporaryDirectories.push(outputDirectory);
    const categoryUrl = "https://www.bbb.org/us/fl/example/category/data";
    const profileUrl =
      "https://www.bbb.org/us/fl/example/profile/data/example-data-0633-12345678";
    let currentUrl = categoryUrl;
    let firstProfileAttempt = true;
    let profileNavigationCount = 0;
    let replacementPageCount = 0;
    /** @type {import("puppeteer").Page} */
    let page;
    const browser = /** @type {import("puppeteer").Browser} */ ({
      newPage: async () => {
        replacementPageCount += 1;
        return page;
      },
    });
    page = /** @type {import("puppeteer").Page} */ ({
      browser: () => browser,
      close: async () => undefined,
      setDefaultNavigationTimeout: () => undefined,
      setDefaultTimeout: () => undefined,
      setCacheEnabled: async () => undefined,
      setViewport: async () => undefined,
      setUserAgent: async () => undefined,
      evaluateOnNewDocument: async () => undefined,
      goto: async (url) => {
        currentUrl = url;
        if (url === profileUrl) profileNavigationCount += 1;
        return { status: () => 200 };
      },
      title: async () => "Accessible BBB page",
      evaluate: async (_callback, ...args) => {
        if (args.length === 0) {
          return "";
        }
        if (currentUrl === profileUrl && firstProfileAttempt) {
          firstProfileAttempt = false;
          throw new Error("transient protocol timeout");
        }
        if (currentUrl === categoryUrl) {
          return {
            url: categoryUrl,
            title: "Data near Example",
            text: "Showing: 1 result for Data near Example",
            headings: [],
            links: [{ text: "Example Data", href: profileUrl }],
            jsonLd: [],
            html: null,
          };
        }
        return {
          url: profileUrl,
          title: "Example Data | BBB Business Profile",
          text: "Example Data LLC",
          headings: ["Example Data LLC"],
          links: [],
          jsonLd: [
            JSON.stringify({
              "@context": "https://schema.org",
              "@type": "LocalBusiness",
              name: "Example Data LLC",
            }),
          ],
          html: null,
        };
      },
    });

    const summary = await harvestBbbCategoryInExistingPage(
      {
        categoryUrl,
        outputLocation: { kind: "local", dir: outputDirectory },
        chromiumExecutablePath: null,
        headless: true,
        startPage: 1,
        maxPages: 1,
        maxProfiles: 1,
        partRecordLimit: 100,
        pageDelayMs: 0,
        profileDelayMs: 0,
        profileAttempts: 2,
        challengeAttempts: 1,
        challengeCheckIntervalMs: 0,
        challengeChecksPerAttempt: 1,
        navigationTimeoutMs: 1_000,
        pageReadTimeoutMs: 50,
        snapshotTimeoutMs: 1_000,
        consecutiveEmptyCategoryPagesLimit: 3,
        includeHtml: false,
        profileSubpages: [],
      },
      page,
    );

    expect(profileNavigationCount).toBe(2);
    expect(replacementPageCount).toBe(1);
    expect(summary).toMatchObject({
      profilesHarvested: 1,
      profilesFailed: 0,
    });
  });

  it("promotes visible BBB profile, subpage, and raw evidence fields into query-db-ready JSON", () => {
    const mainPage = {
      url: "https://www.bbb.org/us/fl/example/profile/electrician/example-electric-0633-12345678",
      title: "Example Electric | BBB Business Profile",
      text: `
BUSINESS PROFILE

Electrician

Example Electric LLC
BBB Accredited Business
A+
Rated by BBB
Visit Website
Email Business
About This Business
Low voltage and electrical data contractor.

BBB Accredited Since: 10/4/2020

Years in Business: 18

Business Details
Local BBB:
BBB Serving Example
BBB File Opened:
9/2/2020
Business Started:
4/24/2008
Business Incorporated:
5/1/2008
Type of Entity:
Limited Liability Company (LLC)
Alternate Names:
Example Data
Example Low Voltage
Business Management:
Mr. Ada Lovelace, Owner
Additional Contact Information
Principal Contacts
Mr. Ada Lovelace, Owner
Customer Contacts
Ms. Grace Hopper, Operations
Additional Websites
example.invalid/social
Social Media
Facebook
Additional Information
Business Categories
Electrician, Data

BBB Business Profiles are provided solely to assist you in exercising your own best judgment.
      `,
      headings: ["Example Electric LLC"],
      links: [
        { text: "Visit Website", href: "https://example-electric.invalid/" },
        {
          text: "Email Business",
          href: "https://www.bbb.org/us/fl/example/profile/electrician/example-electric-0633-12345678/email-this-business?email=primary",
        },
        {
          text: "Electrician",
          href: "https://www.bbb.org/us/fl/example/category/electrician",
        },
        {
          text: "Data",
          href: "https://www.bbb.org/us/fl/example/category/data",
        },
        { text: "Facebook", href: "https://www.facebook.com/exampleelectric" },
        { text: "BBB National Programs", href: "https://bbbprograms.org/" },
        {
          text: "our Facebook (opens in a new tab)",
          href: "https://www.facebook.com/BetterBusinessBureau",
        },
      ],
      jsonLd: [
        JSON.stringify({
          "@context": "https://schema.org",
          "@type": "LocalBusiness",
          name: "Example Electric LLC",
          telephone: "+1-555-0100",
          address: {
            "@type": "PostalAddress",
            streetAddress: "1 Example Way",
            addressLocality: "Example City",
            addressRegion: "FL",
            postalCode: "33999",
            addressCountry: "US",
          },
          employee: {
            "@type": "Person",
            givenName: "Ada",
            familyName: "Lovelace",
            jobTitle: "Owner",
          },
          image: "https://example-electric.invalid/logo.png",
        }),
      ],
      html: null,
    };
    const moreInfoPage = {
      url: `${mainPage.url}/more-info`,
      title: "More info on Example Electric LLC | BBB Profile",
      text: `
Information and Alerts
Service Area
Lee County, FL
Collier County, FL

BBB Business Profiles are provided solely to assist you in exercising your own best judgment.
      `,
      headings: [],
      links: [],
      jsonLd: [],
      html: null,
    };
    const complaintsPage = {
      url: `${mainPage.url}/complaints`,
      title: "Example Electric LLC | BBB Complaints | Better Business Bureau",
      text: `
Complaints
Customer Complaints Summary
1 complaint in the last 3 years.
0 complaints closed in the last 12 months.
Filter and sort by
Initial Complaint

Date:
12/06/2024

Type:
Service or Repair Issues
Status:
Resolved
More info
Customer reported an unresolved low-voltage repair issue.
Business Response

Date: 12/26/2024

We corrected the issue and contacted the customer.
Customer Answer

Date: 12/31/2024

The resolution is satisfactory.
Example Electric LLC is BBB Accredited.

This business has committed to upholding the BBB Standards for Trust.
      `,
      headings: [],
      links: [],
      jsonLd: [],
      html: null,
    };

    const record = buildBbbBusinessProfileRecord({
      profileUrl: mainPage.url,
      listing: {
        profileUrl: mainPage.url,
        linkText: "Example Electric LLC",
        pageNumber: 1,
        ordinalOnPage: 1,
        categoryUrl: "https://www.bbb.org/us/category/data",
      },
      mainPage,
      subpages: [
        {
          kind: "more-info",
          url: moreInfoPage.url,
          status: 200,
          ok: true,
          page: moreInfoPage,
          error: null,
        },
        {
          kind: "complaints",
          url: complaintsPage.url,
          status: 200,
          ok: true,
          page: complaintsPage,
          error: null,
        },
      ],
      retrievedAt: "2026-06-08T00:00:00.000Z",
    });

    expect(record).toMatchObject({
      recordKind: "bbb_business_profile",
      providerProfileId: "0633:12345678",
      providerBusinessId: "12345678",
      providerBbbId: "0633",
      name: "Example Electric LLC",
      phone: "+1-555-0100",
      websiteUrl: "https://example-electric.invalid/",
      emailUrl:
        "https://www.bbb.org/us/fl/example/profile/electrician/example-electric-0633-12345678/email-this-business?email=primary",
      bbbRating: "A+",
      entityType: "Limited Liability Company (LLC)",
    });
    expect(record.links).toEqual([
      {
        kind: "WEBSITE",
        url: "https://example-electric.invalid/",
        label: "Visit Website",
      },
      {
        kind: "FACEBOOK",
        url: "https://www.facebook.com/exampleelectric",
        label: "Facebook",
      },
    ]);
    expect(record.alternateNames).toEqual([
      { name: "Example Data", source: "visible_text" },
      { name: "Example Low Voltage", source: "visible_text" },
    ]);
    expect(record.businessManagement).toContainEqual(
      expect.objectContaining({
        name: "Ms. Grace Hopper",
        title: "Operations",
        role: "CUSTOMER",
      }),
    );
    expect(record.businessManagement).not.toContainEqual(
      expect.objectContaining({ name: "Additional Websites" }),
    );
    expect(record.serviceAreas).toEqual([
      { name: "Lee County, FL", source: "visible_text" },
      { name: "Collier County, FL", source: "visible_text" },
    ]);
    expect(record.complaints).toHaveLength(1);
    expect(record.complaints[0]).toMatchObject({
      complaintDate: "12/06/2024",
      complaintType: "Service or Repair Issues",
      complaintStatus: "Resolved",
      complaintText:
        "Customer reported an unresolved low-voltage repair issue.",
      events: [
        {
          type: "BUSINESS_RESPONSE",
          actorRole: "BUSINESS",
          date: "12/26/2024",
          text: "We corrected the issue and contacted the customer.",
        },
        {
          type: "CUSTOMER_ANSWER",
          actorRole: "CUSTOMER",
          date: "12/31/2024",
          text: "The resolution is satisfactory.",
        },
      ],
    });
    expect(record.reviewsComplaintsSummary).toMatchObject({
      totalClosedComplaintsPastTwelveMonths: 0,
      complaintsTotal: 1,
    });
    expect(record.bbbHarvest).toMatchObject({
      mainPage: { text: expect.stringContaining("Business Details") },
    });
  });

  it("records a failed profile when snapshot evaluate hits a ProtocolError", async () => {
    const outputDirectory = await mkdtemp(
      path.join(tmpdir(), "bbb-snapshot-protocol-error-"),
    );
    temporaryDirectories.push(outputDirectory);
    const categoryUrl = "https://www.bbb.org/us/fl/example/category/hvac";
    const profileUrl =
      "https://www.bbb.org/us/fl/example/profile/hvac/example-hvac-0633-12345678";
    let currentUrl = categoryUrl;
    /** @type {import("puppeteer").Page} */
    const page = {
      browser: () => ({
        newPage: async () => page,
      }),
      close: async () => undefined,
      setDefaultNavigationTimeout: () => undefined,
      setDefaultTimeout: () => undefined,
      setCacheEnabled: async () => undefined,
      setViewport: async () => undefined,
      setUserAgent: async () => undefined,
      evaluateOnNewDocument: async () => undefined,
      goto: async (url) => {
        currentUrl = url;
        return { status: () => 200 };
      },
      title: async () => "Accessible BBB page",
      evaluate: async (_callback, ...args) => {
        if (args.length === 0) {
          return "";
        }
        if (currentUrl === categoryUrl) {
          return {
            url: categoryUrl,
            title: "HVAC near Example",
            text: "Showing: 1 result for HVAC near Example",
            headings: [],
            links: [{ text: "Example HVAC", href: profileUrl }],
            jsonLd: [],
            html: null,
          };
        }
        throw new Error(
          "ProtocolError: Runtime.callFunctionOn timed out after 90000ms",
        );
      },
    };

    const summary = await harvestBbbCategoryInExistingPage(
      {
        categoryUrl,
        outputLocation: { kind: "local", dir: outputDirectory },
        chromiumExecutablePath: null,
        headless: true,
        startPage: 1,
        maxPages: 1,
        maxProfiles: 1,
        partRecordLimit: 25,
        pageDelayMs: 0,
        profileDelayMs: 0,
        profileAttempts: 1,
        challengeAttempts: 1,
        challengeCheckIntervalMs: 0,
        challengeChecksPerAttempt: 1,
        navigationTimeoutMs: 1_000,
        pageReadTimeoutMs: 50,
        snapshotTimeoutMs: 50,
        consecutiveEmptyCategoryPagesLimit: 3,
        includeHtml: false,
        profileSubpages: [],
      },
      page,
    );

    expect(summary).toMatchObject({
      profilesHarvested: 0,
      profilesFailed: 1,
    });
  });

  it("flushes profile JSONL parts at partRecordLimit", async () => {
    const outputDirectory = await mkdtemp(
      path.join(tmpdir(), "bbb-profile-flush-"),
    );
    temporaryDirectories.push(outputDirectory);
    const categoryUrl = "https://www.bbb.org/us/fl/example/category/hvac";
    const profileUrls = Array.from(
      { length: 3 },
      (_entry, index) =>
        `https://www.bbb.org/us/fl/example/profile/hvac/example-hvac-${index}-0633-1234567${index}`,
    );
    let currentUrl = categoryUrl;
    /** @type {import("puppeteer").Page} */
    const page = {
      browser: () => ({
        newPage: async () => page,
      }),
      close: async () => undefined,
      setDefaultNavigationTimeout: () => undefined,
      setDefaultTimeout: () => undefined,
      setCacheEnabled: async () => undefined,
      setViewport: async () => undefined,
      setUserAgent: async () => undefined,
      evaluateOnNewDocument: async () => undefined,
      goto: async (url) => {
        currentUrl = url;
        return { status: () => 200 };
      },
      title: async () => "Accessible BBB page",
      evaluate: async (_callback, ...args) => {
        if (args.length === 0) {
          return "";
        }
        if (currentUrl === categoryUrl) {
          return {
            url: categoryUrl,
            title: "HVAC near Example",
            text: "Showing: 3 results for HVAC near Example",
            headings: [],
            links: profileUrls.map((profileUrl, index) => ({
              text: `Example HVAC ${index}`,
              href: profileUrl,
            })),
            jsonLd: [],
            html: null,
          };
        }
        const profileIndex = profileUrls.indexOf(currentUrl);
        return {
          url: currentUrl,
          title: `Example HVAC ${profileIndex} | BBB Business Profile`,
          text: `Example HVAC ${profileIndex} LLC`,
          headings: [`Example HVAC ${profileIndex} LLC`],
          links: [],
          jsonLd: [
            JSON.stringify({
              "@context": "https://schema.org",
              "@type": "LocalBusiness",
              name: `Example HVAC ${profileIndex} LLC`,
            }),
          ],
          html: null,
        };
      },
    };

    const summary = await harvestBbbCategoryInExistingPage(
      {
        categoryUrl,
        outputLocation: { kind: "local", dir: outputDirectory },
        chromiumExecutablePath: null,
        headless: true,
        startPage: 1,
        maxPages: 1,
        maxProfiles: 3,
        partRecordLimit: 2,
        pageDelayMs: 0,
        profileDelayMs: 0,
        profileAttempts: 1,
        challengeAttempts: 1,
        challengeCheckIntervalMs: 0,
        challengeChecksPerAttempt: 1,
        navigationTimeoutMs: 1_000,
        pageReadTimeoutMs: 50,
        snapshotTimeoutMs: 1_000,
        consecutiveEmptyCategoryPagesLimit: 3,
        includeHtml: false,
        profileSubpages: [],
      },
      page,
    );

    expect(summary.profilesHarvested).toBe(3);
    const profilePartFiles = await readdir(
      path.join(outputDirectory, "profiles"),
    );
    expect(profilePartFiles).toEqual([
      "profiles-part-0001.jsonl",
      "profiles-part-0002.jsonl",
    ]);
    const firstPart = await readFile(
      path.join(outputDirectory, "profiles", "profiles-part-0001.jsonl"),
      "utf8",
    );
    expect(firstPart.trim().split("\n")).toHaveLength(2);
  });

  it("times out hung snapshot evaluate calls instead of waiting indefinitely", async () => {
    /** @type {import("puppeteer").Page} */
    const page = {
      evaluate: async () => {
        await new Promise(() => {});
        return {};
      },
    };

    await expect(snapshotPage(page, false, 50)).rejects.toThrow(
      "page snapshot timed out after 50ms",
    );
  });
});
