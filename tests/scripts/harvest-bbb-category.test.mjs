import { describe, expect, it } from "vitest";

import {
  buildBbbBusinessProfileRecord,
  parseBbbProfileUrlIdentity,
  parseCategoryCounts,
} from "../../scripts/harvest-bbb-category.mjs";

/**
 * @typedef {import("../../scripts/harvest-bbb-category.mjs").PageSnapshot} PageSnapshotForDocumentation
 */

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
});
