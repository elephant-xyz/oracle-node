import { describe, expect, it } from "vitest";

import {
  POLK_PERMIT_SOURCE_REGISTRY,
  buildPolkAccelaDetailUrl,
  buildPolkPermitEnrichmentReceipt,
  findPolkPermitSource,
  parsePolkAccelaPermitDetailHtml,
} from "../../scripts/polk/permit-enrichment.mjs";

describe("Polk permit source registry", () => {
  it("enables only the verified Polk Accela detail adapter", () => {
    const enabled = POLK_PERMIT_SOURCE_REGISTRY.filter(
      (source) => source.adapter !== null,
    );
    expect(enabled).toEqual([
      expect.objectContaining({
        agency: "POLK COUNTY",
        portalKind: "accela",
        status: "adapter_ready",
        adapter: "polk_accela_cap_detail_v1",
      }),
    ]);
    expect(findPolkPermitSource("lakeland")).toMatchObject({
      status: "portal_verified_adapter_pending",
      adapter: null,
    });
    expect(findPolkPermitSource("unknown")).toBeNull();
  });

  it("builds the certified anonymous Accela altId lookup", () => {
    const url = new URL(buildPolkAccelaDetailUrl("BR-2021-10301"));
    expect(url.hostname).toBe("aca-prod.accela.com");
    expect(url.pathname).toBe("/POLKCO/Cap/CapDetail.aspx");
    expect(url.searchParams.get("Module")).toBe("Building");
    expect(url.searchParams.get("altId")).toBe("BR-2021-10301");
  });
});

describe("Polk Accela detail parser", () => {
  it("promotes only visible contractor, license, parcel, status, and value evidence", () => {
    const detail = parsePolkAccelaPermitDetailHtml(`
      <html><body>
        <h1>Record BR-2021-10301:</h1>
        <h2>Residential Renovation/Addition Permit</h2>
        <div>Record Status: Closed-Complete</div>
        <h2>Work Location</h2>
        <div>3075 BAKER DAIRY RD HAINES CITY FL 33844</div>
        <h2>Record Details</h2>
        <div>Applicant: Licensed Professional:
          MARCOS DAVILA megan@mdconstructionfl.com MD CONSTRUCTION LLC
          6656 SR 544 WINTER HAVEN FL Alum Specialty Structure SCC131151708
          863-286-4248
          Project Description: New concrete slab with footings
          Owner: GRANDINETTE FRANK
        </div>
        <h2>Additional Information</h2>
        <div>Job Value($): $4,678.00</div>
        <div>Parcel Number:272722000000043010</div>
      </body></html>
    `);

    expect(detail).toMatchObject({
      permitNumber: "BR-2021-10301",
      recordType: "Residential Renovation/Addition Permit",
      recordStatus: "Closed-Complete",
      parcelIdentifier: "272722000000043010",
      workLocation: "3075 BAKER DAIRY RD HAINES CITY FL 33844",
      projectDescription: "New concrete slab with footings",
      jobValuationUsd: 4678,
      contractor: {
        businessName: "MD CONSTRUCTION LLC",
        licenseNumber: "SCC131151708",
        email: "megan@mdconstructionfl.com",
        phone: "8632864248",
      },
    });
  });

  it("normalizes colon-delimited Polk Accela license identifiers", () => {
    const detail = parsePolkAccelaPermitDetailHtml(`
      <h1>Record BR-2020-5395:</h1>
      <div>Record Status: Closed</div>
      <div>Licensed Professional:
        POWELL PHILIP JOSEPH 25383 LESLIES BUNGALOW GROUP CRC:11516516
        Project Description: Residential addition
      </div>
    `);

    expect(detail.contractor?.licenseNumber).toBe("CRC11516516");
  });

  it("keeps absent detail fields null", () => {
    expect(
      parsePolkAccelaPermitDetailHtml("<h1>No records found</h1>"),
    ).toEqual({
      permitNumber: null,
      recordType: null,
      recordStatus: null,
      parcelIdentifier: null,
      workLocation: null,
      projectDescription: null,
      jobValuationUsd: null,
      contractor: null,
    });
  });
});

describe("Polk permit enrichment receipt", () => {
  it("stays incomplete when municipal source adapters are not certified", () => {
    const receipt = buildPolkPermitEnrichmentReceipt(
      {
        permitCount: 3,
        agencies: [
          { value: "POLK COUNTY", count: 2 },
          { value: "LAKELAND", count: 1 },
        ],
      },
      [
        {
          permitNumber: "BR-1",
          agency: "POLK COUNTY",
          sourceKey: "polk_county_accela",
          sourceUrl: buildPolkAccelaDetailUrl("BR-1"),
          status: "enriched",
          detail: {
            permitNumber: "BR-1",
            recordType: "Re-Roof Permit",
            recordStatus: "Issued",
            parcelIdentifier: "1",
            workLocation: "1 MAIN ST",
            projectDescription: "Reroof",
            jobValuationUsd: 10000,
            contractor: {
              businessName: "ROOF CO LLC",
              contactName: null,
              licenseNumber: "CCC1234567",
              licenseType: "Roofing",
              email: null,
              phone: null,
              raw: "ROOF CO LLC Roofing CCC1234567",
            },
          },
          error: null,
          retrievedAt: "2026-08-31T00:00:00.000Z",
        },
      ],
    );

    expect(receipt).toMatchObject({
      officialPermitCount: 3,
      adapterEligiblePermitCount: 2,
      unsupportedPermitCount: 1,
      attemptedAdapterRecords: 1,
      contractorEvidenceCount: 1,
      licenseEvidenceCount: 1,
      complete: false,
    });
    expect(receipt.blocker).toMatch(/1 official bulk permit row/);
  });

  it("stays incomplete when a certified adapter returns no detail evidence", () => {
    const receipt = buildPolkPermitEnrichmentReceipt(
      {
        permitCount: 1,
        agencies: [{ value: "POLK COUNTY", count: 1 }],
      },
      [
        {
          permitNumber: "BR-MISSING",
          agency: "POLK COUNTY",
          sourceKey: "polk_county_accela",
          sourceUrl: buildPolkAccelaDetailUrl("BR-MISSING"),
          status: "no_detail",
          detail: null,
          error: null,
          retrievedAt: "2026-08-31T00:00:00.000Z",
        },
      ],
    );

    expect(receipt.complete).toBe(false);
    expect(receipt.blocker).toMatch(/1 adapter-eligible permit row/);
  });
});
