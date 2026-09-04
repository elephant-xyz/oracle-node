import { describe, it, expect } from "vitest";
import {
  parseClick2GovValuation,
  parseClick2GovSquareFeet,
  parseClick2GovAppYearAndNumber,
  parseClick2GovStatusDetailHtml,
} from "../../scripts/hillsborough/adapters/temple-terrace-click2gov.mjs";

describe("Temple Terrace Click2Gov Adapter", () => {
  it("parses valuation dollar strings to numbers", () => {
    expect(parseClick2GovValuation("$64,225")).toBe(64225);
    expect(parseClick2GovValuation("$1,250,000.50")).toBe(1250000.5);
    expect(parseClick2GovValuation("$0")).toBe(0);
    expect(parseClick2GovValuation(null)).toBeNull();
    expect(parseClick2GovValuation("")).toBeNull();
  });

  it("parses square feet strings to numbers", () => {
    expect(parseClick2GovSquareFeet("000020000")).toBe(20000);
    expect(parseClick2GovSquareFeet("1,500 sq ft")).toBe(1500);
    expect(parseClick2GovSquareFeet(null)).toBeNull();
    expect(parseClick2GovSquareFeet("0")).toBeNull();
  });

  it("extracts application year and number from permit strings", () => {
    expect(parseClick2GovAppYearAndNumber("TT-18-1413")).toEqual({
      appYear: "18",
      appNumber: "1413",
    });
    expect(parseClick2GovAppYearAndNumber("22-2683")).toEqual({
      appYear: "22",
      appNumber: "2683",
    });
    expect(parseClick2GovAppYearAndNumber("invalid")).toBeNull();
  });

  it("parses complete Click2Gov Status Detail HTML into structured permit record", () => {
    const fixtureHtml = `
      <!DOCTYPE html>
      <html>
        <head><title>Temple Terrace Building Permit - Status Detail</title></head>
        <body>
          <div class="list-group-item active">Status Detail</div>
          <label class="col-md-5"><span>Parcel ID:</span></label>
          <div class="col-md-7"><p class="form-control-static">192810ZZZ000001309400T</p></div>
          <label class="col-md-5"><span>Address:</span></label>
          <div class="col-md-7"><p class="form-control-static">5610 GRADUATE CIR</p></div>
          <label class="col-md-5"><span>Application Date:</span></label>
          <div class="col-md-7"><p class="form-control-static">06/22/18</p></div>
          <label class="col-md-5"><span>Owner:</span></label>
          <div class="col-md-7"><p class="form-control-static">CF CAMPUS CLUB LLC</p></div>
          <label class="col-md-5"><span>Application Number:</span></label>
          <div class="col-md-7"><p class="form-control-static">18 - 1413</p></div>
          <label class="col-md-5"><span>Application Type:</span></label>
          <div class="col-md-7"><p class="form-control-static">COMMERCIAL ROOF</p></div>
          <label class="col-md-5"><span>Valuation:</span></label>
          <div class="col-md-7"><p class="form-control-static">$64,225</p></div>
          <label class="col-md-5"><span>Square Footage:</span></label>
          <div class="col-md-7"><p class="form-control-static">000020000</p></div>
          <label class="col-md-5"><span>General Contractor:</span></label>
          <div class="col-md-7"><p class="form-control-static">1619 WESTFALL ROOFING</p></div>
          <label class="col-md-5"><span>Application Status:</span></label>
          <div class="col-md-7"><p class="form-control-static">Permit Issued</p></div>
        </body>
      </html>
    `;

    const parsed = parseClick2GovStatusDetailHtml(fixtureHtml);
    expect(parsed).toBeDefined();
    expect(parsed.permitNumber).toBe("18 - 1413");
    expect(parsed.applicationType).toBe("COMMERCIAL ROOF");
    expect(parsed.jobValuation).toBe(64225);
    expect(parsed.squareFeet).toBe(20000);
    expect(parsed.contractor.businessName).toBe("1619 WESTFALL ROOFING");
    expect(parsed.recordStatus).toBe("Permit Issued");
    expect(parsed.applicationDate).toBe("06/22/18");
    expect(parsed.isRoofPermit).toBe(true);
  });
});
