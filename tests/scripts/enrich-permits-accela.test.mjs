import { describe, expect, it } from "vitest";
import { parseAccelaCapDetailHtml } from "../../scripts/hillsborough/enrich-permits-accela.mjs";

describe("parseAccelaCapDetailHtml", () => {
  it("returns null fields for empty or invalid input", () => {
    const res = parseAccelaCapDetailHtml("");
    expect(res.permitNumber).toBeNull();
    expect(res.contractor).toBeNull();
    expect(res.jobValuation).toBeNull();
  });

  it("extracts record status, expiration, contractor and valuation from sample HTML", () => {
    const mockHtml = `
      <div id="ctl00_PlaceHolderMain_dvContent">
        Record HC-BTR-21-0087659: Residential Roof Trade Permit Record Status: Complete Expiration Date: 07/04/2022
      </div>
      <table>
        <tr>
          <td class="td_parent_left">
            Licensed Professional: Kirk Randall Westfall WESTFALL CONSTRUCTION INC 5413 W SLIGH AVE TAMPA, FL, 33634 Certified Roofing CCC056392 CAITLIN@WESTFALLROOFING.COM Phone: 8132645690
          </td>
        </tr>
      </table>
      <div>
        <span>Total Project Value: $25,500.00</span>
        <span>Total Sq Ft: 48</span>
        <span>Type of Material: Asphalt Shingles</span>
        <span>Storm Related: No</span>
        <span>Project Description: Full Re-Roof 48 Squares</span>
        <span>Owner: JOHN DOE *123 MAIN ST</span>
      </div>
    `;

    const res = parseAccelaCapDetailHtml(mockHtml);
    expect(res.permitNumber).toBe("HC-BTR-21-0087659");
    expect(res.recordStatus).toBe("Complete");
    expect(res.expirationDate).toBe("07/04/2022");
    expect(res.contractor?.licenseNumber).toBe("CCC056392");
    expect(res.contractor?.phone).toBe("8132645690");
    expect(res.contractor?.email).toBe("caitlin@westfallroofing.com");
    expect(res.jobValuation).toBe(25500);
    expect(res.squareFeet).toBe(48);
    expect(res.material).toBe("Asphalt Shingles");
    expect(res.stormRelated).toBe(false);
    expect(res.description).toBe("Full Re-Roof 48 Squares");
    expect(res.ownerName).toBe("JOHN DOE");
  });
});
