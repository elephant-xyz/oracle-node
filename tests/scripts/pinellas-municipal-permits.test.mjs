import { describe, expect, it } from "vitest";

import { resolveAccelaAgency } from "../../scripts/pinellas/accela-agencies.mjs";
import {
  PINELLAS_MUNICIPAL_PERMIT_SOURCES,
  isHttpsProbeUrl,
} from "../../scripts/pinellas/municipal-sources.mjs";

describe("Pinellas Accela agencies", () => {
  it("defaults to county PINELLAS and resolves Clearwater CapHome", () => {
    expect(resolveAccelaAgency(undefined).agencyCode).toBe("PINELLAS");
    const clearwater = resolveAccelaAgency("CLEARWATER");
    expect(clearwater.portalUrl).toContain("/CLEARWATER/Cap/CapHome.aspx");
    expect(clearwater.sourceStamp).toBe("clearwater-city-accela");
    expect(() => resolveAccelaAgency("st-pete")).toThrow(
      /--agency must be one of/,
    );
  });
});

describe("Pinellas municipal permit catalog", () => {
  it("lists https probe URLs for every non-county source", () => {
    expect(PINELLAS_MUNICIPAL_PERMIT_SOURCES.length).toBeGreaterThanOrEqual(6);
    for (const source of PINELLAS_MUNICIPAL_PERMIT_SOURCES) {
      expect(isHttpsProbeUrl(source.probeUrl)).toBe(true);
    }
    expect(
      PINELLAS_MUNICIPAL_PERMIT_SOURCES.map((source) => source.key),
    ).toContain("clearwater-accela");
    const byKey = Object.fromEntries(
      PINELLAS_MUNICIPAL_PERMIT_SOURCES.map((source) => [source.key, source]),
    );
    expect(byKey["largo-energov"]?.status).toBe("adapter-ready");
    expect(byKey["tarpon-springs-click2gov"]?.status).toBe("adapter-ready");
    expect(byKey["pinellas-park-tyler"]?.status).toBe("adapter-ready");
    expect(byKey["pinellas-park-tyler"]?.vendor).toBe("tyler-energov-css");
    expect(byKey["st-petersburg"]?.status).toBe("needs-review");
    expect(byKey["dunedin-css"]?.status).toBe("needs-review");
  });
});
