import { describe, expect, it } from "vitest";

import { extractPermitLinksFromSearchHtml } from "../../workflow/lambdas/permit-harvest-worker/lee-accela.mjs";
import {
  PINELLAS_PORTAL_URL,
  PINELLAS_RECORD_NUMBER_PATTERN,
  createAccelaDateWindows,
  inclusiveDaySpan,
  shouldSplitAccelaWindow,
  splitAccelaWindow,
} from "../../scripts/pinellas/accela-pinellas.mjs";
import {
  isBrowserDisconnectedError,
  parseCliOptions,
  windowArtifactPaths,
} from "../../scripts/run-pinellas-permit-harvest.mjs";

describe("Pinellas Accela date-window helpers", () => {
  it("tiles an inclusive range into 30-day windows", () => {
    const windows = createAccelaDateWindows("1990-01-01", "1990-02-15", 30);
    expect(windows).toEqual([
      { startDate: "1990-01-01", endDate: "1990-01-30" },
      { startDate: "1990-01-31", endDate: "1990-02-15" },
    ]);
    expect(inclusiveDaySpan("1990-01-01", "1990-01-30")).toBe(30);
  });

  it("splits at-cap multi-day windows and treats a missing total as at-cap", () => {
    expect(
      shouldSplitAccelaWindow({
        startDate: "2026-01-01",
        endDate: "2026-01-30",
        reportedTotal: 100,
      }),
    ).toBe(true);
    expect(
      shouldSplitAccelaWindow({
        startDate: "2026-01-01",
        endDate: "2026-01-01",
        reportedTotal: 100,
      }),
    ).toBe(false);
    expect(
      shouldSplitAccelaWindow({
        startDate: "2026-01-01",
        endDate: "2026-01-30",
        reportedTotal: 40,
      }),
    ).toBe(false);
    expect(
      shouldSplitAccelaWindow({
        startDate: "2026-01-01",
        endDate: "2026-01-30",
        reportedTotal: null,
      }),
    ).toBe(true);
    expect(splitAccelaWindow("2026-01-01", "2026-01-04")).toEqual([
      { startDate: "2026-01-01", endDate: "2026-01-02" },
      { startDate: "2026-01-03", endDate: "2026-01-04" },
    ]);
  });

  it("resolves PINELLAS CapDetail hrefs and PER-H / EBP record numbers", () => {
    const html = `
      <table>
        <thead>
          <tr>
            <th></th><th>Record Number</th><th>Address</th><th>Description</th>
            <th>Status</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td></td>
            <td><a href="/PINELLAS/Cap/CapDetail.aspx?Module=Building&amp;capID1=26CAP&amp;capID2=00000&amp;capID3=01BA1">BC-RMR-26-00368</a></td>
            <td>3400 RUGBY CT, PALM HARBOR FL 34684</td>
            <td>RE-ROOF</td>
            <td>Finaled</td>
          </tr>
          <tr>
            <td></td>
            <td><a href="CapDetail.aspx?Module=Building&amp;altId=EBP-24-00999">EBP-24-00999</a></td>
            <td>1403 CIRCLE DR</td>
            <td>ELECTRICAL</td>
            <td>Issued</td>
          </tr>
        </tbody>
      </table>`;
    const links = extractPermitLinksFromSearchHtml(
      html,
      "20240101_20240131",
      1,
      undefined,
      {
        baseUrl: PINELLAS_PORTAL_URL,
        recordNumberPattern: PINELLAS_RECORD_NUMBER_PATTERN,
      },
    );
    expect(links.map((link) => link.recordNumber)).toEqual([
      "BC-RMR-26-00368",
      "EBP-24-00999",
    ]);
    expect(links[0]?.url).toContain("/PINELLAS/Cap/CapDetail.aspx");
    expect(links[1]?.url).toContain("/PINELLAS/Cap/CapDetail.aspx");
    expect(links[1]?.url).not.toContain("/LEECO/");
  });

  it("keeps terminal window JSON and split markers on distinct paths", () => {
    const paths = windowArtifactPaths("/tmp/pinellas-job", "19900101_19900130");
    expect(paths.terminalPath).toBe(
      "/tmp/pinellas-job/windows/19900101_19900130.json",
    );
    expect(paths.splitPath).toBe(
      "/tmp/pinellas-job/windows/19900101_19900130.split.json",
    );
  });
});

describe("Pinellas permit harvest CLI", () => {
  it("defaults skip-existing on and maps --probe to a 3-day cap of one detail", () => {
    const probe = parseCliOptions(["--probe"]);
    expect(probe.skipExisting).toBe(true);
    expect(probe.probe).toBe(true);
    expect(probe.maxDetails).toBe(1);
    expect(probe.windowDays).toBe(14);
    const full = parseCliOptions(["--job-id", "pinellas-accela-full-20260903"]);
    expect(full.startDate).toBe("1990-01-01");
    expect(full.probe).toBe(false);
    expect(full.maxDetails).toBe(0);
    expect(full.jobId).toBe("pinellas-accela-full-20260903");
  });

  it("stops harvest on browser disconnect instead of skipping remaining windows", () => {
    expect(isBrowserDisconnectedError(new Error("Connection closed."))).toBe(
      true,
    );
    expect(isBrowserDisconnectedError(new Error("Navigation timeout"))).toBe(
      false,
    );
  });
});
