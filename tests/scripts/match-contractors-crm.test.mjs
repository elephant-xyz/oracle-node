import { describe, expect, it } from "vitest";
import { runContractorJoin } from "../../scripts/hillsborough/match-contractors-crm.mjs";
import { writeFile, unlink } from "node:fs/promises";
import path from "node:path";

describe("match-contractors-crm", () => {
  it("aggregates permits by contractor license and joins with BBB profiles", async () => {
    const tmpEnriched = path.resolve("/tmp/test-enriched.jsonl");
    const tmpBbb = path.resolve("/tmp/test-bbb.jsonl");
    const tmpOut = path.resolve("/tmp/test-leaderboard.json");

    const sampleEnriched = [
      JSON.stringify({
        permit_number: "HC-BTR-21-0087659",
        is_roof_permit: true,
        job_valuation: 25000,
        contractor: {
          licenseNumber: "CCC056392",
          businessName: "WESTFALL ROOFING",
          contactName: "Kirk Westfall",
          phone: "8132645690",
          email: "caitlin@westfallroofing.com",
        },
      }),
      JSON.stringify({
        permit_number: "HC-BTR-21-0087660",
        is_roof_permit: true,
        job_valuation: 15000,
        contractor: {
          licenseNumber: "CCC056392",
          businessName: "WESTFALL ROOFING",
          contactName: "Kirk Westfall",
          phone: "8132645690",
          email: "caitlin@westfallroofing.com",
        },
      }),
    ].join("\n");

    const sampleBbb = JSON.stringify({
      businessName: "Westfall Roofing",
      phone: "(813) 264-5690",
      rating: "A+",
      accredited: true,
      profileUrl:
        "https://www.bbb.org/us/fl/tampa/profile/roofing-contractors/westfall-roofing-0653-90001234",
    });

    await writeFile(tmpEnriched, sampleEnriched, "utf8");
    await writeFile(tmpBbb, sampleBbb, "utf8");

    const res = await runContractorJoin({
      enrichedJsonl: tmpEnriched,
      bbbJsonl: tmpBbb,
      outputPath: tmpOut,
    });

    expect(res.scannedPermits).toBe(2);
    expect(res.permitsWithLicense).toBe(2);
    expect(res.uniqueContractors).toBe(1);
    expect(res.matchedInBbbCrm).toBe(1);

    const top = res.topContractors[0];
    expect(top.licenseNumber).toBe("CCC056392");
    expect(top.permitCount).toBe(2);
    expect(top.totalValuationUsd).toBe(40000);
    expect(top.averageValuationUsd).toBe(20000);
    expect(top.inBbbCrm).toBe(true);
    expect(top.bbbRating).toBe("A+");

    await unlink(tmpEnriched).catch(() => {});
    await unlink(tmpBbb).catch(() => {});
    await unlink(tmpOut).catch(() => {});
  });
});
