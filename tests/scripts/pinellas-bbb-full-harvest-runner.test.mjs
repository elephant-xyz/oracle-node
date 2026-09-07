import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import {
  installBrokenPipeGuards,
  parsePinellasBbbFullHarvestCliArgs,
  runPinellasBbbFullHarvest,
} from "../../scripts/pinellas/run-bbb-full-harvest.mjs";

describe("Pinellas BBB full harvest runner", () => {
  /** @type {string[]} */
  const tempDirectories = [];

  afterEach(() => {
    for (const directory of tempDirectories.splice(0)) {
      fs.rmSync(directory, { recursive: true, force: true });
    }
  });

  /**
   * @returns {string} Temporary output root for runner tests.
   */
  function createTempOutputRoot() {
    const directory = fs.mkdtempSync(
      path.join(os.tmpdir(), "pinellas-bbb-runner-"),
    );
    tempDirectories.push(directory);
    return directory;
  }

  it("parses output root and --from-job", () => {
    expect(
      parsePinellasBbbFullHarvestCliArgs([
        "/tmp/custom-bbb",
        "--from-job",
        "3",
      ]),
    ).toEqual({
      outputRoot: "/tmp/custom-bbb",
      fromJob: 3,
    });
  });

  it("requires a value for --from-job", () => {
    expect(() => parsePinellasBbbFullHarvestCliArgs(["--from-job"])).toThrow(
      "--from-job requires a 1-based job index",
    );
  });

  it("chains remaining jobs after fromJob without stopping after the first", async () => {
    const outputRoot = createTempOutputRoot();
    /** @type {string[]} */
    const invokedCommands = [];

    await runPinellasBbbFullHarvest(outputRoot, {
      fromJob: 2,
      runShellCommand: async (command) => {
        invokedCommands.push(command);
      },
    });

    expect(invokedCommands).toHaveLength(5);
    expect(invokedCommands[0]).toContain(
      "/clearwater/category/heating-and-air-conditioning",
    );
    expect(invokedCommands[1]).toContain(
      "/clearwater/category/solar-energy-contractors",
    );
    expect(invokedCommands[4]).toContain(
      "/st-petersburg/category/solar-energy-contractors",
    );

    const runnerLog = fs.readFileSync(
      path.join(outputRoot, "logs", "runner.log"),
      "utf8",
    );
    expect(runnerLog).toContain("=== START clearwater/hvac ");
    expect(runnerLog).toContain("=== END st-petersburg/solar ");
    expect(runnerLog).toContain(
      "=== ALL PINELLAS BBB FULL HARVEST JOBS COMPLETE ",
    );
  });

  it("writes END before starting the next job so a broken stdout pipe cannot lose progress", async () => {
    const outputRoot = createTempOutputRoot();
    /** @type {string[]} */
    const timeline = [];

    await runPinellasBbbFullHarvest(outputRoot, {
      fromJob: 1,
      runShellCommand: async (command) => {
        timeline.push(
          command.includes("clearwater/category/roofing-contractors")
            ? "run:clearwater-roofing"
            : "run:next",
        );
      },
    });

    expect(timeline).toHaveLength(6);
    expect(timeline[0]).toBe("run:clearwater-roofing");
    expect(timeline.slice(1).every((entry) => entry === "run:next")).toBe(true);
    const roofingLog = fs.readFileSync(
      path.join(outputRoot, "logs", "clearwater-roofing.log"),
      "utf8",
    );
    expect(roofingLog).toMatch(/=== END clearwater\/roofing /);
    const hvacLog = fs.readFileSync(
      path.join(outputRoot, "logs", "clearwater-hvac.log"),
      "utf8",
    );
    expect(hvacLog).toMatch(/=== START clearwater\/hvac /);
  });

  it("installs broken-pipe guards without throwing", () => {
    expect(() => installBrokenPipeGuards()).not.toThrow();
  });
});
