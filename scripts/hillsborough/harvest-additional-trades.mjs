#!/usr/bin/env node
/**
 * Executes full BBB category crawls for HVAC and Solar Contractor categories in Hillsborough/Tampa,
 * then updates the cross-joined contractor CRM leaderboard.
 *
 * @module scripts/hillsborough/harvest-additional-trades
 */

import { existsSync, mkdirSync } from "node:fs";
import path from "node:path";
import { harvestBbbCategory } from "../harvest-bbb-category.mjs";
import { runContractorJoin } from "./match-contractors-crm.mjs";

const TRADES = [
  {
    name: "Heating and Air Conditioning (HVAC)",
    categoryUrl:
      "https://www.bbb.org/us/fl/tampa/category/heating-and-air-conditioning",
    outputDir: path.resolve(
      process.cwd(),
      "downloads/hillsborough/bbb-harvest-hvac",
    ),
    maxPages: 10,
  },
  {
    name: "Solar Energy Contractors",
    categoryUrl:
      "https://www.bbb.org/us/fl/tampa/category/solar-energy-contractors",
    outputDir: path.resolve(
      process.cwd(),
      "downloads/hillsborough/bbb-harvest-solar",
    ),
    maxPages: 10,
  },
];

async function main() {
  console.log(
    "=== Starting Multi-Trade BBB Contractor Harvest (HVAC & Solar) ===",
  );

  for (const trade of TRADES) {
    console.log(`\n[Trade Harvest] Starting: ${trade.name}`);
    console.log(`  Category URL: ${trade.categoryUrl}`);
    console.log(`  Output Dir:   ${trade.outputDir}`);

    if (!existsSync(trade.outputDir)) {
      mkdirSync(trade.outputDir, { recursive: true });
    }

    try {
      const summary = await harvestBbbCategory({
        categoryUrl: trade.categoryUrl,
        outputLocation: { kind: "local", dir: trade.outputDir },
        chromiumExecutablePath:
          process.platform === "darwin"
            ? "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
            : "/usr/bin/chromium",
        headless: true,
        startPage: 1,
        maxPages: trade.maxPages,
        maxProfiles: null,
        partRecordLimit: 100,
        pageDelayMs: 2500,
        profileDelayMs: 3000,
        challengeAttempts: 5,
        challengeCheckIntervalMs: 3000,
        challengeChecksPerAttempt: 12,
        navigationTimeoutMs: 90000,
        profileSubpages: ["customer-reviews", "more-info"],
        storeHtml: false,
      });

      console.log(`[Trade Harvest] Completed: ${trade.name}`);
      console.log(`  Pages Visited:       ${summary.categoryPagesVisited}`);
      console.log(`  Profiles Harvested:  ${summary.profilesHarvested}`);
      console.log(`  Profiles Failed:     ${summary.profilesFailed}`);
    } catch (err) {
      console.error(`[Trade Harvest] Error in ${trade.name}:`, err);
    }
  }

  console.log(
    "\n[Contractor CRM] Rebuilding combined multi-trade contractor leaderboard...",
  );
  const leaderboard = await runContractorJoin({
    enrichedJsonl: path.resolve(
      process.cwd(),
      "downloads/hillsborough/full-permits/enriched-permits.jsonl",
    ),
    outputPath: path.resolve(
      process.cwd(),
      "downloads/hillsborough/full-permits/contractor-leaderboard.json",
    ),
  });
  console.log(
    `[Contractor CRM] Leaderboard updated with ${leaderboard.topContractors.length} top contractors, ${leaderboard.matchedInBbbCrm} matched in BBB CRM!`,
  );
  console.log("=== Multi-Trade BBB Contractor Harvest Finished ===");
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
