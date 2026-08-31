import { readFile } from "node:fs/promises";
import { describe, expect, it } from "vitest";

const FLOW_PATH = new URL("../../browser-flows/Polk.json", import.meta.url);

describe("Polk appraisal browser flow", () => {
  it("searches by the seed parcel identifier and captures the detail page", async () => {
    const flow = JSON.parse(await readFile(FLOW_PATH, "utf8"));

    expect(flow.starts_at).toBe("open_search_page");
    expect(flow.states.open_search_page.input).toMatchObject({
      url: "{{=it.url}}",
      wait_until: "domcontentloaded",
    });
    expect(flow.states.wait_for_parcel_input.input.selector).toBe(
      "#searchRE_id",
    );
    expect(flow.states.enter_parcel_id.input).toMatchObject({
      selector: "#searchRE_id",
      value: "{{=it.request_identifier}}",
      clear: true,
    });
    expect(flow.states.submit_search.input.selector).toBe("#submitSearch");
    expect(flow.states.wait_for_result.input.selector).toBe(
      "a[href*='CamaDisplay.aspx?OutputMode=Display'][href*='ParcelID=']",
    );
    expect(flow.states.open_result.input.selector).toBe(
      flow.states.wait_for_result.input.selector,
    );
    expect(flow.states.wait_for_property_details.input).toMatchObject({
      selector: "#valueSummary",
      visible: false,
    });
    expect(flow.states.wait_for_property_details.end).toBe(true);
    expect(flow.capture).toEqual({ type: "page" });
  });
});
