import { readFile } from "fs/promises";
import { describe, expect, it } from "vitest";

const FLOW_PATH = new URL(
  "../../multi-request-flows/Broward.json",
  import.meta.url,
);

describe("Broward multi-request appraisal flow", () => {
  it("POSTs getParcelInformation with an empty taxyear and request_identifier template", async () => {
    const flow = JSON.parse(await readFile(FLOW_PATH, "utf8"));
    const [detailRequest] = flow.requests;

    expect(detailRequest.key).toBe("input");
    expect(detailRequest.request.method).toBe("POST");
    expect(detailRequest.request.url).toBe(
      "https://web.bcpa.net/BcpaClient/search.aspx/getParcelInformation",
    );
    expect(detailRequest.request.headers["content-type"]).toMatch(
      /application\/json/i,
    );
    expect(detailRequest.request.body).toContain("{{=it.request_identifier}}");
    expect(detailRequest.request.body).toContain('"taxyear":""');
    expect(detailRequest.request.body).not.toContain('"taxyear":"CURRENT"');
    expect(detailRequest.request.body).toContain('"action":"CURRENT"');
  });
});
