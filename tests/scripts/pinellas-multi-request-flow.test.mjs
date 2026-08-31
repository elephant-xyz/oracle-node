import { readFile } from "fs/promises";
import { describe, expect, it } from "vitest";

const FLOW_PATH = new URL(
  "../../multi-request-flows/Pinellas.json",
  import.meta.url,
);

describe("Pinellas multi-request appraisal flow", () => {
  it("fetches the PCPAO print page by STRAP without a browser", async () => {
    const flow = JSON.parse(await readFile(FLOW_PATH, "utf8"));
    const [printRequest] = flow.requests;

    expect(printRequest.key).toBe("PropertyPrint");
    expect(printRequest.request.method).toBe("GET");
    expect(printRequest.request.url).toBe(
      "https://www.pcpao.gov/property/detail/print",
    );
    expect(printRequest.request.multiValueQueryString).toEqual({
      is_print: ["1"],
      s: ["{{=it.request_identifier}}"],
    });
    expect(printRequest.request.headers["User-Agent"]).toMatch(/Chrome/);
  });
});
