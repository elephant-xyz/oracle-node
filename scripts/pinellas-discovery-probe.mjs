#!/usr/bin/env node
/**
 * Pinellas county-discovery probe: curl + Playwright.
 * Writes timings/JSON to downloads/pinellas/probe-results.json
 * and sample HTML to downloads/pinellas/samples/.
 */
import { mkdirSync, writeFileSync, existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = dirname(fileURLToPath(import.meta.url));
const OUT = join(ROOT, "..", "downloads", "pinellas");
const SAMPLES = join(OUT, "samples");
mkdirSync(SAMPLES, { recursive: true });

const results = {
  probed_at: new Date().toISOString(),
  egress: null,
  curl: {},
  gis: {},
  playwright: {},
};

async function timedFetch(url, opts = {}) {
  const started = Date.now();
  try {
    const res = await fetch(url, {
      redirect: "follow",
      headers: {
        "user-agent":
          "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        accept: "text/html,application/json,*/*",
        ...(opts.headers || {}),
      },
      signal: AbortSignal.timeout(opts.timeoutMs || 25000),
    });
    const buf = Buffer.from(await res.arrayBuffer());
    const text = buf.toString("utf8");
    const cf = res.headers.get("cf-ray") || res.headers.get("server") || "";
    return {
      url,
      ok: res.ok,
      status: res.status,
      bytes: buf.length,
      ms: Date.now() - started,
      server: cf,
      contentType: res.headers.get("content-type"),
      snippet: text.slice(0, 400).replace(/\s+/g, " "),
      challenge:
        /cloudflare|captcha|access denied|attention required|just a moment/i.test(
          text.slice(0, 4000),
        ),
      text,
      buf,
    };
  } catch (err) {
    return {
      url,
      ok: false,
      status: 0,
      bytes: 0,
      ms: Date.now() - started,
      error: String(err),
    };
  }
}

function summarize(r) {
  const { text, buf, ...rest } = r;
  return rest;
}

async function main() {
  const ip = await timedFetch("https://ipinfo.io/json", { timeoutMs: 10000 });
  try {
    results.egress = JSON.parse(ip.text);
  } catch {
    results.egress = { raw: ip.snippet };
  }

  const gisCount = await timedFetch(
    "https://egis.pinellas.gov/pcpagis/rest/services/PcpaBaseMap/BaseMapParcelAerials/MapServer/157/query?where=1%3D1&returnCountOnly=true&f=json",
  );
  results.gis.count = summarize(gisCount);
  try {
    results.gis.featureCount = JSON.parse(gisCount.text).count;
  } catch {
    results.gis.featureCount = null;
  }

  const gisSamples = await timedFetch(
    "https://egis.pinellas.gov/pcpagis/rest/services/PcpaBaseMap/BaseMapParcelAerials/MapServer/157/query?where=1%3D1&outFields=STRAP,PARCELID,PARCELNO,PARCEL_ID,NAME&returnGeometry=true&resultRecordCount=8&f=json",
  );
  results.gis.sampleQuery = summarize(gisSamples);
  let sampleAttrs = [];
  try {
    const parsed = JSON.parse(gisSamples.text);
    sampleAttrs = (parsed.features || []).map((f) => ({
      ...f.attributes,
      hasPolygon: Boolean(f.geometry?.rings),
      ringCount: f.geometry?.rings?.length || 0,
    }));
  } catch {
    sampleAttrs = [];
  }
  results.gis.samples = sampleAttrs;

  const urls = {
    pcpaoHome: "https://www.pcpao.gov/",
    propertyDetails: "https://www.pcpao.gov/property-details",
    downloads: "https://www.pcpao.gov/tools-data/data-downloads/raw-database-files",
    shapefiles: "https://www.pcpao.gov/tools-data/maps-gis/shape-files",
    useCodes: "https://www.pcpao.gov/learn-about/use-codes",
    accelaPinellas: "https://aca-prod.accela.com/PINELLAS/default.aspx",
    accelaBuilding:
      "https://aca-prod.accela.com/PINELLAS/Cap/CapHome.aspx?TabName=Home&module=Building",
    accelaClearwater: "https://aca-prod.accela.com/CLEARWATER/Default.aspx",
    epermitClearwater: "https://epermit.myclearwater.com/CitizenAccess/Default.aspx",
    sunbiz: "https://dos.fl.gov/sunbiz/other-services/data-downloads/",
    sunbizSearch: "https://search.sunbiz.org/Inquiry/CorporationSearch/ByName",
    bbb: "https://www.bbb.org/us/category/data",
    clerk: "https://officialrecords.mypinellasclerk.gov/",
    taxCollector: "https://www.pinellas.gov/taxcollector/",
    openData: "https://egis.pinellas.gov/apps/egis/apps.html",
    netr: "https://publicrecords.netronline.com/state/florida/county/pinellas",
  };

  for (const [key, url] of Object.entries(urls)) {
    const r = await timedFetch(url);
    results.curl[key] = summarize(r);
    console.log(`${key}: ${r.status} ${r.ms}ms ${r.bytes}b challenge=${r.challenge} ${r.error || ""}`);
  }

  const first = sampleAttrs[0];
  const strap = first?.STRAP || first?.PARCELID;
  results.gis.firstStrap = strap || null;
  if (strap) {
    const detailUrls = [
      `https://www.pcpao.gov/property-details?s=${encodeURIComponent(strap)}`,
      `https://www.pcpao.gov/property-details?s=${encodeURIComponent(String(strap).replace(/\D/g, ""))}`,
      `https://www.pcpao.gov/quick-search?s=${encodeURIComponent(strap)}`,
    ];
    results.curl.propertyDetailVariants = [];
    for (const u of detailUrls) {
      const r = await timedFetch(u);
      results.curl.propertyDetailVariants.push(summarize(r));
      console.log(`detail ${u} -> ${r.status} ${r.ms}ms ${r.bytes}b`);
      if (r.ok && r.bytes > 2000) {
        writeFileSync(join(SAMPLES, `curl-property-details-${strap}.html`), r.text);
      }
    }
  }

  // Playwright probes
  const playwrightModule = join(ROOT, "..", "downloads", "pinellas", "node_modules", "playwright", "index.js");
  const { chromium } = existsSync(playwrightModule)
    ? await import(playwrightModule)
    : await import("playwright");

  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({
    userAgent:
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    viewport: { width: 1400, height: 900 },
  });

  async function probePage(name, url, extra) {
    const page = await context.newPage();
    const network = [];
    page.on("response", (res) => {
      const u = res.url();
      if (
        /json|api|property|parcel|search|autocomplete|accela|pcpao/i.test(u) &&
        network.length < 80
      ) {
        network.push({ url: u.slice(0, 300), status: res.status(), type: res.request().resourceType() });
      }
    });
    const started = Date.now();
    let status = 0;
    try {
      const resp = await page.goto(url, { waitUntil: "domcontentloaded", timeout: 45000 });
      status = resp?.status() || 0;
      await page.waitForTimeout(2500);
      if (extra) await extra(page);
      const html = await page.content();
      const title = await page.title();
      const bodyText = (await page.locator("body").innerText().catch(() => "")).slice(0, 1500);
      writeFileSync(join(SAMPLES, `${name}.html`), html);
      const shot = join(SAMPLES, `${name}.png`);
      await page.screenshot({ path: shot, fullPage: false });
      const rec = {
        url,
        status,
        ms: Date.now() - started,
        title,
        bytes: Buffer.byteLength(html),
        challenge: /cloudflare|captcha|just a moment|access denied/i.test(html.slice(0, 8000)),
        bodyPreview: bodyText.replace(/\s+/g, " ").slice(0, 600),
        network: network.slice(0, 40),
        screenshot: shot,
      };
      results.playwright[name] = rec;
      console.log(`pw ${name}: ${status} ${rec.ms}ms title=${title}`);
    } catch (err) {
      results.playwright[name] = { url, status, ms: Date.now() - started, error: String(err) };
      console.log(`pw ${name} FAIL ${err}`);
    } finally {
      await page.close();
    }
  }

  await probePage("pw-pcpao-home", "https://www.pcpao.gov/");

  if (strap) {
    const detailUrl = `https://www.pcpao.gov/property-details?s=${encodeURIComponent(strap)}`;
    await probePage("pw-property-details", detailUrl, async (page) => {
      // click tabs/expanders if present
      const tabs = page.locator("a, button, [role=tab]");
      const n = Math.min(await tabs.count(), 25);
      for (let i = 0; i < n; i++) {
        const t = (await tabs.nth(i).innerText().catch(() => "")).trim();
        if (/building|sale|value|owner|permit|extra|land|tax|sketch/i.test(t) && t.length < 40) {
          await tabs.nth(i).click({ timeout: 1500 }).catch(() => {});
          await page.waitForTimeout(400);
        }
      }
    });
  }

  await probePage(
    "pw-accela-pinellas",
    "https://aca-prod.accela.com/PINELLAS/Cap/CapHome.aspx?TabName=Home&module=Building",
  );
  await probePage("pw-accela-clearwater", "https://aca-prod.accela.com/CLEARWATER/Default.aspx");
  await probePage("pw-sunbiz-downloads", "https://dos.fl.gov/sunbiz/other-services/data-downloads/");
  await probePage("pw-bbb", "https://www.bbb.org/us/category/data");

  await browser.close();
  writeFileSync(join(OUT, "probe-results.json"), JSON.stringify(results, null, 2));
  console.log("wrote", join(OUT, "probe-results.json"));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
