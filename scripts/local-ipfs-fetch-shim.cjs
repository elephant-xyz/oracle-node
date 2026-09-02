/**
 * Redirect elephant-cli public IPFS gateway fetches to a local Kubo gateway.
 *
 * elephant-cli@1.58.1 hardcodes ipfs.io / dweb.link / w3s.link. Load this with
 * `NODE_OPTIONS=--require <this-file>` so `validate` can use a local node.
 *
 * Gateway URL: `PINELLAS_IPFS_GATEWAY` or `http://127.0.0.1:8080`.
 */

"use strict";

const LOCAL_GATEWAY = (
  process.env.PINELLAS_IPFS_GATEWAY || "http://127.0.0.1:8080"
).replace(/\/$/, "");

const PUBLIC_GATEWAY_ORIGIN =
  /^https:\/\/(ipfs\.io|gateway\.ipfs\.io|dweb\.link|w3s\.link)(?=\/|$)/i;

/**
 * @param {string} url - Absolute fetch URL.
 * @returns {string} Local-gateway URL when the host is a public IPFS gateway.
 */
function rewriteIpfsGatewayUrl(url) {
  if (typeof url !== "string" || url.length === 0) return url;
  return url.replace(PUBLIC_GATEWAY_ORIGIN, LOCAL_GATEWAY);
}

const originalFetch = globalThis.fetch.bind(globalThis);

/**
 * @param {RequestInfo | URL} input - Fetch input.
 * @param {RequestInit} [init] - Fetch init.
 * @returns {Promise<Response>} Upstream fetch.
 */
function patchedFetch(input, init) {
  if (typeof input === "string") {
    return originalFetch(rewriteIpfsGatewayUrl(input), init);
  }
  if (input instanceof URL) {
    return originalFetch(rewriteIpfsGatewayUrl(input.toString()), init);
  }
  if (typeof Request !== "undefined" && input instanceof Request) {
    const rewritten = rewriteIpfsGatewayUrl(input.url);
    if (rewritten !== input.url) {
      return originalFetch(new Request(rewritten, input), init);
    }
  }
  return originalFetch(input, init);
}

globalThis.fetch = patchedFetch;
module.exports = { rewriteIpfsGatewayUrl, LOCAL_GATEWAY };
