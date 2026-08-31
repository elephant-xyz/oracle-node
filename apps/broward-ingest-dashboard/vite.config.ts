import { defineConfig } from "vite";

/**
 * Build the browser-only dashboard bundle.
 *
 * Vercel compiles the separate `api/status.ts` function. Keeping the Vite
 * entry under `src/client.ts` prevents PostgreSQL and environment-variable
 * access from entering the public bundle.
 */
export default defineConfig({
  build: {
    sourcemap: false,
  },
  server: {
    port: 4_783,
  },
});
