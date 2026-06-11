# Dev environment tips

- When working with JavaScript always create JSDoc with very detailed type definitions for functions input/output and
  variable types.
- NEVER use `any` type as an option no matter the case
- Run `npm run typecheck` after making any changes to the code
- Make sure to cover code changes with tests
- Run `npm run test` to run tests
- NEVER use `FilterExpression` when working with DynamoDB.
- Always use aws knowledge MCP to get the docs and best practices when doing any changes related to any AWS services.

# Durable markdown notes created during the permit/Sunbiz/query-db work

- `docs/lee-county-permit-findings.md` — Lee County Accela/permit workflow findings, local E2E notes, and CLI behavior.
- `docs/permit-harvest-todo.md` — durable TODO/status list for permit harvest and Sunbiz extraction work.
- `docs/sunbiz-bulk-download-runbook.md` — Sunbiz bulk download path, browser/Cloudflare notes, S3 staging, and Deflate64 workaround.
- `docs/sunbiz-lexicon-transform-findings.md` — Sunbiz lexicon additions, transform output, and mapping notes.
- `../elephant-query-db/README.md` — query database package overview and usage.
- `../elephant-query-db/docs/data-load-and-matching-plan.md` — plan for loading appraisal, permits, and Sunbiz into Postgres and joining by parcel id/address hash.
- `../elephant-query-db/docs/schema-design.md` — logical-only Postgres schema design and rerunnable loading plan.
- `../elephant-query-db/docs/lexicon-alignment.md` — mapping from Elephant lexicon classes/relationships to logical tables and direct FKs.
- `../elephant-query-db/docs/open-lexicon-gaps.md` — permit, Sunbiz, and appraisal facts preserved pending lexicon expansion.
- `../elephant-query-db/docs/vercel-neon-query-db.md` — Vercel Neon `elephant-query-db` provisioning details and follow-up steps.
