# Preserved patches

## Elephant query-db Broward local loader

- Source: `elephant-xyz/elephant-query-db`
- Source commits: `de4f9d7fb626a9d7c8907d1e91b74e45d72cb23a` and
  `ddf253f208528190dffb69152b2d46b3179ded38`
- Target branch: `main` (base commit
  `15187e2d8709115635620ea7113e915b1e9a0651`)
- Patch: `elephant-query-db-broward-local-loader.patch`
- SHA-256: `bca216a3c68339abe31dca1b34ff524d4df23606972684837bc268ebae40c083`
- Contents: loader source, CLI and README updates, and
  `tests/broward-local-loader.test.ts`; no secrets or generated data
- Verification: `npm run typecheck` passed; `npm run test` passed (30 test
  files, 398 tests); `git diff --check` passed; applying the patch to `main`
  reproduced the `ddf253f` tree exactly

Apply from a clean checkout of `elephant-xyz/elephant-query-db`:

```bash
git switch main
git am /workspace/docs/patches/elephant-query-db-broward-local-loader.patch
```
