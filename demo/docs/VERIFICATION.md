# Verification

Verified on Node.js v24.19.0 on Linux. RxJS version: 7.8.2. Upstream reference: `ef790af138811c3268c9bb687886c2acbd0b66c0`.

- **167 JavaScript tests passed**, zero failures or skipped tests.
- Strict TypeScript consumer tests passed through the package self-reference and all seven module subpaths. Declaration validation also passed without `skipLibCheck`.
- ESM, CommonJS, self-contained browser ESM and browser global builds completed. A CommonJS PascalCase pipeline smoke test passed.
- The DOM integration test exercised all seven sample workspaces and **73 executable recipes**, including filtering, editing, paging, grouping, joining, async races, property notifications, benchmarks, API search and theme switching.
- Collection tests include 100,000-item caches, 150,000-item list ranges, sorted virtual-window backfill, randomized list edits, a 240-operation grouped-list replay, and 2,500 ordered reentrant edits.
- Retention tests feed 10,000 items into bounded cache/list conversions and check actual retained entry counts and timer cleanup.
- Async, timer and subscription tests cover stale results, cancellation, concurrency limits, nested property rewiring, parent/child ownership and duplicate occurrences.

The sample interaction tests use Happy DOM in Node. They do not certify pixel layout or a browser-engine rendering pass. A cloud-browser connection could not open the local preview; no visual browser pass is claimed.

These are regression and integration tests for the JavaScript port. The entire upstream C# suite has not been translated or run as a differential oracle. API name coverage is recorded separately from behavior conformance in `API-MAPPING.md` and `COMPATIBILITY.md`.

`BENCHMARK.json` contains one local benchmark run. The browser Performance workspace measures its own device and workload; it never substitutes fixed benchmark results.
