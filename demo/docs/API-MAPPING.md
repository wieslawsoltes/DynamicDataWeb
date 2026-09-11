# Web API availability map

This is a runtime-export/type-declaration availability map, not a behavior conformance score. adapted means a web counterpart exists; missing means this scanner found no named counterpart. Overload and member parity are not certified. Re-run after source changes.

Upstream revision: `ef790af138811c3268c9bb687886c2acbd0b66c0`.

Recreate this report with `node docs/generate-api-mapping.mjs` after building or editing the source. The inventory source is [`upstream-api.json`](upstream-api.json), and the full per-name results are in [`api-mapping.json`](api-mapping.json).

| Item | Count |
| --- | ---: |
| Source-module runtime export names (including aliases) | 418 |
| Upstream method owner/name pairs | 273 |
| Methods with adapted counterparts | 269 |
| Methods without named counterparts | 4 |
| Public types with mapped representations | 43 |
| Public types without mapped representations | 101 |

## Cache/list operator names without named counterparts

All cache/list operator names have a mapped web counterpart. This is name-level availability; C# overload, type-member and behavioral compatibility must be assessed separately.

## Other methods without named counterparts

| Owner | Name | Overloads |
| --- | --- | ---: |
| `ParallelOperators` | [`Filter`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Platforms/net45/ParallelOperators.cs#L25) | 1 |
| `ParallelOperators` | [`SubscribeMany`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Platforms/net45/ParallelOperators.cs#L49) | 2 |
| `ParallelOperators` | [`Transform`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Platforms/net45/ParallelOperators.cs#L101) | 2 |
| `ParallelOperators` | [`TransformSafe`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Platforms/net45/ParallelOperators.cs#L152) | 2 |

## Adaptations needing per-contract assessment

- C# overload families are folded into JavaScript functions and options. Numeric aggregation, source-vs-stream overloads, selectors, optional values, and comparator streams need overload-specific validation.
- .NET property/collection notifications, expression trees, binding out parameters, decimal arithmetic, thread synchronization, and Task cancellation have explicit web contracts.
- Matching final collection contents is insufficient for change-trace parity. Use the fixture-oriented checklist in [`UPSTREAM-API.md`](UPSTREAM-API.md).
- Interface/type declarations and convenience aliases do not prove full runtime inheritance, member signatures, or platform-specific API support.

Package entrypoint imports successfully with 418 exports. Type declarations inspected: `types/advanced.d.ts`, `types/compatibility.d.ts`, `types/core.d.ts`, `types/extras.d.ts`, `types/helpers.d.ts`, `types/index.d.ts`, `types/kernel.d.ts`, `types/lifecycle.d.ts`, `types/operators.d.ts`.
