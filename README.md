# DynamicDataWeb

[![CI and distribution](https://github.com/wieslawsoltes/DynamicDataWeb/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/wieslawsoltes/DynamicDataWeb/actions/workflows/ci.yml)
[![npm version](https://img.shields.io/npm/v/%40wieslawsoltes%2Fdynamicdataweb)](https://www.npmjs.com/package/@wieslawsoltes/dynamicdataweb)
[![npm downloads](https://img.shields.io/npm/dm/%40wieslawsoltes%2Fdynamicdataweb)](https://www.npmjs.com/package/@wieslawsoltes/dynamicdataweb)
[![Latest release](https://img.shields.io/github/v/release/wieslawsoltes/DynamicDataWeb)](https://github.com/wieslawsoltes/DynamicDataWeb/releases/latest)
[![License](https://img.shields.io/github/license/wieslawsoltes/DynamicDataWeb)](LICENSE)
[![Live demo](https://img.shields.io/badge/demo-GitHub%20Pages-blue)](https://wieslawsoltes.github.io/DynamicDataWeb/)

[Web demo](https://wieslawsoltes.github.io/DynamicDataWeb/) ·
[Releases](https://github.com/wieslawsoltes/DynamicDataWeb/releases) ·
[Compatibility](docs/COMPATIBILITY.md) · [Publishing](docs/publishing.md)

Reactive keyed caches and ordered lists for JavaScript and TypeScript, built on **RxJS 7.8.2**. The package supplies actual RxJS observables, pipeable collection operators, and an opt-in-per-instance fluent API. It does not modify the global RxJS Observable prototype.

This is a substantial web adaptation of DynamicData, pinned to upstream commit `ef790af138811c3268c9bb687886c2acbd0b66c0` (10.0 preview). It includes counterparts for all 118 core cache/list method names. **Name coverage does not establish identical behavior for all 437 C# overloads.** Read [the compatibility report](docs/COMPATIBILITY.md) for exact adaptations, tested contracts and remaining boundaries.

## Run the project

Use **Node 22 or newer** for development and Node consumers.

```sh
npm ci
npm test
npm run check:types
npm run build
npm run demo
```

The demo opens at `http://localhost:4173`. The CI and distribution workflow validates Node 22 and 24 consumers, publishes immutable release archives, and deploys the built demo using GitHub's official Pages artifact and deployment actions. It can also be run manually. Deployment runs from `main` through the `github-pages` environment. No backend, account, API key, CDN or remote runtime dependency is required. The sample market data is generated locally and is not financial data.

## Use in a JavaScript application

Install the public npm package together with its RxJS peer:

```sh
npm install @wieslawsoltes/dynamicdataweb rxjs@^7.8.2
```

```js
import { BehaviorSubject } from 'rxjs';
import { SourceCache, filter, transform, sort, bind } from '@wieslawsoltes/dynamicdataweb';

const assets = new SourceCache(asset => asset.id);
const predicate$ = new BehaviorSubject(asset => asset.enabled);
const visibleAssets = [];

const subscription = assets.connect().pipe(
  filter(predicate$),
  transform(asset => ({ ...asset, displayPrice: asset.price.toFixed(2) })),
  sort((a, b) => a.name.localeCompare(b.name)),
  bind(visibleAssets)
).subscribe();

assets.edit(cache => {
  cache.addOrUpdate({ id: 1, name: 'Lumen', price: 142.80, enabled: true });
  cache.addOrUpdate({ id: 2, name: 'Atlas', price: 98.20, enabled: false });
});
predicate$.next(asset => asset.price > 100);

// Dispose the pipeline when its owner goes away.
subscription.unsubscribe();
assets.dispose();
```

`bind` also accepts a callback, RxJS subject, or supported observable collection. A callback integrates directly with a component's state setter. You own the subscription and must release it when the component is unmounted.

```js
// Familiar C#-style naming, with explicit web binding instead of an out parameter:
const subscription = assets.Connect()
  .Filter(asset => asset.enabled)
  .Transform(asset => ({ ...asset, label: asset.name.toUpperCase() }))
  .Sort((a, b) => a.label.localeCompare(b.label))
  .Bind(render)
  .Subscribe();
subscription.Dispose();
```

## Plain HTML and browser modules

The self-contained browser ESM and global bundles include RxJS and work without a bundler:

```html
<script type="module">
  import { SourceList, toCollection, rxjs } from './dist/dynamicdata.js';
  const source = new SourceList();
  source.connect().pipe(toCollection()).subscribe(items => console.log(items));
  source.addRange(['alpha', 'beta']);
  source.move(0, 1);
</script>
```

Or load `dist/dynamicdata.global.js` with a normal script tag and use `DynamicData.SourceCache`, `DynamicData.Filter`, and `DynamicData.rxjs`. Node CommonJS consumers can `require('@wieslawsoltes/dynamicdataweb')`. The normal package entry point keeps RxJS external for dependency sharing. Imports from `@wieslawsoltes/dynamicdataweb/core` and `@wieslawsoltes/dynamicdataweb/operators` avoid fluent registration and allow narrower bundles.

## Included capabilities

- Keyed caches, ordered lists with duplicates, change-aware collections, intermediate caches, read-only views, Optional values, batching, preview, key watching, rollback on failed edits, notification suspension and ordered reentrant delivery.
- Static and observable filters, cached transforms, safe and inline transforms, nested child collections, asynchronous and observable transforms, key conversion, runtime casts and distinct values.
- Stable sorting, dynamic comparers, paging, index-window virtualisation, reverse/top, binding and collection snapshots.
- Live/immutable/property/observable grouping; specified groups; all four join kinds and their Many variants; static and dynamic union/intersection/exclusion/exclusive membership.
- Sum/count/average/minimum/maximum/standard deviation, aggregation change enumeration and update diagnostics.
- Property notification proxies, per-item subscriptions, disposal and async disposal, auto-refresh, expiry, retention limits, buffering, source switching, ref-counted snapshots and stream conversion.
- Tree transformation, adapters, update/reason filtering, source merge operators, optional/iterable helpers and a framework-neutral observable collection.

## Sample app

The sample has light and dark themes and seven workspaces:

1. Live cache: generated assets, search, reactive sector filters, sorting, paging, batch edits and change records.
2. Ordered lists: append, move, replace, delete and batched edits.
3. Groups and joins: live sector membership and coverage updates from either side of a join.
4. Stream lifecycle: property refresh, timed expiry and stale async result cancellation.
5. Operator recipes: 73 executable recipes with source and emitted output.
6. Performance: actual browser timings for up to 100,000 items, with a bounded rendered window.
7. API reference: searchable runtime exports and links to the compatibility report.

The recipes demonstrate the major families; they are not a separate example for every upstream overload. The test suite covers many additional operators and failure paths.

## Change model and performance

A `ChangeSet` is array-compatible and contains `Change` or `ListChange` records with camelCase and PascalCase properties. Cache records carry `key`; list records carry indices and may contain a range. Source cache filtering/transformation emits incremental deltas without rebuilding full snapshots. Sorted/windowed streams additionally expose `.items`, `.keys`, `.sortedItems`, and `.response` where applicable.

Cache edits are expected O(1) per key. Cache filter/transform and several aggregates are incremental. Sorting uses binary placement but still shifts/copies arrays and produces ordered snapshots. Generic list reordering can be quadratic, and full regrouping/min/max scan retained items. `npm run benchmark` measures these costs separately; [BENCHMARK.json](docs/BENCHMARK.json) records one local run, not a portable performance guarantee.

## Source and licensing

DynamicData is MIT licensed, Copyright (c) Roland Pheasant 2011–2022. Its license is retained in `LICENSE`. RxJS is Apache-2.0 licensed; its notice and license are retained in `docs/RXJS-LICENSE.txt`. See `NOTICE`. This port is not an official release from either upstream project.

The upstream inventory includes all public types and method signatures at the pinned revision, plus immutable source links. [UPSTREAM-API.md](docs/UPSTREAM-API.md), [API-MAPPING.md](docs/API-MAPPING.md), and the machine-readable JSON files allow independent review. No unimplemented overload is certified merely because an export has the same name.

## Releases and npm publishing

Tagged versions are available on [npm](https://www.npmjs.com/package/@wieslawsoltes/dynamicdataweb), GitHub Packages, and [GitHub Releases](https://github.com/wieslawsoltes/DynamicDataWeb/releases). Each release includes the npm tarball, browser bundles, complete showcase, and SHA-256 checksums. Publishing verifies the exact tarball before and after npm publication, including installed ESM, CommonJS and strict TypeScript consumers and RxJS interoperability. See [publishing instructions](docs/publishing.md) for release preparation, token setup, provenance and safe retries.
