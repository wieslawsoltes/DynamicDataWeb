# Changelog

## 0.1.1

- Observe native ReactiveWeb `Changed`/`PropertyChanged` streams without proxies,
  including nested path replacement, unspecified-property notifications and
  shared observer cleanup.
- Support `.Dispose()` and `.DisposeAsync()` resources in lifecycle operators.
- Bind original change batches through `ApplyChanges`/`applyChanges` targets and
  support collections with PascalCase editing methods.
- Release item subscriptions after synchronous source errors and continue cleanup
  when individual disposers throw.
- Share `ItemWithIndex` identity between helper and kernel/root entry points.
- Add native reactive-model, lifecycle, binding and strict type regression tests.

## 0.1.0

First public npm release of DynamicDataWeb as `@wieslawsoltes/dynamicdataweb`.

- Reactive caches, ordered lists, change sets and the existing DynamicData operator families backed by real RxJS observables.
- ESM and CommonJS entry points with typed subpaths, shared module identity within each module format, and standalone browser bundles.
- Node 22/24 CI, installed-package consumers, immutable GitHub release assets, GitHub Packages, public npm publication with provenance and anonymous registry verification.
- Published collection laboratory on GitHub Pages, operator recipes, compatibility inventory and lifecycle documentation.

The compatibility report documents web adaptations and remaining upstream overload boundaries; this release does not claim exhaustive .NET overload equivalence.
