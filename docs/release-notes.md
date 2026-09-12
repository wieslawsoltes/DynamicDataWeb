DynamicDataWeb 0.1.1 adds native ReactiveWeb integration and lifecycle fixes.

- ReactiveObject property streams now drive AutoRefresh, property observation,
  property filters and grouping directly, including nested object replacement.
- Collection binding accepts original change batches through ApplyChanges and
  supports .NET-style collection editing.
- Lifecycle operators dispose .Dispose/.DisposeAsync resources and correctly
  release subscriptions after synchronous errors and throwing cleanup callbacks.
- Helper/root ItemWithIndex exports share their class identity.

Install:

```sh
npm install @wieslawsoltes/dynamicdataweb@0.1.1 rxjs
```

The same verified tarball is distributed through GitHub Packages and npm with
provenance. The release includes standalone browser modules, a static showcase,
SHA-256 checksums and complete source archives.

The compatibility report documents the web adaptations; this release does not
claim exact equivalence to every DynamicData C# overload or native collection API.
