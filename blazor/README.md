# DynamicDataWeb.Blazor

Self-contained .NET 8 / .NET 10 components and typed collection services backed by the actual DynamicDataWeb/RxJS browser implementation.

```sh
dotnet add package DynamicDataWeb.Blazor --version 0.2.0
```

`DynamicDataProvider` manages the browser session and supplies a `BrowserModule` through `Ready` and a `RenderFragment<BrowserModule>` child context. `DynamicDataView<TItem>` observes a native collection and renders a `RenderFragment<IReadOnlyList<TItem>>` whenever the collection changes. These are lifecycle-aware Blazor components, not placeholder HTML controls.

```csharp
// Execute from an interactive component's Ready/OnAfterRenderAsync, not during prerender.
await using var module = new DynamicDataModule(JS);
await using var cache = await module.CreateCacheAsync<Person, int>("id");
await cache.AddOrUpdateAsync(new Person(1, "Ada"));
await using var subscription = await cache.ObserveItemsAsync(items =>
    InvokeAsync(() => { people = items; StateHasChanged(); }));
```

Keep the module, collection and subscription alive for the component's lifetime in a real application; the using declarations illustrate ownership, not a one-shot live UI scope. Key selectors refer to JSON property names (normally camelCase).

## Typed collection APIs

`SourceCache<T,TKey>` supports creation from a key property or native callback, single/batch add-or-update, key removal, keys, lookup, count, items, clear, Connect and collection observation. `SourceList<T>` supports add/range-add, insert, remove/range-remove, replacement, moving, loading, refreshing, indexing, clear and observation. `Handle` retains native identity; `Module` accesses the underlying session.

`ObserveItemsAsync` reports typed arrays, suppresses completion as a data item and forwards stream errors explicitly. `DynamicDataView` handles subscriptions and renderer dispatch automatically without owning the supplied collection. Updates in JavaScript are snapshots across the .NET boundary, not modifications to arbitrary CLR object identity. Dispose collections after their views/subscriptions detach.

## Every bundled native export

`BrowserModule` exposes `GetExportsAsync`, `CreateAsync`, `InvokeAsync`, `CallAsync`, `GetAsync`, `SetAsync`, `SubscribeAsync`, and `ReleaseAsync`. Native DynamicData exports are available at the root, with RxJS under `Rx` and RxJS operators under `RxOperators`. `ConnectAsync` returns the real fluent DataObservable. Invoke its Filter, Transform, Sort, Group, Page, Virtualise and other operators through native references, then subscribe or bind to their results. Generic interop is not an exhaustive strongly typed C# reimplementation of every operator.

`BrowserFunction.Property`, `Setter` and `Constant` create synchronous native functions. `BrowserFunction.Module("./selectors.js", "predicate")` loads exported JavaScript callbacks without eval, supporting synchronous selectors/comparers and high-throughput operators. `BrowserFunction.DotNet` is asynchronous and only valid for promise-aware operators such as asynchronous transforms; it cannot replace a synchronous key selector on Blazor Server. Cancellation of a .NET interop wait does not necessarily stop an already running JavaScript algorithm.

Callbacks preserve ordering. Session disposal removes listeners, native resources and .NET references; it tolerates disconnected Server circuits. Module instances belong to a component or circuit, never a process-wide singleton. `CreateAsync` results are session-owned; native objects returned by other calls remain parent-owned unless explicitly released. `IJSObjectReference.DisposeAsync` releases the interop handle, while `ReleaseAsync` also invokes native cleanup. Do not call native methods after disposal.

## Hosting and build

The package contains its JavaScript and RxJS dependency. Consumers need no Node, npm, CDN, global script or Dockyard NuGet package. Use interactive WebAssembly/Server render modes. Static prerender avoids JavaScript; assets resolve relative to the app base URI under `_content/DynamicDataWeb.Blazor`.

```sh
git submodule update --init --recursive
npm ci
npm run build
node blazor/build.mjs
dotnet run --project blazor/sample/Sample.csproj
dotnet run --project blazor/server/Server.csproj --urls http://localhost:5080
# Server sample: http://localhost:5080/probe/
```

The common lifecycle code, sample hosts and test harness come from the commit-pinned `blazor/runtime-source` submodule. `blazor/config.json` deterministically generates namespace-specific projects and runtime code before packing; project-specific services, Razor view and sample are reviewed source in this repository. Generated files are ignored. Each published package is independent of that source-build dependency.

CI compiles/packs both frameworks, runs bridge and managed lifecycle/prerender tests, inspects the nupkg, restores both sample hosts from the package and drives them in Chromium. The sample checks native cache lookup/count and an actual RxJS notification, then unmounts/remounts. Packages, samples and screenshots are retained as artifacts. Source and workflow pins must be updated together through PRs.

## NuGet releases

`blazor/Version.props` controls an independent NuGet version. Validated version-changing PRs merged to main publish with `NUGET_API_KEY`, falling back to `NUGET_TOKEN` or `NUGET_KEY`. Manual dispatch is validation-only unless publication is selected. GitHub releases use `blazor-v<version>` and attach packages and the runnable WebAssembly sample without changing npm releases. Existing DynamicDataWeb compatibility limits still apply.
