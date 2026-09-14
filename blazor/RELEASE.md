# DynamicDataWeb.Blazor 0.2.1

Updates the pinned interop runtime to the tested Dockyard revision `1c895b7184451071e1c7131063249d2d9eb145b9`, without introducing a Dockyard runtime dependency.

- Preserve cyclic/deep native argument graphs and shared callback identity without mutating inputs.
- Await concurrent native/module/subscription cleanup and asynchronous unsubscribe; continue cleanup after individual failures.
- Preserve property, method and disposal access through native callable handles.
- Honor initialization-wait cancellation without cancelling other callers; prevent disposed owners from starting late native work.
- Add `CallFunctionJsonAsync<T>` for complete streamed callable results.
- Run expanded shared JavaScript and managed regressions against actual .NET 8/.NET 10 package consumers.

Typed caches/lists, RxJS pipelines, live Razor views, complete collection notifications and all native interop APIs remain available. WebAssembly and Interactive Server samples are validated before publication; downloaded public NuGet payloads are compared before creating the versioned release.
