# DynamicDataWeb.Blazor 0.2.2

Adopts validated shared runtime c833be49d472583b6f56225862e0aa7d201c1da7 from merged Dockyard PR #5. Fixes concurrent visual disposal, late Razor imports/creation, queued callbacks after removal and retained cleanup failures. Includes awaitable template teardown, coalesced updates and lifecycle state properties.

Typed SourceCache/SourceList services, full DTO notifications, native RxJS operators and live Razor views remain intact. Root and package guides identify the new version. Eight shared JavaScript lifecycle cases, managed visual/template tests and package-restored template movement/update/recreation run on .NET 8/.NET 10 in WebAssembly and Interactive Server.

NuGet publication remains validation-gated with complete public-payload verification and package/symbol/sample release artifacts. No runtime Dockyard/npm/CDN dependency is introduced. Native callback and engine compatibility contracts remain unchanged.
