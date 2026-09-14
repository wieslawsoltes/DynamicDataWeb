# Blazor hosting and interop contract

Use interactive WebAssembly or Interactive Server. Static SSR can render a provider/loading fragment but cannot initialize the browser engine. Wait for `Ready`, and keep browser sessions per app/circuit rather than cross-user singletons. Package assets resolve relative to the base URI under `_content/DynamicDataWeb.Blazor/`.

`DynamicDataView<TItem>` is an ordinary Razor view. Optional native DOM factories use `BrowserTemplate<TItem>` and `BrowserFunction.RazorTemplate`; register `builder.Services.AddDynamicDataWebBlazor()` and, on WASM, `builder.RootComponents.RegisterDynamicDataWebBlazor()`. Server registration is `AddRazorComponents().AddInteractiveServerComponents(options => options.RootComponents.RegisterDynamicDataWebBlazor())`.

Native templates are independent Blazor roots. They support nested components, callbacks and shadow-DOM input binding, but do not inherit outer cascading values automatically. Declare required `CascadingValue` components inside templates and keep durable state outside recreated/virtualized roots. Use unique template IDs and explicit serializable native-model fields or DTO contexts.

`BrowserModule` supports constructor/invoke/call/get/set/events. Use `InvokeReferenceAsync`, `CallReferenceAsync` and `GetReferenceAsync` for returned native functions, then `CallFunctionAsync` or pass the handle into another native method. Synchronous native selectors/comparers remain browser functions; `BrowserFunction.DotNet` is asynchronous, only appropriate for promise-compatible native APIs. Cancelling an interop wait does not stop arbitrary synchronous native computation.

`CallJsonAsync`, `InvokeJsonAsync`, `GetJsonAsync` and binary equivalents transfer complete data through streams (64 MiB explicit default). `SubscribeJsonAsync` provides complete JSON DTO notifications; `SubscribeAsync` is a bounded diagnostic snapshot of live graphs. `CallBatchAsync` preserves order, not atomicity. Generic application arguments should use `BrowserValue.Literal` so `$fn` data is not interpreted as callbacks. Never use untrusted module URLs as executable callbacks.

Dispose subscriptions, collections and owned modules asynchronously. Dispose borrowed native handles without calling native disposal; `ReleaseAsync` is for resources you own. JSON values are copies, unlike identity-preserving native references. Rendering notifications are dispatched through the component's Blazor renderer.

Source builds use recursive submodule initialization, npm ci/build and `node blazor/build.mjs`, then .NET build/pack. Consumers require neither Node nor Dockyard. The WASM and Server (`/probe/`) samples are restored from the produced nupkg in CI for net8.0/net10.0 and checked in Chromium for native collection behavior, callbacks, full streams and remounting. These tests do not qualify all browsers, hybrid WebViews or physical GPUs.

NuGet versions are independent of npm in `Version.props`. Version-changing main merges publish after validation using `NUGET_API_KEY` (`NUGET_TOKEN`/`NUGET_KEY` aliases), reject conflicting immutable versions, verify downloaded public package payloads and attach symbols, runnable samples and checksums to `blazor-v*` releases.
