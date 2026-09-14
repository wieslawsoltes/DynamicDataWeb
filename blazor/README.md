# DynamicDataWeb.Blazor

Install `DynamicDataWeb.Blazor` 0.2.2 for .NET 8/.NET 10. The package includes the actual JavaScript engine and RxJS as local static assets for interactive WebAssembly and Server.

## Collections and Razor views

Use `DynamicDataProvider` to own an initialized `BrowserModule`, then `SourceCache<T,TKey>.CreateAsync(module, "id")` or `SourceList<T>.CreateAsync(module, items)`. Key property names use serialized JSON casing. Cache operations include add/update, bulk add/update, key removal, keys, count, items and optional lookup; list operations include insert, range operations, replacement, movement, refresh, load and indexed reads.

```razor
<DynamicDataView TItem="Item" Source="cache">
    <ChildContent Context="items">
        @foreach (var item in items) { <p @key="item.Id">@item.Name</p> }
    </ChildContent>
</DynamicDataView>
```

Create the cache after provider `Ready`, and keep it in the parent as shown in the [sample](sample/Demo.razor). `DynamicDataView` handles subscription replacement and disposal. `ObserveItemsAsync` provides complete JSON DTO arrays rather than truncated diagnostic snapshots; returned subscriptions are asynchronously disposable. Typed application item/key parameters are literal data and cannot be interpreted as executable callback descriptors.

## Native operators

`ConnectAsync` returns the real change stream. Use native method calls and the bundled `Rx` namespace through `BrowserModule`; returned operator functions use `InvokeReferenceAsync` and can be passed into native pipelines without losing identity. Selectors/comparers required synchronously by the engine must execute as browser callbacks, not Server .NET delegates.

See [INTEGRATION.md](INTEGRATION.md) for hosting, optional native Razor factories, streaming and ownership. The package is self-contained; its pinned shared source is not a Dockyard runtime dependency. The wrapper complements typed helpers with native API access and retains the engine's documented compatibility limits.

## Lifecycle in 0.2.2

The shared runtime adds deterministic visual/template cleanup, callback suppression after removal, late-import cleanup and awaitable Razor factory disposal. Independent template roots retain state during synchronous DOM movement and coalesce parameter updates. New managed and JavaScript regressions plus actual-package WebAssembly/Server movement/update/recreation tests run alongside collection and RxJS checks.
