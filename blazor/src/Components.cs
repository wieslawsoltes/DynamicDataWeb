using System.Text.Json;
using Microsoft.JSInterop;
namespace DynamicDataWeb.Blazor;

public sealed class DynamicDataProvider : BrowserProvider { }
public sealed class DynamicDataModule(IJSRuntime js) : BrowserModule(js)
{
    public ValueTask<SourceCache<T, TKey>> CreateCacheAsync<T, TKey>(string keyProperty) => SourceCache<T, TKey>.CreateAsync(this, keyProperty);
    public ValueTask<SourceList<T>> CreateListAsync<T>(IEnumerable<T>? initial = null) => SourceList<T>.CreateAsync(this, initial);
}
/// <summary>Owns a native collection while preserving its JavaScript identity.</summary>
public abstract class BrowserCollection<T>(BrowserModule module, IJSObjectReference handle) : IAsyncDisposable
{
    public BrowserModule Module { get; } = module;
    public IJSObjectReference Handle { get; } = handle;
    private int _disposed;
    private static readonly JsonSerializerOptions Json = new() { PropertyNameCaseInsensitive = true };
    public ValueTask<int> GetCountAsync() => Module.GetAsync<int>(Handle, "Count");
    public ValueTask<T[]> GetItemsAsync() => Module.GetAsync<T[]>(Handle, "Items");
    public ValueTask ClearAsync() => Module.CallVoidAsync(Handle, "Clear");
    public ValueTask<IJSObjectReference> ConnectAsync(object? predicate = null) => predicate is null ? Module.CallAsync<IJSObjectReference>(Handle, "Connect") : Module.CallAsync<IJSObjectReference>(Handle, "Connect", [predicate]);
    public async ValueTask<BrowserSubscription> ObserveItemsAsync(Func<T[], Task> next, Func<JsonElement, Task>? error = null)
    {
        await using var changes = await ConnectAsync();
        await using var items = await Module.CallAsync<IJSObjectReference>(changes, "ToCollection");
        return await Module.SubscribeAsync(items, "", value =>
        {
            if (value.ValueKind == JsonValueKind.Array) return next(value.Deserialize<T[]>(Json) ?? []);
            if (value.ValueKind == JsonValueKind.Object && value.TryGetProperty("error", out var failure)) return error is not null ? error(failure) : Task.FromException(new JSException(failure.ToString()));
            return Task.CompletedTask;
        });
    }
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        try { await Module.ReleaseAsync(Handle); }
        catch (ObjectDisposedException) { await Handle.DisposeAsync(); }
        catch (JSDisconnectedException) { }
    }
}
public sealed class SourceCache<T, TKey> : BrowserCollection<T>
{
    private SourceCache(BrowserModule module, IJSObjectReference handle) : base(module, handle) { }
    public static async ValueTask<SourceCache<T, TKey>> CreateAsync(BrowserModule module, string keyProperty) => new(module, await module.CreateAsync("SourceCache", [BrowserFunction.Property(keyProperty)]));
    public static async ValueTask<SourceCache<T, TKey>> CreateWithSelectorAsync(BrowserModule module, object selector) => new(module, await module.CreateAsync("SourceCache", [selector]));
    public ValueTask AddOrUpdateAsync(T item) => Module.CallVoidAsync(Handle, "AddOrUpdate", [item]);
    public ValueTask AddOrUpdateManyAsync(IEnumerable<T> items) => Module.CallVoidAsync(Handle, "AddOrUpdate", [items]);
    public ValueTask RemoveKeyAsync(TKey key) => Module.CallVoidAsync(Handle, "RemoveKey", [key]);
    public ValueTask RemoveKeysAsync(IEnumerable<TKey> keys) => Module.CallVoidAsync(Handle, "RemoveKeys", [keys]);
    public ValueTask<TKey[]> GetKeysAsync() => Module.GetAsync<TKey[]>(Handle, "Keys");
    public async ValueTask<(bool Found, T? Value)> LookupAsync(TKey key)
    {
        await using var optional = await Module.CallAsync<IJSObjectReference>(Handle, "Lookup", [key]);
        var found = await Module.GetAsync<bool>(optional, "HasValue");
        return (found, found ? await Module.GetAsync<T>(optional, "Value") : default);
    }
}
public sealed class SourceList<T> : BrowserCollection<T>
{
    private SourceList(BrowserModule module, IJSObjectReference handle) : base(module, handle) { }
    public static async ValueTask<SourceList<T>> CreateAsync(BrowserModule module, IEnumerable<T>? initial = null) => new(module, await module.CreateAsync("SourceList", [initial ?? []]));
    public ValueTask AddAsync(T item) => Module.CallVoidAsync(Handle, "Add", [item]);
    public ValueTask AddRangeAsync(IEnumerable<T> items) => Module.CallVoidAsync(Handle, "AddRange", [items]);
    public ValueTask InsertAsync(int index, T item) => Module.CallVoidAsync(Handle, "Insert", [index, item]);
    public ValueTask RemoveAtAsync(int index) => Module.CallVoidAsync(Handle, "RemoveAt", [index]);
    public ValueTask RemoveRangeAsync(int index, int count) => Module.CallVoidAsync(Handle, "RemoveRange", [index, count]);
    public ValueTask ReplaceAtAsync(int index, T item) => Module.CallVoidAsync(Handle, "ReplaceAt", [index, item]);
    public ValueTask MoveAsync(int oldIndex, int newIndex) => Module.CallVoidAsync(Handle, "Move", [oldIndex, newIndex]);
    public ValueTask LoadAsync(IEnumerable<T> items) => Module.CallVoidAsync(Handle, "Load", [items]);
    public ValueTask RefreshAtAsync(int index) => Module.CallVoidAsync(Handle, "RefreshAt", [index]);
    public ValueTask<T> GetAsync(int index) => Module.CallAsync<T>(Handle, "Get", [index]);
}
