# Observable lifetimes, mutable properties and asynchronous work

The lifecycle operators are ordinary RxJS operators. Use them in `source.connect().pipe(...)`; their results remain RxJS observables. Public package exports also provide PascalCase names. Durations use milliseconds and schedulers use the RxJS `SchedulerLike` contract.

## Mutable property notifications

JavaScript has no built-in equivalent of `INotifyPropertyChanged`. Keep and mutate the object returned by `createObservableObject`, or explicitly call `notifyPropertyChanged` after editing an ordinary object.

```js
import {
  SourceCache, createObservableObject, autoRefresh,
  filter, sort, page, bind,
} from 'dynamicdata-rxjs';

const people = new SourceCache(person => person.id);
const person = createObservableObject({ id: 1, name: 'Ada', active: true, score: 10 });
const visibleRows = [];
const subscription = people.connect().pipe(
  autoRefresh(),
  filter(person => person.active),
  sort((a, b) => b.score - a.score),
  page({ page: 1, size: 20 }),
  bind(visibleRows),
).subscribe();

people.addOrUpdate(person);
person.score = 30; // Produces Refresh, then reevaluates the filter and sorted page.
person.active = false;
subscription.unsubscribe();
people.dispose();
```

The proxy is shallow. A write through the original unwrapped object cannot be intercepted. For a dotted property path such as `'profile.address.city'`, the observer subscribes at each path segment and automatically rewires when a parent object is replaced or becomes null. Wrap the mutable objects at those segments, or use `notifyPropertyChanged` on the object being edited. Old branches are disconnected. Accessor functions can read nested values but cannot be introspected for dependency paths; use a dotted string when nested rewiring is required.

| Function | JavaScript contract |
|---|---|
| `createObservableObject(object)` / `observableObject(object)` | Returns a cached shallow notification proxy. Equal property assignments produce no notification. |
| `notifyPropertyChanged(object, property, previous?)` | Explicit notification for an ordinary object or proxy. |
| `whenPropertyChanged(objectOrChangeStream, property, notifyInitial = true)` | Emits `{ sender, propertyName, value, previous }`. Change streams subscribe to every current item and release removed items. |
| `observeProperty(...)` / `whenValueChanged(...)` | Emits distinct property values using the same arguments. |
| `whenAnyPropertyChanged(objectOrChangeStream, ...propertyNames)` | Emits the changed object; an omitted property list watches every notified property. |
| `whenChanged(object, properties, resultSelector?)` | `properties` is one property or an array of property names/accessors. The selector receives `(object, ...values)`. An omitted selector returns the single value or value array. |
| `autoRefresh(property?, options?)` | Refreshes items when notified properties change. Options: `{ throttle, buffer, scheduler }`. Throttle is a per-item debounce. |
| `autoRefreshOnObservable(selector, options?)` | Selector receives `(item, key)`; every inner emission refreshes that item. Options: `{ buffer, scheduler }`; a number means buffer duration. |
| `filterOnProperty(property, predicate, options?)` | Composes notification refresh with a predicate over the complete item. |

`autoRefreshOnObservable` forwards the source transaction before synchronous inner notifications. Removed items' subscriptions are disposed. Buffered refreshes are deduplicated per occurrence; removed occurrences are omitted from the buffered result. Lists track repeated references as distinct occurrences.

## Per-item observables and predicates

```js
import { BehaviorSubject } from 'rxjs';
import { SourceCache, filterOnObservable, transformOnObservable, bind } from 'dynamicdata-rxjs';

const source = new SourceCache(item => item.id);
const row = { id: 1, visible: new BehaviorSubject(true), label: new BehaviorSubject('Ready') };
const labels = [];
const connection = source.connect().pipe(
  filterOnObservable(item => item.visible),
  transformOnObservable(item => item.label),
  bind(labels),
).subscribe();
source.addOrUpdate(row);
row.label.next('Running');
row.visible.next(false);
connection.unsubscribe();
source.dispose();
```

`filterOnObservable` excludes an item until its observable emits `true`. `transformOnObservable` emits the latest transformed value. Replacing an item releases its previous observable. A pending transformed replacement retains the last projected value until a new value arrives. Cache keys and upstream sorted order survive projection; list output indices are remapped to the visible/projected collection.

`trueForAll(selector, predicate = Boolean)` and `trueForAny(selector, predicate = Boolean)` produce distinct booleans. Items whose observable has not emitted fail the predicate. An empty collection satisfies `trueForAll` and does not satisfy `trueForAny`. A one-argument predicate receives the value; a two-argument predicate receives `(item, value)`.

`mergeMany(selector)` emits values from all live item observables. `mergeManyItems(selector)` emits `{ item, key, value }`. Both release an item's inner subscription when the item is removed or replaced, and release all inners when the source completes, errors or the consumer unsubscribes.

## Async transformations and cancellation

```js
import { transformAsync } from 'dynamicdata-rxjs';

const details = source.connect().pipe(
  transformAsync(
    async (item, key, previous, signal) => {
      const response = await fetch(`/api/items/${encodeURIComponent(key)}`, { signal });
      if (!response.ok) throw new Error(`Request failed: ${response.status}`);
      return { ...item, detail: await response.json() };
    },
    { maximumConcurrency: 8, transformOnRefresh: true },
  ),
);
```

The argument order is **`(item, key, previous, signal)`**. The fourth argument is an `AbortSignal`, not the third. Factories may return promises, observables or immediate values. Defaults are unlimited concurrent subscriptions and no retransform on Refresh. `MaximumConcurrency` and `TransformOnRefresh` option spellings are accepted too.

Updates/removals/unsubscription abort prior work; stale results never enter the output. Removed queued items never call the factory. A previously emitted value remains visible while its replacement is pending. Refresh reruns the factory only with `transformOnRefresh: true`. Source completion waits for the latest outstanding transformations. A rejected transformation errors the stream and tears down other subscriptions.

Cancellation is cooperative: a promise that ignores the supplied signal can continue doing external work after its subscription is cancelled. The concurrency limit counts active subscriptions, not uncooperative background work.

`transformSafeAsync(factory, onError, options?)` reports `{ error, item, key }` and continues processing other items. `transformManyAsync(factory, childKeySelector?, options?)` transforms parents asynchronously into child iterables, then flattens them. `transformManySafeAsync(factory, childKeySelector, onError, options?)` adds the same error handler. These are JavaScript signatures; the C# generic/overload syntax is not executable JavaScript.

## Cleanup and item callbacks

`subscribeMany(selector)` attaches one returned disposable per occurrence. The selector may return a function, RxJS subscription, an object with `dispose`, or an object with `Symbol.dispose`. Replacing/removing the item or terminating the outer subscription releases that resource.

`disposeMany(disposer?)` disposes removed and replaced items **after** forwarding their changes. An update to the identical object does not dispose that retained object. Completion, error and unsubscription dispose the remaining objects. With no argument, it accepts `unsubscribe`, `dispose`, `Symbol.dispose`, or an item that is itself a cleanup function.

```js
import { asyncDisposeMany } from 'dynamicdata-rxjs';

let disposalsCompleted;
const connection = resources.connect().pipe(
  asyncDisposeMany(completed => { disposalsCompleted = completed; }),
).subscribe();

// Observe this separate channel before or after ending the main subscription.
disposalsCompleted.subscribe({ complete: () => console.log('All cleanup finished') });
connection.unsubscribe();
```

`asyncDisposeMany(completedAccessor, disposer?)` prefers `Symbol.asyncDispose` or `disposeAsync`, then ordinary disposal. Its replayed completion channel waits for asynchronous cleanup even after the main stream is unsubscribed or errors. Cleanup rejection errors that separate channel.

`onItemAdded`, `onItemRemoved`, and `onItemRefreshed` call `(item, key)`. `onItemUpdated` calls `(current, previous, key)`. List range events invoke add/remove callbacks for every affected item; list keys are undefined. `finallySafe(action)` uses the RxJS `finalize` contract.

## Time windows, expiry and size

| Operator | Behavior |
|---|---|
| `expireAfter(selectorOrDuration, optionsOrScheduler?)` | Emits downstream removals after each item's duration. A selector returning `null` means never expire. Updates reset timers; Refresh does not. Source termination cancels pending timers. Options accept `{ scheduler }`, or pass the scheduler directly. |
| `limitSizeTo(size)` | Positive FIFO capacity. Updates preserve insertion order. Evicted items stay absent until a new upstream add/update reintroduces them. |
| `batch(duration = 16, scheduler?)` | Concatenates sequential change records per time window, suppresses empty batches and flushes at completion. |
| `bufferIf(pause$, options?)` / `batchIf(...)` | Buffers while paused; resumes/flushed batches on false or timeout. Options: `{ initialPauseState, timeout, scheduler }`; a boolean selects the initial pause state. Completion flushes; errors discard pending records. |
| `bufferInitial(duration, scheduler?)` | Buffers the first nonempty burst once, then forwards live transactions directly. |
| `deferUntilLoaded()` | Skips empty sets until the first nonempty set. |
| `notEmpty()` | Suppresses empty sets throughout the stream. |
| `skipInitial()` | Skips exactly the first emission. |

The pipeable expiry and size operators affect the downstream view. The direct-source overloads below remove actual source entries and publish arrays of evicted values. Cache arrays contain `[key, item]` pairs; list arrays contain items.

```js
import { asyncScheduler } from 'rxjs';
import { SourceCache, limitSizeTo, expireAfter } from 'dynamicdata-rxjs';

const sourceCache = new SourceCache(item => item.id);
const evictions = limitSizeTo(sourceCache, 1_000).subscribe(removedPairs => {
  console.log('Removed', removedPairs.length, 'oldest source entries');
});
const expiration = expireAfter(sourceCache, item => item.ttl, {
  scheduler: asyncScheduler,
  pollingInterval: 1_000, // Optional; omit for per-item timers.
}).subscribe(expiredPairs => console.log(expiredPairs));
```

The signatures are `limitSizeTo(source, size, optionsOrScheduler?)` and `expireAfter(source, selectorOrDuration, optionsOrScheduler?)`. Size options accept `{ scheduler }`; without a scheduler eviction occurs within the source update cycle. Expiry options accept `{ scheduler, pollingInterval }`. Polling batches due entries at each positive interval. Both overloads also accept a scheduler directly. Unsubscription cancels retention work without disposing the supplied source. Refresh does not restart expiry; update does. Duplicate list occurrences have independent deadlines.

`toObservableChangeSet` uses these source-mutating retention mechanisms internally: evicted/expired values are removed from the backing ingestion collection, and their timers are cancelled. Long-running input therefore remains bounded by the configured size or currently unexpired values, including the state received by a new backing-source subscriber. An individual input batch can temporarily hold its own incoming values while it is processed.

## Connections and conversion

`watch(key)` emits matching change records. `watchValue(key)` emits their current values, including the removed value on Remove. Neither synthesizes a value for a missing key.

`refCount()` shares one upstream connection. A late subscriber receives the full current collection snapshot. The final unsubscription disconnects and drops that shared state; a subsequent subscription reconnects afresh.

`switchLatest()` (also exported as `switch` and `Switch`) consumes an observable of change-set observables or source collections. Switching removes the previous inner collection, disconnects it and subscribes to its replacement. Completion waits for both the outer and active inner source. `monitorStatus()` emits `pending`, `loaded`, and then `completed` or `errored`; the error status completes its status stream.

`toObservableChangeSet(keySelector?, options?)` converts item/iterable emissions to cache changes when a selector is supplied, or list changes otherwise. An options object may instead contain `keySelector`. Options include `singleItem`, `limitSize`, `expireAfter`, `scheduler`, and optional `pollingInterval` for expiry. Strings count as individual items. Lists append emissions; caches add/update by key.

`ObservableChangeSet.create(subscribe, keySelector?)`, `createCache(subscribe, keySelector)`, and `createList(subscribe)` create an independent source per subscription. The callback receives that mutable source and may return a disposable, cleanup function or a promise resolving to cleanup. Unsubscription disposes the source and callback resource. PascalCase factory methods are available too.

`observeCollectionChanges(collection, eventName = 'collectionchange')` adapts a source collection, change observable, or native `EventTarget` into `{ sender, eventArgs }`. For browser events, `event.detail` becomes `eventArgs` when present. It does not monkey-patch normal arrays or implement platform-specific .NET event interfaces.

The lifecycle test suite uses RxJS `TestScheduler` to exercise expiry and buffering. Integration tests cover property refresh → filtering → sorted paging → binding; async projection → joins → binding; and sorted observable filtering/projection. Cleanup, duplicate list occurrences, queued cancellations, stale promises and async disposal completion have separate regression tests. A long-running retention regression ingests 10,000 values and checks actual backing collection counts, late snapshots and live scheduled timer counts, then verifies expiration releases all retained entries and timers. Nested path tests cover null parents, branch replacement and removal from a source.
