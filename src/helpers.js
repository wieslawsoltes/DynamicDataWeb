import { Subject, Subscription, asyncScheduler, observeOn, retry, timer } from 'rxjs';
import { ChangeSet, ChangeType, ListChange, ObservableCache, Optional, SourceCache, SourceList } from './core.js';
import { ChangeSummary, collectUpdateStats } from './extras.js';

const streamOf = source => typeof source?.connect === 'function' ? source.connect() : source;
const equality = comparer => typeof comparer === 'function' ? comparer : comparer?.equals?.bind(comparer) ?? comparer?.Equals?.bind(comparer) ?? Object.is;
const compareDefault = (a, b) => a < b ? -1 : a > b ? 1 : 0;
const validateList = source => { if (!Array.isArray(source) && typeof source?.insertRange !== 'function') throw new TypeError('source must be an Array or SourceList'); };

export function addOrInsertRange(source, items, index = -1) {
  validateList(source);
  const values = Array.from(items);
  const count = Array.isArray(source) ? source.length : source.count;
  if (!Number.isInteger(index) || index > count) throw new RangeError('index must be an integer no greater than the list length');
  if (Array.isArray(source)) {
    const at = index < 0 ? source.length : index;
    // Avoid argument-count limits for very large ranges.
    const tail = source.splice(at);
    for (const item of values) source.push(item);
    for (const item of tail) source.push(item);
  } else if (index < 0) source.addRange(values);
  else source.insertRange(values, index);
  return source;
}

/** .NET-compatible binary search: absent values return -(insertionIndex + 1). */
export function binarySearch(source, value, comparer = compareDefault) {
  const compare = typeof comparer === 'function' ? comparer : comparer?.compare?.bind(comparer) ?? comparer?.Compare?.bind(comparer);
  if (typeof compare !== 'function') throw new TypeError('comparer must be a function or comparer object');
  let lower = 0, upper = (source.length ?? source.count) - 1;
  const at = index => Array.isArray(source) || ArrayBuffer.isView(source) ? source[index] : source.get(index);
  while (lower <= upper) {
    const middle = lower + Math.floor((upper - lower) / 2), result = compare(value, at(middle));
    if (result < 0) upper = middle - 1;
    else if (result > 0) lower = middle + 1;
    else return middle;
  }
  return -lower - 1;
}

export function getChangeType(reason) {
  if (['add', 'refresh', 'replace', 'move', 'moved', 'remove'].includes(reason)) return ChangeType.Item;
  if (['addRange', 'removeRange', 'clear'].includes(reason)) return ChangeType.Range;
  throw new RangeError(`Unknown list change reason: ${reason}`);
}

export class ItemWithIndex {
  constructor(item, index) { this.item = item; this.index = index; }
  get Item() { return this.item; }
  get Index() { return this.index; }
  equals(other) { return other instanceof ItemWithIndex && Object.is(this.item, other.item); }
  Equals(other) { return this.equals(other); }
  toString() { return `${this.item} (${this.index})`; }
}

export function indexOfOptional(source, item, comparer) {
  const equals = equality(comparer);
  let index = 0;
  for (const candidate of source) { if (equals(candidate, item)) return Optional.some(new ItemWithIndex(item, index)); index++; }
  return Optional.none();
}

export function replaceOrAdd(source, original, replaceWith, comparer) {
  validateList(source);
  const found = indexOfOptional(source, original, comparer);
  if (found.hasValue) {
    if (Array.isArray(source)) source[found.value.index] = replaceWith;
    else source.replaceAt(found.value.index, replaceWith);
  } else if (Array.isArray(source)) source.push(replaceWith);
  else source.add(replaceWith);
  return source;
}

/** Remove positions from records; move records are omitted because they require positions. */
export function* yieldWithoutIndex(source) {
  for (const change of source) {
    if (change.reason === 'move' || change.reason === 'moved') continue;
    yield new ListChange({ ...change, currentIndex: -1, previousIndex: -1, ...(change.range ? { range: { items: [...change.range.items], index: -1 } } : {}) });
  }
}

/** Test utility which records messages, materialized data, termination and statistics. */
export class ChangeSetAggregator {
  constructor(source, options = {}) {
    if (typeof options === 'string') options = { kind: options };
    this.messages = []; this.error = null; this.isCompleted = false; this.isDisposed = false;
    this.summary = ChangeSummary.empty;
    this._kind = options.kind ?? source.kind ?? null;
    this._collection = this._kind === 'list' ? new SourceList() : new SourceCache(options.keySelector ?? (item => item?.id ?? item?.key ?? item));
    this.data = this._kind === 'list' ? this._collection.asObservableList() : this._collection.asObservableCache();
    const updates = new Subject();
    this._subscriptions = new Subscription();
    this._subscriptions.add(updates.pipe(collectUpdateStats()).subscribe(summary => { this.summary = summary; }));
    this._subscriptions.add(streamOf(source).subscribe({
      next: changes => {
        const kind = changes.kind ?? (changes.some(change => 'key' in change) ? 'cache' : 'list');
        if (this._kind == null && kind === 'list') {
          this.data.dispose(); this._collection.dispose();
          this._collection = new SourceList(); this.data = this._collection.asObservableList();
        }
        this._kind ??= kind;
        if (kind !== this._kind) { this.error = new TypeError('An aggregator cannot mix list and cache changesets'); return; }
        this.messages.push(changes);
        this._collection.clone(changes);
        updates.next(changes);
      },
      error: error => { this.error = error; updates.complete(); },
      complete: () => { this.isCompleted = true; updates.complete(); }
    }));
    this._subscriptions.add(() => updates.complete());
  }
  get Data() { return this.data; }
  get Messages() { return this.messages; }
  get Error() { return this.error; }
  get exception() { return this.error; }
  get Exception() { return this.error; }
  get IsCompleted() { return this.isCompleted; }
  get Summary() { return this.summary; }
  dispose() { if (!this.isDisposed) { this.isDisposed = true; this._subscriptions.unsubscribe(); this.data.dispose(); this._collection.dispose(); } }
  unsubscribe() { this.dispose(); }
  Dispose() { this.dispose(); }
}

export function asAggregator(source, options) { return new ChangeSetAggregator(source, options); }

/** A shared cache specialized for watching individual keys; one subscription to its source. */
export class Watcher {
  constructor(source, scheduler) {
    this.scheduler = scheduler;
    this.cache = new ObservableCache(streamOf(source));
  }
  watch(key) { const observable = this.cache.watch(key); return this.scheduler ? observable.pipe(observeOn(this.scheduler)) : observable; }
  Watch(key) { return this.watch(key); }
  dispose() { this.cache.dispose(); }
  unsubscribe() { this.dispose(); }
  Dispose() { this.dispose(); }
}

export function asWatcher(source, scheduler) { return new Watcher(source, scheduler); }

/** Retry with zero-based failure counts and millisecond delays; null stops retrying. */
export function retryWithBackOff(strategyOrOptions = {}) {
  const options = typeof strategyOrOptions === 'function' ? { backOffStrategy: strategyOrOptions } : strategyOrOptions;
  const count = options.count ?? options.maxRetries ?? Infinity;
  const scheduler = options.scheduler ?? asyncScheduler;
  const strategy = options.backOffStrategy ?? (typeof options.delay === 'function' ? options.delay : null);
  const initialDelay = options.initialDelay ?? (typeof options.delay === 'number' ? options.delay : 100);
  return retry({
    count,
    resetOnSuccess: options.resetOnSuccess ?? false,
    delay(error, retryCount) {
      if (options.errorPredicate && !options.errorPredicate(error)) throw error;
      const delay = strategy ? strategy(error, retryCount - 1) : Math.min(options.maxDelay ?? 30000, initialDelay * (options.factor ?? 2) ** (retryCount - 1));
      if (delay == null) throw error;
      if (typeof delay?.subscribe === 'function') return delay;
      if (typeof delay !== 'number' || !Number.isFinite(delay) || delay < 0) throw new RangeError('Retry delay must be a finite nonnegative number of milliseconds, an Observable, or null');
      return timer(delay, scheduler);
    }
  });
}

/** Recursively schedule work, reevaluating an interval function after each invocation. */
export function scheduleRecurringAction(scheduler, interval, action) {
  if (typeof scheduler?.schedule !== 'function') {
    action = interval; interval = scheduler; scheduler = asyncScheduler;
  }
  if (typeof action !== 'function') throw new TypeError('action must be a function');
  const delay = () => {
    const value = typeof interval === 'function' ? interval() : interval;
    if (typeof value !== 'number' || !Number.isFinite(value) || value < 0) throw new RangeError('interval must be a finite nonnegative number of milliseconds');
    return value;
  };
  const subscription = new Subscription();
  subscription.dispose = subscription.unsubscribe.bind(subscription); subscription.Dispose = subscription.dispose;
  subscription.add(scheduler.schedule(function tick() {
    if (subscription.closed) return;
    try { action(); if (!subscription.closed) this.schedule(undefined, delay()); }
    catch (error) { subscription.unsubscribe(); throw error; }
  }, delay()));
  return subscription;
}
