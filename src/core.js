import { Observable, Subject, Subscription } from 'rxjs';

let observableDecorator = observable => observable;
export function setObservableDecorator(decorator) {
  if (typeof decorator !== 'function') throw new TypeError('Observable decorator must be a function');
  observableDecorator = decorator;
}

/** A value which explicitly distinguishes absence from a present undefined. */
export class Optional {
  constructor(value, hasValue = arguments.length > 0) { this._value = value; this.hasValue = !!hasValue; }
  static some(value) { return new Optional(value, true); }
  static none() { return new Optional(undefined, false); }
  static of(value) { return value == null ? Optional.none() : Optional.some(value); }
  get value() { if (!this.hasValue) throw new Error('Optional has no value'); return this._value; }
  get valueOrDefault() { return this._value; }
  get HasValue() { return this.hasValue; }
  get Value() { return this.value; }
  get ValueOrDefault() { return this.valueOrDefault; }
  getOrElse(fallback) { return this.hasValue ? this._value : typeof fallback === 'function' ? fallback() : fallback; }
  map(project) { return this.hasValue ? Optional.some(project(this._value)) : Optional.none(); }
  ifHasValue(action) { if (this.hasValue) action(this._value); return this; }
  equals(other) { return other instanceof Optional && other.hasValue === this.hasValue && (!this.hasValue || Object.is(this._value, other._value)); }
  toString() { return this.hasValue ? `Some(${String(this._value)})` : 'None'; }
}
Optional.Some = Optional.some; Optional.None = Optional.none; Optional.Of = Optional.of;
Optional.create = Optional.of; Optional.Create = Optional.of; Optional.toOptional = Optional.of; Optional.ToOptional = Optional.of;
Optional.fromOptional = optional => optional.value; Optional.FromOptional = Optional.fromOptional;

function reasons(names) {
  const result = {};
  for (const [name, value] of Object.entries(names)) { result[name] = value; result[name.toUpperCase()] = value; result[value] = value; }
  return Object.freeze(result);
}
export const ChangeReason = reasons({ Add: 'add', Update: 'update', Remove: 'remove', Refresh: 'refresh', Moved: 'move', Move: 'move' });
export const ListChangeReason = reasons({ Add: 'add', AddRange: 'addRange', Replace: 'replace', Remove: 'remove', RemoveRange: 'removeRange', Refresh: 'refresh', Moved: 'move', Move: 'move', Clear: 'clear' });
export const ChangeType = Object.freeze({ Item: 'item', Range: 'range' });

export class Change {
  constructor(reason, key, current, previous, currentIndex = -1, previousIndex = -1) {
    if (reason && typeof reason === 'object') Object.assign(this, reason);
    else Object.assign(this, { reason, key, current, previous: previous instanceof Optional ? previous.valueOrDefault : previous, currentIndex, previousIndex });
    this.currentIndex ??= -1; this.previousIndex ??= -1;
  }
  get Reason() { return this.reason; } get Key() { return this.key; } get Current() { return this.current; }
  get Previous() { return this.reason === 'update' || this.reason === 'replace' ? Optional.some(this.previous) : Optional.none(); }
  get CurrentIndex() { return this.currentIndex; } get PreviousIndex() { return this.previousIndex; }
  equals(other) { return !!other && ['reason', 'key', 'current', 'previous', 'currentIndex', 'previousIndex'].every(key => Object.is(this[key], other[key])); }
  toString() { return `${this.reason}, Key: ${String(this.key)}, Current: ${String(this.current)}`; }
}
export class RangeChange {
  constructor(items = [], index = -1) { this.items = Array.from(items); this.index = index; }
  get count() { return this.items.length; } get Count() { return this.count; } get Index() { return this.index; }
  [Symbol.iterator]() { return this.items[Symbol.iterator](); }
}
export class ListChange extends Change {
  constructor(reason, current, currentIndex = -1, previous, previousIndex = -1) {
    if (reason && typeof reason === 'object') {
      super(reason);
      if (this.range && !(this.range instanceof RangeChange)) this.range = new RangeChange(Array.isArray(this.range) ? this.range : this.range.items, this.range.index ?? this.currentIndex);
    }
    else if (['addRange', 'removeRange', 'clear'].includes(reason)) {
      super({ reason, range: current instanceof RangeChange ? current : new RangeChange(current, currentIndex), currentIndex, previousIndex });
    } else super({ reason, current, previous: previous instanceof Optional ? previous.valueOrDefault : previous, currentIndex, previousIndex });
  }
  get type() { return this.range ? 'range' : 'item'; } get Type() { return this.type; }
  get item() { return this; } get Item() { return this; } get Range() { return this.range; }
}
export const ItemChange = ListChange;
const rangeItems = change => Array.isArray(change.range) ? change.range : change.range?.items ?? [];
const rangeIndex = change => change.range?.index ?? change.currentIndex ?? 0;

/** Array-compatible changes; snapshots are opt-in for sorted/windowed operators. */
export class ChangeSet extends Array {
  constructor(changes = [], kind = 'cache', items) {
    super();
    this.kind = kind;
    if (typeof changes === 'number') this.length = changes;
    else for (const change of changes ?? []) this.push(change instanceof Change ? change : kind === 'list' ? new ListChange(change) : new Change(change));
    // Reserve keys for ordered cache metadata, hiding Array.prototype.keys().
    this.keys = undefined;
    if (items !== undefined) this.items = Array.from(items);
  }
  static get [Symbol.species]() { return Array; }
  push(...changes) {
    for (const change of changes) Array.prototype.push.call(this, change instanceof Change ? change : this.kind === 'list' ? new ListChange(change) : new Change(change));
    return this.length;
  }
  static get empty() { return new ChangeSet(); } static get Empty() { return new ChangeSet(); }
  get size() { return this.length; } get count() { return this.length; } get Count() { return this.length; }
  get adds() { return this.reduce((n, c) => n + (c.reason === 'add' ? 1 : c.reason === 'addRange' ? rangeItems(c).length : 0), 0); }
  get updates() { return this.reduce((n, c) => n + Number(c.reason === 'update' || c.reason === 'replace'), 0); }
  get removes() { return this.reduce((n, c) => n + (c.reason === 'remove' ? 1 : ['removeRange', 'clear'].includes(c.reason) ? rangeItems(c).length : 0), 0); }
  get refreshes() { return this.reduce((n, c) => n + Number(c.reason === 'refresh'), 0); }
  get moves() { return this.reduce((n, c) => n + Number(c.reason === 'move'), 0); }
  get totalChanges() { return this.adds + this.updates + this.removes + this.refreshes + this.moves; }
  get Adds() { return this.adds; } get Updates() { return this.updates; } get Replaced() { return this.updates; }
  get Removes() { return this.removes; } get Refreshes() { return this.refreshes; } get Moves() { return this.moves; }
  get TotalChanges() { return this.totalChanges; } get Size() { return this.size; }
}

export function snapshotChanges(items, kind = 'cache', keySelector = item => item?.id ?? item?.key ?? item) {
  if (items instanceof Map) return new ChangeSet(Array.from(items, ([key, item]) => new Change('add', key, item)), 'cache');
  const values = Array.from(items ?? []);
  return kind === 'list'
    ? new ChangeSet(values.length ? [new ListChange('addRange', values, 0)] : [], 'list')
    : new ChangeSet(values.map(item => new Change('add', keySelector(item), item)), 'cache');
}

/** Applies ordered indexed deltas. A supplied items snapshot is authoritative. */
export function applyChanges(state, changes) {
  if (state instanceof Map) {
    for (const change of changes) {
      switch (change.reason) {
        case 'add': case 'update': case 'replace': state.set(change.key, change.current); break;
        case 'remove': state.delete(change.key); break;
        case 'clear': state.clear(); break;
      }
    }
    if (changes.items && Array.isArray(changes.keys)) {
      state.clear(); changes.items.forEach((item, i) => state.set(changes.keys[i], item));
    }
    return state;
  }
  if (!Array.isArray(state)) throw new TypeError('State must be a Map or Array');
  if (changes.items) { state.length = 0; for (const item of changes.items) state.push(item); return state; }
  for (const change of changes) {
    const index = change.currentIndex ?? -1;
    switch (change.reason) {
      case 'add': state.splice(index < 0 ? state.length : index, 0, change.current); break;
      case 'addRange': { let i = rangeIndex(change); if (i < 0) i = state.length; const values = rangeItems(change); for (let offset = 0; offset < values.length; offset += 8192) state.splice(i + offset, 0, ...values.slice(offset, offset + 8192)); break; }
      case 'remove': { const i = index < 0 ? state.indexOf(change.current) : index; if (i >= 0) state.splice(i, 1); break; }
      case 'removeRange': { const i = rangeIndex(change); if (i >= 0) state.splice(i, rangeItems(change).length); else for (const item of rangeItems(change)) { const j = state.indexOf(item); if (j >= 0) state.splice(j, 1); } break; }
      case 'clear': state.length = 0; break;
      case 'replace': case 'update': {
        const oldIndex = change.previousIndex >= 0 ? change.previousIndex : index >= 0 ? index : state.indexOf(change.previous);
        if (oldIndex >= 0) state.splice(oldIndex, 1);
        state.splice(index < 0 ? (oldIndex < 0 ? state.length : oldIndex) : index, 0, change.current); break;
      }
      case 'move': { const oldIndex = change.previousIndex >= 0 ? change.previousIndex : state.indexOf(change.current); if (oldIndex >= 0) { const [item] = state.splice(oldIndex, 1); state.splice(index, 0, item); } break; }
    }
  }
  return state;
}

function disposable(action) {
  const sub = new Subscription(action);
  sub.dispose = sub.unsubscribe.bind(sub); sub.Dispose = sub.dispose;
  return sub;
}
function equality(comparer) { return typeof comparer === 'function' ? comparer : comparer?.equals?.bind(comparer) ?? comparer?.Equals?.bind(comparer) ?? Object.is; }
function many(value) { return Array.isArray(value) || value instanceof Set || (value != null && typeof value !== 'string' && typeof value[Symbol.iterator] === 'function'); }

function cacheFilter(seed, predicate) {
  const included = new Map([...seed].filter(([key, item]) => predicate(item, key)));
  return { initial: () => snapshotChanges(included), project(changes) {
    const output = new ChangeSet([], 'cache');
    for (const change of changes) {
      const had = included.has(change.key), previous = included.get(change.key);
      const keep = change.reason !== 'remove' && change.reason !== 'clear' && predicate(change.current, change.key);
      if (change.reason === 'clear') { for (const [key, item] of included) output.push(new Change('remove', key, item)); included.clear(); }
      else if (keep) { included.set(change.key, change.current); output.push(new Change(had ? change.reason === 'refresh' ? 'refresh' : 'update' : 'add', change.key, change.current, had ? previous : undefined)); }
      else if (had) { included.delete(change.key); output.push(new Change('remove', change.key, previous)); }
    }
    return output;
  } };
}

function listFilter(seed, predicate) {
  const entries = seed.map(value => ({ value, pass: !!predicate(value) }));
  const filteredIndex = index => { let count = 0; for (let i = 0; i < index; i++) count += +entries[i].pass; return count; };
  return { initial: () => snapshotChanges(entries.filter(e => e.pass).map(e => e.value), 'list'), project(changes) {
    const output = new ChangeSet([], 'list');
    const insert = (value, index) => { const entry = { value, pass: !!predicate(value) }, at = filteredIndex(index); entries.splice(index, 0, entry); if (entry.pass) output.push(new ListChange('add', value, at)); };
    const remove = index => { const at = filteredIndex(index), [entry] = entries.splice(index, 1); if (entry?.pass) output.push(new ListChange('remove', entry.value, at)); };
    for (const change of changes) {
      const i = change.currentIndex >= 0 ? change.currentIndex : entries.findIndex(e => Object.is(e.value, change.current));
      switch (change.reason) {
        case 'add': insert(change.current, change.currentIndex >= 0 ? i : entries.length); break;
        case 'addRange': { let at = rangeIndex(change); if (at < 0) at = entries.length; for (const value of rangeItems(change)) insert(value, at++); break; }
        case 'remove': if (i >= 0) remove(i); break;
        case 'removeRange': { let at = rangeIndex(change); if (at >= 0) for (let j = 0; j < rangeItems(change).length; j++) remove(at); else for (const value of rangeItems(change)) { at = entries.findIndex(e => Object.is(e.value, value)); if (at >= 0) remove(at); } break; }
        case 'clear': { const removed = entries.filter(e => e.pass).map(e => e.value); entries.length = 0; if (removed.length) output.push(new ListChange('clear', removed, 0)); break; }
        case 'move': { const oldIndex = change.previousIndex, from = filteredIndex(oldIndex), [entry] = entries.splice(oldIndex, 1), to = filteredIndex(i); entries.splice(i, 0, entry); if (entry.pass && from !== to) output.push(new ListChange('move', entry.value, to, undefined, from)); break; }
        case 'update': case 'replace': {
          const oldIndex = change.previousIndex >= 0 ? change.previousIndex : change.currentIndex >= 0 ? i : entries.findIndex(e => Object.is(e.value, change.previous));
          const at = filteredIndex(oldIndex), old = entries[oldIndex], pass = !!predicate(change.current);
          if (oldIndex === i || i < 0) {
            entries[oldIndex] = { value: change.current, pass };
            if (old.pass && pass) output.push(new ListChange('replace', change.current, at, old.value, at));
            else if (old.pass) output.push(new ListChange('remove', old.value, at));
            else if (pass) output.push(new ListChange('add', change.current, at));
          } else { remove(oldIndex); insert(change.current, i); }
          break;
        }
        case 'refresh': {
          if (i < 0) break;
          const at = filteredIndex(i), old = entries[i], pass = !!predicate(old.value); entries[i] = { value: old.value, pass };
          if (old.pass && pass) output.push(new ListChange('refresh', old.value, at));
          else if (old.pass) output.push(new ListChange('remove', old.value, at));
          else if (pass) output.push(new ListChange('add', old.value, at)); break;
        }
      }
    }
    return output;
  } };
}

class SourceBase {
  constructor(kind) {
    this.kind = kind; this._data = kind === 'cache' ? new Map() : [];
    this._events = new Subject(); this._previews = new Subject(); this._counts = new Subject();
    this._disposed = false; this._level = 0; this._dispatching = false; this._queue = [];
    this._suspended = 0; this._countSuspended = 0; this._held = []; this._version = 0; this._lastCount = 0;
    this.countChanged = observableDecorator(new Observable(observer => {
      if (this._disposed) { observer.complete(); return; }
      let last = this.count;
      const sub = this._counts.subscribe({ next: count => { if (count !== last) { last = count; observer.next(count); } }, error: e => observer.error(e), complete: () => observer.complete() });
      observer.next(last); return sub;
    }));
  }
  get count() { return this.kind === 'cache' ? this._data.size : this._data.length; }
  get size() { return this.count; } get items() { return this.kind === 'cache' ? Array.from(this._data.values()) : this._data.slice(); }
  get isDisposed() { return this._disposed; }
  get CountChanged() { return this.countChanged; }
  _assert() { if (this._disposed) throw new Error(`${this.constructor.name} is disposed`); }
  connect(predicate, suppressEmptyChangeSets = true) { return this._observe(false, predicate, suppressEmptyChangeSets); }
  preview(predicate) { return this._observe(true, predicate, true); }
  _observe(preview, predicate, suppressEmpty) {
    if (predicate != null && typeof predicate !== 'function') throw new TypeError('Predicate must be a function');
    return observableDecorator(new Observable(observer => {
      if (this._disposed) { observer.complete(); return; }
      let version = this._version - Number(!!this._inPreview), starting = true, draining = false;
      const pending = [];
      let projection;
      try { projection = predicate ? (this.kind === 'cache' ? cacheFilter(this._data, predicate) : listFilter(this._data, predicate)) : null; }
      catch (error) { observer.error(error); return; }
      const receive = event => {
        if (observer.closed) return;
        try {
          const segments = event.segments.filter(segment => segment.version > version);
          if (!segments.length) return;
          version = segments.at(-1).version;
          const raw = segments.length === 1 ? segments[0].changes : new ChangeSet(segments.flatMap(s => Array.from(s.changes)), this.kind);
          const changes = projection ? projection.project(raw) : raw;
          if (changes.length || !suppressEmpty) observer.next(changes);
        } catch (error) { observer.error(error); }
      };
      // Initial snapshot callbacks can mutate and terminate the source before they return.
      // Keep both deltas and terminal events in order, including writes made while draining.
      const drain = () => {
        if (starting || draining) return;
        draining = true;
        try {
          for (let index = 0; index < pending.length && !observer.closed; index++) {
            const notification = pending[index];
            if (notification.kind === 'next') receive(notification.event);
            else if (notification.kind === 'error') observer.error(notification.error);
            else observer.complete();
          }
        } finally { pending.length = 0; draining = false; }
      };
      const enqueue = notification => { if (!observer.closed) { pending.push(notification); drain(); } };
      const stream = preview ? this._previews : this._events;
      if (stream.hasError) { observer.error(stream.thrownError); return; }
      const sub = stream.isStopped ? null : stream.subscribe({
        next: event => enqueue({ kind: 'next', event }),
        error: error => enqueue({ kind: 'error', error }),
        complete: () => enqueue({ kind: 'complete' })
      });
      if (!preview) {
        const initial = projection ? projection.initial() : snapshotChanges(this._data, this.kind, this.keySelector);
        if (initial.length || !suppressEmpty) observer.next(initial);
      }
      starting = false;
      if (!sub) pending.push({ kind: 'complete' });
      drain();
      return sub;
    }));
  }
  edit(action) {
    this._assert(); if (typeof action !== 'function') throw new TypeError('Edit requires an action');
    if (this._dispatching && !this._level) { this._queue.push(() => this.edit(action)); return this; }
    const outer = this._level === 0;
    if (outer) { this._pending = []; this._undo = []; this._before = this._previews.observed ? this.kind === 'cache' ? new Map(this._data) : this._data.slice() : null; }
    this._level++;
    try {
      const result = action(this);
      if (result && typeof result.then === 'function') throw new TypeError('Edit actions must be synchronous');
    } catch (error) {
      this._level--;
      if (outer) { for (let i = this._undo.length - 1; i >= 0; i--) this._undo[i](); this._pending = []; this._undo = []; this._before = null; }
      throw error;
    }
    this._level--;
    if (outer) {
      const changes = new ChangeSet(this._pending, this.kind), before = this._before;
      this._pending = []; this._undo = []; this._before = null;
      if (changes.length) this._publish(changes, before);
    }
    return this;
  }
  _change(change, undo) { this._pending.push(change); this._undo.push(undo); }
  _mutate(action) { if (this._level) { action(); return this; } return this.edit(action); }
  _publish(changes, before) {
    const segment = { changes, version: ++this._version };
    this._dispatching = true;
    try {
      if (this._previews.observed) {
        const after = this._data;
        if (before) this._data = before;
        this._inPreview = true;
        try { this._previews.next({ segments: [segment] }); } finally { this._data = after; this._inPreview = false; }
      }
      if (this._suspended) this._held.push(segment);
      else this._events.next({ segments: [segment] });
      this._emitCount();
    } finally { this._dispatching = false; }
    this._drain();
  }
  _drain() { if (this._draining) return; this._draining = true; try { while (!this._dispatching && this._queue.length && !this._disposed) this._queue.shift()(); } finally { this._draining = false; } }
  _emitCount() {
    if (!this._suspended && !this._countSuspended && this.count !== this._lastCount) { this._lastCount = this.count; this._counts.next(this.count); }
  }
  suspendNotifications() {
    this._assert(); this._suspended++;
    return disposable(() => {
      if (this._disposed || --this._suspended > 0) return;
      const segments = this._held.splice(0);
      this._dispatching = true;
      try { if (segments.length) this._events.next({ segments }); this._emitCount(); }
      finally { this._dispatching = false; }
      this._drain();
    });
  }
  suspendCount() { this._assert(); this._countSuspended++; return disposable(() => { if (!this._disposed && --this._countSuspended === 0) this._emitCount(); }); }
  _error(error) { if (!this._disposed) { this._events.error(error); this._previews.error(error); this._counts.error(error); this.dispose(); } }
  _complete() { if (!this._disposed) { this._events.complete(); this._previews.complete(); this._counts.complete(); } }
  dispose() { if (this._disposed) return; this._disposed = true; this._loader?.unsubscribe(); this._events.complete(); this._previews.complete(); this._counts.complete(); this._queue.length = 0; this._held.length = 0; }
  unsubscribe() { this.dispose(); }
  [Symbol.iterator]() { return this.items[Symbol.iterator](); }
}

/** Keyed mutable change-set source. Keyed edits are O(1); Edit publishes one batch. */
export class SourceCache extends SourceBase {
  constructor(keySelector) { super('cache'); if (typeof keySelector !== 'function') throw new TypeError('SourceCache requires a key selector'); this.keySelector = keySelector; }
  get KeySelector() { return this.keySelector; }
  get keys() { return Array.from(this._data.keys()); }
  get keyValues() { return new Map(this._data); }
  lookup(key) { return this._data.has(key) ? Optional.some(this._data.get(key)) : Optional.none(); }
  getKey(item) { const key = this.keySelector(item); if (key == null) throw new TypeError('Cache keys cannot be null or undefined'); return key; }
  getKeyValues(items) { return Array.from(items, item => [this.getKey(item), item]); }
  _put(item, key, comparer) {
    if (item == null || key == null) throw new TypeError('Cache items and keys cannot be null or undefined');
    const had = this._data.has(key), previous = this._data.get(key);
    if (had && comparer && equality(comparer)(previous, item)) return;
    this._data.set(key, item);
    this._change(new Change(had ? 'update' : 'add', key, item, had ? previous : undefined), () => had ? this._data.set(key, previous) : this._data.delete(key));
  }
  addOrUpdate(itemOrItems, keyOrComparer) {
    return this._mutate(() => {
      const comparer = typeof keyOrComparer === 'function' || keyOrComparer?.equals || keyOrComparer?.Equals ? keyOrComparer : undefined;
      if (itemOrItems instanceof Map) { for (const [key, item] of itemOrItems) this._put(item, key, comparer); }
      else if (many(itemOrItems)) { for (const item of itemOrItems) this._put(item, this.getKey(item), comparer); }
      else this._put(itemOrItems, keyOrComparer !== undefined && !comparer ? keyOrComparer : this.getKey(itemOrItems), comparer);
    });
  }
  addOrUpdatePairs(pairs) { return this._mutate(() => { for (const pair of pairs) { const [key, value] = Array.isArray(pair) ? pair : [pair.key ?? pair.Key, pair.value ?? pair.Value]; this._put(value, key); } }); }
  removeKey(key) { return this._mutate(() => { if (!this._data.has(key)) return; const item = this._data.get(key); this._data.delete(key); this._change(new Change('remove', key, item), () => this._data.set(key, item)); }); }
  removeKeys(keys) { return this._mutate(() => { for (const key of keys) this.removeKey(key); }); }
  remove(itemOrItems) { return this._mutate(() => { for (const item of many(itemOrItems) ? itemOrItems : [itemOrItems]) this.removeKey(this._data.has(item) ? item : this.getKey(item)); }); }
  removeMany(items) { return this.remove(items); }
  removeWhere(predicate) { return this._mutate(() => { for (const [key, item] of this._data) if (predicate(item, key)) this.removeKey(key); }); }
  refreshKey(key) { return this._mutate(() => { if (this._data.has(key)) this._change(new Change('refresh', key, this._data.get(key)), () => {}); }); }
  refreshKeys(keys) { return this._mutate(() => { for (const key of keys) this.refreshKey(key); }); }
  refresh(itemOrItems) { return this._mutate(() => { if (itemOrItems === undefined) return this.refreshKeys(this.keys); for (const item of many(itemOrItems) ? itemOrItems : [itemOrItems]) this.refreshKey(this._data.has(item) ? item : this.getKey(item)); }); }
  clear() { return this._mutate(() => { for (const key of this.keys) this.removeKey(key); }); }
  load(items) { return this._mutate(() => { const values = Array.from(items); this.clear(); this.addOrUpdate(values); }); }
  editDiff(items, comparer = Object.is) { return this._mutate(() => { const incoming = new Map(Array.from(items, item => [this.getKey(item), item])); for (const key of this.keys) if (!incoming.has(key)) this.removeKey(key); for (const [key, item] of incoming) this._put(item, key, comparer); }); }
  clone(changes) { return this._mutate(() => { for (const c of changes) { if (c.reason === 'add' || c.reason === 'update' || c.reason === 'replace') this._put(c.current, c.key); else if (c.reason === 'remove') this.removeKey(c.key); else if (c.reason === 'refresh') this.refreshKey(c.key); else if (c.reason === 'clear') this.clear(); } }); }
  update(changes) { return this.clone(changes); }
  watch(key) { return observableDecorator(new Observable(observer => this.connect((_, candidate) => Object.is(candidate, key)).subscribe({ next: changes => { for (const change of changes) observer.next(change); }, error: e => observer.error(e), complete: () => observer.complete() }))); }
  asObservableCache() { return new ObservableCache(this.connect(), this.keySelector); }
}

/** Ordered list source, including duplicate values and range/indexed deltas. */
export class SourceList extends SourceBase {
  constructor(source) {
    super('list');
    if (source?.subscribe) this._loader = source.subscribe({ next: changes => this.clone(changes), error: e => this._error(e), complete: () => this._complete() });
    else if (source != null) this.addRange(source);
  }
  _index(index, allowEnd = false) { if (!Number.isInteger(index) || index < 0 || index >= this.count + Number(allowEnd)) throw new RangeError(`Index ${index} outside list of length ${this.count}`); }
  get(index) { this._index(index); return this._data[index]; }
  at(index) { return this._data.at(index); }
  indexOf(item) { return this._data.indexOf(item); }
  contains(item) { return this._data.includes(item); }
  add(item) { return this._mutate(() => this.insert(this.count, item)); }
  addRange(items) { return this._mutate(() => this.insertRange(items, this.count)); }
  insert(index, item) { return this._mutate(() => { this._index(index, true); if (item == null) throw new TypeError('List items cannot be null or undefined'); this._data.splice(index, 0, item); this._change(new ListChange('add', item, index), () => this._data.splice(index, 1)); }); }
  insertRange(items, index) {
    if (typeof items === 'number') [items, index] = [index, items];
    return this._mutate(() => {
      this._index(index, true); const values = Array.from(items); if (values.some(item => item == null)) throw new TypeError('List items cannot be null or undefined'); if (!values.length) return;
      // Chunk insertion avoids engine argument limits on large imports.
      for (let offset = 0; offset < values.length; offset += 8192) this._data.splice(index + offset, 0, ...values.slice(offset, offset + 8192));
      this._change(new ListChange('addRange', values, index), () => this._data.splice(index, values.length));
    });
  }
  removeAt(index) { return this._mutate(() => { this._index(index); const [item] = this._data.splice(index, 1); this._change(new ListChange('remove', item, index), () => this._data.splice(index, 0, item)); }); }
  remove(item) { this._assert(); const index = this.indexOf(item); if (index < 0) return false; this.removeAt(index); return true; }
  removeMany(items) { return this._mutate(() => { for (const item of Array.from(items)) this.remove(item); }); }
  removeWhere(predicate) { return this._mutate(() => { for (let index = this.count - 1; index >= 0; index--) if (predicate(this._data[index], index)) this.removeAt(index); }); }
  removeRange(index, count) { return this._mutate(() => { this._index(index, true); if (!Number.isInteger(count) || count < 0 || index + count > this.count) throw new RangeError('Invalid removal range'); if (!count) return; const values = this._data.splice(index, count); this._change(new ListChange('removeRange', values, index), () => { for (let offset = 0; offset < values.length; offset += 8192) this._data.splice(index + offset, 0, ...values.slice(offset, offset + 8192)); }); }); }
  clear() { return this._mutate(() => { if (!this.count) return; const values = this._data.splice(0); this._change(new ListChange('clear', values, 0), () => { for (const item of values) this._data.push(item); }); }); }
  replaceAt(index, item) { return this._mutate(() => { this._index(index); if (item == null) throw new TypeError('List items cannot be null or undefined'); const previous = this._data[index]; this._data[index] = item; this._change(new ListChange('replace', item, index, previous, index), () => { this._data[index] = previous; }); }); }
  set(index, item) { return this.replaceAt(index, item); }
  replace(original, replacement) { const index = this.indexOf(original); if (index < 0) throw new Error('Original item is not in the list'); return this.replaceAt(index, replacement); }
  move(previousIndex, currentIndex) { return this._mutate(() => { this._index(previousIndex); this._index(currentIndex); if (previousIndex === currentIndex) return; const [item] = this._data.splice(previousIndex, 1); this._data.splice(currentIndex, 0, item); this._change(new ListChange('move', item, currentIndex, undefined, previousIndex), () => { this._data.splice(currentIndex, 1); this._data.splice(previousIndex, 0, item); }); }); }
  refreshAt(index) { return this._mutate(() => { this._index(index); this._change(new ListChange('refresh', this._data[index], index), () => {}); }); }
  refresh(item) { return this._mutate(() => { if (arguments.length === 0) for (let index = 0; index < this.count; index++) this.refreshAt(index); else for (let index = 0; index < this.count; index++) if (Object.is(this._data[index], item)) this.refreshAt(index); }); }
  refreshMany(items) { return this._mutate(() => { const set = new Set(items); for (let index = 0; index < this.count; index++) if (set.has(this._data[index])) this.refreshAt(index); }); }
  load(items) { return this._mutate(() => { const values = Array.from(items); this.clear(); this.addRange(values); }); }
  editDiff(items, comparer = Object.is) {
    const values = Array.from(items), equals = equality(comparer);
    return this._mutate(() => {
      for (let index = 0; index < values.length; index++) {
        if (index < this.count && equals(this._data[index], values[index])) continue;
        let found = -1;
        for (let candidate = index + 1; candidate < this.count; candidate++) if (equals(this._data[candidate], values[index])) { found = candidate; break; }
        if (found >= 0) this.move(found, index); else this.insert(index, values[index]);
      }
      if (this.count > values.length) this.removeRange(values.length, this.count - values.length);
    });
  }
  clone(changes) { return this._mutate(() => {
    if (changes.items) { this.editDiff(changes.items); return; }
    for (const c of changes) {
      const i = c.currentIndex ?? -1;
      switch (c.reason) {
        case 'add': this.insert(i < 0 ? this.count : i, c.current); break;
        case 'addRange': this.insertRange(rangeItems(c), rangeIndex(c) < 0 ? this.count : rangeIndex(c)); break;
        case 'remove': if (i >= 0) this.removeAt(i); else this.remove(c.current); break;
        case 'removeRange': if (rangeIndex(c) >= 0) this.removeRange(rangeIndex(c), rangeItems(c).length); else this.removeMany(rangeItems(c)); break;
        case 'replace': case 'update': if (c.previousIndex >= 0 && i >= 0 && c.previousIndex !== i) { this.removeAt(c.previousIndex); this.insert(i, c.current); } else this.replaceAt(i >= 0 ? i : this.indexOf(c.previous), c.current); break;
        case 'clear': this.clear(); break;
        case 'move': this.move(c.previousIndex, i); break;
        case 'refresh': if (i >= 0) this.refreshAt(i); else this.refresh(c.current); break;
      }
    }
  }); }
  asObservableList() { return new ObservableList(this.connect()); }
}

export class ObservableCache {
  constructor(source, keySelector = item => item?.id ?? item?.key ?? item) {
    if (!source?.subscribe && !source?.connect) throw new TypeError('ObservableCache requires a change-set Observable');
    this._source = new SourceCache(keySelector); this.keySelector = keySelector;
    this._subscription = (source.connect ? source.connect() : source).subscribe({ next: changes => this._source.clone(changes), error: error => this._source._error(error), complete: () => this._source._complete() });
  }
  get count() { return this._source.count; } get size() { return this.count; } get items() { return this._source.items; }
  get keys() { return this._source.keys; } get keyValues() { return this._source.keyValues; }
  get countChanged() { return this._source.countChanged; } get isDisposed() { return this._source.isDisposed; }
  connect(...args) { return this._source.connect(...args); } preview(...args) { return this._source.preview(...args); }
  watch(key) { return this._source.watch(key); } lookup(key) { return this._source.lookup(key); }
  dispose() { this._subscription.unsubscribe(); this._source.dispose(); } unsubscribe() { this.dispose(); }
  [Symbol.iterator]() { return this.items[Symbol.iterator](); }
}
export class ObservableList {
  constructor(source) { this._source = new SourceList(source?.connect ? source.connect() : source); }
  get count() { return this._source.count; } get size() { return this.count; } get items() { return this._source.items; }
  get countChanged() { return this._source.countChanged; } get isDisposed() { return this._source.isDisposed; }
  connect(...args) { return this._source.connect(...args); } preview(...args) { return this._source.preview(...args); }
  get(index) { return this._source.get(index); } at(index) { return this._source.at(index); }
  indexOf(item) { return this._source.indexOf(item); } contains(item) { return this._source.contains(item); }
  dispose() { this._source.dispose(); } unsubscribe() { this.dispose(); }
  [Symbol.iterator]() { return this.items[Symbol.iterator](); }
}

/** Editable cache for pipelines which already carry explicit keys. */
export class IntermediateCache extends SourceCache {
  constructor(source) {
    super(item => item?.id ?? item?.key ?? item);
    if (source) this._loader = (source.connect ? source.connect() : source).subscribe({ next: changes => this.clone(changes), error: error => this._error(error), complete: () => this._complete() });
  }
}

/** A mutable cache with an explicit capture-and-clear change log. */
export class ChangeAwareCache extends SourceCache {
  constructor(data, keySelector = item => item?.id ?? item?.key ?? item) {
    super(typeof data === 'function' ? data : keySelector);
    this._captured = new ChangeSet([], 'cache');
    if (data instanceof Map) this._data = new Map(data);
  }
  _publish(changes, before) { for (const change of changes) this._captured.push(change); super._publish(changes, before); }
  captureChanges() { const changes = this._captured; this._captured = new ChangeSet([], 'cache'); return changes; }
  add(item, key = this.getKey(item)) { return this._mutate(() => { if (this._data.has(key)) throw new Error(`Key ${String(key)} already exists`); this._put(item, key); }); }
}

/** A list which captures deltas until captureChanges() consumes its log. */
export class ChangeAwareList extends SourceList {
  constructor(items, copyChanges = false) {
    super(); this._captured = new ChangeSet([], 'list');
    if (items instanceof ChangeAwareList) { this._data = items.items; if (copyChanges) this._captured = new ChangeSet(items._captured, 'list'); }
    else if (items != null && typeof items !== 'number') this.addRange(items);
  }
  get isReadOnly() { return false; }
  _publish(changes, before) { for (const change of changes) this._captured.push(change); super._publish(changes, before); }
  captureChanges() { const changes = this._captured; this._captured = new ChangeSet([], 'list'); return changes; }
  copyTo(array, arrayIndex = 0) { if (!Array.isArray(array) || !Number.isInteger(arrayIndex) || arrayIndex < 0) throw new TypeError('CopyTo requires an array and a nonnegative index'); this._data.forEach((item, i) => { array[arrayIndex + i] = item; }); }
}

// Runtime aliases preserve familiar .NET names while camelCase remains idiomatic JS.
for (const Class of [Optional, Change, ListChange, RangeChange, ChangeSet, SourceBase, SourceCache, SourceList, ObservableCache, ObservableList, IntermediateCache, ChangeAwareCache, ChangeAwareList]) {
  for (const name of Object.getOwnPropertyNames(Class.prototype)) {
    if (name === 'constructor' || name.startsWith('_') || name[0] === name[0].toUpperCase()) continue;
    const pascal = name[0].toUpperCase() + name.slice(1);
    if (!(pascal in Class.prototype)) Object.defineProperty(Class.prototype, pascal, Object.getOwnPropertyDescriptor(Class.prototype, name));
  }
}
