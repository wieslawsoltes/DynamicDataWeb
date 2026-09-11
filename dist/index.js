// src/core.js
import { Observable, Subject, Subscription } from "rxjs";
var observableDecorator = (observable2) => observable2;
function setObservableDecorator(decorator) {
  if (typeof decorator !== "function") throw new TypeError("Observable decorator must be a function");
  observableDecorator = decorator;
}
var Optional = class _Optional {
  constructor(value, hasValue = arguments.length > 0) {
    this._value = value;
    this.hasValue = !!hasValue;
  }
  static some(value) {
    return new _Optional(value, true);
  }
  static none() {
    return new _Optional(void 0, false);
  }
  static of(value) {
    return value == null ? _Optional.none() : _Optional.some(value);
  }
  get value() {
    if (!this.hasValue) throw new Error("Optional has no value");
    return this._value;
  }
  get valueOrDefault() {
    return this._value;
  }
  get HasValue() {
    return this.hasValue;
  }
  get Value() {
    return this.value;
  }
  get ValueOrDefault() {
    return this.valueOrDefault;
  }
  getOrElse(fallback) {
    return this.hasValue ? this._value : typeof fallback === "function" ? fallback() : fallback;
  }
  map(project) {
    return this.hasValue ? _Optional.some(project(this._value)) : _Optional.none();
  }
  ifHasValue(action) {
    if (this.hasValue) action(this._value);
    return this;
  }
  equals(other) {
    return other instanceof _Optional && other.hasValue === this.hasValue && (!this.hasValue || Object.is(this._value, other._value));
  }
  toString() {
    return this.hasValue ? `Some(${String(this._value)})` : "None";
  }
};
Optional.Some = Optional.some;
Optional.None = Optional.none;
Optional.Of = Optional.of;
Optional.create = Optional.of;
Optional.Create = Optional.of;
Optional.toOptional = Optional.of;
Optional.ToOptional = Optional.of;
Optional.fromOptional = (optional2) => optional2.value;
Optional.FromOptional = Optional.fromOptional;
function reasons(names) {
  const result = {};
  for (const [name, value] of Object.entries(names)) {
    result[name] = value;
    result[name.toUpperCase()] = value;
    result[value] = value;
  }
  return Object.freeze(result);
}
var ChangeReason = reasons({ Add: "add", Update: "update", Remove: "remove", Refresh: "refresh", Moved: "move", Move: "move" });
var ListChangeReason = reasons({ Add: "add", AddRange: "addRange", Replace: "replace", Remove: "remove", RemoveRange: "removeRange", Refresh: "refresh", Moved: "move", Move: "move", Clear: "clear" });
var ChangeType = Object.freeze({ Item: "item", Range: "range" });
var Change = class {
  constructor(reason2, key, current, previous, currentIndex = -1, previousIndex = -1) {
    if (reason2 && typeof reason2 === "object") Object.assign(this, reason2);
    else Object.assign(this, { reason: reason2, key, current, previous: previous instanceof Optional ? previous.valueOrDefault : previous, currentIndex, previousIndex });
    this.currentIndex ??= -1;
    this.previousIndex ??= -1;
  }
  get Reason() {
    return this.reason;
  }
  get Key() {
    return this.key;
  }
  get Current() {
    return this.current;
  }
  get Previous() {
    return this.reason === "update" || this.reason === "replace" ? Optional.some(this.previous) : Optional.none();
  }
  get CurrentIndex() {
    return this.currentIndex;
  }
  get PreviousIndex() {
    return this.previousIndex;
  }
  equals(other) {
    return !!other && ["reason", "key", "current", "previous", "currentIndex", "previousIndex"].every((key) => Object.is(this[key], other[key]));
  }
  toString() {
    return `${this.reason}, Key: ${String(this.key)}, Current: ${String(this.current)}`;
  }
};
var RangeChange = class {
  constructor(items = [], index = -1) {
    this.items = Array.from(items);
    this.index = index;
  }
  get count() {
    return this.items.length;
  }
  get Count() {
    return this.count;
  }
  get Index() {
    return this.index;
  }
  [Symbol.iterator]() {
    return this.items[Symbol.iterator]();
  }
};
var ListChange = class extends Change {
  constructor(reason2, current, currentIndex = -1, previous, previousIndex = -1) {
    if (reason2 && typeof reason2 === "object") {
      super(reason2);
      if (this.range && !(this.range instanceof RangeChange)) this.range = new RangeChange(Array.isArray(this.range) ? this.range : this.range.items, this.range.index ?? this.currentIndex);
    } else if (["addRange", "removeRange", "clear"].includes(reason2)) {
      super({ reason: reason2, range: current instanceof RangeChange ? current : new RangeChange(current, currentIndex), currentIndex, previousIndex });
    } else super({ reason: reason2, current, previous: previous instanceof Optional ? previous.valueOrDefault : previous, currentIndex, previousIndex });
  }
  get type() {
    return this.range ? "range" : "item";
  }
  get Type() {
    return this.type;
  }
  get item() {
    return this;
  }
  get Item() {
    return this;
  }
  get Range() {
    return this.range;
  }
};
var ItemChange = ListChange;
var rangeItems = (change) => Array.isArray(change.range) ? change.range : change.range?.items ?? [];
var rangeIndex = (change) => change.range?.index ?? change.currentIndex ?? 0;
var ChangeSet = class _ChangeSet extends Array {
  constructor(changes = [], kind = "cache", items) {
    super();
    this.kind = kind;
    if (typeof changes === "number") this.length = changes;
    else for (const change of changes ?? []) this.push(change instanceof Change ? change : kind === "list" ? new ListChange(change) : new Change(change));
    this.keys = void 0;
    if (items !== void 0) this.items = Array.from(items);
  }
  static get [Symbol.species]() {
    return Array;
  }
  push(...changes) {
    for (const change of changes) Array.prototype.push.call(this, change instanceof Change ? change : this.kind === "list" ? new ListChange(change) : new Change(change));
    return this.length;
  }
  static get empty() {
    return new _ChangeSet();
  }
  static get Empty() {
    return new _ChangeSet();
  }
  get size() {
    return this.length;
  }
  get count() {
    return this.length;
  }
  get Count() {
    return this.length;
  }
  get adds() {
    return this.reduce((n, c) => n + (c.reason === "add" ? 1 : c.reason === "addRange" ? rangeItems(c).length : 0), 0);
  }
  get updates() {
    return this.reduce((n, c) => n + Number(c.reason === "update" || c.reason === "replace"), 0);
  }
  get removes() {
    return this.reduce((n, c) => n + (c.reason === "remove" ? 1 : ["removeRange", "clear"].includes(c.reason) ? rangeItems(c).length : 0), 0);
  }
  get refreshes() {
    return this.reduce((n, c) => n + Number(c.reason === "refresh"), 0);
  }
  get moves() {
    return this.reduce((n, c) => n + Number(c.reason === "move"), 0);
  }
  get totalChanges() {
    return this.adds + this.updates + this.removes + this.refreshes + this.moves;
  }
  get Adds() {
    return this.adds;
  }
  get Updates() {
    return this.updates;
  }
  get Replaced() {
    return this.updates;
  }
  get Removes() {
    return this.removes;
  }
  get Refreshes() {
    return this.refreshes;
  }
  get Moves() {
    return this.moves;
  }
  get TotalChanges() {
    return this.totalChanges;
  }
  get Size() {
    return this.size;
  }
};
function snapshotChanges(items, kind = "cache", keySelector = (item) => item?.id ?? item?.key ?? item) {
  if (items instanceof Map) return new ChangeSet(Array.from(items, ([key, item]) => new Change("add", key, item)), "cache");
  const values = Array.from(items ?? []);
  return kind === "list" ? new ChangeSet(values.length ? [new ListChange("addRange", values, 0)] : [], "list") : new ChangeSet(values.map((item) => new Change("add", keySelector(item), item)), "cache");
}
function applyChanges(state, changes) {
  if (state instanceof Map) {
    for (const change of changes) {
      switch (change.reason) {
        case "add":
        case "update":
        case "replace":
          state.set(change.key, change.current);
          break;
        case "remove":
          state.delete(change.key);
          break;
        case "clear":
          state.clear();
          break;
      }
    }
    if (changes.items && Array.isArray(changes.keys)) {
      state.clear();
      changes.items.forEach((item, i) => state.set(changes.keys[i], item));
    }
    return state;
  }
  if (!Array.isArray(state)) throw new TypeError("State must be a Map or Array");
  if (changes.items) {
    state.length = 0;
    for (const item of changes.items) state.push(item);
    return state;
  }
  for (const change of changes) {
    const index = change.currentIndex ?? -1;
    switch (change.reason) {
      case "add":
        state.splice(index < 0 ? state.length : index, 0, change.current);
        break;
      case "addRange": {
        let i = rangeIndex(change);
        if (i < 0) i = state.length;
        const values = rangeItems(change);
        for (let offset = 0; offset < values.length; offset += 8192) state.splice(i + offset, 0, ...values.slice(offset, offset + 8192));
        break;
      }
      case "remove": {
        const i = index < 0 ? state.indexOf(change.current) : index;
        if (i >= 0) state.splice(i, 1);
        break;
      }
      case "removeRange": {
        const i = rangeIndex(change);
        if (i >= 0) state.splice(i, rangeItems(change).length);
        else for (const item of rangeItems(change)) {
          const j = state.indexOf(item);
          if (j >= 0) state.splice(j, 1);
        }
        break;
      }
      case "clear":
        state.length = 0;
        break;
      case "replace":
      case "update": {
        const oldIndex = change.previousIndex >= 0 ? change.previousIndex : index >= 0 ? index : state.indexOf(change.previous);
        if (oldIndex >= 0) state.splice(oldIndex, 1);
        state.splice(index < 0 ? oldIndex < 0 ? state.length : oldIndex : index, 0, change.current);
        break;
      }
      case "move": {
        const oldIndex = change.previousIndex >= 0 ? change.previousIndex : state.indexOf(change.current);
        if (oldIndex >= 0) {
          const [item] = state.splice(oldIndex, 1);
          state.splice(index, 0, item);
        }
        break;
      }
    }
  }
  return state;
}
function disposable(action) {
  const sub = new Subscription(action);
  sub.dispose = sub.unsubscribe.bind(sub);
  sub.Dispose = sub.dispose;
  return sub;
}
function equality(comparer) {
  return typeof comparer === "function" ? comparer : comparer?.equals?.bind(comparer) ?? comparer?.Equals?.bind(comparer) ?? Object.is;
}
function many(value) {
  return Array.isArray(value) || value instanceof Set || value != null && typeof value !== "string" && typeof value[Symbol.iterator] === "function";
}
function cacheFilter(seed, predicate) {
  const included = new Map([...seed].filter(([key, item]) => predicate(item, key)));
  return { initial: () => snapshotChanges(included), project(changes) {
    const output = new ChangeSet([], "cache");
    for (const change of changes) {
      const had = included.has(change.key), previous = included.get(change.key);
      const keep = change.reason !== "remove" && change.reason !== "clear" && predicate(change.current, change.key);
      if (change.reason === "clear") {
        for (const [key, item] of included) output.push(new Change("remove", key, item));
        included.clear();
      } else if (keep) {
        included.set(change.key, change.current);
        output.push(new Change(had ? change.reason === "refresh" ? "refresh" : "update" : "add", change.key, change.current, had ? previous : void 0));
      } else if (had) {
        included.delete(change.key);
        output.push(new Change("remove", change.key, previous));
      }
    }
    return output;
  } };
}
function listFilter(seed, predicate) {
  const entries = seed.map((value) => ({ value, pass: !!predicate(value) }));
  const filteredIndex = (index) => {
    let count2 = 0;
    for (let i = 0; i < index; i++) count2 += +entries[i].pass;
    return count2;
  };
  return { initial: () => snapshotChanges(entries.filter((e) => e.pass).map((e) => e.value), "list"), project(changes) {
    const output = new ChangeSet([], "list");
    const insert = (value, index) => {
      const entry = { value, pass: !!predicate(value) }, at = filteredIndex(index);
      entries.splice(index, 0, entry);
      if (entry.pass) output.push(new ListChange("add", value, at));
    };
    const remove2 = (index) => {
      const at = filteredIndex(index), [entry] = entries.splice(index, 1);
      if (entry?.pass) output.push(new ListChange("remove", entry.value, at));
    };
    for (const change of changes) {
      const i = change.currentIndex >= 0 ? change.currentIndex : entries.findIndex((e) => Object.is(e.value, change.current));
      switch (change.reason) {
        case "add":
          insert(change.current, change.currentIndex >= 0 ? i : entries.length);
          break;
        case "addRange": {
          let at = rangeIndex(change);
          if (at < 0) at = entries.length;
          for (const value of rangeItems(change)) insert(value, at++);
          break;
        }
        case "remove":
          if (i >= 0) remove2(i);
          break;
        case "removeRange": {
          let at = rangeIndex(change);
          if (at >= 0) for (let j = 0; j < rangeItems(change).length; j++) remove2(at);
          else for (const value of rangeItems(change)) {
            at = entries.findIndex((e) => Object.is(e.value, value));
            if (at >= 0) remove2(at);
          }
          break;
        }
        case "clear": {
          const removed = entries.filter((e) => e.pass).map((e) => e.value);
          entries.length = 0;
          if (removed.length) output.push(new ListChange("clear", removed, 0));
          break;
        }
        case "move": {
          const oldIndex = change.previousIndex, from3 = filteredIndex(oldIndex), [entry] = entries.splice(oldIndex, 1), to = filteredIndex(i);
          entries.splice(i, 0, entry);
          if (entry.pass && from3 !== to) output.push(new ListChange("move", entry.value, to, void 0, from3));
          break;
        }
        case "update":
        case "replace": {
          const oldIndex = change.previousIndex >= 0 ? change.previousIndex : change.currentIndex >= 0 ? i : entries.findIndex((e) => Object.is(e.value, change.previous));
          const at = filteredIndex(oldIndex), old = entries[oldIndex], pass = !!predicate(change.current);
          if (oldIndex === i || i < 0) {
            entries[oldIndex] = { value: change.current, pass };
            if (old.pass && pass) output.push(new ListChange("replace", change.current, at, old.value, at));
            else if (old.pass) output.push(new ListChange("remove", old.value, at));
            else if (pass) output.push(new ListChange("add", change.current, at));
          } else {
            remove2(oldIndex);
            insert(change.current, i);
          }
          break;
        }
        case "refresh": {
          if (i < 0) break;
          const at = filteredIndex(i), old = entries[i], pass = !!predicate(old.value);
          entries[i] = { value: old.value, pass };
          if (old.pass && pass) output.push(new ListChange("refresh", old.value, at));
          else if (old.pass) output.push(new ListChange("remove", old.value, at));
          else if (pass) output.push(new ListChange("add", old.value, at));
          break;
        }
      }
    }
    return output;
  } };
}
var SourceBase = class {
  constructor(kind) {
    this.kind = kind;
    this._data = kind === "cache" ? /* @__PURE__ */ new Map() : [];
    this._events = new Subject();
    this._previews = new Subject();
    this._counts = new Subject();
    this._disposed = false;
    this._level = 0;
    this._dispatching = false;
    this._queue = [];
    this._suspended = 0;
    this._countSuspended = 0;
    this._held = [];
    this._version = 0;
    this._lastCount = 0;
    this.countChanged = observableDecorator(new Observable((observer) => {
      if (this._disposed) {
        observer.complete();
        return;
      }
      let last = this.count;
      const sub = this._counts.subscribe({ next: (count2) => {
        if (count2 !== last) {
          last = count2;
          observer.next(count2);
        }
      }, error: (e) => observer.error(e), complete: () => observer.complete() });
      observer.next(last);
      return sub;
    }));
  }
  get count() {
    return this.kind === "cache" ? this._data.size : this._data.length;
  }
  get size() {
    return this.count;
  }
  get items() {
    return this.kind === "cache" ? Array.from(this._data.values()) : this._data.slice();
  }
  get isDisposed() {
    return this._disposed;
  }
  get CountChanged() {
    return this.countChanged;
  }
  _assert() {
    if (this._disposed) throw new Error(`${this.constructor.name} is disposed`);
  }
  connect(predicate, suppressEmptyChangeSets = true) {
    return this._observe(false, predicate, suppressEmptyChangeSets);
  }
  preview(predicate) {
    return this._observe(true, predicate, true);
  }
  _observe(preview, predicate, suppressEmpty) {
    if (predicate != null && typeof predicate !== "function") throw new TypeError("Predicate must be a function");
    return observableDecorator(new Observable((observer) => {
      if (this._disposed) {
        observer.complete();
        return;
      }
      let version = this._version - Number(!!this._inPreview), starting = true;
      const pending = [];
      let projection;
      try {
        projection = predicate ? this.kind === "cache" ? cacheFilter(this._data, predicate) : listFilter(this._data, predicate) : null;
      } catch (error) {
        observer.error(error);
        return;
      }
      const receive = (event) => {
        if (starting) {
          pending.push(event);
          return;
        }
        if (observer.closed) return;
        try {
          const segments = event.segments.filter((segment) => segment.version > version);
          if (!segments.length) return;
          version = segments.at(-1).version;
          const raw = segments.length === 1 ? segments[0].changes : new ChangeSet(segments.flatMap((s) => Array.from(s.changes)), this.kind);
          const changes = projection ? projection.project(raw) : raw;
          if (changes.length || !suppressEmpty) observer.next(changes);
        } catch (error) {
          observer.error(error);
        }
      };
      const stream = preview ? this._previews : this._events;
      if (stream.hasError) {
        observer.error(stream.thrownError);
        return;
      }
      const sub = stream.isStopped ? null : stream.subscribe({ next: receive, error: (e) => observer.error(e), complete: () => observer.complete() });
      if (!preview) {
        const initial = projection ? projection.initial() : snapshotChanges(this._data, this.kind, this.keySelector);
        if (initial.length || !suppressEmpty) observer.next(initial);
      }
      starting = false;
      for (const event of pending) receive(event);
      if (stream.isStopped) observer.complete();
      return sub;
    }));
  }
  edit(action) {
    this._assert();
    if (typeof action !== "function") throw new TypeError("Edit requires an action");
    if (this._dispatching && !this._level) {
      this._queue.push(() => this.edit(action));
      return this;
    }
    const outer = this._level === 0;
    if (outer) {
      this._pending = [];
      this._undo = [];
      this._before = this._previews.observed ? this.kind === "cache" ? new Map(this._data) : this._data.slice() : null;
    }
    this._level++;
    try {
      const result = action(this);
      if (result && typeof result.then === "function") throw new TypeError("Edit actions must be synchronous");
    } catch (error) {
      this._level--;
      if (outer) {
        for (let i = this._undo.length - 1; i >= 0; i--) this._undo[i]();
        this._pending = [];
        this._undo = [];
        this._before = null;
      }
      throw error;
    }
    this._level--;
    if (outer) {
      const changes = new ChangeSet(this._pending, this.kind), before = this._before;
      this._pending = [];
      this._undo = [];
      this._before = null;
      if (changes.length) this._publish(changes, before);
    }
    return this;
  }
  _change(change, undo) {
    this._pending.push(change);
    this._undo.push(undo);
  }
  _mutate(action) {
    if (this._level) {
      action();
      return this;
    }
    return this.edit(action);
  }
  _publish(changes, before) {
    const segment = { changes, version: ++this._version };
    this._dispatching = true;
    try {
      if (this._previews.observed) {
        const after = this._data;
        if (before) this._data = before;
        this._inPreview = true;
        try {
          this._previews.next({ segments: [segment] });
        } finally {
          this._data = after;
          this._inPreview = false;
        }
      }
      if (this._suspended) this._held.push(segment);
      else this._events.next({ segments: [segment] });
      this._emitCount();
    } finally {
      this._dispatching = false;
    }
    this._drain();
  }
  _drain() {
    if (this._draining) return;
    this._draining = true;
    try {
      while (!this._dispatching && this._queue.length && !this._disposed) this._queue.shift()();
    } finally {
      this._draining = false;
    }
  }
  _emitCount() {
    if (!this._suspended && !this._countSuspended && this.count !== this._lastCount) {
      this._lastCount = this.count;
      this._counts.next(this.count);
    }
  }
  suspendNotifications() {
    this._assert();
    this._suspended++;
    return disposable(() => {
      if (this._disposed || --this._suspended > 0) return;
      const segments = this._held.splice(0);
      this._dispatching = true;
      try {
        if (segments.length) this._events.next({ segments });
        this._emitCount();
      } finally {
        this._dispatching = false;
      }
      this._drain();
    });
  }
  suspendCount() {
    this._assert();
    this._countSuspended++;
    return disposable(() => {
      if (!this._disposed && --this._countSuspended === 0) this._emitCount();
    });
  }
  _error(error) {
    if (!this._disposed) {
      this._events.error(error);
      this._previews.error(error);
      this._counts.error(error);
      this.dispose();
    }
  }
  _complete() {
    if (!this._disposed) {
      this._events.complete();
      this._previews.complete();
      this._counts.complete();
    }
  }
  dispose() {
    if (this._disposed) return;
    this._disposed = true;
    this._loader?.unsubscribe();
    this._events.complete();
    this._previews.complete();
    this._counts.complete();
    this._queue.length = 0;
    this._held.length = 0;
  }
  unsubscribe() {
    this.dispose();
  }
  [Symbol.iterator]() {
    return this.items[Symbol.iterator]();
  }
};
var SourceCache = class extends SourceBase {
  constructor(keySelector) {
    super("cache");
    if (typeof keySelector !== "function") throw new TypeError("SourceCache requires a key selector");
    this.keySelector = keySelector;
  }
  get KeySelector() {
    return this.keySelector;
  }
  get keys() {
    return Array.from(this._data.keys());
  }
  get keyValues() {
    return new Map(this._data);
  }
  lookup(key) {
    return this._data.has(key) ? Optional.some(this._data.get(key)) : Optional.none();
  }
  getKey(item) {
    const key = this.keySelector(item);
    if (key == null) throw new TypeError("Cache keys cannot be null or undefined");
    return key;
  }
  getKeyValues(items) {
    return Array.from(items, (item) => [this.getKey(item), item]);
  }
  _put(item, key, comparer) {
    if (item == null || key == null) throw new TypeError("Cache items and keys cannot be null or undefined");
    const had = this._data.has(key), previous = this._data.get(key);
    if (had && comparer && equality(comparer)(previous, item)) return;
    this._data.set(key, item);
    this._change(new Change(had ? "update" : "add", key, item, had ? previous : void 0), () => had ? this._data.set(key, previous) : this._data.delete(key));
  }
  addOrUpdate(itemOrItems, keyOrComparer) {
    return this._mutate(() => {
      const comparer = typeof keyOrComparer === "function" || keyOrComparer?.equals || keyOrComparer?.Equals ? keyOrComparer : void 0;
      if (itemOrItems instanceof Map) {
        for (const [key, item] of itemOrItems) this._put(item, key, comparer);
      } else if (many(itemOrItems)) {
        for (const item of itemOrItems) this._put(item, this.getKey(item), comparer);
      } else this._put(itemOrItems, keyOrComparer !== void 0 && !comparer ? keyOrComparer : this.getKey(itemOrItems), comparer);
    });
  }
  addOrUpdatePairs(pairs) {
    return this._mutate(() => {
      for (const pair of pairs) {
        const [key, value] = Array.isArray(pair) ? pair : [pair.key ?? pair.Key, pair.value ?? pair.Value];
        this._put(value, key);
      }
    });
  }
  removeKey(key) {
    return this._mutate(() => {
      if (!this._data.has(key)) return;
      const item = this._data.get(key);
      this._data.delete(key);
      this._change(new Change("remove", key, item), () => this._data.set(key, item));
    });
  }
  removeKeys(keys) {
    return this._mutate(() => {
      for (const key of keys) this.removeKey(key);
    });
  }
  remove(itemOrItems) {
    return this._mutate(() => {
      for (const item of many(itemOrItems) ? itemOrItems : [itemOrItems]) this.removeKey(this._data.has(item) ? item : this.getKey(item));
    });
  }
  removeMany(items) {
    return this.remove(items);
  }
  removeWhere(predicate) {
    return this._mutate(() => {
      for (const [key, item] of this._data) if (predicate(item, key)) this.removeKey(key);
    });
  }
  refreshKey(key) {
    return this._mutate(() => {
      if (this._data.has(key)) this._change(new Change("refresh", key, this._data.get(key)), () => {
      });
    });
  }
  refreshKeys(keys) {
    return this._mutate(() => {
      for (const key of keys) this.refreshKey(key);
    });
  }
  refresh(itemOrItems) {
    return this._mutate(() => {
      if (itemOrItems === void 0) return this.refreshKeys(this.keys);
      for (const item of many(itemOrItems) ? itemOrItems : [itemOrItems]) this.refreshKey(this._data.has(item) ? item : this.getKey(item));
    });
  }
  clear() {
    return this._mutate(() => {
      for (const key of this.keys) this.removeKey(key);
    });
  }
  load(items) {
    return this._mutate(() => {
      const values = Array.from(items);
      this.clear();
      this.addOrUpdate(values);
    });
  }
  editDiff(items, comparer = Object.is) {
    return this._mutate(() => {
      const incoming = new Map(Array.from(items, (item) => [this.getKey(item), item]));
      for (const key of this.keys) if (!incoming.has(key)) this.removeKey(key);
      for (const [key, item] of incoming) this._put(item, key, comparer);
    });
  }
  clone(changes) {
    return this._mutate(() => {
      for (const c of changes) {
        if (c.reason === "add" || c.reason === "update" || c.reason === "replace") this._put(c.current, c.key);
        else if (c.reason === "remove") this.removeKey(c.key);
        else if (c.reason === "refresh") this.refreshKey(c.key);
        else if (c.reason === "clear") this.clear();
      }
    });
  }
  update(changes) {
    return this.clone(changes);
  }
  watch(key) {
    return observableDecorator(new Observable((observer) => this.connect((_, candidate) => Object.is(candidate, key)).subscribe({ next: (changes) => {
      for (const change of changes) observer.next(change);
    }, error: (e) => observer.error(e), complete: () => observer.complete() })));
  }
  asObservableCache() {
    return new ObservableCache(this.connect(), this.keySelector);
  }
};
var SourceList = class extends SourceBase {
  constructor(source) {
    super("list");
    if (source?.subscribe) this._loader = source.subscribe({ next: (changes) => this.clone(changes), error: (e) => this._error(e), complete: () => this._complete() });
    else if (source != null) this.addRange(source);
  }
  _index(index, allowEnd = false) {
    if (!Number.isInteger(index) || index < 0 || index >= this.count + Number(allowEnd)) throw new RangeError(`Index ${index} outside list of length ${this.count}`);
  }
  get(index) {
    this._index(index);
    return this._data[index];
  }
  at(index) {
    return this._data.at(index);
  }
  indexOf(item) {
    return this._data.indexOf(item);
  }
  contains(item) {
    return this._data.includes(item);
  }
  add(item) {
    return this._mutate(() => this.insert(this.count, item));
  }
  addRange(items) {
    return this._mutate(() => this.insertRange(items, this.count));
  }
  insert(index, item) {
    return this._mutate(() => {
      this._index(index, true);
      if (item == null) throw new TypeError("List items cannot be null or undefined");
      this._data.splice(index, 0, item);
      this._change(new ListChange("add", item, index), () => this._data.splice(index, 1));
    });
  }
  insertRange(items, index) {
    if (typeof items === "number") [items, index] = [index, items];
    return this._mutate(() => {
      this._index(index, true);
      const values = Array.from(items);
      if (values.some((item) => item == null)) throw new TypeError("List items cannot be null or undefined");
      if (!values.length) return;
      for (let offset = 0; offset < values.length; offset += 8192) this._data.splice(index + offset, 0, ...values.slice(offset, offset + 8192));
      this._change(new ListChange("addRange", values, index), () => this._data.splice(index, values.length));
    });
  }
  removeAt(index) {
    return this._mutate(() => {
      this._index(index);
      const [item] = this._data.splice(index, 1);
      this._change(new ListChange("remove", item, index), () => this._data.splice(index, 0, item));
    });
  }
  remove(item) {
    this._assert();
    const index = this.indexOf(item);
    if (index < 0) return false;
    this.removeAt(index);
    return true;
  }
  removeMany(items) {
    return this._mutate(() => {
      for (const item of Array.from(items)) this.remove(item);
    });
  }
  removeWhere(predicate) {
    return this._mutate(() => {
      for (let index = this.count - 1; index >= 0; index--) if (predicate(this._data[index], index)) this.removeAt(index);
    });
  }
  removeRange(index, count2) {
    return this._mutate(() => {
      this._index(index, true);
      if (!Number.isInteger(count2) || count2 < 0 || index + count2 > this.count) throw new RangeError("Invalid removal range");
      if (!count2) return;
      const values = this._data.splice(index, count2);
      this._change(new ListChange("removeRange", values, index), () => {
        for (let offset = 0; offset < values.length; offset += 8192) this._data.splice(index + offset, 0, ...values.slice(offset, offset + 8192));
      });
    });
  }
  clear() {
    return this._mutate(() => {
      if (!this.count) return;
      const values = this._data.splice(0);
      this._change(new ListChange("clear", values, 0), () => {
        for (const item of values) this._data.push(item);
      });
    });
  }
  replaceAt(index, item) {
    return this._mutate(() => {
      this._index(index);
      if (item == null) throw new TypeError("List items cannot be null or undefined");
      const previous = this._data[index];
      this._data[index] = item;
      this._change(new ListChange("replace", item, index, previous, index), () => {
        this._data[index] = previous;
      });
    });
  }
  set(index, item) {
    return this.replaceAt(index, item);
  }
  replace(original, replacement) {
    const index = this.indexOf(original);
    if (index < 0) throw new Error("Original item is not in the list");
    return this.replaceAt(index, replacement);
  }
  move(previousIndex, currentIndex) {
    return this._mutate(() => {
      this._index(previousIndex);
      this._index(currentIndex);
      if (previousIndex === currentIndex) return;
      const [item] = this._data.splice(previousIndex, 1);
      this._data.splice(currentIndex, 0, item);
      this._change(new ListChange("move", item, currentIndex, void 0, previousIndex), () => {
        this._data.splice(currentIndex, 1);
        this._data.splice(previousIndex, 0, item);
      });
    });
  }
  refreshAt(index) {
    return this._mutate(() => {
      this._index(index);
      this._change(new ListChange("refresh", this._data[index], index), () => {
      });
    });
  }
  refresh(item) {
    return this._mutate(() => {
      if (arguments.length === 0) for (let index = 0; index < this.count; index++) this.refreshAt(index);
      else for (let index = 0; index < this.count; index++) if (Object.is(this._data[index], item)) this.refreshAt(index);
    });
  }
  refreshMany(items) {
    return this._mutate(() => {
      const set2 = new Set(items);
      for (let index = 0; index < this.count; index++) if (set2.has(this._data[index])) this.refreshAt(index);
    });
  }
  load(items) {
    return this._mutate(() => {
      const values = Array.from(items);
      this.clear();
      this.addRange(values);
    });
  }
  editDiff(items, comparer = Object.is) {
    const values = Array.from(items), equals = equality(comparer);
    return this._mutate(() => {
      for (let index = 0; index < values.length; index++) {
        if (index < this.count && equals(this._data[index], values[index])) continue;
        let found = -1;
        for (let candidate = index + 1; candidate < this.count; candidate++) if (equals(this._data[candidate], values[index])) {
          found = candidate;
          break;
        }
        if (found >= 0) this.move(found, index);
        else this.insert(index, values[index]);
      }
      if (this.count > values.length) this.removeRange(values.length, this.count - values.length);
    });
  }
  clone(changes) {
    return this._mutate(() => {
      if (changes.items) {
        this.editDiff(changes.items);
        return;
      }
      for (const c of changes) {
        const i = c.currentIndex ?? -1;
        switch (c.reason) {
          case "add":
            this.insert(i < 0 ? this.count : i, c.current);
            break;
          case "addRange":
            this.insertRange(rangeItems(c), rangeIndex(c) < 0 ? this.count : rangeIndex(c));
            break;
          case "remove":
            if (i >= 0) this.removeAt(i);
            else this.remove(c.current);
            break;
          case "removeRange":
            if (rangeIndex(c) >= 0) this.removeRange(rangeIndex(c), rangeItems(c).length);
            else this.removeMany(rangeItems(c));
            break;
          case "replace":
          case "update":
            if (c.previousIndex >= 0 && i >= 0 && c.previousIndex !== i) {
              this.removeAt(c.previousIndex);
              this.insert(i, c.current);
            } else this.replaceAt(i >= 0 ? i : this.indexOf(c.previous), c.current);
            break;
          case "clear":
            this.clear();
            break;
          case "move":
            this.move(c.previousIndex, i);
            break;
          case "refresh":
            if (i >= 0) this.refreshAt(i);
            else this.refresh(c.current);
            break;
        }
      }
    });
  }
  asObservableList() {
    return new ObservableList(this.connect());
  }
};
var ObservableCache = class {
  constructor(source, keySelector = (item) => item?.id ?? item?.key ?? item) {
    if (!source?.subscribe && !source?.connect) throw new TypeError("ObservableCache requires a change-set Observable");
    this._source = new SourceCache(keySelector);
    this.keySelector = keySelector;
    this._subscription = (source.connect ? source.connect() : source).subscribe({ next: (changes) => this._source.clone(changes), error: (error) => this._source._error(error), complete: () => this._source._complete() });
  }
  get count() {
    return this._source.count;
  }
  get size() {
    return this.count;
  }
  get items() {
    return this._source.items;
  }
  get keys() {
    return this._source.keys;
  }
  get keyValues() {
    return this._source.keyValues;
  }
  get countChanged() {
    return this._source.countChanged;
  }
  get isDisposed() {
    return this._source.isDisposed;
  }
  connect(...args) {
    return this._source.connect(...args);
  }
  preview(...args) {
    return this._source.preview(...args);
  }
  watch(key) {
    return this._source.watch(key);
  }
  lookup(key) {
    return this._source.lookup(key);
  }
  dispose() {
    this._subscription.unsubscribe();
    this._source.dispose();
  }
  unsubscribe() {
    this.dispose();
  }
  [Symbol.iterator]() {
    return this.items[Symbol.iterator]();
  }
};
var ObservableList = class {
  constructor(source) {
    this._source = new SourceList(source?.connect ? source.connect() : source);
  }
  get count() {
    return this._source.count;
  }
  get size() {
    return this.count;
  }
  get items() {
    return this._source.items;
  }
  get countChanged() {
    return this._source.countChanged;
  }
  get isDisposed() {
    return this._source.isDisposed;
  }
  connect(...args) {
    return this._source.connect(...args);
  }
  preview(...args) {
    return this._source.preview(...args);
  }
  get(index) {
    return this._source.get(index);
  }
  at(index) {
    return this._source.at(index);
  }
  indexOf(item) {
    return this._source.indexOf(item);
  }
  contains(item) {
    return this._source.contains(item);
  }
  dispose() {
    this._source.dispose();
  }
  unsubscribe() {
    this.dispose();
  }
  [Symbol.iterator]() {
    return this.items[Symbol.iterator]();
  }
};
var IntermediateCache = class extends SourceCache {
  constructor(source) {
    super((item) => item?.id ?? item?.key ?? item);
    if (source) this._loader = (source.connect ? source.connect() : source).subscribe({ next: (changes) => this.clone(changes), error: (error) => this._error(error), complete: () => this._complete() });
  }
};
var ChangeAwareCache = class extends SourceCache {
  constructor(data, keySelector = (item) => item?.id ?? item?.key ?? item) {
    super(typeof data === "function" ? data : keySelector);
    this._captured = new ChangeSet([], "cache");
    if (data instanceof Map) this._data = new Map(data);
  }
  _publish(changes, before) {
    for (const change of changes) this._captured.push(change);
    super._publish(changes, before);
  }
  captureChanges() {
    const changes = this._captured;
    this._captured = new ChangeSet([], "cache");
    return changes;
  }
  add(item, key = this.getKey(item)) {
    return this._mutate(() => {
      if (this._data.has(key)) throw new Error(`Key ${String(key)} already exists`);
      this._put(item, key);
    });
  }
};
var ChangeAwareList = class _ChangeAwareList extends SourceList {
  constructor(items, copyChanges = false) {
    super();
    this._captured = new ChangeSet([], "list");
    if (items instanceof _ChangeAwareList) {
      this._data = items.items;
      if (copyChanges) this._captured = new ChangeSet(items._captured, "list");
    } else if (items != null && typeof items !== "number") this.addRange(items);
  }
  get isReadOnly() {
    return false;
  }
  _publish(changes, before) {
    for (const change of changes) this._captured.push(change);
    super._publish(changes, before);
  }
  captureChanges() {
    const changes = this._captured;
    this._captured = new ChangeSet([], "list");
    return changes;
  }
  copyTo(array, arrayIndex = 0) {
    if (!Array.isArray(array) || !Number.isInteger(arrayIndex) || arrayIndex < 0) throw new TypeError("CopyTo requires an array and a nonnegative index");
    this._data.forEach((item, i) => {
      array[arrayIndex + i] = item;
    });
  }
};
for (const Class of [Optional, Change, ListChange, RangeChange, ChangeSet, SourceBase, SourceCache, SourceList, ObservableCache, ObservableList, IntermediateCache, ChangeAwareCache, ChangeAwareList]) {
  for (const name of Object.getOwnPropertyNames(Class.prototype)) {
    if (name === "constructor" || name.startsWith("_") || name[0] === name[0].toUpperCase()) continue;
    const pascal = name[0].toUpperCase() + name.slice(1);
    if (!(pascal in Class.prototype)) Object.defineProperty(Class.prototype, pascal, Object.getOwnPropertyDescriptor(Class.prototype, name));
  }
}

// src/operators.js
import { Observable as Observable2, Subscription as Subscription2, isObservable } from "rxjs";
var identity = (value) => value;
var has = (value, key) => Object.prototype.hasOwnProperty.call(value, key);
var reasonOf = (change) => String(change.reason ?? change.Reason ?? "").replace(/^./, (c) => c.toLowerCase());
var observable = (value) => isObservable(value) || !!value?.subscribe;
var compareDefault = (a, b) => a == null ? b == null ? 0 : -1 : b == null ? 1 : a < b ? -1 : a > b ? 1 : 0;
var comparerFunction = (comparer) => typeof comparer === "function" ? comparer : comparer?.compare?.bind(comparer) ?? comparer?.Compare?.bind(comparer) ?? compareDefault;
var indexFor = (index, length) => Number.isInteger(index) && index >= 0 ? Math.min(index, length) : length;
function insertMany(array, index, values) {
  for (let offset = 0; offset < values.length; offset += 8192) array.splice(index + offset, 0, ...values.slice(offset, offset + 8192));
}
var Model = class {
  constructor() {
    this.kind = null;
    this.entries = [];
    this.byKey = /* @__PURE__ */ new Map();
    this.updated = /* @__PURE__ */ new Set();
    this.refreshed = /* @__PURE__ */ new Set();
  }
  ingest(changes) {
    this.kind ??= changes.kind ?? (changes.some((c) => has(c, "key")) ? "cache" : "list");
    this.updated.clear();
    this.refreshed.clear();
    const make = (value, key) => ({ id: this.kind === "cache" ? key : /* @__PURE__ */ Symbol(), key, value, version: 0 });
    const replace = (entry, value) => {
      const next = { ...entry, value, version: entry.version + 1 };
      this.updated.add(entry.id);
      return next;
    };
    for (const c of changes) {
      const reason2 = reasonOf(c), current = has(c, "current") ? c.current : c.item?.current;
      const ci = c.currentIndex ?? c.item?.currentIndex ?? -1;
      const pi = c.previousIndex ?? c.item?.previousIndex ?? -1;
      const range = Array.isArray(c.range) ? c.range : c.range?.items ?? [];
      const ri = c.range?.index ?? ci;
      if (this.kind === "cache") {
        const key = c.key, entry = this.byKey.get(key);
        if (reason2 === "clear") {
          this.entries = [];
          this.byKey.clear();
        } else if (reason2 === "add" || reason2 === "update" || reason2 === "replace") {
          if (entry) {
            const at = this.entries.indexOf(entry), next = replace(entry, current);
            this.entries[at] = next;
            this.byKey.set(key, next);
            if (ci >= 0 && ci !== at) {
              this.entries.splice(at, 1);
              this.entries.splice(indexFor(ci, this.entries.length), 0, next);
            }
          } else {
            const next = make(current, key);
            this.updated.add(next.id);
            this.byKey.set(key, next);
            this.entries.splice(indexFor(ci, this.entries.length), 0, next);
          }
        } else if (reason2 === "remove" && entry) {
          this.entries.splice(this.entries.indexOf(entry), 1);
          this.byKey.delete(key);
        } else if (reason2 === "refresh" && entry) {
          this.refreshed.add(entry.id);
        } else if ((reason2 === "move" || reason2 === "moved") && entry) {
          this.entries.splice(this.entries.indexOf(entry), 1);
          this.entries.splice(indexFor(ci, this.entries.length), 0, entry);
        }
      } else {
        if (reason2 === "add") {
          const next = make(current);
          this.updated.add(next.id);
          this.entries.splice(indexFor(ci, this.entries.length), 0, next);
        } else if (reason2 === "addRange") {
          const next = range.map((value) => make(value));
          next.forEach((e) => this.updated.add(e.id));
          insertMany(this.entries, indexFor(ri, this.entries.length), next);
        } else if (reason2 === "clear") {
          this.entries = [];
        } else if (reason2 === "removeRange") {
          if (ri >= 0) this.entries.splice(ri, range.length || c.count || 0);
          else for (const value of range) {
            const i = this.entries.findIndex((e) => Object.is(e.value, value));
            if (i >= 0) this.entries.splice(i, 1);
          }
        } else if (reason2 === "remove") {
          const i = ci >= 0 ? ci : pi >= 0 ? pi : this.entries.findIndex((e) => Object.is(e.value, current));
          if (i >= 0 && i < this.entries.length) this.entries.splice(i, 1);
        } else if (reason2 === "replace" || reason2 === "update") {
          const i = pi >= 0 ? pi : ci >= 0 ? ci : this.entries.findIndex((e) => Object.is(e.value, c.previous));
          if (i >= 0 && this.entries[i]) {
            const next = replace(this.entries[i], current);
            this.entries.splice(i, 1);
            this.entries.splice(ci >= 0 ? indexFor(ci, this.entries.length) : i, 0, next);
          }
        } else if (reason2 === "refresh") {
          const i = ci >= 0 ? ci : this.entries.findIndex((e) => Object.is(e.value, current));
          if (this.entries[i]) this.refreshed.add(this.entries[i].id);
        } else if (reason2 === "move" || reason2 === "moved") {
          if (pi >= 0 && pi < this.entries.length) {
            const [entry] = this.entries.splice(pi, 1);
            this.entries.splice(indexFor(ci, this.entries.length), 0, entry);
          }
        }
      }
    }
    if (this.kind === "cache" && Array.isArray(changes.keys) && changes.keys.length === this.entries.length) {
      const sorted = changes.keys.map((key) => this.byKey.get(key));
      if (sorted.every(Boolean)) this.entries = sorted;
    }
    return this.entries;
  }
};
function changeSet(changes, kind, entries, metadata = {}) {
  const result = new ChangeSet(changes.map((change) => kind === "cache" ? new Change(change) : new ListChange(change)), kind, entries.map((e) => e.value));
  if (kind === "cache") result.keys = entries.map((e) => e.key);
  Object.assign(result, metadata);
  return result;
}
function orderedView(entries, comparer, sortReason) {
  const items = entries.map((e) => e.value), keys = entries.map((e) => e.key), keyValues = entries.map((e) => [e.key, e.value]);
  return {
    items,
    keys,
    keyValues,
    count: entries.length,
    comparer,
    sortReason,
    Items: items,
    Keys: keys,
    Count: entries.length,
    Comparer: comparer,
    SortReason: sortReason,
    [Symbol.iterator]: function* () {
      yield* keyValues;
    }
  };
}
function difference(previous, next, kind, refreshed = /* @__PURE__ */ new Set(), updated = /* @__PURE__ */ new Set(), trackMoves = true) {
  const changes = [], old = new Map(previous.map((e) => [e.id, e])), live = new Set(next.map((e) => e.id));
  const work = previous.slice();
  for (let i = work.length - 1; i >= 0; --i) if (!live.has(work[i].id)) {
    const e = work[i];
    changes.push({ reason: "remove", key: e.key, current: e.value, currentIndex: i, previousIndex: i });
    work.splice(i, 1);
  }
  for (let i = 0; i < next.length; i++) {
    const e = next[i], before = old.get(e.id);
    if (!before) {
      changes.push({ reason: "add", key: e.key, current: e.value, currentIndex: i });
      work.splice(i, 0, e);
      continue;
    }
    let at = trackMoves ? work[i]?.id === e.id ? i : work.findIndex((x, index) => index >= i && x.id === e.id) : i;
    if (trackMoves && at !== i && at >= 0) {
      changes.push({ reason: "move", key: e.key, current: before.value, previousIndex: at, currentIndex: i });
      const [moved] = work.splice(at, 1);
      work.splice(i, 0, moved);
      at = i;
    }
    if (!Object.is(before.value, e.value) || before.version !== e.version || updated.has(e.id)) {
      changes.push({ reason: kind === "cache" ? "update" : "replace", key: e.key, current: e.value, previous: before.value, currentIndex: i, previousIndex: i });
    } else if (refreshed.has(e.id)) changes.push({ reason: "refresh", key: e.key, current: e.value, currentIndex: i });
    if (trackMoves) work[i] = e;
  }
  if (kind === "list") for (const change of changes) delete change.key;
  return changes;
}
function derived(setup) {
  return (source) => new Observable2((observer) => {
    const subscriptions = new Subscription2(), model = new Model();
    let previous = [], loaded = false;
    const run = (fn) => {
      if (!observer.closed) try {
        fn();
      } catch (error) {
        observer.error(error);
      }
    };
    const emit = (entries, { kind = model.kind ?? "cache", refresh: refresh2 = model.refreshed, updated = /* @__PURE__ */ new Set(), suppressEmpty = true, metadata = {}, moves = true } = {}) => {
      const changes = difference(previous, entries, kind, refresh2, updated, moves);
      previous = entries.map((e) => ({ ...e }));
      if (changes.length || !suppressEmpty) observer.next(changeSet(changes, kind, entries, metadata));
    };
    const context = {
      model,
      emit,
      run,
      observer,
      subscriptions,
      emitRaw(changes, suppressEmpty = true, metadata = {}) {
        if (changes.length || !suppressEmpty) observer.next(Object.assign(new ChangeSet(changes.map((change) => new Change(change)), "cache"), metadata));
      },
      get loaded() {
        return loaded;
      },
      watch(input, next) {
        if (input) subscriptions.add(input.subscribe({ next: (value) => run(() => next(value)), error: (error) => observer.error(error) }));
      }
    };
    let handler;
    run(() => {
      handler = setup(context);
    });
    if (!observer.closed) subscriptions.add(source.subscribe({
      next: (changes) => run(() => {
        loaded = true;
        if (handler.prepare) handler.prepare(changes);
        else model.ingest(changes);
        handler(changes);
      }),
      error: (error) => observer.error(error),
      complete: () => observer.complete()
    }));
    return subscriptions;
  });
}
function filter(predicate, reapplyOrOptions, suppressEmptyChangeSets = true) {
  let statePredicate, reapply, options = {};
  if (typeof reapplyOrOptions === "function" && observable(predicate)) statePredicate = reapplyOrOptions;
  else if (observable(reapplyOrOptions)) reapply = reapplyOrOptions;
  else if (typeof reapplyOrOptions === "boolean") options.suppressEmptyChangeSets = reapplyOrOptions;
  else options = reapplyOrOptions ?? {};
  reapply ??= options.reapply ?? options.reapplyFilter;
  return derived((ctx) => {
    const matches = /* @__PURE__ */ new Map(), input = /* @__PURE__ */ new Map(), included = /* @__PURE__ */ new Map();
    let fast, test = observable(predicate) ? () => false : predicate;
    if (typeof test !== "function") throw new TypeError("filter requires a predicate or Observable.");
    const suppress = options.suppressEmptyChangeSets ?? suppressEmptyChangeSets;
    const evaluateCache = (changes, all = false) => {
      const output = [];
      const check = (key, value, reason2) => {
        const had = included.has(key), previous = included.get(key), keep = !!test(value, key);
        if (keep) {
          included.set(key, value);
          if (!had) output.push({ reason: "add", key, current: value });
          else if (reason2) output.push({ reason: reason2 === "refresh" ? "refresh" : "update", key, current: value, previous });
        } else if (had) {
          included.delete(key);
          output.push({ reason: "remove", key, current: previous });
        }
      };
      if (all) {
        for (const [key, value] of input) check(key, value);
      } else for (const c of changes) {
        if (c.reason === "add" || c.reason === "update" || c.reason === "replace") {
          input.set(c.key, c.current);
          check(c.key, c.current, c.reason);
        } else if (c.reason === "refresh" && !options.ignoreRefresh && input.has(c.key)) check(c.key, input.get(c.key), "refresh");
        else if (c.reason === "remove") {
          input.delete(c.key);
          if (included.has(c.key)) {
            output.push({ reason: "remove", key: c.key, current: included.get(c.key) });
            included.delete(c.key);
          }
        } else if (c.reason === "clear") {
          for (const [key, value] of included) output.push({ reason: "remove", key, current: value });
          included.clear();
          input.clear();
        }
      }
      ctx.emitRaw(output, suppress);
    };
    const evaluate = (all = false) => {
      if (fast) {
        evaluateCache([], all);
        return;
      }
      if (options.ignoreRefresh) ctx.model.refreshed.clear();
      const active = /* @__PURE__ */ new Set(), next = [];
      for (const entry of ctx.model.entries) {
        active.add(entry.id);
        if (all || !matches.has(entry.id) || ctx.model.updated.has(entry.id) || ctx.model.refreshed.has(entry.id)) matches.set(entry.id, !!test(entry.value, entry.key));
        if (matches.get(entry.id)) next.push(entry);
      }
      for (const id of matches.keys()) if (!active.has(id)) matches.delete(id);
      ctx.emit(next, { suppressEmpty: suppress });
    };
    if (observable(predicate)) ctx.watch(predicate, (value) => {
      test = statePredicate ? (item, key) => statePredicate(value, item, key) : value;
      if (typeof test !== "function") throw new TypeError("Predicate Observable must emit functions.");
      if (ctx.loaded) {
        ctx.model.refreshed.clear();
        ctx.model.updated.clear();
        evaluate(true);
      }
    });
    ctx.watch(reapply, () => {
      if (ctx.loaded) {
        ctx.model.refreshed.clear();
        ctx.model.updated.clear();
        evaluate(true);
      }
    });
    const handler = (changes) => fast ? evaluateCache(changes) : evaluate();
    handler.prepare = (changes) => {
      fast ??= changes.kind === "cache" && !changes.items && !Array.isArray(changes.keys);
      if (fast) ctx.model.kind = "cache";
      else ctx.model.ingest(changes);
    };
    return handler;
  });
}
var filterImmutable = (predicate, suppressEmptyChangeSets = true) => filter(predicate, { ignoreRefresh: true, suppressEmptyChangeSets });
var filterWithState = (state, predicate, suppressEmpty = true) => filter(state, predicate, suppressEmpty);
function transform(factory, options = {}) {
  if (typeof factory !== "function") throw new TypeError("transform requires a factory.");
  if (typeof options === "boolean") options = { transformOnRefresh: options };
  else if (observable(options)) options = { forceTransform: options };
  return derived((ctx) => {
    const projected = /* @__PURE__ */ new Map(), sourceValues = /* @__PURE__ */ new Map();
    let fast;
    const evaluateCache = (changes, forcedPredicate) => {
      const output = [];
      const project = (key, value, reason2) => {
        const had = projected.has(key), old = projected.get(key), oldSource = sourceValues.get(key);
        sourceValues.set(key, value);
        if (reason2 === "refresh" && !options.transformOnRefresh && !forcedPredicate) {
          if (had) output.push({ reason: "refresh", key, current: old });
          return;
        }
        try {
          let destination;
          if (had && options.inlineUpdate) {
            options.inlineUpdate(old, value);
            destination = old;
          } else destination = factory(value, key, oldSource);
          projected.set(key, destination);
          output.push({ reason: had ? "update" : "add", key, current: destination, previous: old });
        } catch (error) {
          if (!options.errorHandler) throw error;
          options.errorHandler({ error, value, key });
        }
      };
      if (forcedPredicate) {
        for (const [key, value] of sourceValues) if (forcedPredicate(value, key)) project(key, value, "update");
      } else for (const c of changes) {
        if (c.reason === "add" || c.reason === "update" || c.reason === "replace") project(c.key, c.current, c.reason);
        else if (c.reason === "refresh" && !options.ignoreRefresh && sourceValues.has(c.key)) project(c.key, sourceValues.get(c.key), "refresh");
        else if (c.reason === "remove") {
          sourceValues.delete(c.key);
          if (projected.has(c.key)) {
            output.push({ reason: "remove", key: c.key, current: projected.get(c.key) });
            projected.delete(c.key);
          }
        } else if (c.reason === "clear") {
          for (const [key, value] of projected) output.push({ reason: "remove", key, current: value });
          projected.clear();
          sourceValues.clear();
        }
      }
      ctx.emitRaw(output, options.suppressEmptyChangeSets ?? true);
    };
    const evaluate = (forcedPredicate) => {
      if (fast) {
        evaluateCache([], forcedPredicate);
        return;
      }
      if (options.ignoreRefresh) ctx.model.refreshed.clear();
      const live = /* @__PURE__ */ new Set(), next = [], updated = /* @__PURE__ */ new Set();
      for (const [index, entry] of ctx.model.entries.entries()) {
        live.add(entry.id);
        const old = projected.get(entry.id);
        const should = !projected.has(entry.id) || ctx.model.updated.has(entry.id) || options.transformOnRefresh && ctx.model.refreshed.has(entry.id) || forcedPredicate && forcedPredicate(entry.value, entry.key);
        if (should) {
          try {
            let value;
            if (old && options.inlineUpdate) {
              options.inlineUpdate(old.value, entry.value);
              value = old.value;
            } else value = factory(entry.value, ctx.model.kind === "list" ? index : entry.key, ctx.model.kind === "list" ? old?.value : sourceValues.get(entry.id));
            projected.set(entry.id, { ...entry, value, version: (old?.version ?? -1) + 1 });
            sourceValues.set(entry.id, entry.value);
            updated.add(entry.id);
          } catch (error) {
            if (!options.errorHandler) throw error;
            options.errorHandler({ error, value: entry.value, key: entry.key });
          }
        }
        if (projected.has(entry.id)) next.push(projected.get(entry.id));
      }
      for (const id of projected.keys()) if (!live.has(id)) {
        projected.delete(id);
        sourceValues.delete(id);
      }
      ctx.emit(next, { updated, suppressEmpty: options.suppressEmptyChangeSets ?? true });
    };
    ctx.watch(options.forceTransform, (predicate) => {
      if (ctx.loaded) {
        ctx.model.updated.clear();
        ctx.model.refreshed.clear();
        evaluate(typeof predicate === "function" ? predicate : () => true);
      }
    });
    const handler = (changes) => fast ? evaluateCache(changes) : evaluate();
    handler.prepare = (changes) => {
      fast ??= changes.kind === "cache" && !changes.items && !Array.isArray(changes.keys);
      if (fast) ctx.model.kind = "cache";
      else ctx.model.ingest(changes);
    };
    return handler;
  });
}
var transformImmutable = (factory) => transform(factory, { ignoreRefresh: true });
var convert = transform;
var cast = (converter = identity) => transform(converter);
var castToObject = () => transform(identity);
var ofType = (type) => {
  if (typeof type !== "string" && typeof type !== "function") throw new TypeError("ofType requires a constructor or JavaScript typeof name.");
  return filter((value) => typeof type === "string" ? typeof value === type : type === Number ? typeof value === "number" : type === String ? typeof value === "string" : type === Boolean ? typeof value === "boolean" : value instanceof type);
};
var transformSafe = (factory, errorHandler, options = {}) => transform(factory, { ...options, errorHandler });
var transformWithInlineUpdate = (factory, updateAction, errorHandler, transformOnRefresh = false) => {
  if (typeof updateAction !== "function") throw new TypeError("An update action is required.");
  return transform(factory, { inlineUpdate: updateAction, errorHandler, transformOnRefresh });
};
function transformMany(selector, keySelector) {
  if (typeof selector !== "function") throw new TypeError("transformMany requires a selector.");
  return derived((ctx) => {
    const children = /* @__PURE__ */ new Map();
    let changing = false;
    const flatten2 = () => {
      if (!ctx.loaded || changing) return;
      let next = [];
      const refresh2 = /* @__PURE__ */ new Set();
      for (const record of children.values()) for (const entry of record.entries) if (record.refreshed?.has(entry.id)) refresh2.add(keySelector ? entry.key : entry.id);
      for (const parent of ctx.model.entries) for (const child of children.get(parent.id)?.entries ?? []) next.push(child);
      const kind = keySelector ? "cache" : "list";
      if (keySelector) {
        const unique = /* @__PURE__ */ new Map();
        for (const entry of next) unique.set(entry.key, entry);
        next = [...unique.values()].map((entry) => ({ ...entry, id: entry.key }));
      }
      ctx.emit(next, { kind, refresh: refresh2 });
      for (const record of children.values()) record.refreshed?.clear();
    };
    const setChildren = (record, values, refreshedValues = /* @__PURE__ */ new Set(), updatedValues = /* @__PURE__ */ new Set()) => {
      const previous = record.entries, used = /* @__PURE__ */ new Set();
      record.refreshed ??= /* @__PURE__ */ new Set();
      record.entries = Array.from(values ?? [], (value, index) => {
        const key = keySelector?.(value), old = keySelector ? previous.find((e) => !used.has(e.id) && Object.is(e.key, key)) : previous.find((e) => !used.has(e.id) && Object.is(e.value, value));
        if (old) used.add(old.id);
        const entry = { id: old?.id ?? /* @__PURE__ */ Symbol(), key, value, version: (old?.version ?? 0) + (old && (!Object.is(old.value, value) || updatedValues.has(value)) ? 1 : 0) };
        if (refreshedValues.has(value)) record.refreshed.add(entry.id);
        return entry;
      });
      flatten2();
    };
    ctx.subscriptions.add(() => {
      for (const record of children.values()) record.subscription?.unsubscribe();
      children.clear();
    });
    return () => {
      changing = true;
      try {
        const live = new Set(ctx.model.entries.map((e) => e.id));
        for (const [id, record] of children) if (!live.has(id)) {
          record.subscription?.unsubscribe();
          children.delete(id);
        }
        for (const parent of ctx.model.entries) {
          let record = children.get(parent.id);
          if (record && !ctx.model.updated.has(parent.id) && !ctx.model.refreshed.has(parent.id)) continue;
          const selected = selector(parent.value, parent.key);
          if (record?.selected === selected && (observable(selected) || selected?.connect)) continue;
          if (record) record.subscription?.unsubscribe();
          record = { selected, entries: record?.entries ?? [], model: new Model() };
          children.set(parent.id, record);
          const stream = selected?.connect ? selected.connect() : observable(selected) ? selected : null;
          if (stream) record.subscription = stream.subscribe({
            next: (values) => ctx.run(() => {
              if (values?.kind || Array.isArray(values) && values.length && typeof values[0]?.reason === "string") {
                record.model.ingest(values);
                setChildren(record, record.model.entries.map((e) => e.value), new Set(record.model.entries.filter((e) => record.model.refreshed.has(e.id)).map((e) => e.value)), new Set(record.model.entries.filter((e) => record.model.updated.has(e.id)).map((e) => e.value)));
              } else setChildren(record, values);
            }),
            error: (error) => ctx.observer.error(error)
          });
          else {
            const values = Array.from(selected ?? []);
            setChildren(record, values, ctx.model.refreshed.has(parent.id) ? new Set(values) : /* @__PURE__ */ new Set());
          }
        }
      } finally {
        changing = false;
      }
      flatten2();
    };
  });
}
function distinctValues(selector = identity) {
  return derived((ctx) => {
    const values = /* @__PURE__ */ new Map();
    return () => {
      const live = /* @__PURE__ */ new Set(), distinct = /* @__PURE__ */ new Map();
      for (const e of ctx.model.entries) {
        live.add(e.id);
        if (!values.has(e.id) || ctx.model.updated.has(e.id) || ctx.model.refreshed.has(e.id)) values.set(e.id, selector(e.value, e.key));
        const value = values.get(e.id);
        if (!distinct.has(value)) distinct.set(value, { id: value, key: value, value, version: 0 });
      }
      for (const id of values.keys()) if (!live.has(id)) values.delete(id);
      ctx.emit([...distinct.values()], { kind: "cache", refresh: /* @__PURE__ */ new Set() });
    };
  });
}
function sort(comparer = compareDefault, resortOrOptions = {}) {
  const options = observable(resortOrOptions) ? { resort: resortOrOptions } : resortOrOptions ?? {};
  return derived((ctx) => {
    let compare2 = observable(comparer) ? compareDefault : comparerFunction(comparer);
    let comparerReady = !options.waitForComparer || !observable(comparer), delivered = false;
    const order = /* @__PURE__ */ new Map(), cache = /* @__PURE__ */ new Map();
    let counter = 0, fast, ordered = [];
    const compareEntry = (a, b) => compare2(a.value, b.value) || order.get(a.id) - order.get(b.id);
    const metadata = (entries, why) => {
      const sortedItems = orderedView(entries, compare2, why);
      return { items: sortedItems.items, keys: sortedItems.keys, sortedItems, SortedItems: sortedItems, comparer: compare2, sortReason: why };
    };
    const resort = (why) => {
      const previous = delivered ? ordered : [];
      ordered = [...cache.values()].sort(compareEntry);
      let changes;
      if (previous.length > (options.resetThreshold ?? options.ResetThreshold ?? 50)) {
        const reordered = previous.some((e, index) => e.id !== ordered[index]?.id);
        changes = reordered ? [
          ...previous.map((e, index) => ({ reason: "remove", key: e.key, current: e.value, currentIndex: index })).reverse(),
          ...ordered.map((e, index) => ({ reason: "add", key: e.key, current: e.value, currentIndex: index }))
        ] : [];
      } else changes = difference(previous, ordered, "cache");
      if (comparerReady) {
        ctx.emitRaw(changes, options.suppressEmptyChangeSets ?? true, metadata(ordered, why));
        delivered = true;
      }
    };
    const evaluateCache = (changes) => {
      const output = [], initial = !cache.size;
      if (initial && changes.every((c) => c.reason === "add")) {
        for (const c of changes) {
          if (!order.has(c.key)) order.set(c.key, counter++);
          cache.set(c.key, { id: c.key, key: c.key, value: c.current, version: 0 });
        }
        ordered = [...cache.values()].sort(compareEntry);
        ordered.forEach((e, index) => output.push({ reason: "add", key: e.key, current: e.value, currentIndex: index }));
      } else for (const c of changes) {
        const old = cache.get(c.key), oldIndex = old ? ordered.indexOf(old) : -1;
        if (c.reason === "remove") {
          if (old) {
            ordered.splice(oldIndex, 1);
            cache.delete(c.key);
            order.delete(c.key);
            output.push({ reason: "remove", key: c.key, current: old.value, currentIndex: oldIndex });
          }
        } else if (c.reason === "clear") {
          for (let index = ordered.length - 1; index >= 0; index--) output.push({ reason: "remove", key: ordered[index].key, current: ordered[index].value, currentIndex: index });
          ordered = [];
          cache.clear();
          order.clear();
        } else if (c.reason === "add" || c.reason === "update" || c.reason === "replace" || c.reason === "refresh" && old) {
          if (old) ordered.splice(oldIndex, 1);
          if (!order.has(c.key)) order.set(c.key, counter++);
          const entry = { id: c.key, key: c.key, value: c.reason === "refresh" ? old.value : c.current, version: (old?.version ?? -1) + 1 };
          let lo = 0, hi = ordered.length;
          while (lo < hi) {
            const mid = lo + hi >>> 1;
            if (compareEntry(ordered[mid], entry) <= 0) lo = mid + 1;
            else hi = mid;
          }
          ordered.splice(lo, 0, entry);
          cache.set(c.key, entry);
          if (c.reason === "refresh") {
            if (oldIndex !== lo) output.push({ reason: "move", key: c.key, current: entry.value, previousIndex: oldIndex, currentIndex: lo });
            output.push({ reason: "refresh", key: c.key, current: entry.value, currentIndex: lo });
          } else output.push({ reason: old ? "update" : "add", key: c.key, current: entry.value, previous: old?.value, previousIndex: oldIndex, currentIndex: lo });
        }
      }
      if (comparerReady) {
        const notifications2 = delivered ? output : ordered.map((e, index) => ({ reason: "add", key: e.key, current: e.value, currentIndex: index }));
        ctx.emitRaw(notifications2, options.suppressEmptyChangeSets ?? true, metadata(ordered, initial ? "initialLoad" : "dataChanged"));
        delivered = true;
      }
    };
    const evaluate = (why = "dataChanged") => {
      if (!comparerReady) return;
      if (fast) {
        resort(why);
        return;
      }
      for (const e of ctx.model.entries) if (!order.has(e.id)) order.set(e.id, counter++);
      const live = new Set(ctx.model.entries.map((e) => e.id));
      for (const key of order.keys()) if (!live.has(key)) order.delete(key);
      const entries = ctx.model.entries.slice().sort(compareEntry);
      const sortedItems = orderedView(entries, compare2, why);
      ctx.emit(entries, { suppressEmpty: options.suppressEmptyChangeSets ?? true, metadata: { sortedItems, SortedItems: sortedItems, comparer: compare2, sortReason: why } });
    };
    if (observable(comparer)) ctx.watch(comparer, (next) => {
      compare2 = comparerFunction(next);
      comparerReady = true;
      if (ctx.loaded) {
        ctx.model.updated.clear();
        ctx.model.refreshed.clear();
        evaluate("comparerChanged");
      }
    });
    ctx.watch(options.resort ?? options.resorter, () => {
      if (ctx.loaded) {
        ctx.model.updated.clear();
        ctx.model.refreshed.clear();
        evaluate("reorder");
      }
    });
    const handler = (changes) => fast ? evaluateCache(changes) : evaluate(order.size ? "dataChanged" : "initialLoad");
    handler.prepare = (changes) => {
      fast ??= changes.kind === "cache" && !changes.items && !Array.isArray(changes.keys);
      if (fast) ctx.model.kind = "cache";
      else ctx.model.ingest(changes);
    };
    return handler;
  });
}
function windowed(type, requests, options = {}) {
  return derived((ctx) => {
    let request = type === "page" ? { page: 1, size: 25 } : { startIndex: 0, size: 25 };
    let requestReady = !options.waitForRequest || !observable(requests);
    let currentComparer;
    const evaluate = () => {
      if (!requestReady) return;
      const totalSize = ctx.model.entries.length, size = request.size;
      const pages = Math.max(1, Math.ceil(totalSize / size));
      const pageNumber = Math.min(request.page ?? 1, pages);
      const startIndex = type === "page" ? (pageNumber - 1) * size : request.startIndex;
      const entries = ctx.model.entries.slice(startIndex, startIndex + size);
      const response = type === "page" ? { page: pageNumber, pages, size, totalSize, Page: pageNumber, Pages: pages, Size: size, TotalSize: totalSize } : { startIndex, size, totalSize, StartIndex: startIndex, Size: size, TotalSize: totalSize };
      const sortedItems = orderedView(entries, currentComparer, "dataChanged");
      const context = { response, comparer: currentComparer, options: options.operationOptions ?? {}, request: { ...request }, Response: response, Comparer: currentComparer, Options: options.operationOptions ?? {}, Request: { ...request } };
      ctx.emit(entries, { suppressEmpty: false, metadata: { response, Response: response, sortedItems, SortedItems: sortedItems, context, Context: context, comparer: currentComparer } });
    };
    const setRequest = (value) => {
      if (!value) return;
      const size = value.size ?? value.Size;
      const start = type === "page" ? value.page ?? value.Page ?? 1 : value.startIndex ?? value.StartIndex ?? 0;
      if (!Number.isInteger(size) || size < 1 || !Number.isInteger(start) || start < (type === "page" ? 1 : 0)) return;
      const firstRequest = !requestReady;
      requestReady = true;
      const next = type === "page" ? { page: start, size } : { startIndex: start, size };
      if (!firstRequest && request.size === size && (type === "page" ? request.page === start : request.startIndex === start)) return;
      request = next;
      if (ctx.loaded) {
        ctx.model.refreshed.clear();
        ctx.model.updated.clear();
        evaluate();
      }
    };
    if (observable(requests)) ctx.watch(requests, setRequest);
    else setRequest(requests);
    return (changes) => {
      currentComparer = changes.comparer ?? changes.sortedItems?.comparer ?? currentComparer;
      evaluate();
    };
  });
}
var page = (requests) => windowed("page", requests);
var virtualise = (requests) => windowed("virtual", requests);
var virtualize = virtualise;
var top = (comparerOrSize, requestedSize) => {
  const size = requestedSize ?? comparerOrSize;
  if (!Number.isInteger(size) || size < 1) throw new RangeError("top size must be a positive integer.");
  return requestedSize === void 0 ? virtualise({ startIndex: 0, size }) : sortAndVirtualise(comparerOrSize, { startIndex: 0, size });
};
var reverse = () => derived((ctx) => () => ctx.emit(ctx.model.entries.slice().reverse()));
var sortAndPage = (comparer, requests, options) => (source) => source.pipe(sort(comparer, { ...options, waitForComparer: true, suppressEmptyChangeSets: false }), windowed("page", requests, { waitForRequest: true, operationOptions: options }));
var sortAndVirtualise = (comparer, requests, options) => (source) => source.pipe(sort(comparer, { ...options, waitForComparer: true, suppressEmptyChangeSets: false }), windowed("virtual", requests, { waitForRequest: true, operationOptions: options }));
var sortAndVirtualize = sortAndVirtualise;
var sortBy = (selector, direction = "ascending", options) => {
  const select = typeof selector === "function" ? selector : (value) => value[selector];
  const sign = direction === 1 || String(direction).toLowerCase() === "descending" || direction === "desc" ? -1 : 1;
  return sort((a, b) => sign * compareDefault(select(a), select(b)), options);
};
function toCollection() {
  return (source) => new Observable2((observer) => {
    const model = new Model();
    return source.subscribe({ next: (changes) => {
      try {
        model.ingest(changes);
        observer.next(changes.items ? [...changes.items] : model.entries.map((e) => e.value));
      } catch (error) {
        observer.error(error);
      }
    }, error: (error) => observer.error(error), complete: () => observer.complete() });
  });
}
function bind(target) {
  if (target == null) throw new TypeError("bind requires an array, collection or callback.");
  return (source) => new Observable2((observer) => {
    const model = new Model();
    return source.subscribe({ next: (changes) => {
      try {
        model.ingest(changes);
        const items = changes.items ? [...changes.items] : model.entries.map((e) => e.value);
        if (typeof target === "function") target(items, changes);
        else if (Array.isArray(target)) {
          target.length = 0;
          for (const item of items) target.push(item);
        } else if (typeof target.load === "function") target.load(items);
        else if (typeof target.edit === "function") target.edit((updater) => {
          updater.clear();
          if (updater.addRange) updater.addRange(items);
          else updater.addOrUpdate(items);
        });
        else if (typeof target.next === "function") target.next(items);
        else throw new TypeError("Unsupported binding target.");
        observer.next(changes);
      } catch (error) {
        observer.error(error);
      }
    }, error: (error) => observer.error(error), complete: () => observer.complete() });
  });
}
var bindToObservableList = bind;
var bindToObservableCollection = bind;
var sortAndBind = (targetOrComparer, comparerOrTarget, options) => {
  const isTarget = (value) => Array.isArray(value) || !!value?.load || !!value?.edit;
  const comparerFirst = isTarget(comparerOrTarget) && !isTarget(targetOrComparer);
  const target = comparerFirst ? comparerOrTarget : targetOrComparer, comparer = comparerFirst ? targetOrComparer : comparerOrTarget;
  return (source) => source.pipe(sort(comparer, options), bind(target));
};
var asObservableCache = (sourceOrKeySelector, keySelector) => observable(sourceOrKeySelector) ? new ObservableCache(sourceOrKeySelector, keySelector) : (source) => new ObservableCache(source, sourceOrKeySelector);
var asObservableList = (source) => observable(source) ? new ObservableList(source) : (input) => new ObservableList(input);
function changeKey(selector) {
  if (typeof selector !== "function") throw new TypeError("changeKey requires a selector.");
  return derived((ctx) => {
    const keyed = /* @__PURE__ */ new Map();
    return () => {
      const next = /* @__PURE__ */ new Map(), live = /* @__PURE__ */ new Set(), refresh2 = /* @__PURE__ */ new Set();
      for (const e of ctx.model.entries) {
        live.add(e.id);
        if (!keyed.has(e.id) || ctx.model.updated.has(e.id) || ctx.model.refreshed.has(e.id)) keyed.set(e.id, selector(e.value, e.key));
        const key = keyed.get(e.id);
        next.set(key, { ...e, id: key, key });
        if (ctx.model.refreshed.has(e.id)) refresh2.add(key);
      }
      for (const id of keyed.keys()) if (!live.has(id)) keyed.delete(id);
      ctx.emit([...next.values()], { kind: "cache", refresh: refresh2 });
    };
  });
}
var removeKey = () => derived((ctx) => () => ctx.emit(ctx.model.entries, { kind: "list" }));

// src/advanced.js
import { Observable as Observable4, Subject as Subject3, Subscription as Subscription4, isObservable as isObservable3, of as of2, startWith, switchMap, distinctUntilChanged as distinctUntilChanged2 } from "rxjs";

// src/lifecycle.js
import { Observable as Observable3, Subject as Subject2, ReplaySubject, Subscription as Subscription3, asyncScheduler, from, of, isObservable as isObservable2, filter as rxFilter, map, distinctUntilChanged, debounceTime, bufferTime, skip, finalize, combineLatest, fromEvent } from "rxjs";
var cs = (changes, kind = "cache") => new ChangeSet(changes, kind);
var release = (resource) => {
  if (typeof resource === "function") resource();
  else if (resource?.unsubscribe) resource.unsubscribe();
  else if (resource?.dispose) resource.dispose();
  else if (resource?.[Symbol.dispose]) resource[Symbol.dispose]();
};
var getChanges = (changes) => changes?.changes ?? changes;
function tracker(hooks = {}) {
  const state = { kind: "cache", entries: [], byKey: /* @__PURE__ */ new Map() };
  const add = (item, key, index = state.entries.length, previous, prior) => {
    if (prior) prior.prior = void 0;
    const entry = { item, key, prior, active: true, subscription: new Subscription3() };
    if (state.kind === "cache") {
      const old = state.byKey.get(key);
      if (old) {
        index = state.entries.indexOf(old);
        remove2(old);
      }
      state.byKey.set(key, entry);
    }
    state.entries.splice(Math.max(0, index), 0, entry);
    hooks.add?.(entry, previous);
    return entry;
  };
  const remove2 = (entry) => {
    if (!entry) return;
    entry.active = false;
    const index = state.entries.indexOf(entry);
    if (index >= 0) state.entries.splice(index, 1);
    if (state.kind === "cache" && state.byKey.get(entry.key) === entry) state.byKey.delete(entry.key);
    entry.subscription.unsubscribe();
    hooks.remove?.(entry, index);
  };
  const at = (change) => state.kind === "cache" ? state.byKey.get(change.key) : state.entries[change.currentIndex >= 0 ? change.currentIndex : change.previousIndex >= 0 ? change.previousIndex : state.entries.findIndex((entry) => entry.item === change.current)];
  state.apply = (changes) => {
    state.kind = changes.kind ?? state.kind;
    for (const change of getChanges(changes)) {
      const reason2 = change.reason;
      if (reason2 === "add") add(change.current, change.key, change.currentIndex >= 0 ? change.currentIndex : state.entries.length);
      else if (reason2 === "addRange") {
        const range = change.range ?? { items: change.current ?? [], index: change.currentIndex };
        let index = range.index >= 0 ? range.index : state.entries.length;
        for (const item of range.items) add(item, void 0, index++);
      } else if (reason2 === "update" || reason2 === "replace") {
        const old = at(change);
        const index = old ? state.entries.indexOf(old) : change.currentIndex;
        const previous = old?.item ?? change.previous;
        remove2(old);
        add(change.current, change.key, index >= 0 ? index : state.entries.length, previous, old);
      } else if (reason2 === "remove") remove2(at(change));
      else if (reason2 === "removeRange") {
        const range = change.range ?? { items: change.current ?? [], index: change.currentIndex };
        if (range.index >= 0) {
          for (let i = 0; i < range.items.length; i++) remove2(state.entries[range.index]);
        } else for (const item of range.items) remove2(state.entries.find((entry) => entry.item === item));
      } else if (reason2 === "clear") {
        for (const entry of [...state.entries]) remove2(entry);
      } else if (reason2 === "refresh") {
        const entry = at(change);
        if (entry) hooks.refresh?.(entry);
      } else if (reason2 === "moved" || reason2 === "move") {
        const entry = state.entries.splice(change.previousIndex, 1)[0];
        if (entry) {
          state.entries.splice(change.currentIndex, 0, entry);
          hooks.move?.(entry);
        }
      }
    }
    if (state.kind === "cache" && Array.isArray(changes.keys)) {
      const ordered = changes.keys.map((key) => state.byKey.get(key)).filter(Boolean);
      const included = new Set(ordered);
      state.entries = [...ordered, ...state.entries.filter((entry) => !included.has(entry))];
    }
  };
  state.erase = remove2;
  state.dispose = () => {
    for (const entry of [...state.entries]) remove2(entry);
  };
  return state;
}
var notifications = /* @__PURE__ */ new WeakMap();
var proxyTargets = /* @__PURE__ */ new WeakMap();
var objectProxies = /* @__PURE__ */ new WeakMap();
function propertyEvents(item) {
  const target = proxyTargets.get(item) ?? item;
  if (target == null || typeof target !== "object" && typeof target !== "function") throw new TypeError("Property notifications require an object");
  let subject = notifications.get(target);
  if (!subject) notifications.set(target, subject = new Subject2());
  return subject;
}
function readProperty(item, property) {
  if (typeof property === "function") return property(item);
  if (property == null) return item;
  return String(property).split(".").reduce((value, part) => value?.[part], item);
}
function notifyPropertyChanged(item, propertyName, previous) {
  propertyEvents(item).next({ sender: item, propertyName, value: readProperty(item, propertyName), previous });
}
function createObservableObject(item) {
  if (proxyTargets.has(item)) return item;
  if (objectProxies.has(item)) return objectProxies.get(item);
  const proxy = new Proxy(item, {
    set(target, property, value) {
      const previous = target[property];
      const result = Reflect.set(target, property, value);
      if (result && !Object.is(previous, value)) notifyPropertyChanged(proxy, property, previous);
      return result;
    },
    deleteProperty(target, property) {
      const existed = Reflect.has(target, property);
      const previous = target[property];
      const result = Reflect.deleteProperty(target, property);
      if (result && existed) notifyPropertyChanged(proxy, property, previous);
      return result;
    }
  });
  proxyTargets.set(proxy, item);
  objectProxies.set(item, proxy);
  return proxy;
}
var observableObject = createObservableObject;
function changedProperty(item, property, notifyInitial = true) {
  if (typeof property === "string" && property.includes(".")) {
    const parts = property.split(".");
    return new Observable3((observer) => {
      let connections = new Subscription3(), currentValue = readProperty(item, property);
      const rewire = () => {
        connections.unsubscribe();
        connections = new Subscription3();
        let current = item;
        for (let depth = 0; depth < parts.length; depth++) {
          if (current == null || typeof current !== "object" && typeof current !== "function") break;
          const part = parts[depth], remaining = parts.slice(depth).join(".");
          connections.add(propertyEvents(current).subscribe({
            next(event) {
              if (event.propertyName != null && event.propertyName !== part && event.propertyName !== remaining) return;
              const previous = currentValue;
              try {
                currentValue = readProperty(item, property);
                rewire();
                observer.next({ sender: item, propertyName: property, value: currentValue, previous });
              } catch (error) {
                observer.error(error);
              }
            },
            error: (error) => observer.error(error)
          }));
          current = current[part];
        }
      };
      try {
        rewire();
        if (notifyInitial) observer.next({ sender: item, propertyName: property, value: currentValue, previous: void 0 });
      } catch (error) {
        observer.error(error);
      }
      return () => connections.unsubscribe();
    });
  }
  return new Observable3((observer) => {
    const sub = propertyEvents(item).subscribe({ next(event) {
      if (typeof property === "function" || property == null || event.propertyName === property) observer.next({ sender: item, propertyName: property, value: readProperty(item, property), previous: event.previous });
    }, error: (error) => observer.error(error), complete: () => observer.complete() });
    if (notifyInitial) observer.next({ sender: item, propertyName: property, value: readProperty(item, property), previous: void 0 });
    return sub;
  });
}
function whenPropertyChanged(itemOrChanges, property, notifyInitial = true) {
  return isObservable2(itemOrChanges) ? itemOrChanges.pipe(mergeMany((item) => changedProperty(item, property, notifyInitial))) : changedProperty(itemOrChanges, property, notifyInitial);
}
function observeProperty(itemOrChanges, property, notifyInitial = true) {
  return whenPropertyChanged(itemOrChanges, property, notifyInitial).pipe(map((event) => event.value), distinctUntilChanged());
}
var whenValueChanged = observeProperty;
function autoRefreshOnObservable(selector, options = {}) {
  if (typeof options === "number") options = { buffer: options };
  return (source) => new Observable3((observer) => {
    let state, processing = false, pending = [];
    const flush = () => {
      const live = pending.filter((entry) => entry.active);
      pending = [];
      if (live.length) observer.next(cs([...new Set(live)].map((entry) => ({ reason: "refresh", current: entry.item, key: entry.key, currentIndex: state.kind === "list" ? state.entries.indexOf(entry) : -1 })), state.kind));
    };
    const resources = new Subscription3();
    let timer2;
    const refresh2 = (entry) => {
      pending.push(entry);
      if (processing) return;
      if (options.buffer > 0) {
        if (!timer2) {
          timer2 = (options.scheduler ?? asyncScheduler).schedule(() => {
            timer2 = null;
            flush();
          }, options.buffer);
          resources.add(timer2);
        }
      } else flush();
    };
    state = tracker({ add(entry) {
      entry.subscription.add(from(selector(entry.item, entry.key)).subscribe({ next: () => refresh2(entry), error: (error) => observer.error(error) }));
    } });
    resources.add(() => state.dispose());
    resources.add(source.subscribe({ next(changes) {
      try {
        processing = true;
        state.apply(changes);
        observer.next(changes);
        processing = false;
        if (pending.length) {
          if (options.buffer > 0) {
            const entry = pending.pop();
            refresh2(entry);
          } else flush();
        }
      } catch (error) {
        observer.error(error);
      }
    }, error: (error) => observer.error(error), complete() {
      flush();
      observer.complete();
    } }));
    return resources;
  });
}
function autoRefresh(property, options = {}) {
  if (property && typeof property === "object") {
    options = property;
    property = void 0;
  }
  return autoRefreshOnObservable((item) => {
    const events = changedProperty(item, property, false);
    return options.throttle > 0 ? events.pipe(debounceTime(options.throttle, options.scheduler ?? asyncScheduler)) : events;
  }, options);
}
function observableProjection(selector, mode, options = {}) {
  return (source) => new Observable3((observer) => {
    const resources = new Subscription3();
    let state, processing = false, done = false, output = [];
    const pending = /* @__PURE__ */ new Set();
    const reconcile = () => {
      if (processing || observer.closed) return;
      const desired = state.entries.filter((entry) => mode === "filter" ? entry.value === true : entry.hasValue).map((entry) => ({ entry, value: mode === "filter" ? entry.item : entry.value }));
      const changes = [];
      if (state.kind === "cache") {
        const next = new Map(desired.map((record) => [record.entry.key, record]));
        const old = new Map(output.map((record) => [record.entry.key, record]));
        for (const record of output) if (!next.has(record.entry.key)) changes.push({ reason: "remove", key: record.entry.key, current: record.value });
        const work = output.filter((record) => next.has(record.entry.key)).map((record) => record.entry.key);
        for (let index = 0; index < desired.length; index++) {
          const record = desired[index], key = record.entry.key, previous = old.get(key);
          if (!previous) {
            changes.push({ reason: "add", key, current: record.value, currentIndex: index });
            work.splice(index, 0, key);
          } else {
            const previousIndex = work.indexOf(key);
            if (previousIndex !== index) {
              changes.push({ reason: "move", key, current: previous.value, previousIndex, currentIndex: index });
              work.splice(index, 0, work.splice(previousIndex, 1)[0]);
            }
            if (!Object.is(previous.value, record.value)) changes.push({ reason: "update", key, current: record.value, previous: previous.value, currentIndex: index, previousIndex: index });
            else if (record.entry.refreshed) changes.push({ reason: "refresh", key, current: record.value, currentIndex: index });
          }
        }
      } else {
        const replacements = new Map(desired.filter((record) => record.entry.prior).map((record) => [record.entry.prior, record.entry]));
        const work = output.map((record) => replacements.has(record.entry) ? { ...record, entry: replacements.get(record.entry) } : record);
        const wanted = new Set(desired.map((record) => record.entry));
        for (let index = work.length - 1; index >= 0; index--) if (!wanted.has(work[index].entry)) {
          changes.push({ reason: "remove", current: work[index].value, currentIndex: index });
          work.splice(index, 1);
        }
        for (let index = 0; index < desired.length; index++) {
          const record = desired[index];
          let found = work.findIndex((value) => value.entry === record.entry);
          if (found < 0) {
            changes.push({ reason: "add", current: record.value, currentIndex: index });
            work.splice(index, 0, record);
          } else {
            if (found !== index) {
              changes.push({ reason: "move", current: work[found].value, previousIndex: found, currentIndex: index });
              work.splice(index, 0, work.splice(found, 1)[0]);
            }
            if (!Object.is(work[index].value, record.value)) {
              changes.push({ reason: "replace", current: record.value, previous: work[index].value, currentIndex: index, previousIndex: index });
              work[index] = record;
            } else if (record.entry.refreshed) changes.push({ reason: "refresh", current: record.value, currentIndex: index });
          }
        }
      }
      output = desired;
      for (const entry of state.entries) entry.refreshed = false;
      if (changes.length) {
        const result = cs(changes, state.kind);
        if (state.kind === "cache") {
          result.items = desired.map((record) => record.value);
          result.keys = desired.map((record) => record.entry.key);
        }
        observer.next(result);
      }
      if (done && (!options.waitForCompletion || !pending.size)) observer.complete();
    };
    const start = (entry, previous) => {
      if (mode === "transform" && entry.prior?.hasValue) {
        entry.value = entry.prior.value;
        entry.hasValue = true;
      }
      pending.add(entry);
      entry.subscription.add(() => {
        pending.delete(entry);
        entry.controller?.abort();
      });
      entry.controller = new AbortController();
      let observable2;
      try {
        const selected = selector(entry.item, entry.key, previous, entry.controller.signal);
        observable2 = isObservable2(selected) || selected?.then ? from(selected) : of(selected);
      } catch (error) {
        if (options.onError) {
          options.onError({ error, item: entry.item, key: entry.key });
          pending.delete(entry);
          return;
        }
        throw error;
      }
      entry.subscription.add(observable2.subscribe({ next(value) {
        if (entry.active) {
          entry.value = value;
          entry.hasValue = true;
          reconcile();
        }
      }, error(error) {
        pending.delete(entry);
        if (options.onError) {
          options.onError({ error, item: entry.item, key: entry.key });
          reconcile();
        } else observer.error(error);
      }, complete() {
        pending.delete(entry);
        reconcile();
      } }));
    };
    state = tracker({
      add: start,
      refresh(entry) {
        if (options.transformOnRefresh) {
          entry.subscription.unsubscribe();
          entry.subscription = new Subscription3();
          start(entry, entry.item);
        } else entry.refreshed = true;
      },
      remove() {
      }
    });
    resources.add(() => state.dispose());
    resources.add(source.subscribe({ next(changes) {
      try {
        processing = true;
        state.apply(changes);
        processing = false;
        reconcile();
      } catch (error) {
        observer.error(error);
      }
    }, error: (error) => observer.error(error), complete() {
      done = true;
      reconcile();
    } }));
    return resources;
  });
}
function filterOnObservable(selector, options = {}) {
  return observableProjection(selector, "filter", options);
}
function transformOnObservable(selector, options = {}) {
  return observableProjection(selector, "transform", options);
}
function transformAsync(factory, options = {}) {
  const maximum2 = options.maximumConcurrency ?? options.MaximumConcurrency ?? Infinity;
  if (!(maximum2 === Infinity || Number.isInteger(maximum2) && maximum2 > 0)) throw new RangeError("Maximum concurrency must be a positive integer");
  return (source) => new Observable3((observer) => {
    let running = 0, draining = false;
    const waiting = [];
    const drain = () => {
      if (draining) return;
      draining = true;
      try {
        while (running < maximum2 && waiting.length) {
          const job = waiting.shift();
          if (!job.closed) job.start();
        }
      } finally {
        draining = false;
      }
    };
    const queued = (...arguments_) => new Observable3((destination) => {
      let inner, started = false, finished = false;
      const finish = () => {
        if (finished) return;
        finished = true;
        if (started) running--;
        drain();
      };
      const job = { closed: false, start() {
        started = true;
        running++;
        try {
          const value = factory(...arguments_);
          inner = (isObservable2(value) || value?.then ? from(value) : of(value)).subscribe({ next: (value2) => destination.next(value2), error(error) {
            finish();
            destination.error(error);
          }, complete() {
            finish();
            destination.complete();
          } });
        } catch (error) {
          finish();
          destination.error(error);
        }
      } };
      waiting.push(job);
      drain();
      return () => {
        job.closed = true;
        inner?.unsubscribe();
        finish();
      };
    });
    return source.pipe(observableProjection(queued, "transform", { ...options, transformOnRefresh: options.transformOnRefresh ?? options.TransformOnRefresh, waitForCompletion: true })).subscribe(observer);
  });
}
function transformSafeAsync(factory, onError, options = {}) {
  return transformAsync(factory, { ...options, onError });
}
function mergeMany(selector) {
  return (source) => new Observable3((observer) => {
    const resources = new Subscription3();
    const state = tracker({ add(entry) {
      entry.subscription.add(from(selector(entry.item, entry.key)).subscribe({ next: (value) => observer.next(value), error: (error) => observer.error(error) }));
    } });
    resources.add(() => state.dispose());
    resources.add(source.subscribe({ next(changes) {
      try {
        state.apply(changes);
      } catch (error) {
        observer.error(error);
      }
    }, error: (error) => observer.error(error), complete: () => observer.complete() }));
    return resources;
  });
}
function mergeManyItems(selector) {
  return mergeMany((item, key) => from(selector(item, key)).pipe(map((value) => ({ item, key, value }))));
}
function subscribeMany(selector) {
  return (source) => new Observable3((observer) => {
    const resources = new Subscription3();
    const state = tracker({ add(entry) {
      const subscription = selector(entry.item, entry.key);
      if (subscription) entry.subscription.add(() => release(subscription));
    } });
    resources.add(() => state.dispose());
    resources.add(source.subscribe({ next(changes) {
      try {
        state.apply(changes);
        observer.next(changes);
      } catch (error) {
        observer.error(error);
      }
    }, error: (error) => observer.error(error), complete: () => observer.complete() }));
    return resources;
  });
}
function disposeMany(disposer = release) {
  return disposalOperator(disposer);
}
function onItem(reason2, action) {
  return (source) => new Observable3((observer) => source.subscribe({ next(changes) {
    try {
      for (const change of getChanges(changes)) {
        if (change.reason === reason2 || reason2 === "update" && change.reason === "replace") {
          if (reason2 === "update") action(change.current, change.previous, change.key);
          else action(change.current, change.key);
        } else if (reason2 === "add" && change.reason === "addRange" || reason2 === "remove" && (change.reason === "removeRange" || change.reason === "clear")) {
          for (const item of change.range?.items ?? change.current ?? []) action(item);
        }
      }
      observer.next(changes);
    } catch (error) {
      observer.error(error);
    }
  }, error: (error) => observer.error(error), complete: () => observer.complete() }));
}
var onItemAdded = (action) => onItem("add", action);
var onItemUpdated = (action) => onItem("update", action);
var onItemRemoved = (action) => onItem("remove", action);
var onItemRefreshed = (action) => onItem("refresh", action);
function notEmpty() {
  return rxFilter((changes) => getChanges(changes).length > 0);
}
function skipInitial() {
  return skip(1);
}
function deferUntilLoaded() {
  return (source) => new Observable3((observer) => {
    let loaded = false;
    return source.subscribe({ next(changes) {
      if (getChanges(changes).length > 0) loaded = true;
      if (loaded) observer.next(changes);
    }, error: (error) => observer.error(error), complete: () => observer.complete() });
  });
}
function watch(key) {
  return (source) => source.pipe(map((changes) => [...getChanges(changes)].filter((change) => Object.is(change.key, key))), rxFilter((changes) => changes.length > 0), mergeChangeRecords());
}
function mergeChangeRecords() {
  return (source) => new Observable3((observer) => source.subscribe({ next(changes) {
    for (const change of changes) observer.next(change);
  }, error: (error) => observer.error(error), complete: () => observer.complete() }));
}
function batch(duration = 16, scheduler = asyncScheduler) {
  if (!(duration > 0)) throw new RangeError("Batch duration must be positive");
  return (source) => source.pipe(bufferTime(duration, scheduler), rxFilter((sets) => sets.some((changes) => getChanges(changes).length)), map((sets) => cs(sets.flatMap((changes) => [...getChanges(changes)]), sets[0]?.kind)));
}
function bufferIf(pause, options = {}) {
  if (typeof options === "boolean") options = { initialPauseState: options };
  return (source) => new Observable3((observer) => {
    let paused = options.initialPauseState ?? false, sets = [], timeout;
    const resources = new Subscription3();
    const flush = () => {
      timeout?.unsubscribe();
      timeout = void 0;
      if (sets.length) {
        const current = sets;
        sets = [];
        observer.next(cs(current.flatMap((changes) => [...getChanges(changes)]), current[0].kind));
      }
    };
    const armTimeout = () => {
      if (paused && options.timeout != null && !timeout) {
        timeout = (options.scheduler ?? asyncScheduler).schedule(() => {
          paused = false;
          flush();
        }, options.timeout);
        resources.add(timeout);
      }
    };
    armTimeout();
    resources.add(pause.subscribe({ next(value) {
      paused = !!value;
      if (!paused) flush();
      else armTimeout();
    }, error: (error) => observer.error(error) }));
    resources.add(source.subscribe({ next(changes) {
      if (paused) sets.push(changes);
      else observer.next(changes);
    }, error: (error) => observer.error(error), complete() {
      flush();
      observer.complete();
    } }));
    return resources;
  });
}
function expireAfter(selector, options = {}, sourceOptions = {}) {
  if (selector?.connect && typeof selector.edit === "function") return expireSource(selector, options, sourceOptions);
  if (options?.schedule) options = { scheduler: options };
  const scheduler = options.scheduler ?? asyncScheduler;
  return (source) => new Observable3((observer) => {
    const resources = new Subscription3();
    let state, processing = false, output = [], done = false;
    const reconcile = () => {
      if (processing) return;
      const desired = state.entries.filter((entry) => !entry.expired);
      const changes = [];
      if (state.kind === "cache") {
        const next = new Map(desired.map((entry) => [entry.key, entry]));
        const old = new Map(output.map((entry) => [entry.key, entry]));
        for (const entry of output) if (!next.has(entry.key)) changes.push({ reason: "remove", key: entry.key, current: entry.item });
        for (const entry of desired) {
          const previous = old.get(entry.key);
          if (!previous) changes.push({ reason: "add", key: entry.key, current: entry.item });
          else if (previous !== entry) changes.push({ reason: "update", key: entry.key, current: entry.item, previous: previous.item });
          else if (entry.refreshed) changes.push({ reason: "refresh", key: entry.key, current: entry.item });
        }
      } else {
        const work = output.slice();
        const keep = new Set(desired);
        for (let index = work.length - 1; index >= 0; index--) if (!keep.has(work[index])) {
          changes.push({ reason: "remove", current: work[index].item, currentIndex: index });
          work.splice(index, 1);
        }
        for (let index = 0; index < desired.length; index++) {
          const entry = desired[index];
          const previousIndex = work.indexOf(entry);
          if (previousIndex < 0) {
            changes.push({ reason: "add", current: entry.item, currentIndex: index });
            work.splice(index, 0, entry);
          } else if (previousIndex !== index) {
            changes.push({ reason: "move", current: entry.item, previousIndex, currentIndex: index });
            work.splice(index, 0, work.splice(previousIndex, 1)[0]);
          } else if (entry.refreshed) changes.push({ reason: "refresh", current: entry.item, currentIndex: index });
        }
      }
      output = desired;
      for (const entry of state.entries) entry.refreshed = false;
      if (changes.length) observer.next(cs(changes, state.kind));
      if (done) observer.complete();
    };
    state = tracker({ add(entry) {
      const duration = typeof selector === "function" ? selector(entry.item, entry.key) : selector;
      if (duration != null && Number.isFinite(duration)) {
        entry.timer = scheduler.schedule(() => {
          entry.expired = true;
          reconcile();
          if (state.kind === "cache") state.erase(entry);
        }, Math.max(0, duration));
        entry.subscription.add(entry.timer);
      }
    }, refresh(entry) {
      entry.refreshed = true;
    } });
    resources.add(() => state.dispose());
    resources.add(source.subscribe({ next(changes) {
      try {
        processing = true;
        state.apply(changes);
        processing = false;
        reconcile();
      } catch (error) {
        observer.error(error);
      }
    }, error: (error) => observer.error(error), complete() {
      done = true;
      reconcile();
    } }));
    return resources;
  });
}
function limitSizeTo(size, sourceSize, sourceOptions = {}) {
  if (size?.connect && typeof size.edit === "function") return limitSourceSize(size, sourceSize, sourceOptions);
  if (!Number.isInteger(size) || size <= 0) throw new RangeError("Size limit must be a positive integer");
  return (source) => new Observable3((observer) => {
    let state, output = [], order = 0;
    state = tracker({ add(entry) {
      entry.order = entry.prior?.order ?? order++;
    }, refresh(entry) {
      entry.refreshed = true;
    } });
    const sub = source.subscribe({ next(changes) {
      try {
        state.apply(changes);
        const live = state.entries.filter((entry) => !entry.evicted);
        const selected = new Set([...live].sort((a, b) => b.order - a.order).slice(0, size));
        for (const entry of live) if (!selected.has(entry)) {
          entry.evicted = true;
          if (state.kind === "cache") state.erase(entry);
        }
        const desired = state.entries.filter((entry) => selected.has(entry));
        const result = [];
        if (state.kind === "cache") {
          const next = new Map(desired.map((entry) => [entry.key, entry]));
          const old = new Map(output.map((entry) => [entry.key, entry]));
          for (const entry of output) if (!next.has(entry.key)) result.push({ reason: "remove", key: entry.key, current: entry.item });
          for (const entry of desired) {
            const previous = old.get(entry.key);
            if (!previous) result.push({ reason: "add", key: entry.key, current: entry.item });
            else if (previous !== entry) result.push({ reason: "update", key: entry.key, current: entry.item, previous: previous.item });
            else if (entry.refreshed) result.push({ reason: "refresh", key: entry.key, current: entry.item });
          }
        } else {
          const work = output.slice();
          for (let index = work.length - 1; index >= 0; index--) if (!selected.has(work[index])) {
            result.push({ reason: "remove", current: work[index].item, currentIndex: index });
            work.splice(index, 1);
          }
          for (let index = 0; index < desired.length; index++) {
            const entry = desired[index];
            const previousIndex = work.indexOf(entry);
            if (previousIndex < 0) {
              result.push({ reason: "add", current: entry.item, currentIndex: index });
              work.splice(index, 0, entry);
            } else if (previousIndex !== index) {
              result.push({ reason: "move", current: entry.item, previousIndex, currentIndex: index });
              work.splice(index, 0, work.splice(previousIndex, 1)[0]);
            }
          }
        }
        output = desired;
        for (const entry of state.entries) entry.refreshed = false;
        if (result.length) observer.next(cs(result, state.kind));
      } catch (error) {
        observer.error(error);
      }
    }, error: (error) => observer.error(error), complete: () => observer.complete() });
    return () => {
      sub.unsubscribe();
      state.dispose();
    };
  });
}
function expireSource(source, selector, options = {}) {
  if (options?.schedule) options = { scheduler: options };
  const scheduler = options.scheduler ?? asyncScheduler;
  if (options.pollingInterval != null && !(options.pollingInterval > 0)) throw new RangeError("Polling interval must be positive");
  return new Observable3((observer) => {
    const resources = new Subscription3();
    let state;
    const expireEntries = (candidates) => {
      if (observer.closed || source.isDisposed) return;
      try {
        source.edit((updater) => {
          const live = candidates.filter((entry) => entry.active && state.entries.includes(entry));
          if (!live.length) return;
          if (source.kind === "cache") {
            const expired = live.map((entry) => [entry.key, entry.item]);
            updater.removeKeys(expired.map(([key]) => key));
            observer.next(expired);
          } else {
            const expired = live.map((entry) => ({ entry, index: state.entries.indexOf(entry) })).sort((a, b) => b.index - a.index);
            for (const record of expired) updater.removeAt(record.index);
            observer.next(expired.reverse().map((record) => record.entry.item));
          }
        });
      } catch (error) {
        observer.error(error);
      }
    };
    state = tracker({ add(entry) {
      const duration = typeof selector === "function" ? selector(entry.item, entry.key) : selector;
      if (duration == null || duration === Infinity) return;
      if (!Number.isFinite(duration)) throw new RangeError("Expiry duration must be finite, null or Infinity");
      entry.expiresAt = scheduler.now() + Math.max(0, duration);
      if (options.pollingInterval == null) entry.subscription.add(scheduler.schedule(() => expireEntries([entry]), Math.max(0, duration)));
    } });
    resources.add(() => state.dispose());
    if (options.pollingInterval != null) resources.add(scheduler.schedule(function poll() {
      expireEntries(state.entries.filter((entry) => entry.expiresAt <= scheduler.now()));
      if (!observer.closed) this.schedule(void 0, options.pollingInterval);
    }, options.pollingInterval));
    resources.add(source.connect().subscribe({ next(changes) {
      try {
        state.apply(changes);
      } catch (error) {
        observer.error(error);
      }
    }, error: (error) => observer.error(error), complete: () => observer.complete() }));
    return resources;
  });
}
function limitSourceSize(source, size, options = {}) {
  if (!Number.isInteger(size) || size <= 0) throw new RangeError("Size limit must be a positive integer");
  if (options?.schedule) options = { scheduler: options };
  return new Observable3((observer) => {
    const resources = new Subscription3();
    let scheduled;
    const enforce = () => {
      scheduled = void 0;
      if (observer.closed || source.isDisposed || source.count <= size) return;
      try {
        source.edit((updater) => {
          const excess = source.count - size;
          if (excess <= 0) return;
          if (source.kind === "cache") {
            const removed = source.keys.slice(0, excess).map((key) => [key, source.lookup(key).value]);
            updater.removeKeys(removed.map(([key]) => key));
            observer.next(removed);
          } else {
            const removed = source.items.slice(0, excess);
            updater.removeRange(0, excess);
            observer.next(removed);
          }
        });
      } catch (error) {
        observer.error(error);
      }
    };
    resources.add(source.connect().subscribe({ next() {
      if (source.count <= size) return;
      if (options.scheduler) {
        if (!scheduled) {
          scheduled = options.scheduler.schedule(enforce);
          resources.add(scheduled);
        }
      } else enforce();
    }, error: (error) => observer.error(error), complete: () => observer.complete() }));
    return resources;
  });
}
function toObservableChangeSet(keySelectorOrOptions, maybeOptions = {}) {
  const keySelector = typeof keySelectorOrOptions === "function" ? keySelectorOrOptions : keySelectorOrOptions?.keySelector;
  const options = typeof keySelectorOrOptions === "function" ? maybeOptions : keySelectorOrOptions ?? {};
  return (source) => new Observable3((observer) => {
    const collection = keySelector ? new SourceCache(keySelector) : new SourceList();
    const resources = new Subscription3();
    resources.add(() => collection.dispose());
    resources.add(collection.connect().subscribe(observer));
    if (options.limitSize != null) resources.add(limitSourceSize(collection, options.limitSize).subscribe({ error: (error) => observer.error(error) }));
    if (options.expireAfter != null) resources.add(expireSource(collection, options.expireAfter, { scheduler: options.scheduler, pollingInterval: options.pollingInterval }).subscribe({ error: (error) => observer.error(error) }));
    resources.add(source.subscribe({
      next(value) {
        try {
          const items = !options.singleItem && value != null && typeof value !== "string" && typeof value[Symbol.iterator] === "function" ? [...value] : [value];
          if (keySelector) collection.addOrUpdate(items);
          else collection.addRange(items);
        } catch (error) {
          observer.error(error);
        }
      },
      error: (error) => observer.error(error),
      complete: () => collection.dispose()
    }));
    return resources;
  });
}
function createChangeSet(subscribe, keySelector) {
  return new Observable3((observer) => {
    const source = keySelector ? new SourceCache(keySelector) : new SourceList();
    const resources = new Subscription3();
    resources.add(source.connect().subscribe(observer));
    resources.add(() => source.dispose());
    try {
      const cleanup = subscribe(source);
      if (cleanup?.then) {
        let disposed = false;
        resources.add(() => {
          disposed = true;
        });
        Promise.resolve(cleanup).then((value) => {
          if (disposed) release(value);
          else if (value) resources.add(() => release(value));
        }, (error) => observer.error(error));
      } else if (cleanup) resources.add(() => release(cleanup));
    } catch (error) {
      observer.error(error);
    }
    return resources;
  });
}
var ObservableChangeSet = Object.freeze({ create: createChangeSet, Create: createChangeSet, createCache: (subscribe, keySelector) => createChangeSet(subscribe, keySelector), CreateCache: (subscribe, keySelector) => createChangeSet(subscribe, keySelector), createList: (subscribe) => createChangeSet(subscribe), CreateList: (subscribe) => createChangeSet(subscribe) });
function disposalOperator(disposer, completedAccessor) {
  return (source) => new Observable3((observer) => {
    const completion = new ReplaySubject(1);
    const resources = new Subscription3();
    let queue = [], pending = 0, ended = false;
    completedAccessor?.(completion.asObservable());
    const check = () => {
      if (ended && pending === 0 && !completion.isStopped) {
        completion.next(void 0);
        completion.complete();
      }
    };
    const dispose = (item) => {
      try {
        const result = disposer(item);
        if (result?.then) {
          pending++;
          Promise.resolve(result).then(() => {
            pending--;
            check();
          }, (error) => {
            pending--;
            completion.error(error);
          });
        }
      } catch (error) {
        if (completedAccessor) completion.error(error);
        else throw error;
      }
    };
    const state = tracker({ remove: (entry) => queue.push(entry) });
    const flush = () => {
      const removed = queue;
      queue = [];
      for (const entry of removed) if (!state.entries.some((current) => current.prior === entry && Object.is(current.item, entry.item))) dispose(entry.item);
    };
    resources.add(() => {
      state.dispose();
      flush();
      ended = true;
      check();
    });
    resources.add(source.subscribe({ next(changes) {
      try {
        state.apply(changes);
        observer.next(changes);
        flush();
      } catch (error) {
        observer.error(error);
      }
    }, error: (error) => observer.error(error), complete: () => observer.complete() }));
    return resources;
  });
}
function asyncDisposeMany(completedAccessor, disposer = (item) => item?.[Symbol.asyncDispose] ? item[Symbol.asyncDispose]() : item?.disposeAsync ? item.disposeAsync() : release(item)) {
  return disposalOperator(disposer, completedAccessor);
}
var batchIf = bufferIf;
function bufferInitial(duration, scheduler = asyncScheduler) {
  if (!(duration >= 0)) throw new RangeError("Initial duration must be non-negative");
  return (source) => new Observable3((observer) => {
    let started = false, ready = false, sets = [];
    const resources = new Subscription3();
    const flush = () => {
      ready = true;
      if (sets.length) {
        const current = sets;
        sets = [];
        observer.next(cs(current.flatMap((changes) => [...getChanges(changes)]), current[0].kind));
      }
    };
    resources.add(source.subscribe({ next(changes) {
      if (ready) observer.next(changes);
      else if (getChanges(changes).length) {
        sets.push(changes);
        if (!started) {
          started = true;
          resources.add(scheduler.schedule(flush, duration));
        }
      }
    }, error: (error) => observer.error(error), complete() {
      flush();
      observer.complete();
    } }));
    return resources;
  });
}
function filterOnProperty(property, predicate, options = {}) {
  return (source) => source.pipe(autoRefresh(property, options), filter(predicate));
}
var finallySafe = (action) => finalize(action);
var ConnectionStatus = Object.freeze({ Pending: "pending", Loaded: "loaded", Errored: "errored", Completed: "completed" });
function monitorStatus() {
  return (source) => new Observable3((observer) => {
    let loaded = false;
    observer.next(ConnectionStatus.Pending);
    return source.subscribe({ next() {
      if (!loaded) {
        loaded = true;
        observer.next(ConnectionStatus.Loaded);
      }
    }, error() {
      observer.next(ConnectionStatus.Errored);
      observer.complete();
    }, complete() {
      observer.next(ConnectionStatus.Completed);
      observer.complete();
    } });
  });
}
var watchValue = (key) => (source) => (typeof source.connect === "function" ? source.connect() : source).pipe(watch(key), map((change) => change.current));
function whenAnyPropertyChanged(itemOrChanges, ...propertyNames) {
  const observe = (item) => propertyEvents(item).pipe(rxFilter((event) => !propertyNames.length || propertyNames.includes(event.propertyName)), map(() => item));
  return isObservable2(itemOrChanges) ? itemOrChanges.pipe(mergeMany(observe)) : observe(itemOrChanges);
}
function whenChanged(item, properties, resultSelector) {
  const fields = Array.isArray(properties) ? properties : [properties];
  return combineLatest(fields.map((property) => observeProperty(item, property))).pipe(map((values) => resultSelector ? resultSelector(item, ...values) : values.length === 1 ? values[0] : values));
}
function observeCollectionChanges(collection, eventName = "collectionchange") {
  const events = typeof collection.connect === "function" ? collection.connect() : isObservable2(collection) ? collection : fromEvent(collection, eventName);
  return events.pipe(map((event) => ({ sender: collection, eventArgs: event?.detail ?? event })));
}
function trueFor(selector, condition, all) {
  return (source) => new Observable3((observer) => {
    let state, processing = false, previous;
    const resources = new Subscription3();
    const evaluate = () => {
      if (processing) return;
      try {
        const accepts = (entry) => entry.hasValue && !!(condition.length >= 2 ? condition(entry.item, entry.value) : condition(entry.value));
        const result = all ? state.entries.every(accepts) : state.entries.some(accepts);
        if (result !== previous) {
          previous = result;
          observer.next(result);
        }
      } catch (error) {
        observer.error(error);
      }
    };
    state = tracker({ add(entry) {
      entry.subscription.add(from(selector(entry.item, entry.key)).subscribe({ next(value) {
        entry.value = value;
        entry.hasValue = true;
        evaluate();
      }, error: (error) => observer.error(error) }));
    } });
    resources.add(() => state.dispose());
    resources.add(source.subscribe({ next(changes) {
      try {
        processing = true;
        state.apply(changes);
        processing = false;
        evaluate();
      } catch (error) {
        observer.error(error);
      }
    }, error: (error) => observer.error(error), complete: () => observer.complete() }));
    return resources;
  });
}
var trueForAll = (selector, condition = Boolean) => trueFor(selector, condition, true);
var trueForAny = (selector, condition = Boolean) => trueFor(selector, condition, false);
function transformManyAsync(factory, keySelector, options = {}) {
  return (source) => source.pipe(transformAsync(factory, options), transformMany((items) => items, keySelector));
}
function transformManySafeAsync(factory, keySelector, onError, options = {}) {
  return transformManyAsync(factory, keySelector, { ...options, onError });
}
function refCount() {
  return (source) => {
    let session;
    return new Observable3((observer) => {
      if (!session) session = { subject: new Subject2(), references: 0, state: void 0, kind: "cache", upstream: new Subscription3() };
      const current = session;
      current.references++;
      const subscription = current.subject.subscribe(observer);
      if (current.state) {
        const initial = snapshotChanges(current.state, current.kind);
        if (initial.length) observer.next(initial);
      }
      if (current.references === 1 && !current.started) {
        current.started = true;
        current.upstream.add(source.subscribe({ next(changes) {
          current.kind = changes.kind ?? current.kind;
          if (!current.state) current.state = current.kind === "list" ? [] : /* @__PURE__ */ new Map();
          applyChanges(current.state, changes);
          current.subject.next(changes);
        }, error: (error) => current.subject.error(error), complete: () => current.subject.complete() }));
      }
      return () => {
        subscription.unsubscribe();
        if (--current.references === 0) {
          current.upstream.unsubscribe();
          if (session === current) session = void 0;
        }
      };
    });
  };
}
function switchLatest() {
  return (sources) => new Observable3((observer) => {
    const resources = new Subscription3();
    let inner, kind = "cache", state, outerDone = false, innerDone = true, generation = 0;
    const clear2 = () => {
      if (!state) return;
      const removed = kind === "cache" ? [...state].map(([key, current]) => ({ reason: "remove", key, current })) : state.length ? [{ reason: "clear", range: { items: state.slice(), index: 0 } }] : [];
      if (removed.length) observer.next(cs(removed, kind));
      state = void 0;
    };
    resources.add(() => inner?.unsubscribe());
    resources.add(sources.subscribe({ next(source) {
      const current = ++generation;
      inner?.unsubscribe();
      clear2();
      innerDone = false;
      const incoming = typeof source.connect === "function" ? source.connect() : source;
      inner = from(incoming).subscribe({ next(changes) {
        if (current !== generation) return;
        kind = changes.kind ?? kind;
        if (!state) state = kind === "list" ? [] : /* @__PURE__ */ new Map();
        applyChanges(state, changes);
        if (getChanges(changes).length) observer.next(changes);
      }, error: (error) => observer.error(error), complete() {
        if (current !== generation) return;
        innerDone = true;
        if (outerDone) observer.complete();
      } });
    }, error: (error) => observer.error(error), complete() {
      outerDone = true;
      if (innerDone) observer.complete();
    } }));
    return resources;
  });
}

// src/advanced.js
var identity2 = (x) => x;
var UNASSIGNED_GROUP = /* @__PURE__ */ Symbol("unassigned group");
var asStream = (x) => typeof x?.connect === "function" ? x.connect() : typeof x?.Connect === "function" ? x.Connect() : x;
var set = (changes = [], kind = "cache") => new ChangeSet(changes, kind);
var reason = (c) => String(c.reason).toLowerCase();
var same = Object.is;
var optional = (present, value) => present ? Optional.some(value) : Optional.none();
var safe = (observer, action) => {
  try {
    return action();
  } catch (e) {
    observer.error(e);
  }
};
var Group = class {
  constructor(key, kind = "cache") {
    this.key = key;
    this.kind = kind;
    this._data = kind === "cache" ? /* @__PURE__ */ new Map() : [];
    this._subject = new Subject3();
    this.isDisposed = false;
    this.cache = this;
    this.list = this;
  }
  get Key() {
    return this.key;
  }
  get Cache() {
    return this;
  }
  get List() {
    return this;
  }
  get items() {
    return this.kind === "cache" ? [...this._data.values()] : this._data.slice();
  }
  get Items() {
    return this.items;
  }
  get keys() {
    return this.kind === "cache" ? [...this._data.keys()] : this._data.map((_, i) => i);
  }
  get keyValues() {
    return this.kind === "cache" ? [...this._data.entries()] : this._data.map((v, i) => [i, v]);
  }
  get size() {
    return this.kind === "cache" ? this._data.size : this._data.length;
  }
  get count() {
    return this.size;
  }
  get Count() {
    return this.size;
  }
  lookup(key) {
    return optional(this.kind === "cache" ? this._data.has(key) : key >= 0 && key < this.size, this.kind === "cache" ? this._data.get(key) : this._data[key]);
  }
  Lookup(key) {
    return this.lookup(key);
  }
  connect() {
    return new Observable4((observer) => {
      if (this.isDisposed) {
        observer.complete();
        return;
      }
      const initial = this.kind === "cache" ? set([...this._data].map(([key, current]) => ({ reason: "add", key, current }))) : set(this.size ? [{ reason: "addRange", range: { items: this.items, index: 0 } }] : [], "list");
      const sub = this._subject.subscribe(observer);
      observer.next(initial);
      return sub;
    });
  }
  Connect() {
    return this.connect();
  }
  dispose() {
    if (!this.isDisposed) {
      this.isDisposed = true;
      this._subject.complete();
    }
  }
  Dispose() {
    this.dispose();
  }
  [Symbol.iterator]() {
    return this.items[Symbol.iterator]();
  }
};
var ImmutableGroup = class {
  constructor(key, entries, kind = "cache") {
    this.key = key;
    this.kind = kind;
    this._entries = Object.freeze([...entries].map(([k, v]) => Object.freeze([k, v])));
    this.items = Object.freeze(this._entries.map((x) => x[1]));
    this.cache = this;
    this.list = this;
    Object.freeze(this);
  }
  get Key() {
    return this.key;
  }
  get Items() {
    return this.items;
  }
  get Cache() {
    return this;
  }
  get List() {
    return this;
  }
  get keys() {
    return this._entries.map((x) => x[0]);
  }
  get keyValues() {
    return this._entries.slice();
  }
  get size() {
    return this.items.length;
  }
  get count() {
    return this.size;
  }
  get Count() {
    return this.size;
  }
  lookup(key) {
    const entry = this._entries.find((x) => same(x[0], key) || x[0] === key);
    return optional(!!entry, entry?.[1]);
  }
  Lookup(key) {
    return this.lookup(key);
  }
  connect() {
    return of2(this.kind === "cache" ? set(this._entries.map(([key, current]) => ({ reason: "add", key, current }))) : set(this.size ? [{ reason: "addRange", range: { items: this.items.slice(), index: 0 } }] : [], "list"));
  }
  Connect() {
    return this.connect();
  }
  [Symbol.iterator]() {
    return this.items[Symbol.iterator]();
  }
};
function grouping(selector, regrouper, immutable = false) {
  if (typeof selector !== "function" && !isObservable3(selector)) throw new TypeError("A group selector function or observable is required");
  return (source) => new Observable4((observer) => {
    const subscriptions = new Subscription4();
    const groups = /* @__PURE__ */ new Map(), membership = /* @__PURE__ */ new Map(), data = /* @__PURE__ */ new Map(), snapshots = /* @__PURE__ */ new Map();
    let currentSelector = typeof selector === "function" ? selector : null, kind = "cache", listRecords = [];
    const listGroups = /* @__PURE__ */ new Map();
    const publish = (before, touched, pending) => {
      for (const [key, changes] of pending) if (changes.length) groups.get(key)?._subject.next(set(changes, kind));
      const out = [];
      for (const [key, group2] of groups) {
        if (!group2.size) {
          if (before.has(key)) out.push({ reason: "remove", key, current: immutable ? snapshots.get(key) : group2 });
          groups.delete(key);
          snapshots.delete(key);
          group2.dispose();
        } else if (!before.has(key) || immutable && touched.has(key)) {
          const value = immutable ? new ImmutableGroup(key, group2.keyValues, kind) : group2;
          out.push({ reason: before.has(key) ? "update" : "add", key, current: value, ...before.has(key) ? { previous: snapshots.get(key) } : {} });
          snapshots.set(key, value);
        }
      }
      if (out.length) observer.next(set(out));
    };
    const runCache = (changes) => {
      const before = new Set(groups.keys()), touched = /* @__PURE__ */ new Set(), pending = /* @__PURE__ */ new Map();
      const record = (groupKey, change) => {
        let group2 = groups.get(groupKey);
        if (!group2) {
          group2 = new Group(groupKey);
          groups.set(groupKey, group2);
        }
        if (!pending.has(groupKey)) pending.set(groupKey, []);
        pending.get(groupKey).push(change);
        touched.add(groupKey);
        if (change.reason === "remove") group2._data.delete(change.key);
        else group2._data.set(change.key, change.current);
      };
      for (const c of changes) {
        const key = c.key, oldGroupKey = membership.get(key), had = membership.has(key), r = reason(c);
        if (r === "remove") {
          data.delete(key);
          if (had) {
            record(oldGroupKey, { reason: "remove", key, current: groups.get(oldGroupKey)._data.get(key) });
            membership.delete(key);
          }
          continue;
        }
        if (r === "clear") {
          for (const [oldKey, item] of data) if (membership.has(oldKey)) record(membership.get(oldKey), { reason: "remove", key: oldKey, current: item });
          data.clear();
          membership.clear();
          continue;
        }
        data.set(key, c.current);
        if (!currentSelector) continue;
        const groupKey = currentSelector(c.current, key);
        if (groupKey === UNASSIGNED_GROUP) {
          if (had) {
            record(oldGroupKey, { reason: "remove", key, current: groups.get(oldGroupKey)._data.get(key) });
            membership.delete(key);
          }
          continue;
        }
        if (had && !same(groupKey, oldGroupKey)) record(oldGroupKey, { reason: "remove", key, current: groups.get(oldGroupKey)._data.get(key) });
        const unchanged = had && same(groupKey, oldGroupKey);
        const previous = unchanged ? groups.get(groupKey)._data.get(key) : void 0;
        record(groupKey, { reason: unchanged ? r === "refresh" ? "refresh" : "update" : "add", key, current: c.current, ...unchanged && r !== "refresh" ? { previous } : {} });
        membership.set(key, groupKey);
      }
      publish(before, touched, pending);
    };
    const runList = (changes) => {
      const refreshed = /* @__PURE__ */ new Set(), replaced = /* @__PURE__ */ new Set();
      const makeRecord = (item) => ({ token: {}, item });
      const findItem = (item) => listRecords.findIndex((entry) => same(entry.item, item));
      for (const c of changes || []) {
        const r = reason(c), index = c.currentIndex ?? -1;
        if (r === "add") listRecords.splice(index < 0 ? listRecords.length : index, 0, makeRecord(c.current));
        else if (r === "addrange") {
          const values = c.range?.items || c.range || [], position = c.range?.index ?? index;
          let offset = position < 0 ? listRecords.length : position;
          for (const item of values) listRecords.splice(offset++, 0, makeRecord(item));
        } else if (r === "remove") {
          const position = index >= 0 ? index : findItem(c.current);
          if (position >= 0) listRecords.splice(position, 1);
        } else if (r === "removerange") {
          const values = c.range?.items || c.range || [], position = c.range?.index ?? index;
          if (position >= 0) listRecords.splice(position, values.length);
          else for (const item of values) {
            const found = findItem(item);
            if (found >= 0) listRecords.splice(found, 1);
          }
        } else if (r === "clear") listRecords.length = 0;
        else if (r === "replace" || r === "update") {
          const previousIndex = c.previousIndex >= 0 ? c.previousIndex : index >= 0 ? index : findItem(c.previous);
          const previous = previousIndex >= 0 ? listRecords.splice(previousIndex, 1)[0] : void 0;
          const entry = { token: previous?.token || {}, item: c.current };
          replaced.add(entry.token);
          listRecords.splice(index >= 0 ? index : previousIndex >= 0 ? previousIndex : listRecords.length, 0, entry);
        } else if (r === "move" || r === "moved") {
          const previousIndex = c.previousIndex >= 0 ? c.previousIndex : findItem(c.current);
          if (previousIndex >= 0) {
            const entry = listRecords.splice(previousIndex, 1)[0];
            if (entry) listRecords.splice(index, 0, entry);
          }
        } else if (r === "refresh") {
          const position = index >= 0 ? index : findItem(c.current);
          if (listRecords[position]) refreshed.add(listRecords[position].token);
        }
      }
      if (changes?.items && (changes.items.length !== listRecords.length || changes.items.some((item, i) => !same(item, listRecords[i]?.item)))) {
        const occurrences = /* @__PURE__ */ new Map();
        for (const entry of listRecords) {
          if (!occurrences.has(entry.item)) occurrences.set(entry.item, []);
          occurrences.get(entry.item).push(entry);
        }
        const offsets = /* @__PURE__ */ new Map();
        listRecords = changes.items.map((item) => {
          const position = offsets.get(item) || 0;
          offsets.set(item, position + 1);
          return occurrences.get(item)?.[position] || makeRecord(item);
        });
      }
      if (!currentSelector) return;
      const partitions = /* @__PURE__ */ new Map(), before = new Set(groups.keys()), touched = /* @__PURE__ */ new Set(), pending = /* @__PURE__ */ new Map();
      listRecords.forEach((entry, index) => {
        const key = currentSelector(entry.item, index);
        if (!partitions.has(key)) partitions.set(key, []);
        partitions.get(key).push(entry);
      });
      for (const key of /* @__PURE__ */ new Set([...before, ...partitions.keys()])) {
        let group2 = groups.get(key);
        if (!group2) {
          group2 = new Group(key, "list");
          groups.set(key, group2);
        }
        const next = partitions.get(key) || [], work = (listGroups.get(key) || []).slice(), wanted = new Set(next.map((entry) => entry.token)), delta = [];
        for (let index = work.length - 1; index >= 0; index--) if (!wanted.has(work[index].token)) {
          delta.push({ reason: "remove", current: work[index].item, currentIndex: index });
          work.splice(index, 1);
        }
        const presentTokens = new Set(work.map((entry) => entry.token));
        for (let index = 0; index < next.length; index++) {
          const entry = next[index];
          let previousIndex = !presentTokens.has(entry.token) ? -1 : work[index]?.token === entry.token ? index : work.findIndex((previous) => previous.token === entry.token);
          if (previousIndex < 0) {
            delta.push({ reason: "add", current: entry.item, currentIndex: index });
            work.splice(index, 0, entry);
            presentTokens.add(entry.token);
          } else {
            if (previousIndex !== index) {
              const previous = work.splice(previousIndex, 1)[0];
              work.splice(index, 0, previous);
              delta.push({ reason: "move", current: previous.item, previousIndex, currentIndex: index });
            }
            if (replaced.has(entry.token) || !same(work[index].item, entry.item)) {
              delta.push({ reason: "replace", previous: work[index].item, current: entry.item, previousIndex: index, currentIndex: index });
              work[index] = entry;
            } else if (refreshed.has(entry.token)) delta.push({ reason: "refresh", current: entry.item, currentIndex: index });
          }
        }
        group2._data = next.map((entry) => entry.item);
        if (next.length) listGroups.set(key, next.slice());
        else listGroups.delete(key);
        if (delta.length) {
          pending.set(key, delta);
          touched.add(key);
        }
      }
      publish(before, touched, pending);
    };
    const regroup = () => safe(observer, () => kind === "list" ? runList() : runCache([...data].map(([key, current]) => ({ reason: "refresh", key, current }))));
    if (isObservable3(selector)) subscriptions.add(selector.subscribe({ next: (next) => {
      if (typeof next !== "function") {
        observer.error(new TypeError("Group selector emissions must be functions"));
        return;
      }
      currentSelector = next;
      regroup();
    }, error: (e) => observer.error(e) }));
    if (regrouper) subscriptions.add(asStream(regrouper).subscribe({ next: regroup, error: (e) => observer.error(e) }));
    subscriptions.add(source.subscribe({ next: (changes) => safe(observer, () => {
      kind = changes.kind || kind;
      if (kind === "list") runList(changes);
      else runCache(changes);
    }), error: (e) => observer.error(e), complete: () => observer.complete() }));
    return () => {
      subscriptions.unsubscribe();
      for (const group2 of groups.values()) group2.dispose();
      groups.clear();
      data.clear();
      membership.clear();
      listGroups.clear();
      listRecords.length = 0;
    };
  });
}
var groupOn = (selector, regrouper) => grouping(selector, regrouper);
var group = (selector, regrouperOrOptions) => regrouperOrOptions?.resultGroupSource ? groupWithSpecifiedGroups(selector, regrouperOrOptions.resultGroupSource) : groupOn(selector, regrouperOrOptions);
var groupOnImmutable = (selector, regrouper) => grouping(selector, regrouper, true);
var groupWithImmutableState = groupOnImmutable;
function groupOnObservable(selector) {
  return (source) => new Observable4((observer) => {
    const subs = new Subscription4(), forwarded = new Subject3(), regroup = new Subject3(), streams = /* @__PURE__ */ new Map(), values = /* @__PURE__ */ new Map();
    const active = /* @__PURE__ */ new Set();
    let inBatch = false, sourceDone = false;
    const finish = () => {
      if (sourceDone && !inBatch && !active.size) forwarded.complete();
    };
    subs.add(forwarded.pipe(groupOn((item, key) => values.has(key) ? values.get(key) : UNASSIGNED_GROUP, regroup)).subscribe(observer));
    subs.add(source.subscribe({ next: (changes) => safe(observer, () => {
      if (changes.kind === "list") throw new TypeError("groupOnObservable requires a keyed cache change stream");
      inBatch = true;
      try {
        for (const c of changes) {
          const r = reason(c);
          if (r === "remove" || r === "update" || r === "add") {
            streams.get(c.key)?.unsubscribe();
            streams.delete(c.key);
            active.delete(c.key);
            values.delete(c.key);
          }
          if (r === "clear") {
            for (const sub of streams.values()) sub.unsubscribe();
            streams.clear();
            active.clear();
            values.clear();
          }
          if (r === "add" || r === "update") {
            const stream = selector(c.current, c.key);
            if (!isObservable3(stream)) throw new TypeError("groupOnObservable selector must return an Observable");
            active.add(c.key);
            const sub = stream.pipe(distinctUntilChanged2()).subscribe({ next: (value) => {
              values.set(c.key, value);
              if (!inBatch) regroup.next();
            }, error: (e) => observer.error(e), complete: () => {
              active.delete(c.key);
              finish();
            } });
            streams.set(c.key, sub);
          }
        }
        forwarded.next(changes);
      } finally {
        inBatch = false;
        finish();
      }
    }), error: (e) => observer.error(e), complete: () => {
      sourceDone = true;
      finish();
    } }));
    return () => {
      subs.unsubscribe();
      for (const sub of streams.values()) sub.unsubscribe();
      forwarded.complete();
      regroup.complete();
    };
  });
}
function join(type, rightSource, foreignKeySelector, resultSelector, many2 = false) {
  if (typeof foreignKeySelector !== "function" || typeof resultSelector !== "function") throw new TypeError("Join requires foreign-key and result selectors");
  return (leftSource) => new Observable4((observer) => {
    const subs = new Subscription4(), left = /* @__PURE__ */ new Map(), right = /* @__PURE__ */ new Map(), rightForeign = /* @__PURE__ */ new Map(), buckets = /* @__PURE__ */ new Map();
    const result = /* @__PURE__ */ new Map(), outputByForeign = /* @__PURE__ */ new Map(), tupleKeys = /* @__PURE__ */ new Map();
    let leftDone = false, rightDone = false;
    const tuple = (lk, rk) => {
      if (!tupleKeys.has(lk)) tupleKeys.set(lk, /* @__PURE__ */ new Map());
      const m = tupleKeys.get(lk);
      if (!m.has(rk)) {
        const key = [lk, rk];
        Object.defineProperties(key, { leftKey: { value: lk }, rightKey: { value: rk } });
        m.set(rk, Object.freeze(key));
      }
      return m.get(rk);
    };
    const select = (key, l, r) => resultSelector.length >= 3 ? resultSelector(key, l, r) : resultSelector(l, r);
    const update = (side, changes) => {
      if (changes.kind === "list") throw new TypeError("Join requires keyed cache change streams");
      const affected = /* @__PURE__ */ new Set(), onlyRefresh = /* @__PURE__ */ new Map();
      const touch = (key, refresh2) => {
        affected.add(key);
        onlyRefresh.set(key, (onlyRefresh.get(key) ?? true) && refresh2);
      };
      for (const c of changes) {
        const r = reason(c), key = c.key;
        if (r === "clear") {
          if (side === "left") {
            for (const k of left.keys()) touch(k, false);
            left.clear();
          } else {
            for (const k of buckets.keys()) touch(k, false);
            right.clear();
            rightForeign.clear();
            buckets.clear();
          }
          continue;
        }
        if (side === "left") {
          if (r === "remove") left.delete(key);
          else left.set(key, c.current);
          touch(key, r === "refresh");
        } else {
          const had = rightForeign.has(key), oldFk = rightForeign.get(key), fk = r === "remove" ? oldFk : foreignKeySelector(c.current, key);
          if (had) {
            if (r === "remove" || !same(oldFk, fk)) {
              const bucket = buckets.get(oldFk);
              bucket?.delete(key);
              if (bucket && !bucket.size) buckets.delete(oldFk);
            }
            touch(oldFk, r === "refresh" && same(oldFk, fk));
          }
          if (r === "remove") {
            right.delete(key);
            rightForeign.delete(key);
          } else {
            right.set(key, c.current);
            rightForeign.set(key, fk);
            if (!buckets.has(fk)) buckets.set(fk, /* @__PURE__ */ new Map());
            buckets.get(fk).set(key, c.current);
            touch(fk, r === "refresh" && (!had || same(oldFk, fk)));
          }
        }
      }
      const out = [], plans = /* @__PURE__ */ new Map(), previousKeys = /* @__PURE__ */ new Set(), desiredKeys = /* @__PURE__ */ new Set();
      for (const fk of affected) {
        const hasLeft = left.has(fk), l = left.get(fk), bucket = buckets.get(fk) || /* @__PURE__ */ new Map(), rows = /* @__PURE__ */ new Map(), oldKeys = outputByForeign.get(fk) || /* @__PURE__ */ new Set();
        if (many2) {
          const include = type === "inner" ? hasLeft && bucket.size > 0 : type === "left" ? hasLeft : type === "right" ? bucket.size > 0 : hasLeft || bucket.size > 0;
          if (include) rows.set(fk, [type === "inner" || type === "left" ? l : optional(hasLeft, l), new ImmutableGroup(fk, bucket)]);
        } else if (type === "inner" || type === "right") {
          if (hasLeft || type === "right") for (const [rk, r] of bucket) rows.set(type === "inner" ? tuple(fk, rk) : rk, [type === "inner" ? l : optional(hasLeft, l), r]);
        } else if (hasLeft || type === "full" && bucket.size) {
          const r = [...bucket.values()].at(-1);
          rows.set(fk, [type === "left" ? l : optional(hasLeft, l), optional(bucket.size > 0, r)]);
        }
        plans.set(fk, rows);
        for (const key of oldKeys) previousKeys.add(key);
        for (const key of rows.keys()) desiredKeys.add(key);
      }
      for (const key of previousKeys) if (!desiredKeys.has(key)) {
        out.push({ reason: "remove", key, current: result.get(key) });
        result.delete(key);
      }
      for (const [fk, rows] of plans) {
        const bucket = buckets.get(fk) || /* @__PURE__ */ new Map();
        for (const [key, [lv, rv]] of rows) {
          if (onlyRefresh.get(fk) && result.has(key)) out.push({ reason: "refresh", key, current: result.get(key) });
          else {
            const value = select(key, lv, rv), had = result.has(key), previous = result.get(key);
            result.set(key, value);
            out.push({ reason: had ? "update" : "add", key, current: value, ...had ? { previous } : {} });
          }
        }
        if (rows.size) outputByForeign.set(fk, new Set(rows.keys()));
        else {
          outputByForeign.delete(fk);
          tupleKeys.delete(fk);
        }
        if (type === "inner" && tupleKeys.has(fk)) {
          const tuples = tupleKeys.get(fk);
          for (const rk of tuples.keys()) if (!bucket.has(rk)) tuples.delete(rk);
        }
      }
      if (out.length) observer.next(set(out));
    };
    const done = () => {
      if (leftDone && rightDone) observer.complete();
    };
    subs.add(asStream(rightSource).subscribe({ next: (c) => safe(observer, () => update("right", c)), error: (e) => observer.error(e), complete: () => {
      rightDone = true;
      done();
    } }));
    if (!observer.closed) subs.add(leftSource.subscribe({ next: (c) => safe(observer, () => update("left", c)), error: (e) => observer.error(e), complete: () => {
      leftDone = true;
      done();
    } }));
    return subs;
  });
}
var innerJoin = (right, fk, selector) => join("inner", right, fk, selector);
var leftJoin = (right, fk, selector) => join("left", right, fk, selector);
var rightJoin = (right, fk, selector) => join("right", right, fk, selector);
var fullJoin = (right, fk, selector) => join("full", right, fk, selector);
var innerJoinMany = (right, fk, selector) => join("inner", right, fk, selector, true);
var leftJoinMany = (right, fk, selector) => join("left", right, fk, selector, true);
var rightJoinMany = (right, fk, selector) => join("right", right, fk, selector, true);
var fullJoinMany = (right, fk, selector) => join("full", right, fk, selector, true);
var CombineOperator = Object.freeze({ And: "and", Or: "or", Xor: "xor", Except: "except", and: "and", or: "or", xor: "xor", except: "except" });
function combine(sources, operator = "or") {
  const type = String(operator).toLowerCase();
  if (!["and", "or", "xor", "except"].includes(type)) throw new TypeError(`Unknown combine operator: ${operator}`);
  return new Observable4((observer) => {
    const subs = new Subscription4(), entries = [], output = /* @__PURE__ */ new Map(), pendingLatest = /* @__PURE__ */ new Map();
    let kind = "cache", outerDone = false, suspended = false, initialized = false;
    const matches = (key) => {
      const n = entries.reduce((count2, e) => count2 + e.data.has(key), 0);
      return type === "and" ? entries.length > 0 && n === entries.length : type === "or" ? n > 0 : type === "xor" ? n === 1 : !!entries[0]?.data.has(key) && n === 1;
    };
    const recalculate = (keys, latest, refreshKeys = /* @__PURE__ */ new Set()) => {
      if (observer.closed) return;
      if (suspended) {
        for (const key of keys) {
          if (latest?.has(key)) pendingLatest.set(key, latest.get(key));
          else pendingLatest.delete(key);
        }
        return;
      }
      const out = [];
      for (const key of keys) {
        const had = output.has(key), previous = output.get(key);
        if (!matches(key)) {
          if (had) {
            output.delete(key);
            out.push({ reason: "remove", key, current: previous });
          }
          continue;
        }
        const current = latest?.has(key) ? latest.get(key) : entries.find((e) => e.data.has(key))?.data.get(key);
        if (!had || !same(current, previous)) {
          output.set(key, current);
          out.push({ reason: had ? "update" : "add", key, current, ...had ? { previous } : {} });
        } else if (refreshKeys.has(key)) out.push({ reason: "refresh", key, current });
      }
      if (out.length) {
        if (kind === "list") {
          const listChanges = out.flatMap((c) => c.reason === "update" ? [{ reason: "remove", current: c.previous }, { reason: "add", current: c.current }] : [{ reason: c.reason, current: c.current }]);
          observer.next(set(listChanges, "list"));
        } else observer.next(set(out));
      }
    };
    const allKeys = () => /* @__PURE__ */ new Set([...output.keys(), ...entries.flatMap((e) => [...e.data.keys()])]);
    const complete = () => {
      if (initialized && outerDone && entries.every((e) => e.done)) observer.complete();
    };
    const attach = (entry) => {
      const sub = asStream(entry.source).subscribe({ next: (changes) => safe(observer, () => {
        const keys = /* @__PURE__ */ new Set(), latest = /* @__PURE__ */ new Map(), refresh2 = /* @__PURE__ */ new Set();
        if (changes.kind === "list") {
          kind = "list";
          const before = new Map(entry.data);
          applyChanges(entry.list, changes);
          entry.data = new Map(entry.list.map((item) => [item, item]));
          for (const key of before.keys()) keys.add(key);
          for (const key of entry.data.keys()) keys.add(key);
          for (const [key, value] of entry.data) latest.set(key, value);
          for (const c of changes) if (reason(c) === "refresh") refresh2.add(c.current);
        } else {
          for (const c of changes) {
            const r = reason(c);
            if (r === "clear") {
              for (const key of entry.data.keys()) keys.add(key);
              entry.data.clear();
              continue;
            }
            keys.add(c.key);
            if (r === "remove") entry.data.delete(c.key);
            else {
              entry.data.set(c.key, c.current);
              if (r === "refresh") refresh2.add(c.key);
              else latest.set(c.key, c.current);
            }
          }
        }
        recalculate(keys, latest, refresh2);
      }), error: (e) => observer.error(e), complete: () => {
        entry.done = true;
        complete();
      } });
      entry.sub = sub;
      subs.add(sub);
    };
    const replaceSources = (next) => {
      suspended = true;
      const old = entries.slice(), nextEntries = [], unused = old.slice();
      for (const source of next) {
        const index = unused.findIndex((e) => e.source === source);
        nextEntries.push(index < 0 ? { source, data: /* @__PURE__ */ new Map(), list: [], done: false, sub: null } : unused.splice(index, 1)[0]);
      }
      for (const entry of unused) {
        entry.sub?.unsubscribe();
        subs.remove(entry.sub);
      }
      entries.splice(0, entries.length, ...nextEntries);
      for (const entry of entries) if (!entry.sub && !observer.closed) attach(entry);
      suspended = false;
      initialized = true;
      recalculate(allKeys(), pendingLatest);
      pendingLatest.clear();
      complete();
    };
    if (Array.isArray(sources)) {
      outerDone = true;
      replaceSources(sources);
    } else {
      const sourceList = [], sourceCache = /* @__PURE__ */ new Map();
      subs.add(asStream(sources).subscribe({ next: (changes) => safe(observer, () => {
        if (Array.isArray(changes) && !changes.kind && (changes.length === 0 || !changes[0]?.reason)) {
          sourceList.splice(0, sourceList.length, ...changes);
        } else if (changes.kind === "cache") {
          applyChanges(sourceCache, changes);
          sourceList.splice(0, sourceList.length, ...sourceCache.values());
        } else applyChanges(sourceList, changes);
        replaceSources(sourceList);
      }), error: (e) => observer.error(e), complete: () => {
        outerDone = true;
        initialized = true;
        complete();
      } }));
    }
    return subs;
  });
}
var logical = (type) => (...others) => (source) => {
  if (!others.length) return combine(source, type);
  return combine([source, ...others.flat()], type);
};
var and = logical("and");
var or = logical("or");
var xor = logical("xor");
var except = logical("except");
function aggregate(mode, selector = identity2, fallback = 0) {
  if (typeof selector !== "function") throw new TypeError("Aggregate selector must be a function");
  return (source) => new Observable4((observer) => {
    const values = /* @__PURE__ */ new Map(), list = [], aggregateItems = [];
    let aggregateId = 0;
    let total = 0, squares = 0, count2 = 0, last, hasLast = false;
    const remove2 = (number) => {
      total -= number;
      squares -= number * number;
      count2--;
    };
    const add = (number) => {
      total += number;
      squares += number * number;
      count2++;
    };
    return source.subscribe({ next: (changes) => safe(observer, () => {
      if (changes.kind === "aggregate") {
        for (const c of changes) {
          if (c.type === "remove") {
            const index = aggregateItems.findIndex((x) => same(x.item, c.item));
            if (index >= 0) {
              const entry = aggregateItems.splice(index, 1)[0];
              remove2(values.get(entry.key));
              values.delete(entry.key);
            }
          } else {
            const entry = { key: aggregateId++, item: c.item };
            aggregateItems.push(entry);
            const value2 = selector(c.item);
            values.set(entry.key, value2);
            add(value2);
          }
        }
      } else if (changes.kind === "list") {
        applyChanges(list, changes);
        values.clear();
        total = squares = count2 = 0;
        list.forEach((item, index) => {
          const v = selector(item, index);
          values.set(index, v);
          add(v);
        });
      } else {
        for (const c of changes) {
          if (reason(c) === "clear") {
            values.clear();
            total = squares = count2 = 0;
            continue;
          }
          if (values.has(c.key)) {
            remove2(values.get(c.key));
            values.delete(c.key);
          }
          if (reason(c) !== "remove") {
            const v = selector(c.current, c.key);
            values.set(c.key, v);
            add(v);
          }
        }
      }
      if (!count2) total = squares = 0;
      else if (!Number.isFinite(total) || !Number.isFinite(squares)) {
        total = squares = 0;
        for (const value2 of values.values()) {
          total += value2;
          squares += value2 * value2;
        }
      }
      let value;
      if (mode === "sum" || mode === "count") value = total;
      else if (mode === "avg") value = count2 ? total / count2 : fallback;
      else if (mode === "stdDev") value = count2 < 2 ? fallback : Math.sqrt(Math.max(0, squares - total * total / count2)) / (count2 - 1);
      else if (mode === "standardDeviation") value = count2 < 2 ? fallback : Math.sqrt(Math.max(0, squares - total * total / count2) / (count2 - 1));
      else {
        value = fallback;
        let seen = false;
        for (const v of values.values()) if (!seen || (mode === "min" ? v < value : v > value)) {
          value = v;
          seen = true;
        }
      }
      if (!hasLast || !same(last, value)) {
        hasLast = true;
        last = value;
        observer.next(value);
      }
    }), error: (e) => observer.error(e), complete: () => observer.complete() });
  });
}
var count = (predicate = null) => aggregate("count", predicate ? (item, key) => predicate(item, key) ? 1 : 0 : () => 1);
var sum = (selector = identity2) => aggregate("sum", selector);
var avg = (selector = identity2, fallback = 0) => aggregate("avg", selector, fallback);
var average = avg;
var min = (selector = identity2, fallback = 0) => aggregate("min", selector, fallback);
var max = (selector = identity2, fallback = 0) => aggregate("max", selector, fallback);
var stdDev = (selector = identity2, fallback = 0) => aggregate("stdDev", selector, fallback);
var standardDeviation = (selector = identity2, fallback = 0) => aggregate("standardDeviation", selector, fallback);
var sumMany = (childrenSelector = identity2, valueSelector = identity2) => sum((item, key) => [...childrenSelector(item, key)].reduce((total, child) => total + valueSelector(child), 0));
function groupOnProperty(property, throttleOrOptions = {}, scheduler) {
  const options = typeof throttleOrOptions === "number" ? { throttle: throttleOrOptions, scheduler } : throttleOrOptions || {};
  const selector = typeof property === "function" ? property : (item) => String(property).split(".").reduce((value, key) => value?.[key], item);
  return (source) => source.pipe(autoRefresh(property, options), groupOn(selector));
}
function groupOnPropertyWithImmutableState(property, throttleOrOptions = {}, scheduler) {
  const options = typeof throttleOrOptions === "number" ? { throttle: throttleOrOptions, scheduler } : throttleOrOptions || {};
  const selector = typeof property === "function" ? property : (item) => String(property).split(".").reduce((value, key) => value?.[key], item);
  return (source) => source.pipe(autoRefresh(property, options), groupOnImmutable(selector));
}
var AggregateType = Object.freeze({ Add: "add", Remove: "remove", add: "add", remove: "remove" });
function forAggregation() {
  return (source) => new Observable4((observer) => {
    let kind = "cache", state = /* @__PURE__ */ new Map();
    return source.subscribe({ next: (changes) => safe(observer, () => {
      if (changes.kind === "aggregate") {
        observer.next(changes);
        return;
      }
      if (changes.kind !== kind) {
        kind = changes.kind;
        state = kind === "list" ? [] : /* @__PURE__ */ new Map();
      }
      const out = [];
      const record = (type, item) => out.push({ type, item, Type: type, Item: item });
      for (const c of changes) {
        const r = reason(c);
        if (r === "add") record("add", c.current);
        else if (r === "remove") record("remove", c.current === void 0 && kind === "cache" ? state.get(c.key) : c.current);
        else if (r === "update" || r === "replace") {
          record("remove", c.previous === void 0 && kind === "cache" ? state.get(c.key) : c.previous);
          record("add", c.current);
        } else if (r === "addrange" || r === "removerange") for (const item of c.range?.items || c.range || []) record(r === "addrange" ? "add" : "remove", item);
        else if (r === "clear") for (const item of kind === "list" ? state : state.values()) record("remove", item);
        applyChanges(state, set([c], kind));
      }
      const result = set(out);
      result.kind = "aggregate";
      observer.next(result);
    }), error: (e) => observer.error(e), complete: () => observer.complete() });
  });
}
var invalidateWhen = (invalidate) => (source) => asStream(invalidate).pipe(startWith(void 0), switchMap(() => source), distinctUntilChanged2());
function groupWithSpecifiedGroups(selector, resultGroupSource) {
  return (source) => new Observable4((observer) => {
    const subs = new Subscription4(), available = /* @__PURE__ */ new Map(), exposed = /* @__PURE__ */ new Map(), inner = /* @__PURE__ */ new Map();
    let keys = [], sourceDone = false, keysDone = false;
    const complete = () => {
      if (sourceDone && keysDone) observer.complete();
    };
    const refreshKeys = (changes) => {
      if (changes.kind === "cache") {
        const m = new Map(keys.map((key) => [key, key]));
        for (const c of changes) {
          const key = c.current ?? c.key;
          if (reason(c) === "remove") m.delete(key);
          else if (reason(c) === "clear") m.clear();
          else m.set(key, key);
        }
        keys = [...m.keys()];
      } else applyChanges(keys, changes);
      const wanted = new Set(keys), out = [];
      for (const [key, group2] of exposed) if (!wanted.has(key)) {
        out.push({ reason: "remove", key, current: group2 });
        exposed.delete(key);
        group2.dispose();
      }
      for (const key of wanted) if (!exposed.has(key)) {
        const original = available.get(key), group2 = new Group(key, original?.kind || "cache");
        if (original) group2._data = original.kind === "cache" ? new Map(original.keyValues) : original.items;
        exposed.set(key, group2);
        out.push({ reason: "add", key, current: group2 });
      }
      if (out.length) observer.next(set(out));
    };
    subs.add(asStream(resultGroupSource).subscribe({ next: (c) => safe(observer, () => refreshKeys(c)), error: (e) => observer.error(e), complete: () => {
      keysDone = true;
      complete();
    } }));
    if (!observer.closed) subs.add(source.pipe(groupOn(selector)).subscribe({ next: (changes) => safe(observer, () => {
      for (const c of changes) {
        if (reason(c) === "remove") {
          available.delete(c.key);
          inner.get(c.key)?.unsubscribe();
          inner.delete(c.key);
        } else {
          const original = c.current;
          available.set(c.key, original);
          inner.get(c.key)?.unsubscribe();
          const sub = original.connect().subscribe({ next: (batch2) => {
            const group2 = exposed.get(c.key);
            if (!group2) return;
            if (group2.kind !== batch2.kind) {
              group2.kind = batch2.kind;
              group2._data = batch2.kind === "cache" ? /* @__PURE__ */ new Map() : [];
            }
            applyChanges(group2._data, batch2);
            if (batch2.length) group2._subject.next(batch2);
          }, error: (e) => observer.error(e) });
          inner.set(c.key, sub);
          subs.add(sub);
        }
      }
    }), error: (e) => observer.error(e), complete: () => {
      sourceDone = true;
      complete();
    } }));
    return () => {
      subs.unsubscribe();
      for (const group2 of exposed.values()) group2.dispose();
      available.clear();
      exposed.clear();
      inner.clear();
    };
  });
}

// src/extras.js
import { Observable as Observable5, Subscription as Subscription5, defer, from as from2, map as map2, filter as rxFilter2, mergeMap, tap, startWith as startWith2 } from "rxjs";
var kindOf = (changes) => changes.kind ?? (changes.length && "key" in changes[0] ? "cache" : "list");
var requireFunction = (value, name) => {
  if (typeof value !== "function") throw new TypeError(`${name} must be a function`);
};
var setOf = (values) => new Set(values.flat());
var withoutIndex = (changes) => Array.from(changes).filter((change) => change.reason !== "move" && change.reason !== "moved").map((change) => ({ ...change, currentIndex: -1, previousIndex: -1, ...change.range ? { range: { ...change.range, items: [...change.range.items], index: -1 } } : {} }));
var projectedSet = (changes, predicate) => {
  const kind = kindOf(changes), selected = Array.from(changes).filter(predicate);
  return new ChangeSet(kind === "list" ? withoutIndex(selected) : selected, kind);
};
var filteredChanges = (predicate) => (source) => source.pipe(map2((changes) => projectedSet(changes, predicate)), rxFilter2((changes) => changes.length > 0));
function whereReasonsAre(...reasons2) {
  const allowed = setOf(reasons2);
  return filteredChanges((change) => allowed.has(change.reason));
}
function whereReasonsAreNot(...reasons2) {
  const excluded = setOf(reasons2);
  return filteredChanges((change) => !excluded.has(change.reason));
}
function includeUpdateWhen(predicate) {
  requireFunction(predicate, "predicate");
  return filteredChanges((change) => change.reason !== "update" || predicate(change.current, change.previous, change.key));
}
function excludeUpdateWhen(predicate) {
  requireFunction(predicate, "predicate");
  return includeUpdateWhen((current, previous, key) => !predicate(current, previous, key));
}
var ignoreUpdateWhen = excludeUpdateWhen;
function ignoreSameReferenceUpdate() {
  return excludeUpdateWhen(Object.is);
}
function suppressRefresh() {
  return whereReasonsAreNot("refresh");
}
function invokeEvaluate() {
  return forEachChange((change) => {
    if (change.reason !== "refresh") return;
    const evaluate = change.current?.evaluate ?? change.current?.Evaluate;
    requireFunction(evaluate, "item.evaluate");
    evaluate.call(change.current);
  });
}
function flattenBufferResult() {
  return (source) => source.pipe(rxFilter2((buffer) => buffer.length > 0), map2((buffer) => {
    const result = new ChangeSet(buffer.flatMap((changes) => Array.from(changes)), kindOf(buffer[0]));
    const last = buffer.at(-1);
    for (const key of ["items", "keys", "sortedItems", "SortedItems", "response"]) if (last[key] !== void 0) result[key] = last[key];
    return result;
  }));
}
function treatMovesAsRemoveAdd() {
  return map2((changes) => {
    const result = new ChangeSet([], kindOf(changes));
    for (const change of changes) {
      if (change.reason === "move" || change.reason === "moved") {
        result.push({ ...change, reason: "remove", currentIndex: change.previousIndex, previousIndex: -1 });
        result.push({ ...change, reason: "add", currentIndex: change.currentIndex, previousIndex: -1 });
      } else result.push(change);
    }
    for (const key of ["items", "keys", "sortedItems", "SortedItems", "response"]) if (changes[key] !== void 0) result[key] = changes[key];
    return result;
  });
}
function forEachChange(action) {
  requireFunction(action, "action");
  return tap((changes) => {
    for (const change of changes) action(change);
  });
}
function* itemChanges(changes) {
  for (const change of changes) {
    if (!change.range) {
      yield change;
      continue;
    }
    const reason2 = change.reason === "addRange" ? "add" : "remove";
    const start = change.range.index ?? -1;
    for (let i = 0; i < change.range.items.length; i++) {
      yield { reason: reason2, current: change.range.items[i], currentIndex: start < 0 ? -1 : reason2 === "add" ? start + i : start, previousIndex: -1 };
    }
  }
}
function forEachItemChange(action) {
  requireFunction(action, "action");
  return tap((changes) => {
    for (const change of itemChanges(changes)) action(change);
  });
}
function flattenChanges() {
  return mergeMap((changes) => Array.from(changes));
}
var flatten = flattenChanges;
function startWithEmpty(kind = "cache") {
  return startWith2(new ChangeSet([], kind));
}
function startWithItem(item, key = item?.key ?? item?.Key ?? item?.id) {
  return startWith2(new ChangeSet([{ reason: "add", key, current: item }], "cache"));
}
function removeIndex() {
  return map2((changes) => new ChangeSet(withoutIndex(changes), kindOf(changes)));
}
function addKey(keySelector) {
  requireFunction(keySelector, "keySelector");
  return map2((changes) => {
    const output = [];
    for (const change of itemChanges(changes)) {
      const key = keySelector(change.current);
      if (change.reason === "replace") {
        const previousKey = keySelector(change.previous);
        if (Object.is(key, previousKey) || key === previousKey) {
          output.push({ reason: "update", key, current: change.current, previous: change.previous });
        } else {
          output.push({ reason: "remove", key: previousKey, current: change.previous });
          output.push({ reason: "add", key, current: change.current });
        }
      } else output.push({ reason: change.reason, key, current: change.current, ...change.reason === "move" || change.reason === "moved" ? { currentIndex: change.currentIndex, previousIndex: change.previousIndex } : {} });
    }
    return new ChangeSet(output, "cache");
  });
}
function ensureUniqueKeys() {
  return (source) => defer(() => {
    const state = /* @__PURE__ */ new Map();
    return source.pipe(map2((changes) => {
      const last = /* @__PURE__ */ new Map();
      for (const change of changes) {
        if (change.reason === "move" || change.reason === "moved") continue;
        if (change.reason !== "refresh" || !last.has(change.key)) last.set(change.key, change);
      }
      const result = [];
      for (const [key, change] of last) {
        if (change.reason === "add" || change.reason === "update") {
          result.push(state.has(key) ? { reason: "update", key, current: change.current, previous: state.get(key) } : { reason: "add", key, current: change.current });
          state.set(key, change.current);
        } else if (change.reason === "remove" && state.has(key)) {
          result.push({ reason: "remove", key, current: state.get(key) });
          state.delete(key);
        } else if (change.reason === "refresh" && state.has(key)) result.push({ reason: "refresh", key, current: state.get(key) });
      }
      return new ChangeSet(result, "cache");
    }));
  });
}
function copyInto(target, changes) {
  if (typeof target?.edit === "function") {
    target.edit((updater) => updater.clone(changes));
  } else if (target instanceof Map || Array.isArray(target)) {
    if (Array.isArray(target) && kindOf(changes) === "cache") {
      for (const change of changes) {
        if (change.reason === "add") target.push(change.current);
        else if (change.reason === "remove" || change.reason === "update") {
          const index = target.indexOf(change.reason === "update" ? change.previous : change.current);
          if (index >= 0) target.splice(index, 1);
          if (change.reason === "update") target.push(change.current);
        }
      }
    } else applyChanges(target, changes);
  } else if (target instanceof Set) {
    for (const change of itemChanges(changes)) {
      if (change.reason === "remove") target.delete(change.current);
      else if (change.reason === "update" || change.reason === "replace") {
        target.delete(change.previous);
        target.add(change.current);
      } else if (change.reason === "add") target.add(change.current);
    }
  } else throw new TypeError("target must be a SourceCache, SourceList, Map, Set or Array");
}
function clone(target) {
  return tap((changes) => copyInto(target, changes));
}
function populateInto(source, destination, observer) {
  return source.pipe(clone(destination)).subscribe(observer);
}
function populateFrom(destination, observable2, observer) {
  if (typeof destination?.addOrUpdate !== "function") throw new TypeError("destination must support addOrUpdate");
  return observable2.pipe(tap((items) => destination.addOrUpdate(items))).subscribe(observer);
}
function toObservableOptional(key, initialOptionalWhenMissing = false, comparer = Object.is) {
  if (typeof initialOptionalWhenMissing !== "boolean") {
    comparer = initialOptionalWhenMissing ?? Object.is;
    initialOptionalWhenMissing = false;
  }
  const equals = typeof comparer === "function" ? comparer : comparer?.equals?.bind(comparer) ?? comparer?.Equals?.bind(comparer) ?? Object.is;
  return (source) => new Observable5((observer) => {
    let last = Optional.none(), seen = false, subscribing = true, completed = false;
    const subscription = source.subscribe({
      next(changes) {
        try {
          for (const change of changes) {
            if (!(Object.is(key, change.key) || key === change.key)) continue;
            const next = change.reason === "remove" ? Optional.none() : Optional.some(change.current);
            if (next.hasValue !== last.hasValue || next.hasValue && !equals(last.value, next.value)) {
              last = next;
              seen = true;
              observer.next(next);
            }
          }
        } catch (error) {
          observer.error(error);
        }
      },
      error(error) {
        observer.error(error);
      },
      complete() {
        completed = true;
        if (!subscribing) observer.complete();
      }
    });
    subscribing = false;
    if (initialOptionalWhenMissing && !seen && !observer.closed) observer.next(Optional.none());
    if (completed) observer.complete();
    return subscription;
  });
}
function adapt(adapter) {
  const action = typeof adapter === "function" ? adapter : adapter?.adapt?.bind(adapter) ?? adapter?.Adapt?.bind(adapter);
  requireFunction(action, "adapter.adapt");
  return tap(action);
}
var QuerySnapshot = class {
  constructor(state) {
    this.kind = state instanceof Map ? "cache" : "list";
    this._state = state instanceof Map ? new Map(state) : [...state];
  }
  get count() {
    return this._state instanceof Map ? this._state.size : this._state.length;
  }
  get size() {
    return this.count;
  }
  get items() {
    return this._state instanceof Map ? [...this._state.values()] : [...this._state];
  }
  get keys() {
    return this._state instanceof Map ? [...this._state.keys()] : this._state.map((_, i) => i);
  }
  get keyValues() {
    return this._state instanceof Map ? [...this._state.entries()] : [...this._state.entries()];
  }
  lookup(key) {
    return this._state instanceof Map ? this._state.has(key) ? Optional.some(this._state.get(key)) : Optional.none() : key >= 0 && key < this.count ? Optional.some(this._state[key]) : Optional.none();
  }
  get(key) {
    return this._state instanceof Map ? this._state.get(key) : this._state[key];
  }
  has(key) {
    return this._state instanceof Map ? this._state.has(key) : Number.isInteger(key) && key >= 0 && key < this.count;
  }
  [Symbol.iterator]() {
    return this.items[Symbol.iterator]();
  }
  get Count() {
    return this.count;
  }
  get Items() {
    return this.items;
  }
  get Keys() {
    return this.keys;
  }
  get KeyValues() {
    return this.keyValues;
  }
  Lookup(key) {
    return this.lookup(key);
  }
};
function queryWhenChanged(selector = (query) => query) {
  requireFunction(selector, "selector");
  return (source) => defer(() => {
    let state;
    return source.pipe(map2((changes) => {
      state ??= kindOf(changes) === "cache" ? /* @__PURE__ */ new Map() : [];
      applyChanges(state, changes);
      return selector(new QuerySnapshot(state));
    }));
  });
}
function toSortedCollection(selectorOrComparer = (value) => value, direction = "ascending") {
  let compare2;
  const descending = direction === "descending" || direction === "Descending" || direction === -1;
  if (selectorOrComparer && typeof (selectorOrComparer.compare ?? selectorOrComparer.Compare) === "function") compare2 = (selectorOrComparer.compare ?? selectorOrComparer.Compare).bind(selectorOrComparer);
  else if (typeof selectorOrComparer === "function" && selectorOrComparer.length >= 2) compare2 = selectorOrComparer;
  else {
    requireFunction(selectorOrComparer, "selectorOrComparer");
    compare2 = (a, b) => {
      const x = selectorOrComparer(a), y = selectorOrComparer(b);
      return x < y ? -1 : x > y ? 1 : 0;
    };
  }
  return queryWhenChanged((query) => query.items.sort((a, b) => (descending ? -1 : 1) * compare2(a, b)));
}
function updateIndex(setter = (item, index) => {
  item.index = index;
}) {
  requireFunction(setter, "setter");
  return (source) => defer(() => {
    let state;
    return source.pipe(tap((changes) => {
      let items = changes.sortedItems?.items ?? changes.items;
      if (!items) {
        state ??= kindOf(changes) === "cache" ? /* @__PURE__ */ new Map() : [];
        applyChanges(state, changes);
        items = state instanceof Map ? [...state.values()] : state;
      }
      items.forEach(setter);
    }));
  });
}
var ChangeStatistics = class {
  constructor(index = -1, adds = 0, updates = 0, removes = 0, refreshes = 0, moves = 0, count2 = 0) {
    Object.assign(this, { index, adds, updates, removes, refreshes, moves, count: count2, lastUpdated: /* @__PURE__ */ new Date() });
  }
  get Index() {
    return this.index;
  }
  get Adds() {
    return this.adds;
  }
  get Updates() {
    return this.updates;
  }
  get Removes() {
    return this.removes;
  }
  get Refreshes() {
    return this.refreshes;
  }
  get Moves() {
    return this.moves;
  }
  get Count() {
    return this.count;
  }
  get LastUpdated() {
    return this.lastUpdated;
  }
};
var ChangeSummary = class _ChangeSummary {
  constructor(index = -1, latest = new ChangeStatistics(), overall = new ChangeStatistics()) {
    Object.assign(this, { index, latest, overall });
  }
  get Latest() {
    return this.latest;
  }
  get Overall() {
    return this.overall;
  }
  static get empty() {
    return new _ChangeSummary();
  }
  static get Empty() {
    return this.empty;
  }
};
function collectUpdateStats() {
  return (source) => defer(() => {
    let overall = new ChangeStatistics();
    return source.pipe(map2((input) => {
      const changes = input instanceof ChangeSet ? input : new ChangeSet(input, kindOf(input));
      const index = overall.index + 1;
      const latest = new ChangeStatistics(index, changes.adds, changes.updates, changes.removes, changes.refreshes, changes.moves, changes.length);
      overall = new ChangeStatistics(index, overall.adds + latest.adds, overall.updates + latest.updates, overall.removes + latest.removes, overall.refreshes + latest.refreshes, overall.moves + latest.moves, overall.count + latest.count);
      return new ChangeSummary(index, latest, overall);
    }));
  });
}
var Node = class _Node {
  constructor(item, key, parent = null) {
    this.item = item;
    this.key = key;
    this._parent = parent instanceof Optional ? parent.valueOrDefault : parent;
    this._children = new SourceCache((node) => node.key);
    this.children = this._children.asObservableCache();
    this.isDisposed = false;
  }
  get parent() {
    return this._parent ? Optional.some(this._parent) : Optional.none();
  }
  get isRoot() {
    return this._parent == null;
  }
  get depth() {
    let result = 0, node = this._parent;
    const seen = /* @__PURE__ */ new Set([this]);
    while (node) {
      if (seen.has(node)) throw new Error("Circular tree parent relationship");
      seen.add(node);
      result++;
      node = node._parent;
    }
    return result;
  }
  equals(other) {
    return other instanceof _Node && (Object.is(this.key, other.key) || this.key === other.key);
  }
  dispose() {
    if (!this.isDisposed) {
      this.isDisposed = true;
      this.children.dispose();
      this._children.dispose();
    }
  }
  get Item() {
    return this.item;
  }
  get Key() {
    return this.key;
  }
  get Parent() {
    return this.parent;
  }
  get Children() {
    return this.children;
  }
  get IsRoot() {
    return this.isRoot;
  }
  get Depth() {
    return this.depth;
  }
  Equals(other) {
    return this.equals(other);
  }
  Dispose() {
    this.dispose();
  }
  toString() {
    return `${this.item}${this.children.size ? ` (${this.children.size} children)` : ""}`;
  }
};
function transformToTree(pivotOn, predicateChanged) {
  requireFunction(pivotOn, "pivotOn");
  return (source) => new Observable5((observer) => {
    const nodes = /* @__PURE__ */ new Map();
    const ownedNodes = /* @__PURE__ */ new Set();
    let selected = /* @__PURE__ */ new Map();
    let predicate = typeof predicateChanged === "function" ? predicateChanged : (node) => node.isRoot;
    const subscriptions = new Subscription5();
    const pending = [];
    let processing = false;
    function emitSelection(touched = /* @__PURE__ */ new Set()) {
      const next = /* @__PURE__ */ new Map(), changes = [];
      for (const [key, node] of nodes) if (predicate(node)) next.set(key, node);
      for (const [key, previous] of selected) if (!next.has(key)) changes.push({ reason: "remove", key, current: previous });
      for (const [key, node] of next) {
        if (!selected.has(key)) changes.push({ reason: "add", key, current: node });
        else if (selected.get(key) !== node) changes.push({ reason: "update", key, current: node, previous: selected.get(key) });
        else if (touched.has(key)) changes.push({ reason: "refresh", key, current: node });
      }
      selected = next;
      if (changes.length) observer.next(new ChangeSet(changes, "cache"));
    }
    function process(changes) {
      const retired = [], touched = /* @__PURE__ */ new Set();
      for (const change of changes) {
        const old = nodes.get(change.key);
        if (change.reason === "remove") {
          if (old) retired.push(old);
          nodes.delete(change.key);
        } else if (change.reason === "add" || change.reason === "update") {
          if (old) retired.push(old);
          const replacement = new Node(change.current, change.key);
          ownedNodes.add(replacement);
          nodes.set(change.key, replacement);
        }
        touched.add(change.key);
      }
      const children = /* @__PURE__ */ new Map();
      for (const [key, node] of nodes) {
        const parentKey = pivotOn(node.item);
        const parent = nodes.get(parentKey);
        const nextParent = parent && parent !== node ? parent : null;
        if (node._parent !== nextParent) touched.add(key);
        node._parent = nextParent;
        if (nextParent) {
          if (!children.has(nextParent.key)) children.set(nextParent.key, /* @__PURE__ */ new Map());
          children.get(nextParent.key).set(key, node);
        }
      }
      const visited = /* @__PURE__ */ new Set();
      for (const node of nodes.values()) {
        if (visited.has(node)) continue;
        const path = /* @__PURE__ */ new Set();
        let cursor = node;
        while (cursor && !visited.has(cursor)) {
          if (path.has(cursor)) throw new Error("Circular tree parent relationship");
          path.add(cursor);
          cursor = cursor._parent;
        }
        for (const part of path) visited.add(part);
      }
      for (const [key, node] of nodes) {
        const wanted = children.get(key) ?? /* @__PURE__ */ new Map();
        node._children.edit((updater) => {
          for (const child of node.children.items) if (!wanted.has(child.key)) updater.removeKey(child.key);
          for (const [childKey, child] of wanted) {
            const existing = node.children.lookup(childKey);
            if (!existing.hasValue || existing.value !== child) updater.addOrUpdate(child);
            else if (touched.has(childKey)) updater.refreshKey(childKey);
          }
        });
      }
      emitSelection(touched);
      for (const node of retired) {
        node.dispose();
        ownedNodes.delete(node);
      }
    }
    function enqueue(action) {
      pending.push(action);
      if (processing) return;
      processing = true;
      try {
        while (pending.length && !observer.closed) pending.shift()();
      } catch (error) {
        observer.error(error);
      } finally {
        processing = false;
      }
    }
    if (predicateChanged && typeof predicateChanged.subscribe === "function") subscriptions.add(predicateChanged.subscribe({
      next(value) {
        enqueue(() => {
          requireFunction(value, "predicate");
          predicate = value;
          emitSelection();
        });
      },
      error(error) {
        observer.error(error);
      }
    }));
    if (!observer.closed) subscriptions.add(source.subscribe({ next(changes) {
      enqueue(() => process(changes));
    }, error(error) {
      observer.error(error);
    }, complete() {
      enqueue(() => observer.complete());
    } }));
    return () => {
      subscriptions.unsubscribe();
      for (const node of ownedNodes) node.dispose();
      ownedNodes.clear();
      nodes.clear();
      selected.clear();
      pending.length = 0;
    };
  });
}
var asStream2 = (value) => typeof value?.connect === "function" ? value.connect() : value;
var isStream = (value) => typeof value?.subscribe === "function" || typeof value?.connect === "function";
function mergedStreams(outer, selector, options = {}) {
  if (typeof options === "function") options = { comparer: options };
  const compare2 = typeof options.comparer === "function" ? options.comparer : options.comparer?.compare?.bind(options.comparer) ?? options.comparer?.Compare?.bind(options.comparer);
  const equals = typeof options.equalityComparer === "function" ? options.equalityComparer : options.equalityComparer?.equals?.bind(options.equalityComparer) ?? options.equalityComparer?.Equals?.bind(options.equalityComparer);
  return new Observable5((observer) => {
    const subscriptions = new Subscription5(), records = [], parents = /* @__PURE__ */ new Map(), parentList = [];
    const published = /* @__PURE__ */ new Map();
    let kind = null, sequence = 0, outerDone = false, parentKind;
    const pending = [];
    let draining = false;
    const checkComplete = () => {
      if (outerDone && options.completable !== false && records.every((record) => record.done)) observer.complete();
    };
    function enqueue(work) {
      pending.push(work);
      if (draining) return;
      draining = true;
      try {
        while (pending.length && !observer.closed) pending.shift()();
      } catch (error) {
        observer.error(error);
      } finally {
        draining = false;
      }
    }
    const listOffset = (record) => records.slice(0, records.indexOf(record)).reduce((total, item) => total + (Array.isArray(item.state) ? item.state.length : 0), 0);
    function publishList(changes) {
      if (!changes.length) return;
      const snapshot = records.flatMap((record) => Array.isArray(record.state) ? record.state : []);
      observer.next(new ChangeSet(changes, "list", snapshot));
    }
    function reconcile(keys, incoming, refreshed = /* @__PURE__ */ new Set(), updated = /* @__PURE__ */ new Set()) {
      const output = [];
      for (const key of keys) {
        let candidate;
        for (const record of records) {
          if (!(record.state instanceof Map) || !record.state.has(key)) continue;
          const value = record.state.get(key);
          const rank = candidate && compare2 ? compare2(value, candidate.value) : 0;
          if (!candidate || compare2 && rank < 0 || (!compare2 || rank === 0) && record.order.get(key) < candidate.record.order.get(key)) candidate = { record, value };
        }
        const old = published.get(key);
        if (!candidate) {
          if (old) {
            output.push({ reason: "remove", key, current: old.value });
            published.delete(key);
          }
          continue;
        }
        const sameValue = old && equals && equals(old.value, candidate.value);
        const effectiveValue = sameValue ? old.value : candidate.value;
        const changed = old && !Object.is(old.value, candidate.value);
        const forceUpdate = old && candidate.record === incoming && updated.has(key) && candidate.record === old.record;
        if (!old) output.push({ reason: "add", key, current: candidate.value });
        else if ((changed || forceUpdate) && !sameValue) output.push({ reason: "update", key, current: candidate.value, previous: old.value });
        else if (candidate.record === incoming && refreshed.has(key)) output.push({ reason: "refresh", key, current: effectiveValue });
        candidate.value = effectiveValue;
        published.set(key, candidate);
      }
      if (output.length) observer.next(new ChangeSet(output, "cache"));
    }
    function accept(record, changes) {
      if (!record.alive) return;
      const incomingKind = kindOf(changes);
      if (kind && kind !== incomingKind) throw new TypeError("Merged children must all use the same changeset kind");
      kind ??= incomingKind;
      record.state ??= kind === "cache" ? /* @__PURE__ */ new Map() : [];
      if (kind === "cache") {
        const keys = /* @__PURE__ */ new Set(), refreshed = /* @__PURE__ */ new Set(), updated = /* @__PURE__ */ new Set();
        for (const change of changes) {
          if (change.reason === "clear") {
            for (const key2 of record.state.keys()) keys.add(key2);
            record.state.clear();
            record.order.clear();
            continue;
          }
          const key = change.key;
          keys.add(key);
          if (change.reason === "add" || change.reason === "update" || change.reason === "replace") {
            if (!record.state.has(key)) record.order.set(key, sequence++);
            record.state.set(key, change.current);
            updated.add(key);
          } else if (change.reason === "remove") {
            record.state.delete(key);
            record.order.delete(key);
          } else if (change.reason === "refresh") refreshed.add(key);
        }
        reconcile(keys, record, refreshed, updated);
      } else {
        const offset = listOffset(record), output = [];
        for (const change of changes) {
          const local = record.state;
          if (change.range) {
            if (change.reason === "addRange") {
              const at = change.range.index < 0 ? local.length : change.range.index;
              output.push({ ...change, range: { items: [...change.range.items], index: offset + at }, currentIndex: offset + at });
              local.splice(at, 0, ...change.range.items);
            } else if (change.reason === "clear") {
              if (local.length) output.push({ reason: "removeRange", range: { items: [...local], index: offset }, currentIndex: offset });
              local.length = 0;
            } else if (change.range.index >= 0) {
              const at = change.range.index;
              output.push({ ...change, range: { items: [...change.range.items], index: offset + at }, currentIndex: offset + at });
              local.splice(at, change.range.items.length);
            } else {
              for (const item of change.range.items) {
                const at = local.indexOf(item);
                if (at >= 0) {
                  output.push({ reason: "remove", current: item, currentIndex: offset + at });
                  local.splice(at, 1);
                }
              }
            }
          } else {
            let ci = change.currentIndex ?? -1, pi = change.previousIndex ?? -1;
            if (change.reason === "add") ci = ci < 0 ? local.length : ci;
            else if (change.reason === "replace" || change.reason === "update") {
              pi = pi < 0 ? ci < 0 ? local.indexOf(change.previous) : ci : pi;
              ci = ci < 0 ? pi : ci;
            } else if (change.reason === "remove" || change.reason === "refresh") ci = ci < 0 ? local.indexOf(change.current) : ci;
            else if (change.reason === "move" || change.reason === "moved") pi = pi < 0 ? local.indexOf(change.current) : pi;
            output.push({ ...change, currentIndex: ci < 0 ? -1 : offset + ci, previousIndex: pi < 0 ? -1 : offset + pi });
            applyChanges(local, new ChangeSet([{ ...change, currentIndex: ci, previousIndex: pi }], "list"));
          }
        }
        publishList(output);
      }
    }
    function add(stream, item, key) {
      const record = { stream, item, key, state: null, order: /* @__PURE__ */ new Map(), alive: true, done: false, subscription: new Subscription5() };
      records.push(record);
      subscriptions.add(record.subscription);
      const observable2 = asStream2(stream);
      if (!observable2?.subscribe) throw new TypeError("Child selector must return an Observable or an observable collection");
      record.subscription.add(observable2.subscribe({
        next(changes) {
          enqueue(() => accept(record, changes));
        },
        error(error) {
          observer.error(error);
        },
        complete() {
          enqueue(() => {
            record.done = true;
            checkComplete();
          });
        }
      }));
      return record;
    }
    function remove2(record) {
      if (!record?.alive) return;
      const offset = kind === "list" ? listOffset(record) : 0;
      record.alive = false;
      record.subscription.unsubscribe();
      records.splice(records.indexOf(record), 1);
      if (record.state instanceof Map) reconcile(new Set(record.state.keys()));
      else if (record.state?.length) publishList([{ reason: "removeRange", range: { items: [...record.state], index: offset }, currentIndex: offset }]);
      checkComplete();
    }
    function addParent(item, key, at = parentList.length) {
      const record = add(selector ? selector(item, key) : item, item, key);
      if (parentKind === "cache") parents.set(key, record);
      else parentList.splice(at, 0, record);
      return record;
    }
    function parentChanges(changes) {
      parentKind ??= kindOf(changes);
      for (const change of itemChanges(changes)) {
        if (parentKind === "cache") {
          if (change.reason === "clear") {
            for (const record of parents.values()) remove2(record);
            parents.clear();
          } else if (change.reason === "remove") {
            remove2(parents.get(change.key));
            parents.delete(change.key);
          } else if (change.reason === "add" || change.reason === "update") {
            remove2(parents.get(change.key));
            addParent(change.current, change.key);
          }
        } else {
          let at = change.currentIndex ?? -1;
          if (change.reason === "add") addParent(change.current, void 0, at < 0 ? parentList.length : at);
          else if (change.reason === "remove") {
            if (at < 0) at = parentList.findIndex((record) => Object.is(record.item, change.current));
            if (at >= 0) {
              const [record] = parentList.splice(at, 1);
              remove2(record);
            }
          } else if (change.reason === "replace" || change.reason === "update") {
            let oldIndex = change.previousIndex >= 0 ? change.previousIndex : at >= 0 ? at : parentList.findIndex((record) => Object.is(record.item, change.previous));
            if (oldIndex >= 0) {
              const [record] = parentList.splice(oldIndex, 1);
              remove2(record);
            }
            addParent(change.current, void 0, at < 0 ? Math.max(oldIndex, 0) : at);
          } else if (change.reason === "move") {
            const [record] = parentList.splice(change.previousIndex, 1);
            if (record) parentList.splice(at, 0, record);
          }
        }
      }
    }
    subscriptions.add(asStream2(outer).subscribe({
      next(value) {
        enqueue(() => {
          if (!selector && isStream(value)) add(value);
          else parentChanges(value);
        });
      },
      error(error) {
        observer.error(error);
      },
      complete() {
        enqueue(() => {
          outerDone = true;
          checkComplete();
        });
      }
    }));
    return () => {
      subscriptions.unsubscribe();
      for (const record of records) record.alive = false;
      records.length = 0;
      parents.clear();
      parentList.length = 0;
      published.clear();
      pending.length = 0;
    };
  });
}
function mergeChangeSets(sourcesOrOptions, options = {}) {
  if (Array.isArray(sourcesOrOptions)) return mergedStreams(from2(sourcesOrOptions), null, options);
  if (isStream(sourcesOrOptions)) return (source) => mergedStreams(from2([source, sourcesOrOptions]), null, options);
  return (source) => mergedStreams(source, null, sourcesOrOptions ?? options);
}
function mergeManyChangeSets(observableSelector, options = {}) {
  requireFunction(observableSelector, "observableSelector");
  return (source) => mergedStreams(source, observableSelector, options);
}

// src/helpers.js
import { Subject as Subject4, Subscription as Subscription6, asyncScheduler as asyncScheduler2, observeOn, retry, timer } from "rxjs";
var streamOf = (source) => typeof source?.connect === "function" ? source.connect() : source;
var equality2 = (comparer) => typeof comparer === "function" ? comparer : comparer?.equals?.bind(comparer) ?? comparer?.Equals?.bind(comparer) ?? Object.is;
var compareDefault2 = (a, b) => a < b ? -1 : a > b ? 1 : 0;
var validateList = (source) => {
  if (!Array.isArray(source) && typeof source?.insertRange !== "function") throw new TypeError("source must be an Array or SourceList");
};
function addOrInsertRange(source, items, index = -1) {
  validateList(source);
  const values = Array.from(items);
  const count2 = Array.isArray(source) ? source.length : source.count;
  if (!Number.isInteger(index) || index > count2) throw new RangeError("index must be an integer no greater than the list length");
  if (Array.isArray(source)) {
    const at = index < 0 ? source.length : index;
    const tail = source.splice(at);
    for (const item of values) source.push(item);
    for (const item of tail) source.push(item);
  } else if (index < 0) source.addRange(values);
  else source.insertRange(values, index);
  return source;
}
function binarySearch(source, value, comparer = compareDefault2) {
  const compare2 = typeof comparer === "function" ? comparer : comparer?.compare?.bind(comparer) ?? comparer?.Compare?.bind(comparer);
  if (typeof compare2 !== "function") throw new TypeError("comparer must be a function or comparer object");
  let lower = 0, upper = (source.length ?? source.count) - 1;
  const at = (index) => Array.isArray(source) || ArrayBuffer.isView(source) ? source[index] : source.get(index);
  while (lower <= upper) {
    const middle = lower + Math.floor((upper - lower) / 2), result = compare2(value, at(middle));
    if (result < 0) upper = middle - 1;
    else if (result > 0) lower = middle + 1;
    else return middle;
  }
  return -lower - 1;
}
function getChangeType(reason2) {
  if (["add", "refresh", "replace", "move", "moved", "remove"].includes(reason2)) return ChangeType.Item;
  if (["addRange", "removeRange", "clear"].includes(reason2)) return ChangeType.Range;
  throw new RangeError(`Unknown list change reason: ${reason2}`);
}
var ItemWithIndex = class _ItemWithIndex {
  constructor(item, index) {
    this.item = item;
    this.index = index;
  }
  get Item() {
    return this.item;
  }
  get Index() {
    return this.index;
  }
  equals(other) {
    return other instanceof _ItemWithIndex && Object.is(this.item, other.item);
  }
  Equals(other) {
    return this.equals(other);
  }
  toString() {
    return `${this.item} (${this.index})`;
  }
};
function indexOfOptional(source, item, comparer) {
  const equals = equality2(comparer);
  let index = 0;
  for (const candidate of source) {
    if (equals(candidate, item)) return Optional.some(new ItemWithIndex(item, index));
    index++;
  }
  return Optional.none();
}
function replaceOrAdd(source, original, replaceWith, comparer) {
  validateList(source);
  const found = indexOfOptional(source, original, comparer);
  if (found.hasValue) {
    if (Array.isArray(source)) source[found.value.index] = replaceWith;
    else source.replaceAt(found.value.index, replaceWith);
  } else if (Array.isArray(source)) source.push(replaceWith);
  else source.add(replaceWith);
  return source;
}
function* yieldWithoutIndex(source) {
  for (const change of source) {
    if (change.reason === "move" || change.reason === "moved") continue;
    yield new ListChange({ ...change, currentIndex: -1, previousIndex: -1, ...change.range ? { range: { items: [...change.range.items], index: -1 } } : {} });
  }
}
var ChangeSetAggregator = class {
  constructor(source, options = {}) {
    if (typeof options === "string") options = { kind: options };
    this.messages = [];
    this.error = null;
    this.isCompleted = false;
    this.isDisposed = false;
    this.summary = ChangeSummary.empty;
    this._kind = options.kind ?? source.kind ?? null;
    this._collection = this._kind === "list" ? new SourceList() : new SourceCache(options.keySelector ?? ((item) => item?.id ?? item?.key ?? item));
    this.data = this._kind === "list" ? this._collection.asObservableList() : this._collection.asObservableCache();
    const updates = new Subject4();
    this._subscriptions = new Subscription6();
    this._subscriptions.add(updates.pipe(collectUpdateStats()).subscribe((summary) => {
      this.summary = summary;
    }));
    this._subscriptions.add(streamOf(source).subscribe({
      next: (changes) => {
        const kind = changes.kind ?? (changes.some((change) => "key" in change) ? "cache" : "list");
        if (this._kind == null && kind === "list") {
          this.data.dispose();
          this._collection.dispose();
          this._collection = new SourceList();
          this.data = this._collection.asObservableList();
        }
        this._kind ??= kind;
        if (kind !== this._kind) {
          this.error = new TypeError("An aggregator cannot mix list and cache changesets");
          return;
        }
        this.messages.push(changes);
        this._collection.clone(changes);
        updates.next(changes);
      },
      error: (error) => {
        this.error = error;
        updates.complete();
      },
      complete: () => {
        this.isCompleted = true;
        updates.complete();
      }
    }));
    this._subscriptions.add(() => updates.complete());
  }
  get Data() {
    return this.data;
  }
  get Messages() {
    return this.messages;
  }
  get Error() {
    return this.error;
  }
  get exception() {
    return this.error;
  }
  get Exception() {
    return this.error;
  }
  get IsCompleted() {
    return this.isCompleted;
  }
  get Summary() {
    return this.summary;
  }
  dispose() {
    if (!this.isDisposed) {
      this.isDisposed = true;
      this._subscriptions.unsubscribe();
      this.data.dispose();
      this._collection.dispose();
    }
  }
  unsubscribe() {
    this.dispose();
  }
  Dispose() {
    this.dispose();
  }
};
function asAggregator(source, options) {
  return new ChangeSetAggregator(source, options);
}
var Watcher = class {
  constructor(source, scheduler) {
    this.scheduler = scheduler;
    this.cache = new ObservableCache(streamOf(source));
  }
  watch(key) {
    const observable2 = this.cache.watch(key);
    return this.scheduler ? observable2.pipe(observeOn(this.scheduler)) : observable2;
  }
  Watch(key) {
    return this.watch(key);
  }
  dispose() {
    this.cache.dispose();
  }
  unsubscribe() {
    this.dispose();
  }
  Dispose() {
    this.dispose();
  }
};
function asWatcher(source, scheduler) {
  return new Watcher(source, scheduler);
}
function retryWithBackOff(strategyOrOptions = {}) {
  const options = typeof strategyOrOptions === "function" ? { backOffStrategy: strategyOrOptions } : strategyOrOptions;
  const count2 = options.count ?? options.maxRetries ?? Infinity;
  const scheduler = options.scheduler ?? asyncScheduler2;
  const strategy = options.backOffStrategy ?? (typeof options.delay === "function" ? options.delay : null);
  const initialDelay = options.initialDelay ?? (typeof options.delay === "number" ? options.delay : 100);
  return retry({
    count: count2,
    resetOnSuccess: options.resetOnSuccess ?? false,
    delay(error, retryCount) {
      if (options.errorPredicate && !options.errorPredicate(error)) throw error;
      const delay = strategy ? strategy(error, retryCount - 1) : Math.min(options.maxDelay ?? 3e4, initialDelay * (options.factor ?? 2) ** (retryCount - 1));
      if (delay == null) throw error;
      if (typeof delay?.subscribe === "function") return delay;
      if (typeof delay !== "number" || !Number.isFinite(delay) || delay < 0) throw new RangeError("Retry delay must be a finite nonnegative number of milliseconds, an Observable, or null");
      return timer(delay, scheduler);
    }
  });
}
function scheduleRecurringAction(scheduler, interval, action) {
  if (typeof scheduler?.schedule !== "function") {
    action = interval;
    interval = scheduler;
    scheduler = asyncScheduler2;
  }
  if (typeof action !== "function") throw new TypeError("action must be a function");
  const delay = () => {
    const value = typeof interval === "function" ? interval() : interval;
    if (typeof value !== "number" || !Number.isFinite(value) || value < 0) throw new RangeError("interval must be a finite nonnegative number of milliseconds");
    return value;
  };
  const subscription = new Subscription6();
  subscription.dispose = subscription.unsubscribe.bind(subscription);
  subscription.Dispose = subscription.dispose;
  subscription.add(scheduler.schedule(function tick() {
    if (subscription.closed) return;
    try {
      action();
      if (!subscription.closed) this.schedule(void 0, delay());
    } catch (error) {
      subscription.unsubscribe();
      throw error;
    }
  }, delay()));
  return subscription;
}

// src/kernel.js
import { isObservable as isObservable4, map as map3, filter as filter2, tap as tap2 } from "rxjs";
var identity3 = (value) => value;
var requireFunction2 = (fn, name) => {
  if (typeof fn !== "function") throw new TypeError(`${name} must be a function`);
  return fn;
};
var isOptional = (value) => value instanceof Optional;
var requireOptional = (value) => {
  if (!isOptional(value)) throw new TypeError("Expected Optional.some(value) or Optional.none()");
  return value;
};
var optionalMap = (source, project) => isObservable4(source) ? source.pipe(map3((value) => project(requireOptional(value)))) : project(requireOptional(source));
var fallbackValue = (value) => typeof value === "function" ? value() : value;
var equal = (a, b) => a === b || Number.isNaN(a) && Number.isNaN(b);
function asArray(source) {
  if (source == null || typeof source[Symbol.iterator] !== "function") throw new TypeError("Expected an iterable");
  return Array.isArray(source) ? source : Array.from(source);
}
var asList = asArray;
function duplicates(source, valueSelector = identity3) {
  requireFunction2(valueSelector, "valueSelector");
  const groups = /* @__PURE__ */ new Map();
  for (const item of asArray(source)) {
    const key = valueSelector(item);
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(item);
  }
  return [...groups.values()].filter((items) => items.length > 1).flat();
}
function indexOfMany(source, itemsToFind, resultSelector = (item, index) => new ItemWithIndex2(item, index)) {
  requireFunction2(resultSelector, "resultSelector");
  const index = /* @__PURE__ */ new Map();
  asArray(source).forEach((item, i) => {
    if (!index.has(item)) index.set(item, []);
    index.get(item).push([item, i]);
  });
  const results = [];
  for (const item of asArray(itemsToFind)) for (const [found, i] of index.get(item) || []) results.push(resultSelector(found, i));
  return results;
}
function firstOrOptional(source, predicate = () => true) {
  requireFunction2(predicate, "predicate");
  if (source == null || typeof source[Symbol.iterator] !== "function") throw new TypeError("Expected an iterable");
  for (const item of source) if (predicate(item)) return Optional.some(item);
  return Optional.none();
}
function toOptional(source) {
  if (!arguments.length) return (input) => isObservable4(input) ? input.pipe(map3((value) => Optional.of(value))) : Optional.of(input);
  return isObservable4(source) ? source.pipe(map3((value) => Optional.of(value))) : Optional.of(source);
}
var createOptional = (source) => Optional.of(source);
function fromOptional(source) {
  if (!arguments.length) return (input) => optionalMap(input, (value) => value.value);
  return optionalMap(source, (value) => value.value);
}
function convertOptional(sourceOrConverter, maybeConverter) {
  const direct = arguments.length > 1, converter = requireFunction2(direct ? maybeConverter : sourceOrConverter, "converter");
  const project = (value) => {
    if (!value.hasValue) return Optional.none();
    const result = converter(value.value);
    return isOptional(result) ? result : Optional.some(result);
  };
  return direct ? optionalMap(sourceOrConverter, project) : (source) => optionalMap(source, project);
}
function convertOr(sourceOrConverter, converterOrFallback, maybeFallback) {
  const direct = arguments.length >= 3;
  const converter = requireFunction2(direct ? converterOrFallback : sourceOrConverter, "converter");
  const fallback = requireFunction2(direct ? maybeFallback : converterOrFallback, "fallbackConverter");
  const project = (value) => value.hasValue ? converter(value.value) : fallback();
  return direct ? optionalMap(sourceOrConverter, project) : (source) => optionalMap(source, project);
}
function orElse(sourceOrFallback, maybeFallback) {
  const direct = arguments.length > 1, fallback = requireFunction2(direct ? maybeFallback : sourceOrFallback, "fallbackOperation");
  const project = (value) => value.hasValue ? value : requireOptional(fallback());
  return direct ? optionalMap(sourceOrFallback, project) : (source) => optionalMap(source, project);
}
function valueOr(sourceOrFallback, maybeFallback) {
  const direct = arguments.length > 1, fallback = direct ? maybeFallback : sourceOrFallback;
  const project = (value) => value.hasValue ? value.value : fallbackValue(fallback);
  if (direct && !isOptional(sourceOrFallback) && !isObservable4(sourceOrFallback)) return sourceOrFallback ?? fallbackValue(fallback);
  return direct ? optionalMap(sourceOrFallback, project) : (source) => optionalMap(source, project);
}
function valueOrDefault(source) {
  const project = (value) => value.hasValue ? value.value : void 0;
  return arguments.length ? optionalMap(source, project) : (input) => optionalMap(input, project);
}
function valueOrThrow(sourceOrException, maybeException) {
  const direct = isOptional(sourceOrException) || isObservable4(sourceOrException);
  const exception = requireFunction2((direct ? maybeException : sourceOrException) ?? (() => new globalThis.Error("Optional has no value")), "exceptionGenerator");
  const project = (value) => {
    if (value.hasValue) return value.value;
    throw exception();
  };
  return direct ? optionalMap(sourceOrException, project) : (source) => optionalMap(source, project);
}
function optionalEffect(onPresent, sourceOrAction, actionOrElse, maybeElse) {
  const direct = isOptional(sourceOrAction) || isObservable4(sourceOrAction);
  const action = requireFunction2(direct ? actionOrElse : sourceOrAction, "action"), otherwise = direct ? maybeElse : actionOrElse;
  if (otherwise != null) requireFunction2(otherwise, "elseAction");
  const effect = (value) => {
    requireOptional(value);
    if (onPresent) {
      if (value.hasValue) action(value.value);
      else otherwise?.();
    } else {
      if (!value.hasValue) action();
      else otherwise?.(value.value);
    }
  };
  const run = (source) => {
    if (isObservable4(source)) return source.pipe(tap2(effect));
    effect(source);
    return source;
  };
  return direct ? run(sourceOrAction) : run;
}
function onHasValue(sourceOrAction, actionOrElse, maybeElse) {
  return optionalEffect(true, sourceOrAction, actionOrElse, maybeElse);
}
function onHasNoValue(sourceOrAction, actionOrElse, maybeElse) {
  return optionalEffect(false, sourceOrAction, actionOrElse, maybeElse);
}
function selectValues(source) {
  const run = (input) => {
    if (isObservable4(input)) return input.pipe(filter2((value) => requireOptional(value).hasValue), map3((value) => value.value));
    const values = [];
    for (const optional2 of asArray(input)) if (requireOptional(optional2).hasValue) values.push(optional2.value);
    return values;
  };
  return arguments.length ? run(source) : run;
}
function lookup(source, key) {
  if (source instanceof Map) return source.has(key) ? Optional.some(source.get(key)) : Optional.none();
  if (typeof source?.lookup === "function") return source.lookup(key);
  if (source && typeof source === "object") return Object.hasOwn(source, key) ? Optional.some(source[key]) : Optional.none();
  throw new TypeError("lookup requires a Map, cache or object");
}
function removeIfContained(source, key) {
  if (source instanceof Map) return source.delete(key);
  if (source && typeof source === "object") {
    if (!Object.hasOwn(source, key)) return false;
    return Reflect.deleteProperty(source, key);
  }
  throw new TypeError("removeIfContained requires a Map or object");
}
function getValueOrDefault(source, key, fallback) {
  if (isOptional(source)) return source.hasValue ? source.value : key;
  const value = lookup(source, key);
  return value.hasValue ? value.value : fallback;
}
var OptionElse = class {
  constructor(shouldRunAction = true) {
    this.shouldRunAction = shouldRunAction;
    Object.freeze(this);
  }
  else(action) {
    requireFunction2(action, "action");
    if (this.shouldRunAction) action();
  }
  Else(action) {
    return this.else(action);
  }
};
function ifHasValue(source, action) {
  requireFunction2(action, "action");
  if (source == null) return new OptionElse();
  requireOptional(source);
  if (!source.hasValue) return new OptionElse();
  action(source.value);
  return new OptionElse(false);
}
var ItemWithIndex2 = class _ItemWithIndex {
  constructor(item, index) {
    if (!Number.isInteger(index)) throw new TypeError("index must be an integer");
    this.item = item;
    this.index = index;
    Object.freeze(this);
  }
  get Item() {
    return this.item;
  }
  get Index() {
    return this.index;
  }
  // Upstream equality deliberately compares the item, not its index.
  equals(other) {
    return other instanceof _ItemWithIndex && equal(this.item, other.item);
  }
  Equals(other) {
    return this.equals(other);
  }
  toString() {
    return `${String(this.item)} (${this.index})`;
  }
  ToString() {
    return this.toString();
  }
};
var ItemWithValue = class _ItemWithValue {
  constructor(item, value) {
    this.item = item;
    this.value = value;
    Object.freeze(this);
  }
  get Item() {
    return this.item;
  }
  get Value() {
    return this.value;
  }
  equals(other) {
    return other instanceof _ItemWithValue && equal(this.item, other.item) && equal(this.value, other.value);
  }
  Equals(other) {
    return this.equals(other);
  }
  toString() {
    return `${String(this.item)} (${String(this.value)})`;
  }
  ToString() {
    return this.toString();
  }
};
var ErrorInfo = class _ErrorInfo {
  constructor(exception, value, key) {
    this.exception = exception;
    this.value = value;
    this.key = key;
    Object.freeze(this);
  }
  get Exception() {
    return this.exception;
  }
  get Value() {
    return this.value;
  }
  get Key() {
    return this.key;
  }
  equals(other) {
    return other instanceof _ErrorInfo && equal(this.exception, other.exception) && equal(this.value, other.value) && equal(this.key, other.key);
  }
  Equals(other) {
    return this.equals(other);
  }
  toString() {
    return `Key: ${String(this.key)}, Value: ${String(this.value)}, Exception: ${String(this.exception)}`;
  }
  ToString() {
    return this.toString();
  }
};
var SortExpression = class {
  constructor(expression, direction = "ascending") {
    this.expression = requireFunction2(expression, "expression");
    this.selector = expression;
    if (direction === 0) direction = "ascending";
    else if (direction === 1) direction = "descending";
    direction = String(direction).toLowerCase();
    if (direction !== "ascending" && direction !== "descending") throw new TypeError("direction must be ascending or descending");
    this.direction = direction;
    Object.freeze(this);
  }
  get Expression() {
    return this.expression;
  }
  get Direction() {
    return this.direction;
  }
  compare(left, right) {
    const a = this.expression(left), b = this.expression(right), comparison = equal(a, b) ? 0 : a == null ? -1 : b == null ? 1 : Number.isNaN(a) ? -1 : Number.isNaN(b) ? 1 : a < b ? -1 : a > b ? 1 : 0;
    return this.direction === "descending" ? -comparison : comparison;
  }
  Compare(left, right) {
    return this.compare(left, right);
  }
};

// src/compatibility.js
import { Observable as Observable6, Subscription as Subscription7, animationFrameScheduler, asyncScheduler as asyncScheduler3, observeOn as rxObserveOn, subscribeOn as rxSubscribeOn, map as map4, distinctUntilChanged as distinctUntilChanged3 } from "rxjs";
var compare = (a, b) => a == null ? b == null ? 0 : -1 : b == null ? 1 : a < b ? -1 : a > b ? 1 : 0;
var SortDirection = Object.freeze({ Ascending: "ascending", Descending: "descending" });
var SortExpressionComparer = class _SortExpressionComparer {
  constructor(expressions = []) {
    this.expressions = Array.from(expressions);
  }
  compare(a, b) {
    for (const { selector, direction } of this.expressions) {
      const n = compare(selector(a), selector(b));
      if (n) return direction === "descending" ? -n : n;
    }
    return 0;
  }
  Compare(a, b) {
    return this.compare(a, b);
  }
  thenBy(selector, direction = "ascending") {
    return new _SortExpressionComparer([...this.expressions, { selector, direction }]);
  }
  thenByAscending(selector) {
    return this.thenBy(selector);
  }
  thenByDescending(selector) {
    return this.thenBy(selector, "descending");
  }
  ThenBy(selector, direction) {
    return this.thenBy(selector, direction);
  }
  ThenByAscending(selector) {
    return this.thenByAscending(selector);
  }
  ThenByDescending(selector) {
    return this.thenByDescending(selector);
  }
  static ascending(selector) {
    return new _SortExpressionComparer([{ selector, direction: "ascending" }]);
  }
  static descending(selector) {
    return new _SortExpressionComparer([{ selector, direction: "descending" }]);
  }
  static Ascending(selector) {
    return this.ascending(selector);
  }
  static Descending(selector) {
    return this.descending(selector);
  }
};
var PageRequest = class _PageRequest {
  constructor(page2 = 1, size = 25) {
    if (!Number.isInteger(page2) || page2 < 1 || !Number.isInteger(size) || size < 1) throw new RangeError("Page and size must be positive integers");
    this.page = page2;
    this.size = size;
    Object.freeze(this);
  }
  get Page() {
    return this.page;
  }
  get Size() {
    return this.size;
  }
  equals(other) {
    return other?.page === this.page && other?.size === this.size;
  }
  static get Default() {
    return new _PageRequest();
  }
};
var VirtualRequest = class _VirtualRequest {
  constructor(startIndex = 0, size = 25) {
    if (!Number.isInteger(startIndex) || startIndex < 0 || !Number.isInteger(size) || size < 1) throw new RangeError("Start index must be nonnegative and size positive");
    this.startIndex = startIndex;
    this.size = size;
    Object.freeze(this);
  }
  get StartIndex() {
    return this.startIndex;
  }
  get Size() {
    return this.size;
  }
  equals(other) {
    return other?.startIndex === this.startIndex && other?.size === this.size;
  }
  static get Default() {
    return new _VirtualRequest();
  }
};
var BindingOptions = class _BindingOptions {
  constructor(options = {}) {
    this.resetThreshold = 25;
    Object.assign(this, options);
  }
  static neverFireReset(useReplaceForUpdates = true) {
    return new _BindingOptions({ resetThreshold: Infinity, useReplaceForUpdates });
  }
  static NeverFireReset(value) {
    return this.neverFireReset(value);
  }
};
var SortAndBindOptions = class extends BindingOptions {
  constructor(options = {}) {
    super(options);
  }
};
var SortOptions = Object.freeze({ None: 0, UseBinarySearch: 1, ComparesImmutableValuesOnly: 2 });
var DynamicDataOptions = Object.freeze({ binding: new BindingOptions(), scheduler: asyncScheduler3 });
var ObservableCollectionExtended = class extends SourceList {
  constructor(items = []) {
    super();
    if (items.length) this.addRange(items);
  }
  replaceAll(items) {
    return this.edit((list) => {
      list.clear();
      list.addRange(items);
    });
  }
  ReplaceAll(items) {
    return this.replaceAll(items);
  }
  suspendCountNotifications() {
    return this.suspendCount();
  }
};
var minimum = min;
var maximum = max;
var isEmpty = () => (source) => source.pipe(count(), map4((n) => n === 0), distinctUntilChanged3());
var isNotEmpty = () => (source) => source.pipe(count(), map4((n) => n !== 0), distinctUntilChanged3());
var addOrUpdate = (source, ...args) => source.addOrUpdate(...args);
var clear = (source) => source.clear();
var refresh = (source, ...args) => source.refresh(...args);
var remove = (source, ...args) => source.remove(...args);
var removeKeys = (source, ...args) => source.removeKeys(...args);
function editDiff(source, items, equality3 = Object.is) {
  const incoming = Array.from(items);
  if (source instanceof SourceCache) {
    const byKey = new Map(incoming.map((x) => [source.getKey(x), x]));
    return source.edit((c) => {
      c.removeKeys(c.keys.filter((k) => !byKey.has(k)));
      for (const [key, value] of byKey) {
        const old = c.lookup(key);
        if (!old.hasValue || !equality3(old.value, value)) c.addOrUpdate(value);
      }
    });
  }
  return source.edit((list) => {
    for (let i = list.count - 1; i >= incoming.length; i--) list.removeAt(i);
    for (let i = 0; i < incoming.length; i++) {
      if (i >= list.count) list.add(incoming[i]);
      else if (!equality3(list.items[i], incoming[i])) list.replaceAt(i, incoming[i]);
    }
  });
}
var observeOn2 = (scheduler) => rxObserveOn(scheduler);
var subscribeOn = (scheduler) => rxSubscribeOn(scheduler);
var observeOnDispatcher = () => rxObserveOn(typeof requestAnimationFrame === "function" ? animationFrameScheduler : asyncScheduler3);
var decorated = /* @__PURE__ */ new WeakSet();
var operatorRegistry = {};
function fluent(observable2) {
  if (!(observable2 instanceof Observable6) && !observable2?.subscribe) return observable2;
  if (decorated.has(observable2)) return observable2;
  decorated.add(observable2);
  const originalPipe = observable2.pipe.bind(observable2);
  Object.defineProperty(observable2, "pipe", { value: (...operators) => fluent(originalPipe(...operators)), configurable: true });
  Object.defineProperty(observable2, "Pipe", { value: (...operators) => fluent(originalPipe(...operators)), configurable: true });
  Object.defineProperty(observable2, "Subscribe", { value: (...args) => {
    const sub = observable2.subscribe(...args);
    if (!sub.Dispose) Object.defineProperty(sub, "Dispose", { value: () => sub.unsubscribe() });
    return sub;
  }, configurable: true });
  for (const [name, operator] of Object.entries(operatorRegistry)) {
    if (typeof operator !== "function" || !/^[A-Z]/.test(name) || name in observable2) continue;
    Object.defineProperty(observable2, name, { value: (...args) => {
      const result = operator(...args);
      return typeof result === "function" ? fluent(result(observable2)) : result;
    }, configurable: true });
  }
  return observable2;
}
function installFluentOperators(registry) {
  operatorRegistry = registry;
  setObservableDecorator(fluent);
}

// src/index.js
installFluentOperators({ AsObservableCache: asObservableCache, AsObservableList: asObservableList, Bind: bind, BindToObservableCollection: bindToObservableCollection, BindToObservableList: bindToObservableList, Cast: cast, CastToObject: castToObject, ChangeKey: changeKey, Convert: convert, DistinctValues: distinctValues, Filter: filter, FilterImmutable: filterImmutable, FilterWithState: filterWithState, OfType: ofType, Page: page, RemoveKey: removeKey, Reverse: reverse, Sort: sort, SortAndBind: sortAndBind, SortAndPage: sortAndPage, SortAndVirtualise: sortAndVirtualise, SortAndVirtualize: sortAndVirtualize, SortBy: sortBy, ToCollection: toCollection, Top: top, Transform: transform, TransformImmutable: transformImmutable, TransformMany: transformMany, TransformSafe: transformSafe, TransformWithInlineUpdate: transformWithInlineUpdate, Virtualise: virtualise, Virtualize: virtualize, And: and, Average: average, Avg: avg, Combine: combine, Count: count, Except: except, ForAggregation: forAggregation, FullJoin: fullJoin, FullJoinMany: fullJoinMany, Group: group, GroupOn: groupOn, GroupOnImmutable: groupOnImmutable, GroupOnObservable: groupOnObservable, GroupOnProperty: groupOnProperty, GroupOnPropertyWithImmutableState: groupOnPropertyWithImmutableState, GroupWithImmutableState: groupWithImmutableState, GroupWithSpecifiedGroups: groupWithSpecifiedGroups, InnerJoin: innerJoin, InnerJoinMany: innerJoinMany, InvalidateWhen: invalidateWhen, LeftJoin: leftJoin, LeftJoinMany: leftJoinMany, Max: max, Min: min, Or: or, RightJoin: rightJoin, RightJoinMany: rightJoinMany, StandardDeviation: standardDeviation, StdDev: stdDev, Sum: sum, SumMany: sumMany, Xor: xor, AsyncDisposeMany: asyncDisposeMany, AutoRefresh: autoRefresh, AutoRefreshOnObservable: autoRefreshOnObservable, Batch: batch, BatchIf: batchIf, BufferIf: bufferIf, BufferInitial: bufferInitial, CreateObservableObject: createObservableObject, DeferUntilLoaded: deferUntilLoaded, DisposeMany: disposeMany, ExpireAfter: expireAfter, FilterOnObservable: filterOnObservable, FilterOnProperty: filterOnProperty, FinallySafe: finallySafe, LimitSizeTo: limitSizeTo, MergeMany: mergeMany, MergeManyItems: mergeManyItems, MonitorStatus: monitorStatus, NotEmpty: notEmpty, NotifyPropertyChanged: notifyPropertyChanged, ObservableObject: observableObject, ObserveCollectionChanges: observeCollectionChanges, ObserveProperty: observeProperty, OnItemAdded: onItemAdded, OnItemRefreshed: onItemRefreshed, OnItemRemoved: onItemRemoved, OnItemUpdated: onItemUpdated, RefCount: refCount, SkipInitial: skipInitial, SubscribeMany: subscribeMany, Switch: switchLatest, ToObservableChangeSet: toObservableChangeSet, TransformAsync: transformAsync, TransformManyAsync: transformManyAsync, TransformManySafeAsync: transformManySafeAsync, TransformOnObservable: transformOnObservable, TransformSafeAsync: transformSafeAsync, TrueForAll: trueForAll, TrueForAny: trueForAny, Watch: watch, WatchValue: watchValue, WhenAnyPropertyChanged: whenAnyPropertyChanged, WhenChanged: whenChanged, WhenPropertyChanged: whenPropertyChanged, WhenValueChanged: whenValueChanged, Adapt: adapt, AddKey: addKey, Clone: clone, CollectUpdateStats: collectUpdateStats, EnsureUniqueKeys: ensureUniqueKeys, ExcludeUpdateWhen: excludeUpdateWhen, Flatten: flatten, FlattenBufferResult: flattenBufferResult, FlattenChanges: flattenChanges, ForEachChange: forEachChange, ForEachItemChange: forEachItemChange, IgnoreSameReferenceUpdate: ignoreSameReferenceUpdate, IgnoreUpdateWhen: ignoreUpdateWhen, IncludeUpdateWhen: includeUpdateWhen, InvokeEvaluate: invokeEvaluate, ItemChanges: itemChanges, MergeChangeSets: mergeChangeSets, MergeManyChangeSets: mergeManyChangeSets, PopulateFrom: populateFrom, PopulateInto: populateInto, QueryWhenChanged: queryWhenChanged, RemoveIndex: removeIndex, StartWithEmpty: startWithEmpty, StartWithItem: startWithItem, SuppressRefresh: suppressRefresh, ToObservableOptional: toObservableOptional, ToSortedCollection: toSortedCollection, TransformToTree: transformToTree, TreatMovesAsRemoveAdd: treatMovesAsRemoveAdd, UpdateIndex: updateIndex, WhereReasonsAre: whereReasonsAre, WhereReasonsAreNot: whereReasonsAreNot, AddOrInsertRange: addOrInsertRange, AsAggregator: asAggregator, AsWatcher: asWatcher, BinarySearch: binarySearch, GetChangeType: getChangeType, IndexOfOptional: indexOfOptional, ReplaceOrAdd: replaceOrAdd, RetryWithBackOff: retryWithBackOff, ScheduleRecurringAction: scheduleRecurringAction, YieldWithoutIndex: yieldWithoutIndex, AsArray: asArray, AsList: asList, ConvertOptional: convertOptional, ConvertOr: convertOr, CreateOptional: createOptional, Duplicates: duplicates, FirstOrOptional: firstOrOptional, FromOptional: fromOptional, GetValueOrDefault: getValueOrDefault, IfHasValue: ifHasValue, IndexOfMany: indexOfMany, Lookup: lookup, OnHasNoValue: onHasNoValue, OnHasValue: onHasValue, OrElse: orElse, RemoveIfContained: removeIfContained, SelectValues: selectValues, ToOptional: toOptional, ValueOr: valueOr, ValueOrDefault: valueOrDefault, ValueOrThrow: valueOrThrow, AddOrUpdate: addOrUpdate, Clear: clear, EditDiff: editDiff, IsEmpty: isEmpty, IsNotEmpty: isNotEmpty, Maximum: maximum, Minimum: minimum, ObserveOn: observeOn2, ObserveOnDispatcher: observeOnDispatcher, Refresh: refresh, Remove: remove, RemoveKeys: removeKeys, SubscribeOn: subscribeOn });
export {
  adapt as Adapt,
  addKey as AddKey,
  addOrInsertRange as AddOrInsertRange,
  addOrUpdate as AddOrUpdate,
  AggregateType,
  and as And,
  asAggregator as AsAggregator,
  asArray as AsArray,
  asList as AsList,
  asObservableCache as AsObservableCache,
  asObservableList as AsObservableList,
  asWatcher as AsWatcher,
  asyncDisposeMany as AsyncDisposeMany,
  autoRefresh as AutoRefresh,
  autoRefreshOnObservable as AutoRefreshOnObservable,
  average as Average,
  avg as Avg,
  batch as Batch,
  batchIf as BatchIf,
  binarySearch as BinarySearch,
  bind as Bind,
  bindToObservableCollection as BindToObservableCollection,
  bindToObservableList as BindToObservableList,
  BindingOptions,
  bufferIf as BufferIf,
  bufferInitial as BufferInitial,
  cast as Cast,
  castToObject as CastToObject,
  Change,
  ChangeAwareCache,
  ChangeAwareList,
  changeKey as ChangeKey,
  ChangeReason,
  ChangeSet,
  ChangeSetAggregator,
  ChangeStatistics,
  ChangeSummary,
  ChangeType,
  clear as Clear,
  clone as Clone,
  collectUpdateStats as CollectUpdateStats,
  combine as Combine,
  CombineOperator,
  ConnectionStatus,
  convert as Convert,
  convertOptional as ConvertOptional,
  convertOr as ConvertOr,
  count as Count,
  createObservableObject as CreateObservableObject,
  createOptional as CreateOptional,
  deferUntilLoaded as DeferUntilLoaded,
  disposeMany as DisposeMany,
  distinctValues as DistinctValues,
  duplicates as Duplicates,
  DynamicDataOptions,
  editDiff as EditDiff,
  ensureUniqueKeys as EnsureUniqueKeys,
  ErrorInfo as Error,
  ErrorInfo,
  except as Except,
  excludeUpdateWhen as ExcludeUpdateWhen,
  expireAfter as ExpireAfter,
  filter as Filter,
  filterImmutable as FilterImmutable,
  filterOnObservable as FilterOnObservable,
  filterOnProperty as FilterOnProperty,
  filterWithState as FilterWithState,
  finallySafe as FinallySafe,
  firstOrOptional as FirstOrOptional,
  flatten as Flatten,
  flattenBufferResult as FlattenBufferResult,
  flattenChanges as FlattenChanges,
  forAggregation as ForAggregation,
  forEachChange as ForEachChange,
  forEachItemChange as ForEachItemChange,
  fromOptional as FromOptional,
  fullJoin as FullJoin,
  fullJoinMany as FullJoinMany,
  getChangeType as GetChangeType,
  getValueOrDefault as GetValueOrDefault,
  Group,
  groupOn as GroupOn,
  groupOnImmutable as GroupOnImmutable,
  groupOnObservable as GroupOnObservable,
  groupOnProperty as GroupOnProperty,
  groupOnPropertyWithImmutableState as GroupOnPropertyWithImmutableState,
  groupWithImmutableState as GroupWithImmutableState,
  groupWithSpecifiedGroups as GroupWithSpecifiedGroups,
  ifHasValue as IfHasValue,
  ignoreSameReferenceUpdate as IgnoreSameReferenceUpdate,
  ignoreUpdateWhen as IgnoreUpdateWhen,
  ImmutableGroup,
  includeUpdateWhen as IncludeUpdateWhen,
  indexOfMany as IndexOfMany,
  indexOfOptional as IndexOfOptional,
  innerJoin as InnerJoin,
  innerJoinMany as InnerJoinMany,
  IntermediateCache,
  invalidateWhen as InvalidateWhen,
  invokeEvaluate as InvokeEvaluate,
  isEmpty as IsEmpty,
  isNotEmpty as IsNotEmpty,
  ItemChange,
  itemChanges as ItemChanges,
  ItemWithIndex2 as ItemWithIndex,
  ItemWithValue,
  leftJoin as LeftJoin,
  leftJoinMany as LeftJoinMany,
  limitSizeTo as LimitSizeTo,
  ListChange,
  ListChangeReason,
  lookup as Lookup,
  max as Max,
  maximum as Maximum,
  mergeChangeSets as MergeChangeSets,
  mergeMany as MergeMany,
  mergeManyChangeSets as MergeManyChangeSets,
  mergeManyItems as MergeManyItems,
  min as Min,
  minimum as Minimum,
  monitorStatus as MonitorStatus,
  Node,
  notEmpty as NotEmpty,
  notifyPropertyChanged as NotifyPropertyChanged,
  ObservableCache,
  ObservableChangeSet,
  ObservableCollectionExtended,
  ObservableList,
  observableObject as ObservableObject,
  observeCollectionChanges as ObserveCollectionChanges,
  observeOn2 as ObserveOn,
  observeOnDispatcher as ObserveOnDispatcher,
  observeProperty as ObserveProperty,
  ofType as OfType,
  onHasNoValue as OnHasNoValue,
  onHasValue as OnHasValue,
  onItemAdded as OnItemAdded,
  onItemRefreshed as OnItemRefreshed,
  onItemRemoved as OnItemRemoved,
  onItemUpdated as OnItemUpdated,
  OptionElse,
  Optional,
  or as Or,
  orElse as OrElse,
  page as Page,
  PageRequest,
  populateFrom as PopulateFrom,
  populateInto as PopulateInto,
  QuerySnapshot,
  queryWhenChanged as QueryWhenChanged,
  RangeChange,
  refCount as RefCount,
  refresh as Refresh,
  remove as Remove,
  removeIfContained as RemoveIfContained,
  removeIndex as RemoveIndex,
  removeKey as RemoveKey,
  removeKeys as RemoveKeys,
  replaceOrAdd as ReplaceOrAdd,
  retryWithBackOff as RetryWithBackOff,
  reverse as Reverse,
  rightJoin as RightJoin,
  rightJoinMany as RightJoinMany,
  scheduleRecurringAction as ScheduleRecurringAction,
  selectValues as SelectValues,
  skipInitial as SkipInitial,
  sort as Sort,
  sortAndBind as SortAndBind,
  SortAndBindOptions,
  sortAndPage as SortAndPage,
  sortAndVirtualise as SortAndVirtualise,
  sortAndVirtualize as SortAndVirtualize,
  sortBy as SortBy,
  SortDirection,
  SortExpression,
  SortExpressionComparer,
  SortOptions,
  SourceCache,
  SourceList,
  standardDeviation as StandardDeviation,
  startWithEmpty as StartWithEmpty,
  startWithItem as StartWithItem,
  stdDev as StdDev,
  subscribeMany as SubscribeMany,
  subscribeOn as SubscribeOn,
  sum as Sum,
  sumMany as SumMany,
  suppressRefresh as SuppressRefresh,
  switchLatest as Switch,
  toCollection as ToCollection,
  toObservableChangeSet as ToObservableChangeSet,
  toObservableOptional as ToObservableOptional,
  toOptional as ToOptional,
  toSortedCollection as ToSortedCollection,
  top as Top,
  transform as Transform,
  transformAsync as TransformAsync,
  transformImmutable as TransformImmutable,
  transformMany as TransformMany,
  transformManyAsync as TransformManyAsync,
  transformManySafeAsync as TransformManySafeAsync,
  transformOnObservable as TransformOnObservable,
  transformSafe as TransformSafe,
  transformSafeAsync as TransformSafeAsync,
  transformToTree as TransformToTree,
  transformWithInlineUpdate as TransformWithInlineUpdate,
  treatMovesAsRemoveAdd as TreatMovesAsRemoveAdd,
  trueForAll as TrueForAll,
  trueForAny as TrueForAny,
  updateIndex as UpdateIndex,
  valueOr as ValueOr,
  valueOrDefault as ValueOrDefault,
  valueOrThrow as ValueOrThrow,
  VirtualRequest,
  virtualise as Virtualise,
  virtualize as Virtualize,
  watch as Watch,
  watchValue as WatchValue,
  Watcher,
  whenAnyPropertyChanged as WhenAnyPropertyChanged,
  whenChanged as WhenChanged,
  whenPropertyChanged as WhenPropertyChanged,
  whenValueChanged as WhenValueChanged,
  whereReasonsAre as WhereReasonsAre,
  whereReasonsAreNot as WhereReasonsAreNot,
  xor as Xor,
  yieldWithoutIndex as YieldWithoutIndex,
  adapt,
  addKey,
  addOrInsertRange,
  addOrUpdate,
  and,
  applyChanges,
  asAggregator,
  asArray,
  asList,
  asObservableCache,
  asObservableList,
  asWatcher,
  asyncDisposeMany,
  autoRefresh,
  autoRefreshOnObservable,
  average,
  avg,
  batch,
  batchIf,
  binarySearch,
  bind,
  bindToObservableCollection,
  bindToObservableList,
  bufferIf,
  bufferInitial,
  cast,
  castToObject,
  changeKey,
  clear,
  clone,
  collectUpdateStats,
  combine,
  convert,
  convertOptional,
  convertOr,
  count,
  createObservableObject,
  createOptional,
  deferUntilLoaded,
  disposeMany,
  distinctValues,
  duplicates,
  editDiff,
  ensureUniqueKeys,
  except,
  excludeUpdateWhen,
  expireAfter,
  filter,
  filterImmutable,
  filterOnObservable,
  filterOnProperty,
  filterWithState,
  finallySafe,
  firstOrOptional,
  flatten,
  flattenBufferResult,
  flattenChanges,
  fluent,
  forAggregation,
  forEachChange,
  forEachItemChange,
  fromOptional,
  fullJoin,
  fullJoinMany,
  getChangeType,
  getValueOrDefault,
  group,
  groupOn,
  groupOnImmutable,
  groupOnObservable,
  groupOnProperty,
  groupOnPropertyWithImmutableState,
  groupWithImmutableState,
  groupWithSpecifiedGroups,
  ifHasValue,
  ignoreSameReferenceUpdate,
  ignoreUpdateWhen,
  includeUpdateWhen,
  indexOfMany,
  indexOfOptional,
  innerJoin,
  innerJoinMany,
  installFluentOperators,
  invalidateWhen,
  invokeEvaluate,
  isEmpty,
  isNotEmpty,
  itemChanges,
  leftJoin,
  leftJoinMany,
  limitSizeTo,
  lookup,
  max,
  maximum,
  mergeChangeSets,
  mergeMany,
  mergeManyChangeSets,
  mergeManyItems,
  min,
  minimum,
  monitorStatus,
  notEmpty,
  notifyPropertyChanged,
  observableObject,
  observeCollectionChanges,
  observeOn2 as observeOn,
  observeOnDispatcher,
  observeProperty,
  ofType,
  onHasNoValue,
  onHasValue,
  onItemAdded,
  onItemRefreshed,
  onItemRemoved,
  onItemUpdated,
  or,
  orElse,
  page,
  populateFrom,
  populateInto,
  queryWhenChanged,
  refCount,
  refresh,
  remove,
  removeIfContained,
  removeIndex,
  removeKey,
  removeKeys,
  replaceOrAdd,
  retryWithBackOff,
  reverse,
  rightJoin,
  rightJoinMany,
  scheduleRecurringAction,
  selectValues,
  setObservableDecorator,
  skipInitial,
  snapshotChanges,
  sort,
  sortAndBind,
  sortAndPage,
  sortAndVirtualise,
  sortAndVirtualize,
  sortBy,
  standardDeviation,
  startWithEmpty,
  startWithItem,
  stdDev,
  subscribeMany,
  subscribeOn,
  sum,
  sumMany,
  suppressRefresh,
  switchLatest as switch,
  switchLatest,
  toCollection,
  toObservableChangeSet,
  toObservableOptional,
  toOptional,
  toSortedCollection,
  top,
  transform,
  transformAsync,
  transformImmutable,
  transformMany,
  transformManyAsync,
  transformManySafeAsync,
  transformOnObservable,
  transformSafe,
  transformSafeAsync,
  transformToTree,
  transformWithInlineUpdate,
  treatMovesAsRemoveAdd,
  trueForAll,
  trueForAny,
  updateIndex,
  valueOr,
  valueOrDefault,
  valueOrThrow,
  virtualise,
  virtualize,
  watch,
  watchValue,
  whenAnyPropertyChanged,
  whenChanged,
  whenPropertyChanged,
  whenValueChanged,
  whereReasonsAre,
  whereReasonsAreNot,
  xor,
  yieldWithoutIndex
};
//# sourceMappingURL=index.js.map
