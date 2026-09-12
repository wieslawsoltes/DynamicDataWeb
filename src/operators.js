import { Observable, Subscription, isObservable } from 'rxjs';
import { Change, ListChange, ChangeSet, ObservableCache, ObservableList } from './core.js';

const identity = value => value;
const has = (value, key) => Object.prototype.hasOwnProperty.call(value, key);
const reasonOf = change => String(change.reason ?? change.Reason ?? '').replace(/^./, c => c.toLowerCase());
const observable = value => isObservable(value) || !!value?.subscribe;
const compareDefault = (a, b) => a == null ? (b == null ? 0 : -1) : b == null ? 1 : a < b ? -1 : a > b ? 1 : 0;
const comparerFunction = comparer => typeof comparer === 'function' ? comparer : comparer?.compare?.bind(comparer) ?? comparer?.Compare?.bind(comparer) ?? compareDefault;
const indexFor = (index, length) => Number.isInteger(index) && index >= 0 ? Math.min(index, length) : length;
function insertMany(array, index, values) { for (let offset = 0; offset < values.length; offset += 8192) array.splice(index + offset, 0, ...values.slice(offset, offset + 8192)); }

// List entries carry occurrence identities: equal values and repeated references remain separate.
// Cache entries carry their source keys. Both survive updates, refreshes and reordering.
class Model {
  constructor() { this.kind = null; this.entries = []; this.byKey = new Map(); this.updated = new Set(); this.refreshed = new Set(); }
  ingest(changes) {
    this.kind ??= changes.kind ?? (changes.some(c => has(c, 'key')) ? 'cache' : 'list');
    this.updated.clear(); this.refreshed.clear();
    const make = (value, key) => ({ id: this.kind === 'cache' ? key : Symbol(), key, value, version: 0 });
    const replace = (entry, value) => { const next = { ...entry, value, version: entry.version + 1 }; this.updated.add(entry.id); return next; };
    for (const c of changes) {
      const reason = reasonOf(c), current = has(c, 'current') ? c.current : c.item?.current;
      const ci = c.currentIndex ?? c.item?.currentIndex ?? -1;
      const pi = c.previousIndex ?? c.item?.previousIndex ?? -1;
      const range = Array.isArray(c.range) ? c.range : c.range?.items ?? [];
      const ri = c.range?.index ?? ci;
      if (this.kind === 'cache') {
        const key = c.key, entry = this.byKey.get(key);
        if (reason === 'clear') { this.entries = []; this.byKey.clear(); }
        else if (reason === 'add' || reason === 'update' || reason === 'replace') {
          if (entry) {
            const at = this.entries.indexOf(entry), next = replace(entry, current);
            this.entries[at] = next; this.byKey.set(key, next);
            if (ci >= 0 && ci !== at) { this.entries.splice(at, 1); this.entries.splice(indexFor(ci, this.entries.length), 0, next); }
          } else {
            const next = make(current, key); this.updated.add(next.id); this.byKey.set(key, next);
            this.entries.splice(indexFor(ci, this.entries.length), 0, next);
          }
        } else if (reason === 'remove' && entry) {
          this.entries.splice(this.entries.indexOf(entry), 1); this.byKey.delete(key);
        } else if (reason === 'refresh' && entry) { this.refreshed.add(entry.id); }
        else if ((reason === 'move' || reason === 'moved') && entry) {
          this.entries.splice(this.entries.indexOf(entry), 1); this.entries.splice(indexFor(ci, this.entries.length), 0, entry);
        }
      } else {
        if (reason === 'add') {
          const next = make(current); this.updated.add(next.id); this.entries.splice(indexFor(ci, this.entries.length), 0, next);
        } else if (reason === 'addRange') {
          const next = range.map(value => make(value)); next.forEach(e => this.updated.add(e.id));
          insertMany(this.entries, indexFor(ri, this.entries.length), next);
        } else if (reason === 'clear') { this.entries = []; }
        else if (reason === 'removeRange') {
          if (ri >= 0) this.entries.splice(ri, range.length || c.count || 0);
          else for (const value of range) { const i = this.entries.findIndex(e => Object.is(e.value, value)); if (i >= 0) this.entries.splice(i, 1); }
        } else if (reason === 'remove') {
          const i = ci >= 0 ? ci : pi >= 0 ? pi : this.entries.findIndex(e => Object.is(e.value, current));
          if (i >= 0 && i < this.entries.length) this.entries.splice(i, 1);
        } else if (reason === 'replace' || reason === 'update') {
          const i = pi >= 0 ? pi : ci >= 0 ? ci : this.entries.findIndex(e => Object.is(e.value, c.previous));
          if (i >= 0 && this.entries[i]) {
            const next = replace(this.entries[i], current); this.entries.splice(i, 1);
            this.entries.splice(ci >= 0 ? indexFor(ci, this.entries.length) : i, 0, next);
          }
        } else if (reason === 'refresh') {
          const i = ci >= 0 ? ci : this.entries.findIndex(e => Object.is(e.value, current));
          if (this.entries[i]) this.refreshed.add(this.entries[i].id);
        } else if (reason === 'move' || reason === 'moved') {
          if (pi >= 0 && pi < this.entries.length) { const [entry] = this.entries.splice(pi, 1); this.entries.splice(indexFor(ci, this.entries.length), 0, entry); }
        }
      }
    }
    // A cache can carry a sorted snapshot even when a move-free operator upstream supplied it.
    if (this.kind === 'cache' && Array.isArray(changes.keys) && changes.keys.length === this.entries.length) {
      const sorted = changes.keys.map(key => this.byKey.get(key));
      if (sorted.every(Boolean)) this.entries = sorted;
    }
    return this.entries;
  }
}

function changeSet(changes, kind, entries, metadata = {}) {
  const result = new ChangeSet(changes.map(change => kind === 'cache' ? new Change(change) : new ListChange(change)), kind, entries.map(e => e.value));
  if (kind === 'cache') result.keys = entries.map(e => e.key);
  Object.assign(result, metadata);
  return result;
}

function orderedView(entries, comparer, sortReason) {
  const items = entries.map(e => e.value), keys = entries.map(e => e.key), keyValues = entries.map(e => [e.key, e.value]);
  return { items, keys, keyValues, count: entries.length, comparer, sortReason,
    Items: items, Keys: keys, Count: entries.length, Comparer: comparer, SortReason: sortReason,
    [Symbol.iterator]: function* () { yield* keyValues; } };
}

// Produce sequentially applicable changes. All indices refer to the result of preceding changes.
function difference(previous, next, kind, refreshed = new Set(), updated = new Set(), trackMoves = true) {
  const changes = [], old = new Map(previous.map(e => [e.id, e])), live = new Set(next.map(e => e.id));
  const work = previous.slice();
  for (let i = work.length - 1; i >= 0; --i) if (!live.has(work[i].id)) {
    const e = work[i]; changes.push({ reason: 'remove', key: e.key, current: e.value, currentIndex: i, previousIndex: i }); work.splice(i, 1);
  }
  for (let i = 0; i < next.length; i++) {
    const e = next[i], before = old.get(e.id);
    if (!before) {
      changes.push({ reason: 'add', key: e.key, current: e.value, currentIndex: i });
      work.splice(i, 0, e); continue;
    }
    let at = trackMoves ? (work[i]?.id === e.id ? i : work.findIndex((x, index) => index >= i && x.id === e.id)) : i;
    if (trackMoves && at !== i && at >= 0) {
      changes.push({ reason: 'move', key: e.key, current: before.value, previousIndex: at, currentIndex: i });
      const [moved] = work.splice(at, 1); work.splice(i, 0, moved); at = i;
    }
    if (!Object.is(before.value, e.value) || before.version !== e.version || updated.has(e.id)) {
      changes.push({ reason: kind === 'cache' ? 'update' : 'replace', key: e.key, current: e.value, previous: before.value, currentIndex: i, previousIndex: i });
    } else if (refreshed.has(e.id)) changes.push({ reason: 'refresh', key: e.key, current: e.value, currentIndex: i });
    if (trackMoves) work[i] = e;
  }
  if (kind === 'list') for (const change of changes) delete change.key;
  return changes;
}

function derived(setup) {
  return source => new Observable(observer => {
    const subscriptions = new Subscription(), model = new Model();
    let previous = [], loaded = false;
    const run = fn => { if (!observer.closed) try { fn(); } catch (error) { observer.error(error); } };
    const emit = (entries, { kind = model.kind ?? 'cache', refresh = model.refreshed, updated = new Set(), suppressEmpty = true, metadata = {}, moves = true } = {}) => {
      const changes = difference(previous, entries, kind, refresh, updated, moves);
      previous = entries.map(e => ({ ...e }));
      if (changes.length || !suppressEmpty) observer.next(changeSet(changes, kind, entries, metadata));
    };
    const context = {
      model, emit, run, observer, subscriptions,
      emitRaw(changes, suppressEmpty = true, metadata = {}) { if (changes.length || !suppressEmpty) observer.next(Object.assign(new ChangeSet(changes.map(change => new Change(change)), 'cache'), metadata)); },
      get loaded() { return loaded; },
      watch(input, next) {
        if (input) subscriptions.add(input.subscribe({ next: value => run(() => next(value)), error: error => observer.error(error) }));
      }
    };
    let handler;
    run(() => { handler = setup(context); });
    if (!observer.closed) subscriptions.add(source.subscribe({
      next: changes => run(() => { loaded = true; if (handler.prepare) handler.prepare(changes); else model.ingest(changes); handler(changes); }),
      error: error => observer.error(error), complete: () => observer.complete()
    }));
    return subscriptions;
  });
}

/** Filter a changeset with a predicate, a predicate Observable, or (state$, (state,item,key)=>boolean). */
export function filter(predicate, reapplyOrOptions, suppressEmptyChangeSets = true) {
  let statePredicate, reapply, options = {};
  if (typeof reapplyOrOptions === 'function' && observable(predicate)) statePredicate = reapplyOrOptions;
  else if (observable(reapplyOrOptions)) reapply = reapplyOrOptions;
  else if (typeof reapplyOrOptions === 'boolean') options.suppressEmptyChangeSets = reapplyOrOptions;
  else options = reapplyOrOptions ?? {};
  reapply ??= options.reapply ?? options.reapplyFilter;
  return derived(ctx => {
    const matches = new Map(), input = new Map(), included = new Map();
    let fast, test = observable(predicate) ? () => false : predicate;
    if (typeof test !== 'function') throw new TypeError('filter requires a predicate or Observable.');
    const suppress = options.suppressEmptyChangeSets ?? suppressEmptyChangeSets;
    const evaluateCache = (changes, all = false) => {
      const output = [];
      const check = (key, value, reason) => {
        const had = included.has(key), previous = included.get(key), keep = !!test(value, key);
        if (keep) {
          included.set(key, value);
          if (!had) output.push({ reason: 'add', key, current: value });
          else if (reason) output.push({ reason: reason === 'refresh' ? 'refresh' : 'update', key, current: value, previous });
        } else if (had) { included.delete(key); output.push({ reason: 'remove', key, current: previous }); }
      };
      if (all) { for (const [key, value] of input) check(key, value); }
      else for (const c of changes) {
        if (c.reason === 'add' || c.reason === 'update' || c.reason === 'replace') { input.set(c.key, c.current); check(c.key, c.current, c.reason); }
        else if (c.reason === 'refresh' && !options.ignoreRefresh && input.has(c.key)) check(c.key, input.get(c.key), 'refresh');
        else if (c.reason === 'remove') {
          input.delete(c.key); if (included.has(c.key)) { output.push({ reason: 'remove', key: c.key, current: included.get(c.key) }); included.delete(c.key); }
        } else if (c.reason === 'clear') {
          for (const [key, value] of included) output.push({ reason: 'remove', key, current: value }); included.clear(); input.clear();
        }
      }
      ctx.emitRaw(output, suppress);
    };
    const evaluate = (all = false) => {
      if (fast) { evaluateCache([], all); return; }
      if (options.ignoreRefresh) ctx.model.refreshed.clear();
      const active = new Set(), next = [];
      for (const entry of ctx.model.entries) {
        active.add(entry.id);
        if (all || !matches.has(entry.id) || ctx.model.updated.has(entry.id) || ctx.model.refreshed.has(entry.id)) matches.set(entry.id, !!test(entry.value, entry.key));
        if (matches.get(entry.id)) next.push(entry);
      }
      for (const id of matches.keys()) if (!active.has(id)) matches.delete(id);
      ctx.emit(next, { suppressEmpty: suppress });
    };
    if (observable(predicate)) ctx.watch(predicate, value => {
      test = statePredicate ? (item, key) => statePredicate(value, item, key) : value;
      if (typeof test !== 'function') throw new TypeError('Predicate Observable must emit functions.');
      if (ctx.loaded) { ctx.model.refreshed.clear(); ctx.model.updated.clear(); evaluate(true); }
    });
    ctx.watch(reapply, () => { if (ctx.loaded) { ctx.model.refreshed.clear(); ctx.model.updated.clear(); evaluate(true); } });
    const handler = changes => fast ? evaluateCache(changes) : evaluate();
    handler.prepare = changes => { fast ??= changes.kind === 'cache' && !changes.items && !Array.isArray(changes.keys); if (fast) ctx.model.kind = 'cache'; else ctx.model.ingest(changes); };
    return handler;
  });
}

export const filterImmutable = (predicate, suppressEmptyChangeSets = true) => filter(predicate, { ignoreRefresh: true, suppressEmptyChangeSets });
export const filterWithState = (state, predicate, suppressEmpty = true) => filter(state, predicate, suppressEmpty);

/** Project only added/updated entries. Set transformOnRefresh to recompute refreshes. */
export function transform(factory, options = {}) {
  if (typeof factory !== 'function') throw new TypeError('transform requires a factory.');
  if (typeof options === 'boolean') options = { transformOnRefresh: options };
  else if (observable(options)) options = { forceTransform: options };
  return derived(ctx => {
    const projected = new Map(), sourceValues = new Map();
    let fast;
    const evaluateCache = (changes, forcedPredicate) => {
      const output = [];
      const project = (key, value, reason) => {
        const had = projected.has(key), old = projected.get(key), oldSource = sourceValues.get(key);
        sourceValues.set(key, value);
        if (reason === 'refresh' && !options.transformOnRefresh && !forcedPredicate) {
          if (had) output.push({ reason: 'refresh', key, current: old });
          return;
        }
        try {
          let destination;
          if (had && options.inlineUpdate) { options.inlineUpdate(old, value); destination = old; }
          else destination = factory(value, key, oldSource);
          projected.set(key, destination);
          output.push({ reason: had ? 'update' : 'add', key, current: destination, previous: old });
        } catch (error) {
          if (!options.errorHandler) throw error;
          options.errorHandler({ error, value, key });
        }
      };
      if (forcedPredicate) {
        for (const [key, value] of sourceValues) if (forcedPredicate(value, key)) project(key, value, 'update');
      } else for (const c of changes) {
        if (c.reason === 'add' || c.reason === 'update' || c.reason === 'replace') project(c.key, c.current, c.reason);
        else if (c.reason === 'refresh' && !options.ignoreRefresh && sourceValues.has(c.key)) project(c.key, sourceValues.get(c.key), 'refresh');
        else if (c.reason === 'remove') {
          sourceValues.delete(c.key); if (projected.has(c.key)) { output.push({ reason: 'remove', key: c.key, current: projected.get(c.key) }); projected.delete(c.key); }
        } else if (c.reason === 'clear') {
          for (const [key, value] of projected) output.push({ reason: 'remove', key, current: value }); projected.clear(); sourceValues.clear();
        }
      }
      ctx.emitRaw(output, options.suppressEmptyChangeSets ?? true);
    };
    const evaluate = forcedPredicate => {
      if (fast) { evaluateCache([], forcedPredicate); return; }
      if (options.ignoreRefresh) ctx.model.refreshed.clear();
      const live = new Set(), next = [], updated = new Set();
      for (const [index, entry] of ctx.model.entries.entries()) {
        live.add(entry.id);
        const old = projected.get(entry.id);
        const should = !projected.has(entry.id) || ctx.model.updated.has(entry.id) || (options.transformOnRefresh && ctx.model.refreshed.has(entry.id)) || (forcedPredicate && forcedPredicate(entry.value, entry.key));
        if (should) {
          try {
            let value;
            if (old && options.inlineUpdate) { options.inlineUpdate(old.value, entry.value); value = old.value; }
            else value = factory(entry.value, ctx.model.kind === 'list' ? index : entry.key, ctx.model.kind === 'list' ? old?.value : sourceValues.get(entry.id));
            projected.set(entry.id, { ...entry, value, version: (old?.version ?? -1) + 1 });
            sourceValues.set(entry.id, entry.value); updated.add(entry.id);
          } catch (error) {
            if (!options.errorHandler) throw error;
            options.errorHandler({ error, value: entry.value, key: entry.key });
          }
        }
        if (projected.has(entry.id)) next.push(projected.get(entry.id));
      }
      for (const id of projected.keys()) if (!live.has(id)) { projected.delete(id); sourceValues.delete(id); }
      ctx.emit(next, { updated, suppressEmpty: options.suppressEmptyChangeSets ?? true });
    };
    ctx.watch(options.forceTransform, predicate => {
      if (ctx.loaded) { ctx.model.updated.clear(); ctx.model.refreshed.clear(); evaluate(typeof predicate === 'function' ? predicate : () => true); }
    });
    const handler = changes => fast ? evaluateCache(changes) : evaluate();
    handler.prepare = changes => { fast ??= changes.kind === 'cache' && !changes.items && !Array.isArray(changes.keys); if (fast) ctx.model.kind = 'cache'; else ctx.model.ingest(changes); };
    return handler;
  });
}

export const transformImmutable = factory => transform(factory, { ignoreRefresh: true });
export const convert = transform;
export const cast = (converter = identity) => transform(converter);
export const castToObject = () => transform(identity);
export const ofType = type => {
  if (typeof type !== 'string' && typeof type !== 'function') throw new TypeError('ofType requires a constructor or JavaScript typeof name.');
  return filter(value => typeof type === 'string' ? typeof value === type : type === Number ? typeof value === 'number' : type === String ? typeof value === 'string' : type === Boolean ? typeof value === 'boolean' : value instanceof type);
};
export const transformSafe = (factory, errorHandler, options = {}) => transform(factory, { ...options, errorHandler });
export const transformWithInlineUpdate = (factory, updateAction, errorHandler, transformOnRefresh = false) => {
  if (typeof updateAction !== 'function') throw new TypeError('An update action is required.');
  return transform(factory, { inlineUpdate: updateAction, errorHandler, transformOnRefresh });
};

/** Flatten enumerable children, SourceList/SourceCache children, or Observable child snapshots/changes. */
export function transformMany(selector, keySelector) {
  if (typeof selector !== 'function') throw new TypeError('transformMany requires a selector.');
  return derived(ctx => {
    const children = new Map(); let changing = false;
    const flatten = () => {
      if (!ctx.loaded || changing) return;
      let next = []; const refresh = new Set();
      for (const record of children.values()) for (const entry of record.entries) if (record.refreshed?.has(entry.id)) refresh.add(keySelector ? entry.key : entry.id);
      for (const parent of ctx.model.entries) for (const child of children.get(parent.id)?.entries ?? []) next.push(child);
      const kind = keySelector ? 'cache' : 'list';
      if (keySelector) {
        const unique = new Map();
        for (const entry of next) unique.set(entry.key, entry);
        next = [...unique.values()].map(entry => ({ ...entry, id: entry.key }));
      }
      ctx.emit(next, { kind, refresh });
      for (const record of children.values()) record.refreshed?.clear();
    };
    const setChildren = (record, values, refreshedValues = new Set(), updatedValues = new Set()) => {
      const previous = record.entries, used = new Set();
      record.refreshed ??= new Set();
      record.entries = Array.from(values ?? [], (value, index) => {
        const key = keySelector?.(value), old = keySelector ? previous.find(e => !used.has(e.id) && Object.is(e.key, key)) : previous.find(e => !used.has(e.id) && Object.is(e.value, value));
        if (old) used.add(old.id);
        const entry = { id: old?.id ?? Symbol(), key, value, version: (old?.version ?? 0) + (old && (!Object.is(old.value, value) || updatedValues.has(value)) ? 1 : 0) };
        if (refreshedValues.has(value)) record.refreshed.add(entry.id);
        return entry;
      });
      flatten();
    };
    ctx.subscriptions.add(() => { for (const record of children.values()) record.subscription?.unsubscribe(); children.clear(); });
    return () => {
      changing = true;
      try {
        const live = new Set(ctx.model.entries.map(e => e.id));
        for (const [id, record] of children) if (!live.has(id)) { record.subscription?.unsubscribe(); children.delete(id); }
        for (const parent of ctx.model.entries) {
          let record = children.get(parent.id);
          if (record && !ctx.model.updated.has(parent.id) && !ctx.model.refreshed.has(parent.id)) continue;
          const selected = selector(parent.value, parent.key);
          if (record?.selected === selected && (observable(selected) || selected?.connect)) continue;
          if (record) record.subscription?.unsubscribe();
          record = { selected, entries: record?.entries ?? [], model: new Model() }; children.set(parent.id, record);
          const stream = selected?.connect ? selected.connect() : observable(selected) ? selected : null;
          if (stream) record.subscription = stream.subscribe({
            next: values => ctx.run(() => {
              if (values?.kind || (Array.isArray(values) && values.length && typeof values[0]?.reason === 'string')) {
                record.model.ingest(values);
                setChildren(record, record.model.entries.map(e => e.value), new Set(record.model.entries.filter(e => record.model.refreshed.has(e.id)).map(e => e.value)), new Set(record.model.entries.filter(e => record.model.updated.has(e.id)).map(e => e.value)));
              } else setChildren(record, values);
            }), error: error => ctx.observer.error(error)
          });
          else { const values = Array.from(selected ?? []); setChildren(record, values, ctx.model.refreshed.has(parent.id) ? new Set(values) : new Set()); }
        }
      } finally { changing = false; }
      flatten();
    };
  });
}

export function distinctValues(selector = identity) {
  return derived(ctx => {
    const values = new Map();
    return () => {
      const live = new Set(), distinct = new Map();
      for (const e of ctx.model.entries) {
        live.add(e.id);
        if (!values.has(e.id) || ctx.model.updated.has(e.id) || ctx.model.refreshed.has(e.id)) values.set(e.id, selector(e.value, e.key));
        const value = values.get(e.id); if (!distinct.has(value)) distinct.set(value, { id: value, key: value, value, version: 0 });
      }
      for (const id of values.keys()) if (!live.has(id)) values.delete(id);
      ctx.emit([...distinct.values()], { kind: 'cache', refresh: new Set() });
    };
  });
}

/** Stable order; comparers can be functions, Compare/compare objects, or Observables of either. */
export function sort(comparer = compareDefault, resortOrOptions = {}) {
  const options = observable(resortOrOptions) ? { resort: resortOrOptions } : resortOrOptions ?? {};
  return derived(ctx => {
    let compare = observable(comparer) ? compareDefault : comparerFunction(comparer);
    let comparerReady = !options.waitForComparer || !observable(comparer), delivered = false;
    const order = new Map(), cache = new Map(); let counter = 0, fast, ordered = [];
    const compareEntry = (a, b) => compare(a.value, b.value) || order.get(a.id) - order.get(b.id);
    const metadata = (entries, why) => {
      const sortedItems = orderedView(entries, compare, why);
      return { items: sortedItems.items, keys: sortedItems.keys, sortedItems, SortedItems: sortedItems, comparer: compare, sortReason: why };
    };
    const resort = why => {
      const previous = delivered ? ordered : []; ordered = [...cache.values()].sort(compareEntry);
      let changes;
      if (previous.length > (options.resetThreshold ?? options.ResetThreshold ?? 50)) {
        const reordered = previous.some((e, index) => e.id !== ordered[index]?.id);
        changes = reordered ? [
          ...previous.map((e, index) => ({ reason: 'remove', key: e.key, current: e.value, currentIndex: index })).reverse(),
          ...ordered.map((e, index) => ({ reason: 'add', key: e.key, current: e.value, currentIndex: index }))
        ] : [];
      } else changes = difference(previous, ordered, 'cache');
      if (comparerReady) { ctx.emitRaw(changes, options.suppressEmptyChangeSets ?? true, metadata(ordered, why)); delivered = true; }
    };
    const evaluateCache = changes => {
      const output = [], initial = !cache.size;
      if (initial && changes.every(c => c.reason === 'add')) {
        for (const c of changes) { if (!order.has(c.key)) order.set(c.key, counter++); cache.set(c.key, { id: c.key, key: c.key, value: c.current, version: 0 }); }
        ordered = [...cache.values()].sort(compareEntry);
        ordered.forEach((e, index) => output.push({ reason: 'add', key: e.key, current: e.value, currentIndex: index }));
      } else for (const c of changes) {
        const old = cache.get(c.key), oldIndex = old ? ordered.indexOf(old) : -1;
        if (c.reason === 'remove') {
          if (old) { ordered.splice(oldIndex, 1); cache.delete(c.key); order.delete(c.key); output.push({ reason: 'remove', key: c.key, current: old.value, currentIndex: oldIndex }); }
        } else if (c.reason === 'clear') {
          for (let index = ordered.length - 1; index >= 0; index--) output.push({ reason: 'remove', key: ordered[index].key, current: ordered[index].value, currentIndex: index });
          ordered = []; cache.clear(); order.clear();
        } else if (c.reason === 'add' || c.reason === 'update' || c.reason === 'replace' || (c.reason === 'refresh' && old)) {
          if (old) ordered.splice(oldIndex, 1);
          if (!order.has(c.key)) order.set(c.key, counter++);
          const entry = { id: c.key, key: c.key, value: c.reason === 'refresh' ? old.value : c.current, version: (old?.version ?? -1) + 1 };
          let lo = 0, hi = ordered.length;
          while (lo < hi) { const mid = (lo + hi) >>> 1; if (compareEntry(ordered[mid], entry) <= 0) lo = mid + 1; else hi = mid; }
          ordered.splice(lo, 0, entry); cache.set(c.key, entry);
          if (c.reason === 'refresh') {
            if (oldIndex !== lo) output.push({ reason: 'move', key: c.key, current: entry.value, previousIndex: oldIndex, currentIndex: lo });
            output.push({ reason: 'refresh', key: c.key, current: entry.value, currentIndex: lo });
          } else output.push({ reason: old ? 'update' : 'add', key: c.key, current: entry.value, previous: old?.value, previousIndex: oldIndex, currentIndex: lo });
        }
      }
      if (comparerReady) {
        const notifications = delivered ? output : ordered.map((e, index) => ({ reason: 'add', key: e.key, current: e.value, currentIndex: index }));
        ctx.emitRaw(notifications, options.suppressEmptyChangeSets ?? true, metadata(ordered, initial ? 'initialLoad' : 'dataChanged')); delivered = true;
      }
    };
    const evaluate = (why = 'dataChanged') => {
      if (!comparerReady) return;
      if (fast) { resort(why); return; }
      for (const e of ctx.model.entries) if (!order.has(e.id)) order.set(e.id, counter++);
      const live = new Set(ctx.model.entries.map(e => e.id)); for (const key of order.keys()) if (!live.has(key)) order.delete(key);
      const entries = ctx.model.entries.slice().sort(compareEntry);
      const sortedItems = orderedView(entries, compare, why);
      ctx.emit(entries, { suppressEmpty: options.suppressEmptyChangeSets ?? true, metadata: { sortedItems, SortedItems: sortedItems, comparer: compare, sortReason: why } });
    };
    if (observable(comparer)) ctx.watch(comparer, next => { compare = comparerFunction(next); comparerReady = true; if (ctx.loaded) { ctx.model.updated.clear(); ctx.model.refreshed.clear(); evaluate('comparerChanged'); } });
    ctx.watch(options.resort ?? options.resorter, () => { if (ctx.loaded) { ctx.model.updated.clear(); ctx.model.refreshed.clear(); evaluate('reorder'); } });
    const handler = changes => fast ? evaluateCache(changes) : evaluate(order.size ? 'dataChanged' : 'initialLoad');
    handler.prepare = changes => { fast ??= changes.kind === 'cache' && !changes.items && !Array.isArray(changes.keys); if (fast) ctx.model.kind = 'cache'; else ctx.model.ingest(changes); };
    return handler;
  });
}

function windowed(type, requests, options = {}) {
  return derived(ctx => {
    let request = type === 'page' ? { page: 1, size: 25 } : { startIndex: 0, size: 25 };
    let requestReady = !options.waitForRequest || !observable(requests);
    let currentComparer;
    const evaluate = () => {
      if (!requestReady) return;
      const totalSize = ctx.model.entries.length, size = request.size;
      const pages = Math.max(1, Math.ceil(totalSize / size));
      const pageNumber = Math.min(request.page ?? 1, pages);
      const startIndex = type === 'page' ? (pageNumber - 1) * size : request.startIndex;
      const entries = ctx.model.entries.slice(startIndex, startIndex + size);
      const response = type === 'page'
        ? { page: pageNumber, pages, size, totalSize, Page: pageNumber, Pages: pages, Size: size, TotalSize: totalSize }
        : { startIndex, size, totalSize, StartIndex: startIndex, Size: size, TotalSize: totalSize };
      const sortedItems = orderedView(entries, currentComparer, 'dataChanged');
      const context = { response, comparer: currentComparer, options: options.operationOptions ?? {}, request: { ...request }, Response: response, Comparer: currentComparer, Options: options.operationOptions ?? {}, Request: { ...request } };
      ctx.emit(entries, { suppressEmpty: false, metadata: { response, Response: response, sortedItems, SortedItems: sortedItems, context, Context: context, comparer: currentComparer } });
    };
    const setRequest = value => {
      if (!value) return;
      const size = value.size ?? value.Size;
      const start = type === 'page' ? value.page ?? value.Page ?? 1 : value.startIndex ?? value.StartIndex ?? 0;
      if (!Number.isInteger(size) || size < 1 || !Number.isInteger(start) || start < (type === 'page' ? 1 : 0)) return;
      const firstRequest = !requestReady; requestReady = true;
      const next = type === 'page' ? { page: start, size } : { startIndex: start, size };
      if (!firstRequest && request.size === size && (type === 'page' ? request.page === start : request.startIndex === start)) return;
      request = next;
      if (ctx.loaded) { ctx.model.refreshed.clear(); ctx.model.updated.clear(); evaluate(); }
    };
    if (observable(requests)) ctx.watch(requests, setRequest); else setRequest(requests);
    return changes => { currentComparer = changes.comparer ?? changes.sortedItems?.comparer ?? currentComparer; evaluate(); };
  });
}

export const page = requests => windowed('page', requests);
export const virtualise = requests => windowed('virtual', requests);
export const virtualize = virtualise;
export const top = (comparerOrSize, requestedSize) => {
  const size = requestedSize ?? comparerOrSize;
  if (!Number.isInteger(size) || size < 1) throw new RangeError('top size must be a positive integer.');
  return requestedSize === undefined ? virtualise({ startIndex: 0, size }) : sortAndVirtualise(comparerOrSize, { startIndex: 0, size });
};
export const reverse = () => derived(ctx => () => ctx.emit(ctx.model.entries.slice().reverse()));
export const sortAndPage = (comparer, requests, options) => source => source.pipe(sort(comparer, { ...options, waitForComparer: true, suppressEmptyChangeSets: false }), windowed('page', requests, { waitForRequest: true, operationOptions: options }));
export const sortAndVirtualise = (comparer, requests, options) => source => source.pipe(sort(comparer, { ...options, waitForComparer: true, suppressEmptyChangeSets: false }), windowed('virtual', requests, { waitForRequest: true, operationOptions: options }));
export const sortAndVirtualize = sortAndVirtualise;
export const sortBy = (selector, direction = 'ascending', options) => {
  const select = typeof selector === 'function' ? selector : value => value[selector];
  const sign = direction === 1 || String(direction).toLowerCase() === 'descending' || direction === 'desc' ? -1 : 1;
  return sort((a, b) => sign * compareDefault(select(a), select(b)), options);
};

export function toCollection() {
  return source => new Observable(observer => {
    const model = new Model();
    return source.subscribe({ next: changes => { try { model.ingest(changes); observer.next(changes.items ? [...changes.items] : model.entries.map(e => e.value)); } catch (error) { observer.error(error); } }, error: error => observer.error(error), complete: () => observer.complete() });
  });
}

/** Bind preserves the target identity and forwards the original changesets. */
export function bind(target) {
  if (target == null) throw new TypeError('bind requires an array, collection or callback.');
  return source => new Observable(observer => {
    const model = new Model();
    return source.subscribe({ next: changes => {
      try {
        const apply = typeof target.ApplyChanges === 'function' ? target.ApplyChanges : target.applyChanges;
        if (typeof apply === 'function') {
          apply.call(target, changes);
          observer.next(changes);
          return;
        }
        model.ingest(changes); const items = changes.items ? [...changes.items] : model.entries.map(e => e.value);
        if (typeof target === 'function') target(items, changes);
        else if (Array.isArray(target)) { target.length = 0; for (const item of items) target.push(item); }
        else if (typeof target.load === 'function') target.load(items);
        else if (typeof target.edit === 'function' || typeof target.Edit === 'function') (typeof target.edit === 'function' ? target.edit : target.Edit).call(target, updater => {
          const collection = updater ?? target;
          const clear = collection.clear ?? collection.Clear;
          const add = collection.addRange ?? collection.AddRange ?? collection.addOrUpdate ?? collection.AddOrUpdate;
          if (typeof clear !== 'function' || typeof add !== 'function') throw new TypeError('Binding collection updater requires Clear/clear and AddRange/addRange or AddOrUpdate/addOrUpdate.');
          clear.call(collection); add.call(collection, items);
        });
        else if (typeof target.next === 'function') target.next(items);
        else throw new TypeError('Unsupported binding target.');
        observer.next(changes);
      } catch (error) { observer.error(error); }
    }, error: error => observer.error(error), complete: () => observer.complete() });
  });
}

export const bindToObservableList = bind;
export const bindToObservableCollection = bind;
export const sortAndBind = (targetOrComparer, comparerOrTarget, options) => {
  const isTarget = value => Array.isArray(value) || !!value?.load || !!value?.edit || !!value?.Edit || !!value?.applyChanges || !!value?.ApplyChanges;
  const comparerFirst = isTarget(comparerOrTarget) && !isTarget(targetOrComparer);
  const target = comparerFirst ? comparerOrTarget : targetOrComparer, comparer = comparerFirst ? targetOrComparer : comparerOrTarget;
  return source => source.pipe(sort(comparer, options), bind(target));
};
export const asObservableCache = (sourceOrKeySelector, keySelector) => observable(sourceOrKeySelector) ? new ObservableCache(sourceOrKeySelector, keySelector) : source => new ObservableCache(source, sourceOrKeySelector);
export const asObservableList = source => observable(source) ? new ObservableList(source) : input => new ObservableList(input);

export function changeKey(selector) {
  if (typeof selector !== 'function') throw new TypeError('changeKey requires a selector.');
  return derived(ctx => {
    const keyed = new Map();
    return () => {
      const next = new Map(), live = new Set(), refresh = new Set();
      for (const e of ctx.model.entries) {
        live.add(e.id);
        if (!keyed.has(e.id) || ctx.model.updated.has(e.id) || ctx.model.refreshed.has(e.id)) keyed.set(e.id, selector(e.value, e.key));
        const key = keyed.get(e.id); next.set(key, { ...e, id: key, key });
        if (ctx.model.refreshed.has(e.id)) refresh.add(key);
      }
      for (const id of keyed.keys()) if (!live.has(id)) keyed.delete(id);
      ctx.emit([...next.values()], { kind: 'cache', refresh });
    };
  });
}

export const removeKey = () => derived(ctx => () => ctx.emit(ctx.model.entries, { kind: 'list' }));

export {
  filter as Filter, filterImmutable as FilterImmutable, filterWithState as FilterWithState,
  transform as Transform, transformImmutable as TransformImmutable, transformSafe as TransformSafe,
  convert as Convert, cast as Cast, castToObject as CastToObject, ofType as OfType,
  transformWithInlineUpdate as TransformWithInlineUpdate, transformMany as TransformMany,
  distinctValues as DistinctValues, sort as Sort, page as Page, virtualise as Virtualise,
  virtualize as Virtualize, top as Top, reverse as Reverse, sortAndPage as SortAndPage,
  sortAndVirtualise as SortAndVirtualise, sortAndVirtualize as SortAndVirtualize, sortBy as SortBy, toCollection as ToCollection,
  bind as Bind, bindToObservableList as BindToObservableList, bindToObservableCollection as BindToObservableCollection,
  sortAndBind as SortAndBind, asObservableCache as AsObservableCache, asObservableList as AsObservableList,
  changeKey as ChangeKey, removeKey as RemoveKey
};
