import { Observable, Subscription, defer, from, map, filter as rxFilter, mergeMap, tap, startWith } from 'rxjs';
import { ChangeSet, SourceCache, Optional, applyChanges } from './core.js';

const kindOf = changes => changes.kind ?? (changes.length && 'key' in changes[0] ? 'cache' : 'list');
const requireFunction = (value, name) => { if (typeof value !== 'function') throw new TypeError(`${name} must be a function`); };
const setOf = values => new Set(values.flat());
const withoutIndex = changes => Array.from(changes).filter(change => change.reason !== 'move' && change.reason !== 'moved').map(change => ({ ...change, currentIndex: -1, previousIndex: -1, ...(change.range ? { range: { ...change.range, items: [...change.range.items], index: -1 } } : {}) }));
const projectedSet = (changes, predicate) => {
  const kind = kindOf(changes), selected = Array.from(changes).filter(predicate);
  return new ChangeSet(kind === 'list' ? withoutIndex(selected) : selected, kind);
};
const filteredChanges = predicate => source => source.pipe(map(changes => projectedSet(changes, predicate)), rxFilter(changes => changes.length > 0));

/** Forward only the requested reasons; empty result batches are suppressed. */
export function whereReasonsAre(...reasons) {
  const allowed = setOf(reasons);
  return filteredChanges(change => allowed.has(change.reason));
}

export function whereReasonsAreNot(...reasons) {
  const excluded = setOf(reasons);
  return filteredChanges(change => !excluded.has(change.reason));
}

/** The predicate receives (current, previous, key). Other reasons pass through. */
export function includeUpdateWhen(predicate) {
  requireFunction(predicate, 'predicate');
  return filteredChanges(change => change.reason !== 'update' || predicate(change.current, change.previous, change.key));
}

export function excludeUpdateWhen(predicate) {
  requireFunction(predicate, 'predicate');
  return includeUpdateWhen((current, previous, key) => !predicate(current, previous, key));
}

export const ignoreUpdateWhen = excludeUpdateWhen;
export function ignoreSameReferenceUpdate() { return excludeUpdateWhen(Object.is); }
export function suppressRefresh() { return whereReasonsAreNot('refresh'); }

export function invokeEvaluate() {
  return forEachChange(change => {
    if (change.reason !== 'refresh') return;
    const evaluate = change.current?.evaluate ?? change.current?.Evaluate;
    requireFunction(evaluate, 'item.evaluate');
    evaluate.call(change.current);
  });
}

/** Combine each RxJS buffer of changesets into one batch, preserving record order. */
export function flattenBufferResult() {
  return source => source.pipe(rxFilter(buffer => buffer.length > 0), map(buffer => {
    const result = new ChangeSet(buffer.flatMap(changes => Array.from(changes)), kindOf(buffer[0]));
    const last = buffer.at(-1);
    // The final snapshot is authoritative for sorted/windowed pipelines.
    for (const key of ['items', 'keys', 'sortedItems', 'SortedItems', 'response']) if (last[key] !== undefined) result[key] = last[key];
    return result;
  }));
}

export function treatMovesAsRemoveAdd() {
  return map(changes => {
    const result = new ChangeSet([], kindOf(changes));
    for (const change of changes) {
      if (change.reason === 'move' || change.reason === 'moved') {
        result.push({ ...change, reason: 'remove', currentIndex: change.previousIndex, previousIndex: -1 });
        result.push({ ...change, reason: 'add', currentIndex: change.currentIndex, previousIndex: -1 });
      } else result.push(change);
    }
    for (const key of ['items', 'keys', 'sortedItems', 'SortedItems', 'response']) if (changes[key] !== undefined) result[key] = changes[key];
    return result;
  });
}

export function forEachChange(action) {
  requireFunction(action, 'action');
  return tap(changes => { for (const change of changes) action(change); });
}

/** Range records become individual add/remove records, preserving application order. */
export function* itemChanges(changes) {
  for (const change of changes) {
    if (!change.range) { yield change; continue; }
    const reason = change.reason === 'addRange' ? 'add' : 'remove';
    const start = change.range.index ?? -1;
    for (let i = 0; i < change.range.items.length; i++) {
      yield { reason, current: change.range.items[i], currentIndex: start < 0 ? -1 : reason === 'add' ? start + i : start, previousIndex: -1 };
    }
  }
}

export function forEachItemChange(action) {
  requireFunction(action, 'action');
  return tap(changes => { for (const change of itemChanges(changes)) action(change); });
}

export function flattenChanges() { return mergeMap(changes => Array.from(changes)); }
export const flatten = flattenChanges;

/** Emit an empty changeset before subscribing to the underlying source. */
export function startWithEmpty(kind = 'cache') { return startWith(new ChangeSet([], kind)); }

export function startWithItem(item, key = item?.key ?? item?.Key ?? item?.id) {
  return startWith(new ChangeSet([{ reason: 'add', key, current: item }], 'cache'));
}

export function removeIndex() {
  return map(changes => new ChangeSet(withoutIndex(changes), kindOf(changes)));
}

/** Convert list change records to keyed cache changes. Keys must uniquely identify list items. */
export function addKey(keySelector) {
  requireFunction(keySelector, 'keySelector');
  return map(changes => {
    const output = [];
    for (const change of itemChanges(changes)) {
      const key = keySelector(change.current);
      if (change.reason === 'replace') {
        const previousKey = keySelector(change.previous);
        if (Object.is(key, previousKey) || key === previousKey) {
          output.push({ reason: 'update', key, current: change.current, previous: change.previous });
        } else {
          output.push({ reason: 'remove', key: previousKey, current: change.previous });
          output.push({ reason: 'add', key, current: change.current });
        }
      } else output.push({ reason: change.reason, key, current: change.current, ...(change.reason === 'move' || change.reason === 'moved' ? { currentIndex: change.currentIndex, previousIndex: change.previousIndex } : {}) });
    }
    return new ChangeSet(output, 'cache');
  });
}

/** Coalesce repeated keys within each batch, as upstream UniquenessEnforcer does. */
export function ensureUniqueKeys() {
  return source => defer(() => {
    const state = new Map();
    return source.pipe(map(changes => {
      const last = new Map();
      for (const change of changes) {
        if (change.reason === 'move' || change.reason === 'moved') continue;
        if (change.reason !== 'refresh' || !last.has(change.key)) last.set(change.key, change);
      }
      const result = [];
      for (const [key, change] of last) {
        if (change.reason === 'add' || change.reason === 'update') {
          result.push(state.has(key) ? { reason: 'update', key, current: change.current, previous: state.get(key) } : { reason: 'add', key, current: change.current });
          state.set(key, change.current);
        } else if (change.reason === 'remove' && state.has(key)) {
          result.push({ reason: 'remove', key, current: state.get(key) });
          state.delete(key);
        } else if (change.reason === 'refresh' && state.has(key)) result.push({ reason: 'refresh', key, current: state.get(key) });
      }
      return new ChangeSet(result, 'cache');
    }));
  });
}

function copyInto(target, changes) {
  if (typeof target?.edit === 'function') {
    target.edit(updater => updater.clone(changes));
  } else if (target instanceof Map || Array.isArray(target)) {
    if (Array.isArray(target) && kindOf(changes) === 'cache') {
      for (const change of changes) {
        if (change.reason === 'add') target.push(change.current);
        else if (change.reason === 'remove' || change.reason === 'update') {
          const index = target.indexOf(change.reason === 'update' ? change.previous : change.current);
          if (index >= 0) target.splice(index, 1);
          if (change.reason === 'update') target.push(change.current);
        }
      }
    } else applyChanges(target, changes);
  } else if (target instanceof Set) {
    for (const change of itemChanges(changes)) {
      if (change.reason === 'remove') target.delete(change.current);
      else if (change.reason === 'update' || change.reason === 'replace') { target.delete(change.previous); target.add(change.current); }
      else if (change.reason === 'add') target.add(change.current);
    }
  } else throw new TypeError('target must be a SourceCache, SourceList, Map, Set or Array');
}

export function clone(target) { return tap(changes => copyInto(target, changes)); }

/** Subscribe immediately, forwarding each changeset as one destination edit. */
export function populateInto(source, destination, observer) { return source.pipe(clone(destination)).subscribe(observer); }

/** Add each emitted item or item batch to a destination cache; return its RxJS subscription. */
export function populateFrom(destination, observable, observer) {
  if (typeof destination?.addOrUpdate !== 'function') throw new TypeError('destination must support addOrUpdate');
  return observable.pipe(tap(items => destination.addOrUpdate(items))).subscribe(observer);
}

/** Observe one key as Optional; refreshes and equal updates do not repeat values. */
export function toObservableOptional(key, initialOptionalWhenMissing = false, comparer = Object.is) {
  if (typeof initialOptionalWhenMissing !== 'boolean') { comparer = initialOptionalWhenMissing ?? Object.is; initialOptionalWhenMissing = false; }
  const equals = typeof comparer === 'function' ? comparer : comparer?.equals?.bind(comparer) ?? comparer?.Equals?.bind(comparer) ?? Object.is;
  return source => new Observable(observer => {
    let last = Optional.none(), seen = false, subscribing = true, completed = false;
    const subscription = source.subscribe({
      next(changes) {
        try {
          for (const change of changes) {
            if (!(Object.is(key, change.key) || key === change.key)) continue;
            const next = change.reason === 'remove' ? Optional.none() : Optional.some(change.current);
            if (next.hasValue !== last.hasValue || next.hasValue && !equals(last.value, next.value)) {
              last = next; seen = true; observer.next(next);
            }
          }
        } catch (error) { observer.error(error); }
      },
      error(error) { observer.error(error); },
      complete() { completed = true; if (!subscribing) observer.complete(); }
    });
    subscribing = false;
    if (initialOptionalWhenMissing && !seen && !observer.closed) observer.next(Optional.none());
    if (completed) observer.complete();
    return subscription;
  });
}

export function adapt(adapter) {
  const action = typeof adapter === 'function' ? adapter : adapter?.adapt?.bind(adapter) ?? adapter?.Adapt?.bind(adapter);
  requireFunction(action, 'adapter.adapt');
  return tap(action);
}

/** A stable collection snapshot. Item references are preserved; collection membership is copied. */
export class QuerySnapshot {
  constructor(state) {
    this.kind = state instanceof Map ? 'cache' : 'list';
    this._state = state instanceof Map ? new Map(state) : [...state];
  }
  get count() { return this._state instanceof Map ? this._state.size : this._state.length; }
  get size() { return this.count; }
  get items() { return this._state instanceof Map ? [...this._state.values()] : [...this._state]; }
  get keys() { return this._state instanceof Map ? [...this._state.keys()] : this._state.map((_, i) => i); }
  get keyValues() { return this._state instanceof Map ? [...this._state.entries()] : [...this._state.entries()]; }
  lookup(key) { return this._state instanceof Map ? this._state.has(key) ? Optional.some(this._state.get(key)) : Optional.none() : key >= 0 && key < this.count ? Optional.some(this._state[key]) : Optional.none(); }
  get(key) { return this._state instanceof Map ? this._state.get(key) : this._state[key]; }
  has(key) { return this._state instanceof Map ? this._state.has(key) : Number.isInteger(key) && key >= 0 && key < this.count; }
  [Symbol.iterator]() { return this.items[Symbol.iterator](); }
  get Count() { return this.count; }
  get Items() { return this.items; }
  get Keys() { return this.keys; }
  get KeyValues() { return this.keyValues; }
  Lookup(key) { return this.lookup(key); }
}

/** Emit a snapshot or its projection after every source batch. */
export function queryWhenChanged(selector = query => query) {
  requireFunction(selector, 'selector');
  return source => defer(() => {
    let state;
    return source.pipe(map(changes => {
      state ??= kindOf(changes) === 'cache' ? new Map() : [];
      applyChanges(state, changes);
      return selector(new QuerySnapshot(state));
    }));
  });
}

export function toSortedCollection(selectorOrComparer = value => value, direction = 'ascending') {
  let compare;
  const descending = direction === 'descending' || direction === 'Descending' || direction === -1;
  if (selectorOrComparer && typeof (selectorOrComparer.compare ?? selectorOrComparer.Compare) === 'function') compare = (selectorOrComparer.compare ?? selectorOrComparer.Compare).bind(selectorOrComparer);
  else if (typeof selectorOrComparer === 'function' && selectorOrComparer.length >= 2) compare = selectorOrComparer;
  else {
    requireFunction(selectorOrComparer, 'selectorOrComparer');
    compare = (a, b) => { const x = selectorOrComparer(a), y = selectorOrComparer(b); return x < y ? -1 : x > y ? 1 : 0; };
  }
  return queryWhenChanged(query => query.items.sort((a, b) => (descending ? -1 : 1) * compare(a, b)));
}

/** Update mutable item indices from each full sorted snapshot. */
export function updateIndex(setter = (item, index) => { item.index = index; }) {
  requireFunction(setter, 'setter');
  return source => defer(() => {
    let state;
    return source.pipe(tap(changes => {
      let items = changes.sortedItems?.items ?? changes.items;
      if (!items) {
        state ??= kindOf(changes) === 'cache' ? new Map() : [];
        applyChanges(state, changes);
        items = state instanceof Map ? [...state.values()] : state;
      }
      items.forEach(setter);
    }));
  });
}

export class ChangeStatistics {
  constructor(index = -1, adds = 0, updates = 0, removes = 0, refreshes = 0, moves = 0, count = 0) {
    Object.assign(this, { index, adds, updates, removes, refreshes, moves, count, lastUpdated: new Date() });
  }
  get Index() { return this.index; }
  get Adds() { return this.adds; }
  get Updates() { return this.updates; }
  get Removes() { return this.removes; }
  get Refreshes() { return this.refreshes; }
  get Moves() { return this.moves; }
  get Count() { return this.count; }
  get LastUpdated() { return this.lastUpdated; }
}

export class ChangeSummary {
  constructor(index = -1, latest = new ChangeStatistics(), overall = new ChangeStatistics()) { Object.assign(this, { index, latest, overall }); }
  get Latest() { return this.latest; }
  get Overall() { return this.overall; }
  static get empty() { return new ChangeSummary(); }
  static get Empty() { return this.empty; }
}

/** Per-batch and cumulative item change counts; count measures change records. */
export function collectUpdateStats() {
  return source => defer(() => {
    let overall = new ChangeStatistics();
    return source.pipe(map(input => {
      const changes = input instanceof ChangeSet ? input : new ChangeSet(input, kindOf(input));
      const index = overall.index + 1;
      const latest = new ChangeStatistics(index, changes.adds, changes.updates, changes.removes, changes.refreshes, changes.moves, changes.length);
      overall = new ChangeStatistics(index, overall.adds + latest.adds, overall.updates + latest.updates, overall.removes + latest.removes, overall.refreshes + latest.refreshes, overall.moves + latest.moves, overall.count + latest.count);
      return new ChangeSummary(index, latest, overall);
    }));
  });
}

/** A node owns an observable keyed collection of direct children. */
export class Node {
  constructor(item, key, parent = null) {
    this.item = item;
    this.key = key;
    this._parent = parent instanceof Optional ? parent.valueOrDefault : parent;
    this._children = new SourceCache(node => node.key);
    this.children = this._children.asObservableCache();
    this.isDisposed = false;
  }
  get parent() { return this._parent ? Optional.some(this._parent) : Optional.none(); }
  get isRoot() { return this._parent == null; }
  get depth() {
    let result = 0, node = this._parent;
    const seen = new Set([this]);
    while (node) {
      if (seen.has(node)) throw new Error('Circular tree parent relationship');
      seen.add(node); result++; node = node._parent;
    }
    return result;
  }
  equals(other) { return other instanceof Node && (Object.is(this.key, other.key) || this.key === other.key); }
  dispose() { if (!this.isDisposed) { this.isDisposed = true; this.children.dispose(); this._children.dispose(); } }
  get Item() { return this.item; }
  get Key() { return this.key; }
  get Parent() { return this.parent; }
  get Children() { return this.children; }
  get IsRoot() { return this.isRoot; }
  get Depth() { return this.depth; }
  Equals(other) { return this.equals(other); }
  Dispose() { this.dispose(); }
  toString() { return `${this.item}${this.children.size ? ` (${this.children.size} children)` : ''}`; }
}

/** Build a live hierarchy; by default the emitted cache contains only root nodes. */
export function transformToTree(pivotOn, predicateChanged) {
  requireFunction(pivotOn, 'pivotOn');
  return source => new Observable(observer => {
    const nodes = new Map();
    const ownedNodes = new Set();
    let selected = new Map();
    let predicate = typeof predicateChanged === 'function' ? predicateChanged : node => node.isRoot;
    const subscriptions = new Subscription();
    const pending = [];
    let processing = false;

    function emitSelection(touched = new Set()) {
      const next = new Map(), changes = [];
      for (const [key, node] of nodes) if (predicate(node)) next.set(key, node);
      for (const [key, previous] of selected) if (!next.has(key)) changes.push({ reason: 'remove', key, current: previous });
      for (const [key, node] of next) {
        if (!selected.has(key)) changes.push({ reason: 'add', key, current: node });
        else if (selected.get(key) !== node) changes.push({ reason: 'update', key, current: node, previous: selected.get(key) });
        else if (touched.has(key)) changes.push({ reason: 'refresh', key, current: node });
      }
      selected = next;
      if (changes.length) observer.next(new ChangeSet(changes, 'cache'));
    }

    function process(changes) {
      const retired = [], touched = new Set();
      for (const change of changes) {
        const old = nodes.get(change.key);
        if (change.reason === 'remove') { if (old) retired.push(old); nodes.delete(change.key); }
        else if (change.reason === 'add' || change.reason === 'update') {
          if (old) retired.push(old);
          const replacement = new Node(change.current, change.key);
          ownedNodes.add(replacement);
          nodes.set(change.key, replacement);
        }
        touched.add(change.key);
      }
      const children = new Map();
      for (const [key, node] of nodes) {
        const parentKey = pivotOn(node.item);
        const parent = nodes.get(parentKey);
        const nextParent = parent && parent !== node ? parent : null;
        if (node._parent !== nextParent) touched.add(key);
        node._parent = nextParent;
        if (nextParent) {
          if (!children.has(nextParent.key)) children.set(nextParent.key, new Map());
          children.get(nextParent.key).set(key, node);
        }
      }
      // Validate cycles before publishing any children changes or root changes.
      const visited = new Set();
      for (const node of nodes.values()) {
        if (visited.has(node)) continue;
        const path = new Set();
        let cursor = node;
        while (cursor && !visited.has(cursor)) {
          if (path.has(cursor)) throw new Error('Circular tree parent relationship');
          path.add(cursor); cursor = cursor._parent;
        }
        for (const part of path) visited.add(part);
      }
      for (const [key, node] of nodes) {
        const wanted = children.get(key) ?? new Map();
        node._children.edit(updater => {
          for (const child of node.children.items) if (!wanted.has(child.key)) updater.removeKey(child.key);
          for (const [childKey, child] of wanted) {
            const existing = node.children.lookup(childKey);
            if (!existing.hasValue || existing.value !== child) updater.addOrUpdate(child);
            else if (touched.has(childKey)) updater.refreshKey(childKey);
          }
        });
      }
      emitSelection(touched);
      for (const node of retired) { node.dispose(); ownedNodes.delete(node); }
    }

    function enqueue(action) {
      pending.push(action);
      if (processing) return;
      processing = true;
      try { while (pending.length && !observer.closed) pending.shift()(); }
      catch (error) { observer.error(error); }
      finally { processing = false; }
    }

    if (predicateChanged && typeof predicateChanged.subscribe === 'function') subscriptions.add(predicateChanged.subscribe({
      next(value) { enqueue(() => { requireFunction(value, 'predicate'); predicate = value; emitSelection(); }); },
      error(error) { observer.error(error); }
    }));
    if (!observer.closed) subscriptions.add(source.subscribe({ next(changes) { enqueue(() => process(changes)); }, error(error) { observer.error(error); }, complete() { enqueue(() => observer.complete()); } }));
    return () => { subscriptions.unsubscribe(); for (const node of ownedNodes) node.dispose(); ownedNodes.clear(); nodes.clear(); selected.clear(); pending.length = 0; };
  });
}

const asStream = value => typeof value?.connect === 'function' ? value.connect() : value;
const isStream = value => typeof value?.subscribe === 'function' || typeof value?.connect === 'function';

/** Merge engine with per-source ownership, first-arrival key priority and list offsets. */
function mergedStreams(outer, selector, options = {}) {
  if (typeof options === 'function') options = { comparer: options };
  const compare = typeof options.comparer === 'function' ? options.comparer : options.comparer?.compare?.bind(options.comparer) ?? options.comparer?.Compare?.bind(options.comparer);
  const equals = typeof options.equalityComparer === 'function' ? options.equalityComparer : options.equalityComparer?.equals?.bind(options.equalityComparer) ?? options.equalityComparer?.Equals?.bind(options.equalityComparer);
  return new Observable(observer => {
    const subscriptions = new Subscription(), records = [], parents = new Map(), parentList = [];
    const published = new Map();
    let kind = null, sequence = 0, outerDone = false, parentKind;
    const pending = []; let draining = false;
    const checkComplete = () => { if (outerDone && options.completable !== false && records.every(record => record.done)) observer.complete(); };
    function enqueue(work) {
      pending.push(work);
      if (draining) return;
      draining = true;
      try { while (pending.length && !observer.closed) pending.shift()(); }
      catch (error) { observer.error(error); }
      finally { draining = false; }
    }
    const listOffset = record => records.slice(0, records.indexOf(record)).reduce((total, item) => total + (Array.isArray(item.state) ? item.state.length : 0), 0);
    function publishList(changes) {
      if (!changes.length) return;
      const snapshot = records.flatMap(record => Array.isArray(record.state) ? record.state : []);
      observer.next(new ChangeSet(changes, 'list', snapshot));
    }
    function reconcile(keys, incoming, refreshed = new Set(), updated = new Set()) {
      const output = [];
      for (const key of keys) {
        let candidate;
        for (const record of records) {
          if (!(record.state instanceof Map) || !record.state.has(key)) continue;
          const value = record.state.get(key);
          const rank = candidate && compare ? compare(value, candidate.value) : 0;
          if (!candidate || compare && rank < 0 || (!compare || rank === 0) && record.order.get(key) < candidate.record.order.get(key)) candidate = { record, value };
        }
        const old = published.get(key);
        if (!candidate) {
          if (old) { output.push({ reason: 'remove', key, current: old.value }); published.delete(key); }
          continue;
        }
        const sameValue = old && equals && equals(old.value, candidate.value);
        const effectiveValue = sameValue ? old.value : candidate.value;
        const changed = old && !Object.is(old.value, candidate.value);
        const forceUpdate = old && candidate.record === incoming && updated.has(key) && candidate.record === old.record;
        if (!old) output.push({ reason: 'add', key, current: candidate.value });
        else if ((changed || forceUpdate) && !sameValue) output.push({ reason: 'update', key, current: candidate.value, previous: old.value });
        else if (candidate.record === incoming && refreshed.has(key)) output.push({ reason: 'refresh', key, current: effectiveValue });
        // Preserve the actual published reference when an equality comparer suppresses an update.
        candidate.value = effectiveValue;
        published.set(key, candidate);
      }
      if (output.length) observer.next(new ChangeSet(output, 'cache'));
    }
    function accept(record, changes) {
      if (!record.alive) return;
      const incomingKind = kindOf(changes);
      if (kind && kind !== incomingKind) throw new TypeError('Merged children must all use the same changeset kind');
      kind ??= incomingKind;
      record.state ??= kind === 'cache' ? new Map() : [];
      if (kind === 'cache') {
        const keys = new Set(), refreshed = new Set(), updated = new Set();
        for (const change of changes) {
          if (change.reason === 'clear') { for (const key of record.state.keys()) keys.add(key); record.state.clear(); record.order.clear(); continue; }
          const key = change.key; keys.add(key);
          if (change.reason === 'add' || change.reason === 'update' || change.reason === 'replace') {
            if (!record.state.has(key)) record.order.set(key, sequence++);
            record.state.set(key, change.current); updated.add(key);
          } else if (change.reason === 'remove') { record.state.delete(key); record.order.delete(key); }
          else if (change.reason === 'refresh') refreshed.add(key);
        }
        reconcile(keys, record, refreshed, updated);
      } else {
        const offset = listOffset(record), output = [];
        for (const change of changes) {
          const local = record.state;
          if (change.range) {
            if (change.reason === 'addRange') {
              const at = change.range.index < 0 ? local.length : change.range.index;
              output.push({ ...change, range: { items: [...change.range.items], index: offset + at }, currentIndex: offset + at });
              local.splice(at, 0, ...change.range.items);
            } else if (change.reason === 'clear') {
              if (local.length) output.push({ reason: 'removeRange', range: { items: [...local], index: offset }, currentIndex: offset });
              local.length = 0;
            } else if (change.range.index >= 0) {
              const at = change.range.index;
              output.push({ ...change, range: { items: [...change.range.items], index: offset + at }, currentIndex: offset + at });
              local.splice(at, change.range.items.length);
            } else {
              for (const item of change.range.items) { const at = local.indexOf(item); if (at >= 0) { output.push({ reason: 'remove', current: item, currentIndex: offset + at }); local.splice(at, 1); } }
            }
          } else {
            let ci = change.currentIndex ?? -1, pi = change.previousIndex ?? -1;
            if (change.reason === 'add') ci = ci < 0 ? local.length : ci;
            else if (change.reason === 'replace' || change.reason === 'update') {
              pi = pi < 0 ? ci < 0 ? local.indexOf(change.previous) : ci : pi;
              ci = ci < 0 ? pi : ci;
            } else if (change.reason === 'remove' || change.reason === 'refresh') ci = ci < 0 ? local.indexOf(change.current) : ci;
            else if (change.reason === 'move' || change.reason === 'moved') pi = pi < 0 ? local.indexOf(change.current) : pi;
            output.push({ ...change, currentIndex: ci < 0 ? -1 : offset + ci, previousIndex: pi < 0 ? -1 : offset + pi });
            applyChanges(local, new ChangeSet([{ ...change, currentIndex: ci, previousIndex: pi }], 'list'));
          }
        }
        publishList(output);
      }
    }
    function add(stream, item, key) {
      const record = { stream, item, key, state: null, order: new Map(), alive: true, done: false, subscription: new Subscription() };
      records.push(record); subscriptions.add(record.subscription);
      const observable = asStream(stream);
      if (!observable?.subscribe) throw new TypeError('Child selector must return an Observable or an observable collection');
      record.subscription.add(observable.subscribe({
        next(changes) { enqueue(() => accept(record, changes)); },
        error(error) { observer.error(error); },
        complete() { enqueue(() => { record.done = true; checkComplete(); }); }
      }));
      return record;
    }
    function remove(record) {
      if (!record?.alive) return;
      const offset = kind === 'list' ? listOffset(record) : 0;
      record.alive = false; record.subscription.unsubscribe();
      records.splice(records.indexOf(record), 1);
      if (record.state instanceof Map) reconcile(new Set(record.state.keys()));
      else if (record.state?.length) publishList([{ reason: 'removeRange', range: { items: [...record.state], index: offset }, currentIndex: offset }]);
      checkComplete();
    }
    function addParent(item, key, at = parentList.length) {
      const record = add(selector ? selector(item, key) : item, item, key);
      if (parentKind === 'cache') parents.set(key, record); else parentList.splice(at, 0, record);
      return record;
    }
    function parentChanges(changes) {
      parentKind ??= kindOf(changes);
      for (const change of itemChanges(changes)) {
        if (parentKind === 'cache') {
          if (change.reason === 'clear') { for (const record of parents.values()) remove(record); parents.clear(); }
          else if (change.reason === 'remove') { remove(parents.get(change.key)); parents.delete(change.key); }
          else if (change.reason === 'add' || change.reason === 'update') { remove(parents.get(change.key)); addParent(change.current, change.key); }
        } else {
          let at = change.currentIndex ?? -1;
          if (change.reason === 'add') addParent(change.current, undefined, at < 0 ? parentList.length : at);
          else if (change.reason === 'remove') {
            if (at < 0) at = parentList.findIndex(record => Object.is(record.item, change.current));
            if (at >= 0) { const [record] = parentList.splice(at, 1); remove(record); }
          } else if (change.reason === 'replace' || change.reason === 'update') {
            let oldIndex = change.previousIndex >= 0 ? change.previousIndex : at >= 0 ? at : parentList.findIndex(record => Object.is(record.item, change.previous));
            if (oldIndex >= 0) { const [record] = parentList.splice(oldIndex, 1); remove(record); }
            addParent(change.current, undefined, at < 0 ? Math.max(oldIndex, 0) : at);
          } else if (change.reason === 'move') {
            const [record] = parentList.splice(change.previousIndex, 1); if (record) parentList.splice(at, 0, record);
          }
        }
      }
    }
    subscriptions.add(asStream(outer).subscribe({
      next(value) { enqueue(() => { if (!selector && isStream(value)) add(value); else parentChanges(value); }); },
      error(error) { observer.error(error); },
      complete() { enqueue(() => { outerDone = true; checkComplete(); }); }
    }));
    return () => { subscriptions.unsubscribe(); for (const record of records) record.alive = false; records.length = 0; parents.clear(); parentList.length = 0; published.clear(); pending.length = 0; };
  });
}

/** Merge an array of streams, pair of streams, or a stream of dynamically arriving streams. */
export function mergeChangeSets(sourcesOrOptions, options = {}) {
  if (Array.isArray(sourcesOrOptions)) return mergedStreams(from(sourcesOrOptions), null, options);
  if (isStream(sourcesOrOptions)) return source => mergedStreams(from([source, sourcesOrOptions]), null, options);
  return source => mergedStreams(source, null, sourcesOrOptions ?? options);
}

/** Merge child changesets while their parent item remains in the source collection. */
export function mergeManyChangeSets(observableSelector, options = {}) {
  requireFunction(observableSelector, 'observableSelector');
  return source => mergedStreams(source, observableSelector, options);
}
