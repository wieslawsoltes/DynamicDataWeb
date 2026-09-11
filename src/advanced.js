/** Dynamic grouping, joins, set algebra and aggregation over RxJS change streams. */
import { Observable, Subject, Subscription, isObservable, of, startWith, switchMap, distinctUntilChanged } from 'rxjs';
import { autoRefresh } from './lifecycle.js';
import { ChangeSet, Optional, applyChanges } from './core.js';

const identity = x => x;
const UNASSIGNED_GROUP = Symbol('unassigned group');
const asStream = x => typeof x?.connect === 'function' ? x.connect() : typeof x?.Connect === 'function' ? x.Connect() : x;
const set = (changes = [], kind = 'cache') => new ChangeSet(changes, kind);
const reason = c => String(c.reason).toLowerCase();
const same = Object.is;
const optional = (present, value) => present ? Optional.some(value) : Optional.none();
const safe = (observer, action) => { try { return action(); } catch (e) { observer.error(e); } };

/** A live, read-only group; its stream completes when its last member leaves. */
export class Group {
  constructor(key, kind = 'cache') {
    this.key = key;
    this.kind = kind;
    this._data = kind === 'cache' ? new Map() : [];
    this._subject = new Subject();
    this.isDisposed = false;
    this.cache = this;
    this.list = this;
  }
  get Key() { return this.key; }
  get Cache() { return this; }
  get List() { return this; }
  get items() { return this.kind === 'cache' ? [...this._data.values()] : this._data.slice(); }
  get Items() { return this.items; }
  get keys() { return this.kind === 'cache' ? [...this._data.keys()] : this._data.map((_, i) => i); }
  get keyValues() { return this.kind === 'cache' ? [...this._data.entries()] : this._data.map((v, i) => [i, v]); }
  get size() { return this.kind === 'cache' ? this._data.size : this._data.length; }
  get count() { return this.size; }
  get Count() { return this.size; }
  lookup(key) { return optional(this.kind === 'cache' ? this._data.has(key) : key >= 0 && key < this.size, this.kind === 'cache' ? this._data.get(key) : this._data[key]); }
  Lookup(key) { return this.lookup(key); }
  connect() {
    return new Observable(observer => {
      if (this.isDisposed) { observer.complete(); return; }
      const initial = this.kind === 'cache'
        ? set([...this._data].map(([key, current]) => ({ reason: 'add', key, current })))
        : set(this.size ? [{ reason: 'addRange', range: { items: this.items, index: 0 } }] : [], 'list');
      const sub = this._subject.subscribe(observer);
      observer.next(initial);
      return sub;
    });
  }
  Connect() { return this.connect(); }
  dispose() { if (!this.isDisposed) { this.isDisposed = true; this._subject.complete(); } }
  Dispose() { this.dispose(); }
  [Symbol.iterator]() { return this.items[Symbol.iterator](); }
}

/** An immutable grouping used by immutable grouping and *JoinMany operators. */
export class ImmutableGroup {
  constructor(key, entries, kind = 'cache') {
    this.key = key;
    this.kind = kind;
    this._entries = Object.freeze([...entries].map(([k, v]) => Object.freeze([k, v])));
    this.items = Object.freeze(this._entries.map(x => x[1]));
    this.cache = this;
    this.list = this;
    Object.freeze(this);
  }
  get Key() { return this.key; }
  get Items() { return this.items; }
  get Cache() { return this; }
  get List() { return this; }
  get keys() { return this._entries.map(x => x[0]); }
  get keyValues() { return this._entries.slice(); }
  get size() { return this.items.length; }
  get count() { return this.size; }
  get Count() { return this.size; }
  lookup(key) { const entry = this._entries.find(x => same(x[0], key) || x[0] === key); return optional(!!entry, entry?.[1]); }
  Lookup(key) { return this.lookup(key); }
  connect() { return of(this.kind === 'cache' ? set(this._entries.map(([key, current]) => ({ reason: 'add', key, current }))) : set(this.size ? [{ reason: 'addRange', range: { items: this.items.slice(), index: 0 } }] : [], 'list')); }
  Connect() { return this.connect(); }
  [Symbol.iterator]() { return this.items[Symbol.iterator](); }
}

function grouping(selector, regrouper, immutable = false) {
  if (typeof selector !== 'function' && !isObservable(selector)) throw new TypeError('A group selector function or observable is required');
  return source => new Observable(observer => {
    const subscriptions = new Subscription();
    const groups = new Map(), membership = new Map(), data = new Map(), snapshots = new Map();
    let currentSelector = typeof selector === 'function' ? selector : null, kind = 'cache', listRecords = [];
    const listGroups = new Map();
    const publish = (before, touched, pending) => {
      for (const [key, changes] of pending) if (changes.length) groups.get(key)?._subject.next(set(changes, kind));
      const out = [];
      for (const [key, group] of groups) {
        if (!group.size) {
          if (before.has(key)) out.push({ reason: 'remove', key, current: immutable ? snapshots.get(key) : group });
          groups.delete(key); snapshots.delete(key); group.dispose();
        } else if (!before.has(key) || (immutable && touched.has(key))) {
          const value = immutable ? new ImmutableGroup(key, group.keyValues, kind) : group;
          out.push({ reason: before.has(key) ? 'update' : 'add', key, current: value, ...(before.has(key) ? { previous: snapshots.get(key) } : {}) });
          snapshots.set(key, value);
        }
      }
      if (out.length) observer.next(set(out));
    };
    const runCache = changes => {
      const before = new Set(groups.keys()), touched = new Set(), pending = new Map();
      const record = (groupKey, change) => {
        let group = groups.get(groupKey);
        if (!group) { group = new Group(groupKey); groups.set(groupKey, group); }
        if (!pending.has(groupKey)) pending.set(groupKey, []);
        pending.get(groupKey).push(change); touched.add(groupKey);
        if (change.reason === 'remove') group._data.delete(change.key);
        else group._data.set(change.key, change.current);
      };
      for (const c of changes) {
        const key = c.key, oldGroupKey = membership.get(key), had = membership.has(key), r = reason(c);
        if (r === 'remove') {
          data.delete(key);
          if (had) { record(oldGroupKey, { reason: 'remove', key, current: groups.get(oldGroupKey)._data.get(key) }); membership.delete(key); }
          continue;
        }
        if (r === 'clear') {
          for (const [oldKey, item] of data) if (membership.has(oldKey)) record(membership.get(oldKey), { reason: 'remove', key: oldKey, current: item });
          data.clear(); membership.clear(); continue;
        }
        data.set(key, c.current);
        if (!currentSelector) continue;
        const groupKey = currentSelector(c.current, key);
        if (groupKey === UNASSIGNED_GROUP) {
          if (had) { record(oldGroupKey, { reason: 'remove', key, current: groups.get(oldGroupKey)._data.get(key) }); membership.delete(key); }
          continue;
        }
        if (had && !same(groupKey, oldGroupKey)) record(oldGroupKey, { reason: 'remove', key, current: groups.get(oldGroupKey)._data.get(key) });
        const unchanged = had && same(groupKey, oldGroupKey);
        const previous = unchanged ? groups.get(groupKey)._data.get(key) : undefined;
        record(groupKey, { reason: unchanged ? r === 'refresh' ? 'refresh' : 'update' : 'add', key, current: c.current, ...(unchanged && r !== 'refresh' ? { previous } : {}) });
        membership.set(key, groupKey);
      }
      publish(before, touched, pending);
    };
    const runList = changes => {
      const refreshed = new Set(), replaced = new Set();
      const makeRecord = item => ({ token: {}, item });
      const findItem = item => listRecords.findIndex(entry => same(entry.item, item));
      // Follow indexed source deltas so equal references still retain distinct occurrence lifetimes.
      for (const c of changes || []) {
        const r = reason(c), index = c.currentIndex ?? -1;
        if (r === 'add') listRecords.splice(index < 0 ? listRecords.length : index, 0, makeRecord(c.current));
        else if (r === 'addrange') {
          const values = c.range?.items || c.range || [], position = c.range?.index ?? index;
          let offset = position < 0 ? listRecords.length : position;
          for (const item of values) listRecords.splice(offset++, 0, makeRecord(item));
        } else if (r === 'remove') {
          const position = index >= 0 ? index : findItem(c.current); if (position >= 0) listRecords.splice(position, 1);
        } else if (r === 'removerange') {
          const values = c.range?.items || c.range || [], position = c.range?.index ?? index;
          if (position >= 0) listRecords.splice(position, values.length);
          else for (const item of values) { const found = findItem(item); if (found >= 0) listRecords.splice(found, 1); }
        } else if (r === 'clear') listRecords.length = 0;
        else if (r === 'replace' || r === 'update') {
          const previousIndex = c.previousIndex >= 0 ? c.previousIndex : index >= 0 ? index : findItem(c.previous);
          const previous = previousIndex >= 0 ? listRecords.splice(previousIndex, 1)[0] : undefined;
          const entry = { token: previous?.token || {}, item: c.current }; replaced.add(entry.token);
          listRecords.splice(index >= 0 ? index : previousIndex >= 0 ? previousIndex : listRecords.length, 0, entry);
        } else if (r === 'move' || r === 'moved') {
          const previousIndex = c.previousIndex >= 0 ? c.previousIndex : findItem(c.current);
          if (previousIndex >= 0) { const entry = listRecords.splice(previousIndex, 1)[0]; if (entry) listRecords.splice(index, 0, entry); }
        } else if (r === 'refresh') {
          const position = index >= 0 ? index : findItem(c.current); if (listRecords[position]) refreshed.add(listRecords[position].token);
        }
      }
      // Most producers supply matching deltas plus a snapshot. Reconcile only when a snapshot differs.
      if (changes?.items && (changes.items.length !== listRecords.length || changes.items.some((item, i) => !same(item, listRecords[i]?.item)))) {
        const occurrences = new Map();
        for (const entry of listRecords) { if (!occurrences.has(entry.item)) occurrences.set(entry.item, []); occurrences.get(entry.item).push(entry); }
        const offsets = new Map();
        listRecords = changes.items.map(item => { const position = offsets.get(item) || 0; offsets.set(item, position + 1); return occurrences.get(item)?.[position] || makeRecord(item); });
      }
      if (!currentSelector) return;
      const partitions = new Map(), before = new Set(groups.keys()), touched = new Set(), pending = new Map();
      listRecords.forEach((entry, index) => { const key = currentSelector(entry.item, index); if (!partitions.has(key)) partitions.set(key, []); partitions.get(key).push(entry); });
      for (const key of new Set([...before, ...partitions.keys()])) {
        let group = groups.get(key); if (!group) { group = new Group(key, 'list'); groups.set(key, group); }
        const next = partitions.get(key) || [], work = (listGroups.get(key) || []).slice(), wanted = new Set(next.map(entry => entry.token)), delta = [];
        for (let index = work.length - 1; index >= 0; index--) if (!wanted.has(work[index].token)) {
          delta.push({ reason: 'remove', current: work[index].item, currentIndex: index }); work.splice(index, 1);
        }
        const presentTokens = new Set(work.map(entry => entry.token));
        for (let index = 0; index < next.length; index++) {
          const entry = next[index];
          let previousIndex = !presentTokens.has(entry.token) ? -1 : work[index]?.token === entry.token ? index : work.findIndex(previous => previous.token === entry.token);
          if (previousIndex < 0) { delta.push({ reason: 'add', current: entry.item, currentIndex: index }); work.splice(index, 0, entry); presentTokens.add(entry.token); }
          else {
            if (previousIndex !== index) {
              const previous = work.splice(previousIndex, 1)[0]; work.splice(index, 0, previous);
              delta.push({ reason: 'move', current: previous.item, previousIndex, currentIndex: index });
            }
            if (replaced.has(entry.token) || !same(work[index].item, entry.item)) {
              delta.push({ reason: 'replace', previous: work[index].item, current: entry.item, previousIndex: index, currentIndex: index }); work[index] = entry;
            } else if (refreshed.has(entry.token)) delta.push({ reason: 'refresh', current: entry.item, currentIndex: index });
          }
        }
        group._data = next.map(entry => entry.item);
        if (next.length) listGroups.set(key, next.slice()); else listGroups.delete(key);
        if (delta.length) { pending.set(key, delta); touched.add(key); }
      }
      publish(before, touched, pending);
    };
    const regroup = () => safe(observer, () => kind === 'list' ? runList() : runCache([...data].map(([key, current]) => ({ reason: 'refresh', key, current }))));
    if (isObservable(selector)) subscriptions.add(selector.subscribe({ next: next => { if (typeof next !== 'function') { observer.error(new TypeError('Group selector emissions must be functions')); return; } currentSelector = next; regroup(); }, error: e => observer.error(e) }));
    if (regrouper) subscriptions.add(asStream(regrouper).subscribe({ next: regroup, error: e => observer.error(e) }));
    subscriptions.add(source.subscribe({ next: changes => safe(observer, () => { kind = changes.kind || kind; if (kind === 'list') runList(changes); else runCache(changes); }), error: e => observer.error(e), complete: () => observer.complete() }));
    return () => { subscriptions.unsubscribe(); for (const group of groups.values()) group.dispose(); groups.clear(); data.clear(); membership.clear(); listGroups.clear(); listRecords.length = 0; };
  });
}

export const groupOn = (selector, regrouper) => grouping(selector, regrouper);
export const group = (selector, regrouperOrOptions) => regrouperOrOptions?.resultGroupSource ? groupWithSpecifiedGroups(selector, regrouperOrOptions.resultGroupSource) : groupOn(selector, regrouperOrOptions);
export const groupOnImmutable = (selector, regrouper) => grouping(selector, regrouper, true);
export const groupWithImmutableState = groupOnImmutable;

/** Per-item observables control membership. Item subscriptions are replaced on updates and removed on deletion. */
export function groupOnObservable(selector) {
  return source => new Observable(observer => {
    const subs = new Subscription(), forwarded = new Subject(), regroup = new Subject(), streams = new Map(), values = new Map();
    const active = new Set();
    let inBatch = false, sourceDone = false;
    const finish = () => { if (sourceDone && !inBatch && !active.size) forwarded.complete(); };
    subs.add(forwarded.pipe(groupOn((item, key) => values.has(key) ? values.get(key) : UNASSIGNED_GROUP, regroup)).subscribe(observer));
    subs.add(source.subscribe({ next: changes => safe(observer, () => {
      if (changes.kind === 'list') throw new TypeError('groupOnObservable requires a keyed cache change stream');
      inBatch = true;
      try {
        for (const c of changes) {
          const r = reason(c);
          if (r === 'remove' || r === 'update' || r === 'add') { streams.get(c.key)?.unsubscribe(); streams.delete(c.key); active.delete(c.key); values.delete(c.key); }
          if (r === 'clear') { for (const sub of streams.values()) sub.unsubscribe(); streams.clear(); active.clear(); values.clear(); }
          if (r === 'add' || r === 'update') {
            const stream = selector(c.current, c.key);
            if (!isObservable(stream)) throw new TypeError('groupOnObservable selector must return an Observable');
            active.add(c.key);
            const sub = stream.pipe(distinctUntilChanged()).subscribe({ next: value => { values.set(c.key, value); if (!inBatch) regroup.next(); }, error: e => observer.error(e), complete: () => { active.delete(c.key); finish(); } });
            streams.set(c.key, sub);
          }
        }
        forwarded.next(changes);
      } finally { inBatch = false; finish(); }
    }), error: e => observer.error(e), complete: () => { sourceDone = true; finish(); } }));
    return () => { subs.unsubscribe(); for (const sub of streams.values()) sub.unsubscribe(); forwarded.complete(); regroup.complete(); };
  });
}

function join(type, rightSource, foreignKeySelector, resultSelector, many = false) {
  if (typeof foreignKeySelector !== 'function' || typeof resultSelector !== 'function') throw new TypeError('Join requires foreign-key and result selectors');
  return leftSource => new Observable(observer => {
    const subs = new Subscription(), left = new Map(), right = new Map(), rightForeign = new Map(), buckets = new Map();
    const result = new Map(), outputByForeign = new Map(), tupleKeys = new Map();
    let leftDone = false, rightDone = false;
    const tuple = (lk, rk) => { if (!tupleKeys.has(lk)) tupleKeys.set(lk, new Map()); const m = tupleKeys.get(lk); if (!m.has(rk)) { const key = [lk, rk]; Object.defineProperties(key, { leftKey: { value: lk }, rightKey: { value: rk } }); m.set(rk, Object.freeze(key)); } return m.get(rk); };
    const select = (key, l, r) => resultSelector.length >= 3 ? resultSelector(key, l, r) : resultSelector(l, r);
    const update = (side, changes) => {
      if (changes.kind === 'list') throw new TypeError('Join requires keyed cache change streams');
      const affected = new Set(), onlyRefresh = new Map();
      const touch = (key, refresh) => { affected.add(key); onlyRefresh.set(key, (onlyRefresh.get(key) ?? true) && refresh); };
      for (const c of changes) {
        const r = reason(c), key = c.key;
        if (r === 'clear') {
          if (side === 'left') { for (const k of left.keys()) touch(k, false); left.clear(); }
          else { for (const k of buckets.keys()) touch(k, false); right.clear(); rightForeign.clear(); buckets.clear(); }
          continue;
        }
        if (side === 'left') { if (r === 'remove') left.delete(key); else left.set(key, c.current); touch(key, r === 'refresh'); }
        else {
          const had = rightForeign.has(key), oldFk = rightForeign.get(key), fk = r === 'remove' ? oldFk : foreignKeySelector(c.current, key);
          if (had) {
            if (r === 'remove' || !same(oldFk, fk)) { const bucket = buckets.get(oldFk); bucket?.delete(key); if (bucket && !bucket.size) buckets.delete(oldFk); }
            touch(oldFk, r === 'refresh' && same(oldFk, fk));
          }
          if (r === 'remove') { right.delete(key); rightForeign.delete(key); }
          else { right.set(key, c.current); rightForeign.set(key, fk); if (!buckets.has(fk)) buckets.set(fk, new Map()); buckets.get(fk).set(key, c.current); touch(fk, r === 'refresh' && (!had || same(oldFk, fk))); }
        }
      }
      const out = [], plans = new Map(), previousKeys = new Set(), desiredKeys = new Set();
      for (const fk of affected) {
        const hasLeft = left.has(fk), l = left.get(fk), bucket = buckets.get(fk) || new Map(), rows = new Map(), oldKeys = outputByForeign.get(fk) || new Set();
        if (many) {
          const include = type === 'inner' ? hasLeft && bucket.size > 0 : type === 'left' ? hasLeft : type === 'right' ? bucket.size > 0 : hasLeft || bucket.size > 0;
          if (include) rows.set(fk, [type === 'inner' || type === 'left' ? l : optional(hasLeft, l), new ImmutableGroup(fk, bucket)]);
        } else if (type === 'inner' || type === 'right') {
          if (hasLeft || type === 'right') for (const [rk, r] of bucket) rows.set(type === 'inner' ? tuple(fk, rk) : rk, [type === 'inner' ? l : optional(hasLeft, l), r]);
        } else if (hasLeft || (type === 'full' && bucket.size)) {
          const r = [...bucket.values()].at(-1);
          rows.set(fk, [type === 'left' ? l : optional(hasLeft, l), optional(bucket.size > 0, r)]);
        }
        plans.set(fk, rows);
        for (const key of oldKeys) previousKeys.add(key);
        for (const key of rows.keys()) desiredKeys.add(key);
      }
      for (const key of previousKeys) if (!desiredKeys.has(key)) { out.push({ reason: 'remove', key, current: result.get(key) }); result.delete(key); }
      for (const [fk, rows] of plans) {
        const bucket = buckets.get(fk) || new Map();
        for (const [key, [lv, rv]] of rows) {
          if (onlyRefresh.get(fk) && result.has(key)) out.push({ reason: 'refresh', key, current: result.get(key) });
          else { const value = select(key, lv, rv), had = result.has(key), previous = result.get(key); result.set(key, value); out.push({ reason: had ? 'update' : 'add', key, current: value, ...(had ? { previous } : {}) }); }
        }
        if (rows.size) outputByForeign.set(fk, new Set(rows.keys())); else { outputByForeign.delete(fk); tupleKeys.delete(fk); }
        if (type === 'inner' && tupleKeys.has(fk)) { const tuples = tupleKeys.get(fk); for (const rk of tuples.keys()) if (!bucket.has(rk)) tuples.delete(rk); }
      }
      if (out.length) observer.next(set(out));
    };
    const done = () => { if (leftDone && rightDone) observer.complete(); };
    subs.add(asStream(rightSource).subscribe({ next: c => safe(observer, () => update('right', c)), error: e => observer.error(e), complete: () => { rightDone = true; done(); } }));
    if (!observer.closed) subs.add(leftSource.subscribe({ next: c => safe(observer, () => update('left', c)), error: e => observer.error(e), complete: () => { leftDone = true; done(); } }));
    return subs;
  });
}
export const innerJoin = (right, fk, selector) => join('inner', right, fk, selector);
export const leftJoin = (right, fk, selector) => join('left', right, fk, selector);
export const rightJoin = (right, fk, selector) => join('right', right, fk, selector);
export const fullJoin = (right, fk, selector) => join('full', right, fk, selector);
export const innerJoinMany = (right, fk, selector) => join('inner', right, fk, selector, true);
export const leftJoinMany = (right, fk, selector) => join('left', right, fk, selector, true);
export const rightJoinMany = (right, fk, selector) => join('right', right, fk, selector, true);
export const fullJoinMany = (right, fk, selector) => join('full', right, fk, selector, true);

export const CombineOperator = Object.freeze({ And: 'and', Or: 'or', Xor: 'xor', Except: 'except', and: 'and', or: 'or', xor: 'xor', except: 'except' });

/** Combine static sources, or a SourceList/Observable emitting changes of source streams. */
export function combine(sources, operator = 'or') {
  const type = String(operator).toLowerCase();
  if (!['and', 'or', 'xor', 'except'].includes(type)) throw new TypeError(`Unknown combine operator: ${operator}`);
  return new Observable(observer => {
    const subs = new Subscription(), entries = [], output = new Map(), pendingLatest = new Map();
    let kind = 'cache', outerDone = false, suspended = false, initialized = false;
    const matches = key => { const n = entries.reduce((count, e) => count + e.data.has(key), 0); return type === 'and' ? entries.length > 0 && n === entries.length : type === 'or' ? n > 0 : type === 'xor' ? n === 1 : !!entries[0]?.data.has(key) && n === 1; };
    const recalculate = (keys, latest, refreshKeys = new Set()) => {
      if (observer.closed) return;
      if (suspended) { for (const key of keys) { if (latest?.has(key)) pendingLatest.set(key, latest.get(key)); else pendingLatest.delete(key); } return; }
      const out = [];
      for (const key of keys) {
        const had = output.has(key), previous = output.get(key);
        if (!matches(key)) { if (had) { output.delete(key); out.push({ reason: 'remove', key, current: previous }); } continue; }
        const current = latest?.has(key) ? latest.get(key) : entries.find(e => e.data.has(key))?.data.get(key);
        if (!had || !same(current, previous)) { output.set(key, current); out.push({ reason: had ? 'update' : 'add', key, current, ...(had ? { previous } : {}) }); }
        else if (refreshKeys.has(key)) out.push({ reason: 'refresh', key, current });
      }
      if (out.length) {
        if (kind === 'list') {
          const listChanges = out.flatMap(c => c.reason === 'update' ? [{ reason: 'remove', current: c.previous }, { reason: 'add', current: c.current }] : [{ reason: c.reason, current: c.current }]);
          observer.next(set(listChanges, 'list'));
        } else observer.next(set(out));
      }
    };
    const allKeys = () => new Set([...output.keys(), ...entries.flatMap(e => [...e.data.keys()])]);
    const complete = () => { if (initialized && outerDone && entries.every(e => e.done)) observer.complete(); };
    const attach = entry => {
      const sub = asStream(entry.source).subscribe({ next: changes => safe(observer, () => {
        const keys = new Set(), latest = new Map(), refresh = new Set();
        if (changes.kind === 'list') {
          kind = 'list'; const before = new Map(entry.data); applyChanges(entry.list, changes); entry.data = new Map(entry.list.map(item => [item, item]));
          for (const key of before.keys()) keys.add(key); for (const key of entry.data.keys()) keys.add(key);
          for (const [key, value] of entry.data) latest.set(key, value);
          for (const c of changes) if (reason(c) === 'refresh') refresh.add(c.current);
        } else {
          for (const c of changes) {
            const r = reason(c); if (r === 'clear') { for (const key of entry.data.keys()) keys.add(key); entry.data.clear(); continue; }
            keys.add(c.key);
            if (r === 'remove') entry.data.delete(c.key);
            else { entry.data.set(c.key, c.current); if (r === 'refresh') refresh.add(c.key); else latest.set(c.key, c.current); }
          }
        }
        recalculate(keys, latest, refresh);
      }), error: e => observer.error(e), complete: () => { entry.done = true; complete(); } });
      entry.sub = sub; subs.add(sub);
    };
    const replaceSources = next => {
      suspended = true;
      const old = entries.slice(), nextEntries = [], unused = old.slice();
      for (const source of next) { const index = unused.findIndex(e => e.source === source); nextEntries.push(index < 0 ? { source, data: new Map(), list: [], done: false, sub: null } : unused.splice(index, 1)[0]); }
      for (const entry of unused) { entry.sub?.unsubscribe(); subs.remove(entry.sub); }
      entries.splice(0, entries.length, ...nextEntries);
      for (const entry of entries) if (!entry.sub && !observer.closed) attach(entry);
      suspended = false; initialized = true;
      recalculate(allKeys(), pendingLatest); pendingLatest.clear(); complete();
    };
    if (Array.isArray(sources)) { outerDone = true; replaceSources(sources); }
    else {
      const sourceList = [], sourceCache = new Map();
      subs.add(asStream(sources).subscribe({ next: changes => safe(observer, () => {
        if (Array.isArray(changes) && !changes.kind && (changes.length === 0 || !changes[0]?.reason)) { sourceList.splice(0, sourceList.length, ...changes); }
        else if (changes.kind === 'cache') { applyChanges(sourceCache, changes); sourceList.splice(0, sourceList.length, ...sourceCache.values()); }
        else applyChanges(sourceList, changes);
        replaceSources(sourceList);
      }), error: e => observer.error(e), complete: () => { outerDone = true; initialized = true; complete(); } }));
    }
    return subs;
  });
}
const logical = type => (...others) => source => {
  if (!others.length) return combine(source, type);
  return combine([source, ...others.flat()], type);
};
export const and = logical('and');
export const or = logical('or');
export const xor = logical('xor');
export const except = logical('except');

function aggregate(mode, selector = identity, fallback = 0) {
  if (typeof selector !== 'function') throw new TypeError('Aggregate selector must be a function');
  return source => new Observable(observer => {
    const values = new Map(), list = [], aggregateItems = [];
    let aggregateId = 0;
    let total = 0, squares = 0, count = 0, last, hasLast = false;
    const remove = number => { total -= number; squares -= number * number; count--; };
    const add = number => { total += number; squares += number * number; count++; };
    return source.subscribe({ next: changes => safe(observer, () => {
      if (changes.kind === 'aggregate') {
        for (const c of changes) {
          if (c.type === 'remove') { const index = aggregateItems.findIndex(x => same(x.item, c.item)); if (index >= 0) { const entry = aggregateItems.splice(index, 1)[0]; remove(values.get(entry.key)); values.delete(entry.key); } }
          else { const entry = { key: aggregateId++, item: c.item }; aggregateItems.push(entry); const value = selector(c.item); values.set(entry.key, value); add(value); }
        }
      } else if (changes.kind === 'list') {
        applyChanges(list, changes); values.clear(); total = squares = count = 0;
        list.forEach((item, index) => { const v = selector(item, index); values.set(index, v); add(v); });
      } else {
        for (const c of changes) {
          if (reason(c) === 'clear') { values.clear(); total = squares = count = 0; continue; }
          if (values.has(c.key)) { remove(values.get(c.key)); values.delete(c.key); }
          if (reason(c) !== 'remove') { const v = selector(c.current, c.key); values.set(c.key, v); add(v); }
        }
      }
      if (!count) total = squares = 0;
      else if (!Number.isFinite(total) || !Number.isFinite(squares)) { total = squares = 0; for (const value of values.values()) { total += value; squares += value * value; } }
      let value;
      if (mode === 'sum' || mode === 'count') value = total;
      else if (mode === 'avg') value = count ? total / count : fallback;
      else if (mode === 'stdDev') value = count < 2 ? fallback : Math.sqrt(Math.max(0, squares - total * total / count)) / (count - 1);
      else if (mode === 'standardDeviation') value = count < 2 ? fallback : Math.sqrt(Math.max(0, squares - total * total / count) / (count - 1));
      else { value = fallback; let seen = false; for (const v of values.values()) if (!seen || (mode === 'min' ? v < value : v > value)) { value = v; seen = true; } }
      if (!hasLast || !same(last, value)) { hasLast = true; last = value; observer.next(value); }
    }), error: e => observer.error(e), complete: () => observer.complete() });
  });
}
export const count = (predicate = null) => aggregate('count', predicate ? (item, key) => predicate(item, key) ? 1 : 0 : () => 1);
export const sum = (selector = identity) => aggregate('sum', selector);
export const avg = (selector = identity, fallback = 0) => aggregate('avg', selector, fallback);
export const average = avg;
export const min = (selector = identity, fallback = 0) => aggregate('min', selector, fallback);
export const max = (selector = identity, fallback = 0) => aggregate('max', selector, fallback);
/** Preserves upstream DynamicData's formula: sqrt(sum(x²)−sum(x)²/n)/(n−1). */
export const stdDev = (selector = identity, fallback = 0) => aggregate('stdDev', selector, fallback);
/** Conventional sample standard deviation, provided as an explicit web extension. */
export const standardDeviation = (selector = identity, fallback = 0) => aggregate('standardDeviation', selector, fallback);
export const sumMany = (childrenSelector = identity, valueSelector = identity) => sum((item, key) => [...childrenSelector(item, key)].reduce((total, child) => total + valueSelector(child), 0));

/** Property notifications use the lifecycle module's observableObject/notifyPropertyChanged contract. */
export function groupOnProperty(property, throttleOrOptions = {}, scheduler) {
  const options = typeof throttleOrOptions === 'number' ? { throttle: throttleOrOptions, scheduler } : throttleOrOptions || {};
  const selector = typeof property === 'function' ? property : item => String(property).split('.').reduce((value, key) => value?.[key], item);
  return source => source.pipe(autoRefresh(property, options), groupOn(selector));
}
export function groupOnPropertyWithImmutableState(property, throttleOrOptions = {}, scheduler) {
  const options = typeof throttleOrOptions === 'number' ? { throttle: throttleOrOptions, scheduler } : throttleOrOptions || {};
  const selector = typeof property === 'function' ? property : item => String(property).split('.').reduce((value, key) => value?.[key], item);
  return source => source.pipe(autoRefresh(property, options), groupOnImmutable(selector));
}

export const AggregateType = Object.freeze({ Add: 'add', Remove: 'remove', add: 'add', remove: 'remove' });
/** Convert structural changes into add/remove aggregate records; refresh and move are omitted. */
export function forAggregation() {
  return source => new Observable(observer => {
    let kind = 'cache', state = new Map();
    return source.subscribe({ next: changes => safe(observer, () => {
      if (changes.kind === 'aggregate') { observer.next(changes); return; }
      if (changes.kind !== kind) { kind = changes.kind; state = kind === 'list' ? [] : new Map(); }
      const out = [];
      const record = (type, item) => out.push({ type, item, Type: type, Item: item });
      for (const c of changes) {
        const r = reason(c);
        if (r === 'add') record('add', c.current);
        else if (r === 'remove') record('remove', c.current === undefined && kind === 'cache' ? state.get(c.key) : c.current);
        else if (r === 'update' || r === 'replace') { record('remove', c.previous === undefined && kind === 'cache' ? state.get(c.key) : c.previous); record('add', c.current); }
        else if (r === 'addrange' || r === 'removerange') for (const item of c.range?.items || c.range || []) record(r === 'addrange' ? 'add' : 'remove', item);
        else if (r === 'clear') for (const item of kind === 'list' ? state : state.values()) record('remove', item);
        applyChanges(state, set([c], kind));
      }
      const result = set(out); result.kind = 'aggregate'; observer.next(result);
    }), error: e => observer.error(e), complete: () => observer.complete() });
  });
}
/** Resubscribe to the aggregation pipeline when mutable external state invalidates it. */
export const invalidateWhen = invalidate => source => asStream(invalidate).pipe(startWith(undefined), switchMap(() => source), distinctUntilChanged());

/** Keep groups named by another change stream, including empty groups. */
export function groupWithSpecifiedGroups(selector, resultGroupSource) {
  return source => new Observable(observer => {
    const subs = new Subscription(), available = new Map(), exposed = new Map(), inner = new Map();
    let keys = [], sourceDone = false, keysDone = false;
    const complete = () => { if (sourceDone && keysDone) observer.complete(); };
    const refreshKeys = changes => {
      if (changes.kind === 'cache') {
        const m = new Map(keys.map(key => [key, key]));
        for (const c of changes) { const key = c.current ?? c.key; if (reason(c) === 'remove') m.delete(key); else if (reason(c) === 'clear') m.clear(); else m.set(key, key); }
        keys = [...m.keys()];
      } else applyChanges(keys, changes);
      const wanted = new Set(keys), out = [];
      for (const [key, group] of exposed) if (!wanted.has(key)) { out.push({ reason: 'remove', key, current: group }); exposed.delete(key); group.dispose(); }
      for (const key of wanted) if (!exposed.has(key)) {
        const original = available.get(key), group = new Group(key, original?.kind || 'cache');
        if (original) group._data = original.kind === 'cache' ? new Map(original.keyValues) : original.items;
        exposed.set(key, group); out.push({ reason: 'add', key, current: group });
      }
      if (out.length) observer.next(set(out));
    };
    subs.add(asStream(resultGroupSource).subscribe({ next: c => safe(observer, () => refreshKeys(c)), error: e => observer.error(e), complete: () => { keysDone = true; complete(); } }));
    if (!observer.closed) subs.add(source.pipe(groupOn(selector)).subscribe({ next: changes => safe(observer, () => {
      for (const c of changes) {
        if (reason(c) === 'remove') { available.delete(c.key); inner.get(c.key)?.unsubscribe(); inner.delete(c.key); }
        else {
          const original = c.current; available.set(c.key, original);
          inner.get(c.key)?.unsubscribe();
          const sub = original.connect().subscribe({ next: batch => {
            const group = exposed.get(c.key); if (!group) return;
            if (group.kind !== batch.kind) { group.kind = batch.kind; group._data = batch.kind === 'cache' ? new Map() : []; }
            applyChanges(group._data, batch); if (batch.length) group._subject.next(batch);
          }, error: e => observer.error(e) });
          inner.set(c.key, sub); subs.add(sub);
        }
      }
    }), error: e => observer.error(e), complete: () => { sourceDone = true; complete(); } }));
    return () => { subs.unsubscribe(); for (const group of exposed.values()) group.dispose(); available.clear(); exposed.clear(); inner.clear(); };
  });
}
