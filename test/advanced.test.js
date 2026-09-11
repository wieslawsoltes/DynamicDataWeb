import test from 'node:test';
import assert from 'node:assert/strict';
import { Subject, BehaviorSubject, of } from 'rxjs';
import { ChangeSet, applyChanges } from '../src/core.js';
import { groupOn, groupOnImmutable, groupOnObservable, innerJoin, leftJoin, rightJoin, fullJoin, innerJoinMany, leftJoinMany, rightJoinMany, fullJoinMany, combine, and, or, xor, except, count, sum, avg, min, max, stdDev, standardDeviation, sumMany } from '../src/advanced.js';

const changes = (...items) => new ChangeSet(items, 'cache');
const add = (key, current) => ({ reason: 'add', key, current });
const update = (key, current, previous) => ({ reason: 'update', key, current, previous });
const remove = (key, current) => ({ reason: 'remove', key, current });
const refresh = (key, current) => ({ reason: 'refresh', key, current });
const collect = stream => { const state = new Map(), history = []; let done = false; const sub = stream.subscribe({ next: c => { history.push(c); applyChanges(state, c); }, complete: () => done = true }); return { state, history, sub, get done() { return done; } }; };

test('cache grouping moves on updates and refreshes; removed groups complete', () => {
  const source = new Subject(), result = collect(source.pipe(groupOn(x => x.team)));
  const a = { team: 'red', score: 3 }, b = { team: 'red', score: 4 };
  source.next(changes(add(1, a), add(2, b)));
  const red = result.state.get('red'); assert.equal(red.size, 2);
  let completed = false; const child = collect(red.connect()); red.connect().subscribe({ complete: () => completed = true });
  assert.equal(child.state.size, 2);
  const moved = { team: 'blue', score: 3 }; source.next(changes(update(1, moved, a)));
  assert.equal(result.state.get('blue').size, 1); assert.equal(red.size, 1); assert.equal(child.state.has(1), false);
  b.team = 'blue'; source.next(changes(refresh(2, b)));
  assert.equal(result.state.has('red'), false); assert.equal(completed, true); assert.equal(child.done, true);
  assert.equal(result.state.get('blue').size, 2);
  source.complete(); assert.equal(result.done, true); assert.equal(result.state.get('blue').isDisposed, true);
});

test('group batches retain the same live group when removed and added in one transaction', () => {
  const source = new Subject(), result = collect(source.pipe(groupOn(x => x.team)));
  const a = { team: 1 }, b = { team: 1 }; source.next(changes(add('a', a)));
  const original = result.state.get(1); const countBefore = result.history.length;
  source.next(changes(remove('a', a), add('b', b)));
  assert.equal(result.state.get(1), original); assert.deepEqual(original.items, [b]); assert.equal(result.history.length, countBefore);
  result.sub.unsubscribe(); assert.equal(original.isDisposed, true);
});

test('dynamic selector and regrouper relocate cached items without source changes', () => {
  const source = new Subject(), selectors = new BehaviorSubject(x => x.a), regroup = new Subject();
  const result = collect(source.pipe(groupOn(selectors, regroup))), item = { a: 'a', b: 'b' };
  source.next(changes(add(1, item))); assert.equal(result.state.has('a'), true);
  selectors.next(x => x.b); assert.equal(result.state.has('a'), false); assert.equal(result.state.has('b'), true);
  item.b = 'c'; regroup.next(); assert.equal(result.state.has('c'), true);
  result.sub.unsubscribe();
});

test('immutable grouping replaces snapshots without mutating previous snapshots', () => {
  const source = new Subject(), result = collect(source.pipe(groupOnImmutable(x => x.team)));
  source.next(changes(add(1, { team: 'A', value: 1 })));
  const before = result.state.get('A'); source.next(changes(add(2, { team: 'A', value: 2 })));
  const after = result.state.get('A'); assert.notEqual(before, after); assert.equal(before.size, 1); assert.equal(after.size, 2);
  assert.equal(Object.isFrozen(before.items), true); assert.equal(result.history.at(-1)[0].reason, 'update');
  result.sub.unsubscribe();
});

test('list groups retain duplicate item occurrences and remove empty groups', () => {
  const source = new Subject(), result = collect(source.pipe(groupOn(x => x.team))), item = { team: 'A' };
  source.next(new ChangeSet([{ reason: 'addRange', range: { items: [item, item, { team: 'B' }], index: 0 } }], 'list'));
  assert.equal(result.state.get('A').size, 2);
  source.next(new ChangeSet([{ reason: 'remove', current: item, currentIndex: 0 }], 'list'));
  assert.equal(result.state.get('A').size, 1);
  source.next(new ChangeSet([{ reason: 'remove', current: item, currentIndex: 0 }], 'list'));
  assert.equal(result.state.has('A'), false); result.sub.unsubscribe();
});

test('per-item group observables change membership and are disposed after removal', () => {
  const source = new Subject(), team = new BehaviorSubject('A'); const result = collect(source.pipe(groupOnObservable(x => x.team)));
  const item = { team }; source.next(changes(add(1, item))); assert.equal(result.state.has('A'), true);
  team.next('B'); assert.equal(result.state.has('A'), false); assert.equal(result.state.has('B'), true);
  source.next(changes(remove(1, item))); assert.equal(result.state.size, 0); assert.equal(team.observed, false);
  team.next('C'); assert.equal(result.state.size, 0); result.sub.unsubscribe();
});

test('inner join uses stable composite keys, supports many right rows and foreign-key changes', () => {
  const left = new Subject(), right = new Subject(); let calls = 0;
  const result = collect(left.pipe(innerJoin(right, x => x.owner, (key, l, r) => { calls++; return { key, name: l.name, value: r.value }; })));
  right.next(changes(add('r1', { owner: 1, value: 10 }), add('r2', { owner: 1, value: 20 })));
  left.next(changes(add(1, { name: 'one' }), add(2, { name: 'two' })));
  assert.equal(result.state.size, 2); const key = [...result.state.keys()].find(k => k[1] === 'r1'); assert.deepEqual(key, [1, 'r1']);
  left.next(changes(update(1, { name: 'ONE' }))); assert.equal(result.state.has(key), true); assert.equal(result.state.get(key).name, 'ONE');
  const before = calls; left.next(changes(refresh(1, { name: 'ONE' }))); assert.equal(calls, before); assert.equal(result.history.at(-1)[0].reason, 'refresh');
  right.next(changes(update('r1', { owner: 2, value: 11 }))); assert.equal(result.state.has(key), false); assert.equal([...result.state.values()].find(v => v.key[1] === 'r1').name, 'two');
  left.next(changes(remove(1))); assert.equal(result.state.size, 1);
  left.complete(); assert.equal(result.done, false); right.complete(); assert.equal(result.done, true);
});

test('outer joins use Optional and retain the appropriate unmatched side', () => {
  const left = new Subject(), right = new Subject();
  const l = collect(left.pipe(leftJoin(right, x => x.owner, (a, b) => [a.name, b.hasValue ? b.value.value : null])));
  const r = collect(left.pipe(rightJoin(right, x => x.owner, (a, b) => [a.hasValue ? a.value.name : null, b.value])));
  const f = collect(left.pipe(fullJoin(right, x => x.owner, (a, b) => [a.hasValue ? a.value.name : null, b.hasValue ? b.value.value : null])));
  left.next(changes(add(1, { name: 'one' }))); assert.deepEqual(l.state.get(1), ['one', null]); assert.equal(r.state.size, 0);
  right.next(changes(add('r', { owner: 1, value: 3 }))); assert.deepEqual(l.state.get(1), ['one', 3]); assert.deepEqual(r.state.get('r'), ['one', 3]);
  left.next(changes(remove(1))); assert.equal(l.state.size, 0); assert.deepEqual(r.state.get('r'), [null, 3]); assert.deepEqual(f.state.get(1), [null, 3]);
  right.next(changes(remove('r'))); assert.equal(r.state.size, 0); assert.equal(f.state.size, 0);
  l.sub.unsubscribe(); r.sub.unsubscribe(); f.sub.unsubscribe();
});

test('all Many joins expose immutable right groups and handle last-member removal', () => {
  for (const [operator, mode] of [[innerJoinMany, 'inner'], [leftJoinMany, 'left'], [rightJoinMany, 'right'], [fullJoinMany, 'full']]) {
    const left = new Subject(), right = new Subject(); const result = collect(left.pipe(operator(right, x => x.owner, (key, l, group) => ({ key, l, group }))));
    left.next(changes(add(1, { name: 'one' })));
    assert.equal(result.state.size, mode === 'left' || mode === 'full' ? 1 : 0);
    right.next(changes(add('r1', { owner: 1 }), add('r2', { owner: 1 })));
    assert.equal(result.state.get(1).group.size, 2); const old = result.state.get(1).group;
    right.next(changes(remove('r1'), remove('r2'))); assert.equal(old.size, 2);
    assert.equal(result.state.size, mode === 'left' || mode === 'full' ? 1 : 0);
    left.next(changes(remove(1))); assert.equal(result.state.size, 0);
    right.next(changes(add('r3', { owner: 2 }))); assert.equal(result.state.size, mode === 'right' || mode === 'full' ? 1 : 0);
    result.sub.unsubscribe();
  }
});

test('logical cache operators handle membership, value replacement and removal fallback', () => {
  const a = new Subject(), b = new Subject(), c = new Subject();
  const results = [and, or, xor, except].map(op => collect(a.pipe(op(b, c))));
  a.next(changes(add(1, 'A'))); assert.deepEqual(results.map(x => x.state.size), [0, 1, 1, 1]);
  b.next(changes(add(1, 'B'))); assert.deepEqual(results.map(x => x.state.size), [0, 1, 0, 0]); assert.equal(results[1].state.get(1), 'B');
  c.next(changes(add(1, 'C'))); assert.deepEqual(results.map(x => x.state.size), [1, 1, 0, 0]);
  c.next(changes(remove(1))); b.next(changes(remove(1))); assert.deepEqual(results.map(x => x.state.size), [0, 1, 1, 1]); assert.equal(results[1].state.get(1), 'A');
  a.complete(); b.complete(); assert.equal(results[1].done, false); c.complete(); assert.equal(results.every(x => x.done), true);
});

test('dynamic logical source collections resubscribe and recalculate intersections', () => {
  const a = new BehaviorSubject(changes(add(1, 'a1'), add(2, 'a2'))), b = new BehaviorSubject(changes(add(2, 'b2'))), sources = new Subject();
  const result = collect(combine(sources, 'and'));
  sources.next(new ChangeSet([{ reason: 'addRange', range: { items: [a, b], index: 0 } }], 'list'));
  assert.deepEqual([...result.state.keys()], [2]);
  sources.next(new ChangeSet([{ reason: 'remove', current: b, currentIndex: 1 }], 'list'));
  assert.deepEqual([...result.state.keys()].sort(), [1, 2]); assert.equal(b.observed, false);
  sources.next(new ChangeSet([{ reason: 'clear', range: { items: [a], index: 0 } }], 'list'));
  assert.equal(result.state.size, 0); assert.equal(a.observed, false); sources.complete(); assert.equal(result.done, true);
});

test('static combine handles synchronous sources and completion', () => {
  const result = collect(combine([of(changes(add(1, 'a'))), of(changes(add(1, 'b')))], 'and'));
  assert.equal(result.state.size, 1); assert.equal(result.state.get(1), 'b'); assert.equal(result.done, true);
});

test('aggregates track cache replacement, in-place refresh, deletion and empty fallback', () => {
  const source = new Subject(), operators = [count(), sum(x => x.v), avg(x => x.v, -1), min(x => x.v, -1), max(x => x.v, -1), stdDev(x => x.v), standardDeviation(x => x.v)];
  const values = [], subs = operators.map((operator, i) => source.pipe(operator).subscribe(v => values[i] = v));
  const a = { v: 2 }, b = { v: 4 }, c = { v: 6 }; source.next(changes(add('a', a), add('b', b), add('c', c)));
  assert.deepEqual(values.slice(0, 5), [3, 12, 4, 2, 6]); assert.equal(values[5], Math.sqrt(8) / 2); assert.equal(values[6], 2);
  b.v = 10; source.next(changes(refresh('b', b))); assert.deepEqual(values.slice(0, 5), [3, 18, 6, 2, 10]);
  source.next(changes(update('a', { v: 5 }, a), remove('b', b))); assert.deepEqual(values.slice(0, 5), [2, 11, 5.5, 5, 6]);
  source.next(changes(remove('a'), remove('c'))); assert.deepEqual(values, [0, 0, -1, -1, -1, 0, 0]); subs.forEach(s => s.unsubscribe());
});

test('list aggregates preserve duplicates, moves and ranges; sumMany tracks child replacement', () => {
  const list = new Subject(); const values = []; list.pipe(sum()).subscribe(v => values.push(v));
  list.next(new ChangeSet([{ reason: 'addRange', range: { items: [2, 2, 5], index: 0 } }], 'list')); assert.equal(values.at(-1), 9);
  list.next(new ChangeSet([{ reason: 'removeRange', range: { items: [2, 2], index: 0 } }], 'list')); assert.equal(values.at(-1), 5);
  const source = new Subject(); let total; source.pipe(sumMany(x => x.children, x => x.v)).subscribe(v => total = v);
  source.next(changes(add(1, { children: [{ v: 2 }, { v: 3 }] }))); assert.equal(total, 5);
  source.next(changes(update(1, { children: [{ v: 7 }] }))); assert.equal(total, 7); source.complete(); list.complete();
});

test('selector failures are observable errors and dispose related sources', () => {
  const source = new Subject(); let error; source.pipe(groupOn(() => { throw new Error('group failure'); })).subscribe({ error: e => error = e });
  source.next(changes(add(1, {}))); assert.match(error.message, /group failure/); assert.equal(source.observed, false);
  const left = new Subject(), right = new Subject(); let joinError;
  left.pipe(innerJoin(right, x => x.owner, () => { throw new Error('join failure'); })).subscribe({ error: e => joinError = e });
  right.next(changes(add(1, { owner: 1 }))); left.next(changes(add(1, {})));
  assert.match(joinError.message, /join failure/); assert.equal(left.observed, false); assert.equal(right.observed, false);
});

test('observable grouping waits for the first selector emission and suppresses identical keys', () => {
  const source = new Subject(), keyStream = new Subject(), result = collect(source.pipe(groupOnObservable(() => keyStream)));
  source.next(changes(add(1, {}))); assert.equal(result.state.size, 0);
  keyStream.next('ready'); assert.equal(result.state.get('ready').size, 1); const group = result.state.get('ready');
  let events = 0; const sub = group.connect().subscribe(() => events++); keyStream.next('ready'); assert.equal(events, 1);
  sub.unsubscribe(); result.sub.unsubscribe(); assert.equal(keyStream.observed, false);
});

test('right join handles two foreign-key swaps atomically without deleting a moved row', () => {
  const left = new Subject(), right = new Subject(), result = collect(left.pipe(rightJoin(right, x => x.owner, (key, l, r) => ({ key, name: l.value.name, r }))));
  left.next(changes(add('A', { name: 'A' }), add('B', { name: 'B' })));
  right.next(changes(add(1, { owner: 'A' }), add(2, { owner: 'B' })));
  right.next(changes(update(1, { owner: 'B' }), update(2, { owner: 'A' })));
  assert.equal(result.state.size, 2); assert.equal(result.state.get(1).name, 'B'); assert.equal(result.state.get(2).name, 'A'); result.sub.unsubscribe();
});

test('property grouping follows observable-object changes and releases observation', async () => {
  const { createObservableObject } = await import('../src/lifecycle.js');
  const { groupOnProperty, groupOnPropertyWithImmutableState } = await import('../src/advanced.js');
  for (const operator of [groupOnProperty, groupOnPropertyWithImmutableState]) {
    const source = new Subject(), item = createObservableObject({ team: 'A' }), result = collect(source.pipe(operator('team')));
    source.next(changes(add(1, item))); item.team = 'B';
    assert.equal(result.state.has('A'), false); assert.equal(result.state.has('B'), true); result.sub.unsubscribe();
  }
});

test('aggregate enumeration preserves update removals, range multiplicity and chaining', async () => {
  const { forAggregation } = await import('../src/advanced.js'); const source = new Subject(); let records, total;
  source.pipe(forAggregation()).subscribe(x => records = x); source.pipe(forAggregation(), sum(x => x.v)).subscribe(x => total = x);
  const a = { v: 2 }, b = { v: 7 }; source.next(changes(add(1, a))); assert.equal(total, 2);
  source.next(changes(update(1, b, a))); assert.deepEqual(records.map(x => [x.type, x.item.v]), [['remove', 2], ['add', 7]]); assert.equal(total, 7);
  source.next(changes(remove(1, b))); assert.equal(total, 0);
  const list = new Subject(); let listTotal; list.pipe(forAggregation(), sum()).subscribe(x => listTotal = x);
  list.next(new ChangeSet([{ reason: 'addRange', range: { items: [2, 2, 5], index: 0 } }], 'list')); assert.equal(listTotal, 9);
  list.next(new ChangeSet([{ reason: 'removeRange', range: { items: [2, 2], index: 0 } }], 'list')); assert.equal(listTotal, 5);
  source.complete(); list.complete();
});

test('invalidation resubscribes to current cache snapshots after mutable values change', async () => {
  const { SourceCache } = await import('../src/core.js'); const { invalidateWhen } = await import('../src/advanced.js');
  const cache = new SourceCache(x => x.id), invalidate = new Subject(), item = { id: 1, v: 2 }; cache.addOrUpdate(item);
  let total; const sub = cache.connect().pipe(sum(x => x.v), invalidateWhen(invalidate)).subscribe(x => total = x);
  assert.equal(total, 2); item.v = 5; invalidate.next(); assert.equal(total, 5); sub.unsubscribe(); cache.dispose();
});

test('specified groups exist while empty, receive later members and dispose only when deselected', async () => {
  const { groupWithSpecifiedGroups } = await import('../src/advanced.js');
  const source = new Subject(), keys = new Subject(), result = collect(source.pipe(groupWithSpecifiedGroups(x => x.team, keys)));
  keys.next(new ChangeSet([{ reason: 'add', current: 'A', currentIndex: 0 }], 'list'));
  const group = result.state.get('A'); assert.equal(group.size, 0); const members = collect(group.connect());
  const item = { team: 'A' }; source.next(changes(add(1, item), add(2, { team: 'B' })));
  assert.equal(result.state.size, 1); assert.equal(group.size, 1); assert.equal(members.state.size, 1);
  source.next(changes(remove(1, item))); assert.equal(group.size, 0); assert.equal(group.isDisposed, false);
  source.next(changes(add(3, item))); assert.equal(group.size, 1);
  keys.next(new ChangeSet([{ reason: 'remove', current: 'A', currentIndex: 0 }], 'list'));
  assert.equal(group.isDisposed, true); assert.equal(members.done, true); assert.equal(result.state.size, 0);
  source.complete(); keys.complete(); assert.equal(result.done, true);
});

test('observable grouping retains active child streams after parent completion', () => {
  const source = new Subject(), keys = new Subject(), result = collect(source.pipe(groupOnObservable(() => keys)));
  source.next(changes(add(1, {}))); source.complete(); assert.equal(result.done, false);
  keys.next('later'); assert.equal(result.state.has('later'), true); keys.complete(); assert.equal(result.done, true);
});

test('keyed dynamic source collections retain source identity across add and remove batches', () => {
  const sources = new Subject(), a = new BehaviorSubject(changes(add(1, 'a'))), b = new BehaviorSubject(changes(add(2, 'b'))), result = collect(combine(sources, 'or'));
  sources.next(changes(add('first', a))); sources.next(changes(add('second', b))); assert.equal(result.state.size, 2);
  sources.next(changes(remove('first', a))); assert.deepEqual([...result.state], [[2, 'b']]); assert.equal(a.observed, false); result.sub.unsubscribe();
});

test('list grouping preserves per-occurrence resources through edits, moves and refreshes', async () => {
  const { subscribeMany, mergeMany } = await import('../src/lifecycle.js'); const { Observable } = await import('rxjs');
  const source = new Subject(), result = collect(source.pipe(groupOn(x => x.team)));
  const signals = new Subject(), shared = { team: 'A', name: 'shared' }, other = { team: 'B', name: 'other' }, newcomer = { team: 'A', name: 'new' };
  source.next(new ChangeSet([{ reason: 'addRange', range: { items: [shared, shared, other], index: 0 } }], 'list'));
  const group = result.state.get('A'), trace = [], disposed = [], mergedDisposed = []; let allocated = 0, mergedAllocated = 0;
  const resources = group.connect().pipe(subscribeMany(() => { const id = ++allocated; return () => disposed.push(id); })).subscribe(c => trace.push(c));
  const mergedValues = []; const merged = group.connect().pipe(mergeMany(() => new Observable(observer => { const id = ++mergedAllocated; const sub = signals.subscribe(observer); return () => { mergedDisposed.push(id); sub.unsubscribe(); }; }))).subscribe(v => mergedValues.push(v));
  assert.equal(allocated, 2); assert.equal(mergedAllocated, 2);
  source.next(new ChangeSet([{ reason: 'add', current: newcomer, currentIndex: 1 }], 'list'));
  assert.equal(allocated, 3); assert.equal(mergedAllocated, 3); assert.deepEqual(disposed, []);
  source.next(new ChangeSet([{ reason: 'move', current: shared, previousIndex: 2, currentIndex: 0 }], 'list'));
  assert.equal(allocated, 3); assert.deepEqual(disposed, []); assert.equal(trace.at(-1)[0].reason, 'move');
  source.next(new ChangeSet([{ reason: 'refresh', current: newcomer, currentIndex: 2 }], 'list'));
  assert.equal(allocated, 3); assert.deepEqual(disposed, []); assert.deepEqual(trace.at(-1).map(c => c.reason), ['refresh']);
  const before = trace.length; source.next(new ChangeSet([{ reason: 'add', current: { team: 'B' }, currentIndex: 4 }], 'list'));
  assert.equal(trace.length, before); assert.equal(allocated, 3);
  // The moved second occurrence now sits at index zero; removing it keeps the first occurrence alive.
  source.next(new ChangeSet([{ reason: 'remove', current: shared, currentIndex: 0 }], 'list'));
  assert.deepEqual(disposed, [2]); assert.deepEqual(mergedDisposed, [2]); assert.equal(allocated, 3);
  signals.next('signal'); assert.deepEqual(mergedValues, ['signal', 'signal']);
  const replacement = { team: 'A', name: 'replacement' };
  source.next(new ChangeSet([{ reason: 'replace', current: replacement, previous: newcomer, currentIndex: 1, previousIndex: 1 }], 'list'));
  assert.equal(allocated, 4); assert.deepEqual(disposed, [2, 3]); assert.deepEqual(trace.at(-1).map(c => c.reason), ['replace']);
  assert.deepEqual(group.items, [shared, replacement]); assert.equal(trace.slice(1).flat().some(c => c.reason === 'clear' || c.reason === 'addRange'), false);
  resources.unsubscribe(); merged.unsubscribe(); result.sub.unsubscribe(); assert.deepEqual(disposed.sort(), [1, 2, 3, 4]);
});

test('list grouping moves refreshed memberships without recreating unaffected group resources', async () => {
  const { subscribeMany } = await import('../src/lifecycle.js');
  const source = new Subject(), result = collect(source.pipe(groupOn(x => x.team))), a = { team: 'A' }, b = { team: 'A' }, c = { team: 'B' };
  source.next(new ChangeSet([{ reason: 'addRange', range: { items: [a, b, c], index: 0 } }], 'list'));
  const counts = { A: 0, B: 0 }, disposed = [];
  const aSub = result.state.get('A').connect().pipe(subscribeMany(item => { counts.A++; return () => disposed.push(item); })).subscribe();
  const bSub = result.state.get('B').connect().pipe(subscribeMany(item => { counts.B++; return () => disposed.push(item); })).subscribe();
  b.team = 'B'; source.next(new ChangeSet([{ reason: 'refresh', current: b, currentIndex: 1 }], 'list'));
  assert.deepEqual(counts, { A: 2, B: 2 }); assert.deepEqual(disposed, [b]); assert.deepEqual(result.state.get('A').items, [a]); assert.deepEqual(result.state.get('B').items, [b, c]);
  aSub.unsubscribe(); bSub.unsubscribe(); result.sub.unsubscribe();
});

test('list group child deltas replay correctly across deterministic mixed indexed edits', () => {
  const source = new Subject(), outer = new Map(), children = new Map(), childSubs = [];
  const sub = source.pipe(groupOn(x => x.team)).subscribe(batch => {
    applyChanges(outer, batch);
    for (const change of batch) if (change.reason === 'add') {
      const values = []; children.set(change.key, values); childSubs.push(change.current.connect().subscribe(delta => applyChanges(values, delta)));
    } else if (change.reason === 'remove') children.delete(change.key);
  });
  const items = [], pool = Array.from({ length: 8 }, (_, id) => ({ id, team: id % 3 })); let random = 991;
  const nextRandom = () => { random = (random * 1664525 + 1013904223) >>> 0; return random; };
  for (let step = 0; step < 240; step++) {
    const op = nextRandom() % 5, index = items.length ? nextRandom() % items.length : 0; let delta;
    if (!items.length || op === 0) { const item = pool[nextRandom() % pool.length]; delta = { reason: 'add', current: item, currentIndex: index }; items.splice(index, 0, item); }
    else if (op === 1) { delta = { reason: 'remove', current: items[index], currentIndex: index }; items.splice(index, 1); }
    else if (op === 2) { const target = nextRandom() % items.length, item = items.splice(index, 1)[0]; items.splice(target, 0, item); delta = { reason: 'move', current: item, previousIndex: index, currentIndex: target }; }
    else if (op === 3) { const item = pool[nextRandom() % pool.length]; delta = { reason: 'replace', previous: items[index], current: item, previousIndex: index, currentIndex: index }; items[index] = item; }
    else delta = { reason: 'refresh', current: items[index], currentIndex: index };
    source.next(new ChangeSet([delta], 'list'));
    for (let team = 0; team < 3; team++) {
      const expected = items.filter(item => item.team === team);
      assert.deepEqual(outer.get(team)?.items || [], expected); assert.deepEqual(children.get(team) || [], expected);
    }
  }
  sub.unsubscribe(); childSubs.forEach(child => child.unsubscribe());
});
