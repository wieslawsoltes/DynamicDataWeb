import test from 'node:test';
import assert from 'node:assert/strict';
import { BehaviorSubject, Subject, Observable } from 'rxjs';
import { SourceCache, SourceList, ChangeSet, applyChanges } from '../src/core.js';
import {
  filter, filterImmutable, filterWithState, transform, transformImmutable, transformSafe, transformWithInlineUpdate, transformMany,
  distinctValues, sort, sortBy, page, virtualise, sortAndPage, sortAndVirtualize, sortAndBind, top, reverse, toCollection, bind,
  changeKey, removeKey, asObservableCache, asObservableList, ofType
} from '../src/operators.js';

function record(source) {
  const messages = [], values = [];
  const subscription = source.subscribe({ next: changes => { messages.push(changes); if (changes.kind === 'cache') applyChanges(values, changes); else applyChanges(values, new ChangeSet([...changes], 'list')); }, error: error => { throw error; } });
  return { messages, values, subscription };
}
function collection(source) { let values = []; const subscription = source.pipe(toCollection()).subscribe(x => { values = x; }); return { get values() { return values; }, subscription }; }

test('cache filter handles every membership transition, refresh and suppressed empty batches', () => {
  const source = new SourceCache(x => x.id), changes = [];
  const sub = source.connect().pipe(filter(x => x.n > 5)).subscribe(x => changes.push(x));
  source.addOrUpdate({ id: 1, n: 2 }); assert.equal(changes.length, 0);
  source.addOrUpdate({ id: 1, n: 7 }); assert.equal(changes.at(-1)[0].reason, 'add');
  source.addOrUpdate({ id: 1, n: 8 }); assert.equal(changes.at(-1)[0].reason, 'update');
  const item = source.lookup(1).value; source.refreshKey(1); assert.equal(changes.at(-1)[0].reason, 'refresh');
  item.n = 3; source.refreshKey(1); assert.equal(changes.at(-1)[0].reason, 'remove');
  item.n = 9; source.refreshKey(1); assert.equal(changes.at(-1)[0].reason, 'add');
  source.removeKey(1); assert.equal(changes.at(-1)[0].reason, 'remove');
  assert.equal(changes.at(-1)[0].Reason, 'remove'); sub.unsubscribe();
});

test('dynamic predicates, state predicates and reapply preserve removals and release subscriptions', () => {
  const source = new SourceCache(x => x.id), predicates = new Subject(), reapply = new Subject();
  const out = collection(source.connect().pipe(filter(predicates, reapply)));
  const one = { id: 1, n: 1 }, two = { id: 2, n: 2 }; source.addOrUpdate([one, two]);
  assert.deepEqual(out.values, []); predicates.next(x => x.n > 1); assert.deepEqual(out.values, [two]);
  one.n = 4; reapply.next(); assert.deepEqual(out.values, [two, one]);
  predicates.next(() => false); assert.deepEqual(out.values, []);
  out.subscription.unsubscribe(); assert.equal(predicates.observers.length, 0); assert.equal(reapply.observers.length, 0);
  const state = new BehaviorSubject(2), second = collection(source.connect().pipe(filterWithState(state, (min, x) => x.n >= min)));
  assert.deepEqual(second.values, [one, two]); state.next(3); assert.deepEqual(second.values, [one]); second.subscription.unsubscribe();
});

test('explicit empty cache filter batches are emitted', () => {
  const source = new SourceCache(x => x), messages = [];
  source.connect(undefined, false).pipe(filter(() => false, false)).subscribe(x => messages.push(x));
  source.addOrUpdate(1); assert.equal(messages.length, 2); assert.ok(messages.every(x => x.length === 0));
});

test('immutable filter and transform drop refresh changes', () => {
  const source = new SourceCache(x => x.id), filtered = [], projected = [];
  source.connect().pipe(filterImmutable(x => x.n > 0)).subscribe(x => filtered.push(x));
  source.connect().pipe(transformImmutable(x => x.n)).subscribe(x => projected.push(x));
  const item = { id: 1, n: 1 }; source.addOrUpdate(item); item.n = -1; source.refreshKey(1);
  assert.equal(filtered.length, 1); assert.equal(projected.length, 1);
  source.addOrUpdate({ id: 1, n: 2 }); assert.equal(filtered.length, 2); assert.equal(projected.at(-1)[0].current, 2);
});

test('list filtering preserves duplicate occurrences, indexed moves, ranges and replacement', () => {
  const source = new SourceList(), out = record(source.connect().pipe(filter(x => x % 2 === 0)));
  source.addRange([2, 1, 2, 4]); assert.deepEqual(out.values, [2, 2, 4]);
  source.move(3, 0); assert.deepEqual(out.values, [4, 2, 2]);
  source.replaceAt(2, 6); assert.deepEqual(out.values, [4, 2, 6, 2]);
  source.removeRange(1, 2); assert.deepEqual(out.values, [4, 2]);
  source.clear(); assert.deepEqual(out.values, []); out.subscription.unsubscribe();
});

test('transforms are incremental and preserve unchanged projection identities', () => {
  const source = new SourceCache(x => x.id), calls = [], messages = [];
  const factory = (x, key, previous) => { calls.push([key, previous]); return { id: x.id, text: String(x.n) }; };
  const projected = source.connect().pipe(transform(factory));
  const out = collection(projected); projected.subscribe(x => messages.push(x));
  source.addOrUpdate([{ id: 1, n: 1 }, { id: 2, n: 2 }]); const first = out.values[0];
  calls.length = 0; source.addOrUpdate({ id: 2, n: 3 }); assert.equal(calls.length, 2); assert.equal(out.values[0], first);
  calls.length = 0; source.refreshKey(1); assert.equal(calls.length, 0); assert.equal(messages.at(-1)[0].reason, 'refresh');
  source.removeKey(2); assert.equal(calls.length, 0); assert.deepEqual(out.values, [first]);
});

test('list transform receives indices and retains separate duplicate projections across moves', () => {
  const source = new SourceList(), out = record(source.connect().pipe(transform((x, index) => ({ x, index }))));
  source.addRange([1, 1, 2]); const [a, b, c] = out.values; assert.notEqual(a, b);
  assert.deepEqual(out.values.map(x => x.index), [0, 1, 2]);
  source.move(0, 2); assert.deepEqual(out.values, [b, c, a]);
  source.replaceAt(0, 9); assert.deepEqual(out.values, [{ x: 9, index: 0 }, c, a]);
});

test('transformOnRefresh and selective forced transformations re-evaluate affected entries', () => {
  const source = new SourceCache(x => x.id), force = new Subject(), item = { id: 1, n: 1 }; let calls = 0;
  const out = collection(source.connect().pipe(transform(x => ({ n: x.n, call: ++calls }), { transformOnRefresh: true, forceTransform: force })));
  source.addOrUpdate(item); item.n = 4; source.refreshKey(1); assert.equal(out.values[0].n, 4); assert.equal(calls, 2);
  force.next(x => x.id === 2); assert.equal(calls, 2); force.next(); assert.equal(calls, 3);
  out.subscription.unsubscribe(); assert.equal(force.observers.length, 0);
});

test('safe transform isolates factory failures and inline updates retain references', () => {
  const source = new SourceCache(x => x.id), errors = [];
  const safe = collection(source.connect().pipe(transformSafe(x => { if (x.n < 0) throw new Error('negative'); return x.n; }, e => errors.push(e))));
  source.addOrUpdate([{ id: 1, n: 2 }, { id: 2, n: -1 }]); assert.deepEqual(safe.values, [2]); assert.equal(errors.length, 1);
  source.addOrUpdate({ id: 2, n: 3 }); assert.deepEqual(safe.values, [2, 3]);
  const inline = collection(source.connect().pipe(transformWithInlineUpdate(x => ({ n: x.n }), (vm, x) => { vm.n = x.n; })));
  const first = inline.values[0]; source.addOrUpdate({ id: 1, n: 8 }); assert.equal(inline.values[0], first); assert.equal(first.n, 8);
});

test('transformMany flattens child collections and follows parent changes', () => {
  const source = new SourceCache(x => x.id), out = collection(source.connect().pipe(transformMany(x => x.children, x => x.id)));
  source.addOrUpdate([{ id: 'a', children: [{ id: 1, n: 1 }, { id: 2, n: 2 }] }, { id: 'b', children: [{ id: 3, n: 3 }] }]);
  assert.deepEqual(out.values.map(x => x.id), [1, 2, 3]);
  source.addOrUpdate({ id: 'a', children: [{ id: 2, n: 4 }] }); assert.deepEqual(out.values.map(x => x.n), [4, 3]);
  source.removeKey('b'); assert.deepEqual(out.values.map(x => x.id), [2]);
});

test('transformMany observes child list changes and unsubscribes removed parents', () => {
  const source = new SourceCache(x => x.id), child = new SourceList(), child2 = new SourceList();
  const out = collection(source.connect().pipe(transformMany(x => x.children)));
  child.addRange([1, 1]); source.addOrUpdate({ id: 1, children: child }); assert.deepEqual(out.values, [1, 1]);
  child.add(2); assert.deepEqual(out.values, [1, 1, 2]);
  child2.add(8); source.addOrUpdate({ id: 1, children: child2 }); assert.deepEqual(out.values, [8]);
  child.add(3); assert.deepEqual(out.values, [8]); source.removeKey(1); child2.add(9); assert.deepEqual(out.values, []);
  out.subscription.unsubscribe();
});

test('transformMany releases observable children on completion and unsubscribe', () => {
  let active = 0;
  const children = new Observable(observer => { active++; observer.next([1, 2]); return () => active--; });
  const source = new SourceCache(x => x.id), out = collection(source.connect().pipe(transformMany(x => x.children)));
  source.addOrUpdate({ id: 1, children }); assert.equal(active, 1); out.subscription.unsubscribe(); assert.equal(active, 0);
});

test('transformMany forwards nested refresh signals to downstream filters', () => {
  const source = new SourceCache(x => x.id), children = new SourceCache(x => x.id), child = { id: 1, n: 1 };
  const out = collection(source.connect().pipe(transformMany(x => x.children, x => x.id), filter(x => x.n > 2)));
  children.addOrUpdate(child); source.addOrUpdate({ id: 1, children }); assert.deepEqual(out.values, []);
  child.n = 4; children.refreshKey(1); assert.deepEqual(out.values, [child]);
  child.n = 0; children.refreshKey(1); assert.deepEqual(out.values, []); out.subscription.unsubscribe();
});

test('distinct values reference counts survive removals, updates and refreshes', () => {
  const source = new SourceCache(x => x.id), out = collection(source.connect().pipe(distinctValues(x => x.group)));
  const a = { id: 1, group: 'a' }, b = { id: 2, group: 'a' }; source.addOrUpdate([a, b, { id: 3, group: 'b' }]);
  assert.deepEqual(out.values, ['a', 'b']); source.removeKey(1); assert.deepEqual(out.values, ['a', 'b']);
  b.group = 'b'; source.refreshKey(2); assert.deepEqual(out.values, ['b']); source.clear(); assert.deepEqual(out.values, []);
});

test('cache sort is stable and emitted deltas apply without relying on snapshots', () => {
  const source = new SourceCache(x => x.id), comparer = new BehaviorSubject((a, b) => a.n - b.n), actual = [], snapshots = [];
  source.connect().pipe(sort(comparer)).subscribe(changes => { applyChanges(actual, new ChangeSet([...changes], 'cache')); snapshots.push(changes); assert.deepEqual(actual, changes.items); });
  const a = { id: 1, n: 2 }, b = { id: 2, n: 2 }, c = { id: 3, n: 1 }; source.addOrUpdate([a, b, c]); assert.deepEqual(actual, [c, a, b]);
  source.addOrUpdate({ id: 1, n: 4 }); assert.deepEqual(actual.map(x => x.id), [3, 2, 1]);
  b.n = 5; source.refreshKey(2); assert.deepEqual(actual.map(x => x.id), [3, 1, 2]);
  comparer.next((a, b) => b.n - a.n); assert.deepEqual(actual.map(x => x.id), [2, 1, 3]);
  source.removeKey(1); assert.deepEqual(actual.map(x => x.id), [2, 3]); assert.equal(snapshots.at(-1).SortedItems.Count, 2);
});

test('large comparer changes reset order in linear output deltas', () => {
  const source = new SourceCache(x => x), comparer = new BehaviorSubject((a, b) => a - b), actual = [];
  source.connect().pipe(sort(comparer)).subscribe(changes => { applyChanges(actual, new ChangeSet([...changes], 'cache')); assert.deepEqual(actual, changes.items); });
  source.addOrUpdate(Array.from({ length: 200 }, (_, i) => i)); comparer.next((a, b) => b - a);
  assert.equal(actual[0], 199); assert.equal(actual.at(-1), 0);
});

test('page requests are one-based, clamp after removal and emit response-only count changes', () => {
  const source = new SourceCache(x => x), requests = new BehaviorSubject({ page: 2, size: 2 }), messages = [];
  source.connect().pipe(sort(), page(requests)).subscribe(x => messages.push(x));
  source.addOrUpdate([1, 2, 3, 4, 5]); assert.deepEqual(messages.at(-1).items, [3, 4]); assert.equal(messages.at(-1).response.pages, 3);
  source.addOrUpdate(6); assert.equal(messages.at(-1).length, 0); assert.equal(messages.at(-1).response.totalSize, 6);
  requests.next({ page: 99, size: 2 }); assert.deepEqual(messages.at(-1).items, [5, 6]); assert.equal(messages.at(-1).response.page, 3);
  source.removeKeys([3, 4, 5, 6]); assert.deepEqual(messages.at(-1).items, [1, 2]); assert.equal(messages.at(-1).response.page, 1);
});

test('combined sorting and windows wait for the first request even when it equals the default', () => {
  const source = new SourceCache(x => x), virtualRequests = new Subject(), pageRequests = new Subject();
  const virtualMessages = [], pageMessages = [];
  source.connect().pipe(sortAndVirtualize((a, b) => a - b, virtualRequests)).subscribe(x => virtualMessages.push(x));
  source.connect().pipe(sortAndPage((a, b) => a - b, pageRequests)).subscribe(x => pageMessages.push(x));
  source.addOrUpdate([3, 1, 2]); assert.equal(virtualMessages.length, 0); assert.equal(pageMessages.length, 0);
  virtualRequests.next({ startIndex: 0, size: 25 }); pageRequests.next({ page: 1, size: 25 });
  assert.deepEqual(virtualMessages.at(-1).items, [1, 2, 3]); assert.deepEqual(pageMessages.at(-1).items, [1, 2, 3]);
  source.addOrUpdate(0); assert.deepEqual(virtualMessages.at(-1).items, [0, 1, 2, 3]); assert.deepEqual(pageMessages.at(-1).items, [0, 1, 2, 3]);
});

test('combined window contexts track comparer, request and options after unseeded comparer streams', () => {
  const source = new SourceCache(x => x), comparers = new Subject(), requests = new Subject(), messages = [];
  const options = { resetThreshold: 12 };
  source.connect().pipe(sortAndVirtualize(comparers, requests, options)).subscribe(x => messages.push(x));
  source.addOrUpdate([3, 1, 2]); requests.next({ startIndex: 0, size: 25 }); assert.equal(messages.length, 0);
  const ascending = (a, b) => a - b; comparers.next(ascending); assert.deepEqual(messages.at(-1).items, [1, 2, 3]);
  assert.equal(messages.at(-1).context.comparer, ascending); assert.equal(messages.at(-1).Context.Options, options);
  assert.deepEqual(messages.at(-1).context.request, { startIndex: 0, size: 25 });
  const sameOrder = (a, b) => (a - b) * 2; comparers.next(sameOrder); assert.equal(messages.at(-1).context.comparer, sameOrder);
  requests.next({ startIndex: 1, size: 1 }); assert.deepEqual(messages.at(-1).items, [2]); assert.equal(messages.at(-1).context.response.totalSize, 3);
});

test('top supports comparer overload and sortAndBind accepts either typed argument order', () => {
  const source = new SourceCache(x => x), first = [], second = [], third = [], comparers = new BehaviorSubject((a, b) => b - a);
  source.connect().pipe(sortAndBind(first, (a, b) => a - b)).subscribe();
  source.connect().pipe(sortAndBind((a, b) => b - a, second)).subscribe();
  source.connect().pipe(sortAndBind(comparers, third)).subscribe();
  const out = collection(source.connect().pipe(top((a, b) => b - a, 2)));
  source.addOrUpdate([2, 1, 3]); assert.deepEqual(first, [1, 2, 3]); assert.deepEqual(second, [3, 2, 1]); assert.deepEqual(third, [3, 2, 1]); assert.deepEqual(out.values, [3, 2]);
});

test('100k sorted virtual windows backfill correctly after updates and request changes', () => {
  const source = new SourceCache(x => x.id), requests = new BehaviorSubject({ startIndex: 49990, size: 25 }); let result;
  source.connect().pipe(sortAndVirtualize((a, b) => a.n - b.n, requests)).subscribe(x => { result = x; });
  source.addOrUpdate(Array.from({ length: 100000 }, (_, id) => ({ id, n: id })));
  assert.equal(result.items[0].id, 49990); assert.equal(result.items.at(-1).id, 50014);
  source.addOrUpdate({ id: 1, n: 50000.5 }); assert.equal(result.items[0].id, 49991); assert.equal(result.items[10].id, 1);
  source.removeKey(50000); assert.equal(result.items[9].id, 1); assert.equal(result.items.length, 25);
  requests.next({ startIndex: 99990, size: 25 }); assert.equal(result.items.length, 9); assert.equal(result.response.totalSize, 99999); assert.equal(result.items.at(-1).id, 99999);
});

test('virtual windows, top, reverse and bind maintain expected ordered snapshots', () => {
  const source = new SourceList(), requests = new BehaviorSubject({ startIndex: 1, size: 2 }), target = [];
  const sub = source.connect().pipe(reverse(), virtualise(requests), bind(target)).subscribe();
  source.addRange([1, 2, 3, 4]); assert.deepEqual(target, [3, 2]); requests.next({ startIndex: 2, size: 2 }); assert.deepEqual(target, [2, 1]);
  const out = collection(source.connect().pipe(top(2))); assert.deepEqual(out.values, [1, 2]); sub.unsubscribe(); out.subscription.unsubscribe();
});

test('changeKey reconciles collisions and removeKey preserves cache order as a list', () => {
  const source = new SourceCache(x => x.id), out = collection(source.connect().pipe(changeKey(x => x.name), removeKey()));
  source.addOrUpdate([{ id: 1, name: 'x', value: 1 }, { id: 2, name: 'x', value: 2 }]); assert.deepEqual(out.values.map(x => x.value), [2]);
  source.removeKey(2); assert.deepEqual(out.values.map(x => x.value), [1]);
  source.addOrUpdate({ id: 1, name: 'y', value: 3 }); assert.deepEqual(out.values.map(x => x.name), ['y']);
});

test('observable wrappers and runtime type filtering work with pipeable inputs', () => {
  const source = new SourceCache(x => x.id), cache = asObservableCache(source.connect(), x => x.id);
  source.addOrUpdate({ id: 1, n: 3 }); assert.equal(cache.lookup(1).value.n, 3); cache.dispose();
  const list = new SourceList(), view = asObservableList(list.connect()), out = collection(list.connect().pipe(ofType(Number), sortBy(x => x, 'descending')));
  list.addRange([2, 'a', 4]); assert.equal(view.count, 3); assert.deepEqual(out.values, [4, 2]); view.dispose();
});

test('cache filter and transform visit only changed entries after a large initial load', () => {
  const source = new Subject(); let predicates = 0, projections = 0, last;
  source.pipe(filter(x => { predicates++; return x.n >= 0; }), transform(x => { projections++; return x.n; })).subscribe(x => { last = x; });
  source.next(new ChangeSet(Array.from({ length: 100000 }, (_, key) => ({ reason: 'add', key, current: { n: key } })), 'cache'));
  assert.equal(predicates, 100000); assert.equal(projections, 100000); assert.equal(last.items, undefined);
  predicates = projections = 0;
  for (let key = 0; key < 100; key++) source.next(new ChangeSet([{ reason: 'update', key, current: { n: key + 1 } }], 'cache'));
  assert.equal(predicates, 100); assert.equal(projections, 100);
});

test('large list ranges and array binding avoid argument-count limits', () => {
  const source = new SourceList(), target = [];
  const sub = source.connect().pipe(transform(x => x + 1), bind(target)).subscribe();
  source.addRange(Array.from({ length: 150000 }, (_, i) => i));
  assert.equal(target.length, 150000); assert.equal(target.at(-1), 150000); sub.unsubscribe();
});

test('random list edits keep filter, transform and sort deltas equivalent to direct evaluation', () => {
  let seed = 213;
  const random = n => { seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0; return seed % n; };
  const source = new SourceList(), actual = [];
  source.connect().pipe(filter(x => x % 2 === 0), transform(x => x * 3), sort((a, b) => a - b)).subscribe(changes => { applyChanges(actual, new ChangeSet([...changes], 'list')); assert.deepEqual(actual, changes.items); });
  for (let i = 0; i < 400; i++) {
    const action = random(5);
    if (!source.count || action === 0) source.insert(random(source.count + 1), random(30));
    else if (action === 1) source.removeAt(random(source.count));
    else if (action === 2) source.replaceAt(random(source.count), random(30));
    else if (action === 3) source.move(random(source.count), random(source.count));
    else source.addRange([random(30), random(30)]);
    assert.deepEqual(actual, source.items.filter(x => x % 2 === 0).map(x => x * 3).sort((a, b) => a - b));
  }
});
