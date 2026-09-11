import test from 'node:test';
import assert from 'node:assert/strict';
import { Subject, BehaviorSubject, of } from 'rxjs';
import { ChangeSet, SourceCache, SourceList, Optional, applyChanges } from '../src/core.js';
import { sort } from '../src/operators.js';
import { addKey, adapt, clone, collectUpdateStats, ensureUniqueKeys, excludeUpdateWhen, flattenBufferResult, flattenChanges, forEachChange, forEachItemChange, ignoreSameReferenceUpdate, includeUpdateWhen, invokeEvaluate, itemChanges, mergeChangeSets, mergeManyChangeSets, Node, populateFrom, populateInto, queryWhenChanged, removeIndex, startWithEmpty, startWithItem, suppressRefresh, toObservableOptional, toSortedCollection, transformToTree, treatMovesAsRemoveAdd, updateIndex, whereReasonsAre, whereReasonsAreNot } from '../src/extras.js';

const cacheSet = (...changes) => new ChangeSet(changes, 'cache');
const listSet = (...changes) => new ChangeSet(changes, 'list');
const capture = observable => { const values = []; const subscription = observable.subscribe(value => values.push(value)); return { values, subscription }; };

test('addKey expands ranges, preserves updates, changed keys and refreshes', () => {
  const source = new SourceList();
  const { values, subscription } = capture(source.connect().pipe(addKey(item => item.id)));
  const one = { id: 1, v: 'a' }, two = { id: 2, v: 'b' };
  source.addRange([one, two]);
  assert.deepEqual(values.at(-1).map(c => [c.reason, c.key]), [['add', 1], ['add', 2]]);
  source.replaceAt(0, { id: 1, v: 'updated' });
  assert.equal(values.at(-1)[0].reason, 'update');
  assert.equal(values.at(-1)[0].previous, one);
  source.replaceAt(1, { id: 3, v: 'changed-key' });
  assert.deepEqual(values.at(-1).map(c => [c.reason, c.key]), [['remove', 2], ['add', 3]]);
  source.refreshAt(0);
  assert.equal(values.at(-1)[0].reason, 'refresh');
  source.clear();
  assert.deepEqual(values.at(-1).map(c => c.reason), ['remove', 'remove']);
  subscription.unsubscribe(); source.dispose();
});

test('reason filters suppress empty batches, update predicates receive previous values', () => {
  const changes = cacheSet({ reason: 'add', key: 1, current: 1 }, { reason: 'update', key: 2, current: 3, previous: 2 }, { reason: 'refresh', key: 1, current: 1 });
  assert.deepEqual(capture(of(changes).pipe(whereReasonsAre('add', 'refresh'))).values[0].map(c => c.reason), ['add', 'refresh']);
  assert.deepEqual(capture(of(changes).pipe(whereReasonsAreNot(['add', 'refresh']))).values[0].map(c => c.reason), ['update']);
  assert.equal(capture(of(changes).pipe(whereReasonsAre('remove'))).values.length, 0);
  assert.equal(capture(of(changes).pipe(includeUpdateWhen((current, previous) => current - previous > 1))).values[0].length, 2);
  assert.equal(capture(of(changes).pipe(excludeUpdateWhen((current, previous) => current === previous + 1))).values[0].length, 2);
});

test('per-change hooks keep ranges intact while item hooks expand ranges', () => {
  const range = listSet({ reason: 'addRange', range: { items: ['a', 'b'], index: 2 } });
  const records = [], items = [];
  const result = capture(of(range).pipe(forEachChange(c => records.push(c)), forEachItemChange(c => items.push(c))));
  assert.equal(result.values[0], range);
  assert.equal(records.length, 1);
  assert.deepEqual(items.map(c => [c.current, c.currentIndex]), [['a', 2], ['b', 3]]);
  assert.deepEqual(capture(of(range).pipe(flattenChanges())).values, [...range]);
  const removals = [...itemChanges(listSet({ reason: 'removeRange', range: { items: ['a', 'b'], index: 2 } }))];
  assert.deepEqual(removals.map(c => c.currentIndex), [2, 2]);
});

test('removeIndex strips ranges and individual indices without altering input', () => {
  const input = listSet({ reason: 'addRange', range: { items: [1, 2], index: 5 } }, { reason: 'move', current: 2, currentIndex: 1, previousIndex: 2 });
  const output = capture(of(input).pipe(removeIndex())).values[0];
  assert.equal(output[0].range.index, -1);
  assert.equal(output.length, 1); // Upstream discards moves because an unindexed move has no meaning.
  assert.equal(input[0].range.index, 5);
});

test('ensureUniqueKeys coalesces actual upstream structural semantics with trailing refresh', () => {
  const source = new Subject();
  const values = capture(source.pipe(ensureUniqueKeys())).values;
  source.next(cacheSet({ reason: 'add', key: 'a', current: 1 }, { reason: 'update', key: 'a', current: 2, previous: 1 }, { reason: 'refresh', key: 'a', current: 2 }));
  assert.deepEqual(values[0], cacheSet({ reason: 'add', key: 'a', current: 2 }));
  assert.equal(values[0][0].Reason, 'add');
  source.next(cacheSet({ reason: 'update', key: 'a', current: 3 }, { reason: 'update', key: 'a', current: 4 }));
  assert.deepEqual(values[1], cacheSet({ reason: 'update', key: 'a', current: 4, previous: 2 }));
  assert.equal(values[1][0].Previous.Value, 2);
  source.next(cacheSet({ reason: 'remove', key: 'a', current: 4 }, { reason: 'refresh', key: 'a', current: 4 }));
  assert.equal(values[2][0].reason, 'remove');
  source.next(cacheSet({ reason: 'add', key: 'b', current: 1 }, { reason: 'remove', key: 'b', current: 1 }));
  assert.equal(values[3].length, 0);
  source.complete();
});

test('clone mutates Map, Set and arrays without consuming changes', () => {
  const source = new SourceCache(x => x.id), map = new Map(), set = new Set(), array = [];
  const sub = source.connect().pipe(clone(map), clone(set), clone(array)).subscribe();
  const one = { id: 1 }, two = { id: 1, v: 2 };
  source.addOrUpdate(one); source.addOrUpdate(two);
  assert.deepEqual([...map.values()], [two]); assert.deepEqual([...set], [two]); assert.deepEqual(array, [two]);
  source.removeKey(1);
  assert.equal(map.size, 0); assert.equal(set.size, 0); assert.equal(array.length, 0);
  sub.unsubscribe(); source.dispose();
});

test('populateInto retains source edit batching, clone supports indexed list edits', () => {
  const source = new SourceList(), destination = new SourceList(), array = [];
  const values = capture(destination.connect()).values;
  const subscription = populateInto(source.connect(), destination);
  const side = source.connect().pipe(clone(array)).subscribe();
  source.edit(list => { list.add(1); list.add(2); list.add(3); });
  assert.equal(values.length, 1);
  source.move(2, 0); source.removeAt(1); source.replaceAt(1, 7);
  assert.deepEqual(destination.items, [3, 7]); assert.deepEqual(array, [3, 7]);
  subscription.unsubscribe(); side.unsubscribe(); source.dispose(); destination.dispose();
});

test('adapt supports function and stateful Pascal/camelCase adapter objects', () => {
  const adapter = { count: 0, adapt(changes) { this.count += changes.length; } };
  const input = cacheSet({ reason: 'add', key: 1, current: 1 });
  assert.equal(capture(of(input).pipe(adapt(adapter))).values[0], input);
  assert.equal(adapter.count, 1);
});

test('query snapshots preserve historical membership and optional undefined values', () => {
  const source = new Subject(), values = capture(source.pipe(queryWhenChanged())).values;
  source.next(cacheSet({ reason: 'add', key: 1, current: undefined }));
  source.next(cacheSet({ reason: 'remove', key: 1, current: undefined }));
  assert.equal(values[0].count, 1); assert.equal(values[1].count, 0);
  assert.equal(values[0].lookup(1).hasValue, true); assert.equal(values[0].lookup(1).value, undefined);
  assert.equal(values[0].Lookup(2).HasValue, false);
  assert.deepEqual([...values[0]], [undefined]);
  source.complete();
});

test('toSortedCollection accepts key selector, descending and two-argument comparers', () => {
  const source = new SourceList();
  const asc = capture(source.connect().pipe(toSortedCollection(x => x.n))).values;
  const desc = capture(source.connect().pipe(toSortedCollection(x => x.n, 'descending'))).values;
  const cmp = capture(source.connect().pipe(toSortedCollection((a, b) => b.n - a.n))).values;
  source.addRange([{ n: 3 }, { n: 1 }, { n: 2 }]);
  assert.deepEqual(asc[0].map(x => x.n), [1, 2, 3]);
  assert.deepEqual(desc[0].map(x => x.n), [3, 2, 1]);
  assert.deepEqual(cmp[0].map(x => x.n), [3, 2, 1]);
  source.dispose();
});

test('updateIndex uses the full sorted snapshot including shifted existing objects', () => {
  const source = new SourceCache(x => x.id);
  const sub = source.connect().pipe(sort((a, b) => a.value - b.value), updateIndex()).subscribe();
  const a = { id: 1, value: 3 }, b = { id: 2, value: 1 }, c = { id: 3, value: 2 };
  source.addOrUpdate([a, b, c]);
  assert.deepEqual([a.index, b.index, c.index], [2, 0, 1]);
  source.removeKey(b.id);
  assert.deepEqual([a.index, c.index], [1, 0]);
  sub.unsubscribe(); source.dispose();
});

test('startWithEmpty precedes a populated batch', () => {
  const input = listSet({ reason: 'add', current: 4, currentIndex: 0 });
  const values = capture(of(input).pipe(startWithEmpty('list'))).values;
  assert.equal(values.length, 2); assert.equal(values[0].kind, 'list'); assert.equal(values[0].length, 0); assert.equal(values[1], input);
});

test('collectUpdateStats counts range items and records independently per subscription', () => {
  const source = new SourceList(), stats = source.connect().pipe(collectUpdateStats());
  const first = capture(stats).values;
  source.addRange([1, 2, 3]); source.move(0, 2); source.replaceAt(0, 4); source.refreshAt(0); source.removeRange(0, 2);
  assert.equal(first[0].latest.adds, 3); assert.equal(first[0].latest.count, 1);
  const total = first.at(-1).overall;
  assert.deepEqual([total.adds, total.updates, total.removes, total.moves, total.refreshes], [3, 1, 2, 1, 1]);
  const second = capture(stats).values;
  assert.equal(second[0].overall.adds, 1);
  source.dispose();
});

test('tree links out-of-order parents, exposes children and promotes orphans', () => {
  const source = new SourceCache(x => x.id), roots = new Map();
  const subscription = source.connect().pipe(transformToTree(x => x.parent)).subscribe(changes => applyChanges(roots, changes));
  source.addOrUpdate({ id: 3, parent: 2 });
  const originalChild = roots.get(3);
  assert.equal(originalChild.isRoot, true);
  source.addOrUpdate([{ id: 2, parent: 1 }, { id: 1, parent: 1 }]);
  assert.deepEqual([...roots.keys()], [1]);
  const root = roots.get(1), middle = root.children.lookup(2).value, leaf = middle.children.lookup(3).value;
  assert.equal(leaf, originalChild); assert.equal(leaf.depth, 2); assert.equal(leaf.Parent.Value, middle);
  source.removeKey(2);
  assert.deepEqual([...roots.keys()].sort(), [1, 3]);
  assert.equal(leaf.depth, 0); assert.equal(root.children.size, 0); assert.equal(middle.isDisposed, true);
  subscription.unsubscribe(); assert.equal(root.isDisposed, true); assert.equal(leaf.isDisposed, true); source.dispose();
});

test('tree refresh reparents mutable items; updates replace nodes and transfer children', () => {
  const source = new SourceCache(x => x.id), all = new Map();
  const sub = source.connect().pipe(transformToTree(x => x.parent, () => true)).subscribe(changes => applyChanges(all, changes));
  const child = { id: 3, parent: 1 };
  source.addOrUpdate([{ id: 1, parent: null }, { id: 2, parent: null }, child, { id: 4, parent: 3 }]);
  const oldNode = all.get(3);
  child.parent = 2; source.refreshKey(3);
  assert.equal(all.get(1).children.size, 0); assert.equal(all.get(2).children.lookup(3).value, oldNode);
  source.addOrUpdate({ id: 3, parent: 1 });
  assert.notEqual(all.get(3), oldNode); assert.equal(oldNode.isDisposed, true);
  assert.equal(all.get(3).children.lookup(4).value.parent.value, all.get(3));
  assert.equal(all.get(1).children.lookup(3).value, all.get(3));
  sub.unsubscribe(); source.dispose();
});

test('tree dynamic predicate reveals descendants and refilters without source edits', () => {
  const source = new SourceCache(x => x.id), predicate = new BehaviorSubject(node => node.isRoot), visible = new Map();
  const sub = source.connect().pipe(transformToTree(x => x.parent, predicate)).subscribe(changes => applyChanges(visible, changes));
  source.addOrUpdate([{ id: 1, parent: null }, { id: 2, parent: 1 }]);
  assert.equal(visible.size, 1);
  predicate.next(() => true); assert.equal(visible.size, 2);
  predicate.next(node => node.depth === 1); assert.deepEqual([...visible.keys()], [2]);
  sub.unsubscribe(); source.dispose(); predicate.complete();
});

test('tree rejects cycles and unsubscribes its source rather than hanging', () => {
  const source = new Subject(); let error;
  const subscription = source.pipe(transformToTree(x => x.parent)).subscribe({ error(e) { error = e; } });
  source.next(cacheSet({ reason: 'add', key: 1, current: { id: 1, parent: 2 } }, { reason: 'add', key: 2, current: { id: 2, parent: 1 } }));
  assert.match(error.message, /Circular/); assert.equal(subscription.closed, true); assert.equal(source.observers.length, 0);
});

test('Node optional parent accepts empty values and key equality', () => {
  const a = new Node('a', 1, Optional.none()), b = new Node('b', 1);
  assert.equal(a.parent.hasValue, false); assert.equal(a.equals(b), true); assert.equal(a.depth, 0);
  a.dispose(); b.dispose();
});

test('toObservableOptional detects present undefined, removal, refresh equality and initial absence', () => {
  const source = new Subject(), values = capture(source.pipe(toObservableOptional('a', true))).values;
  assert.equal(values[0].hasValue, false);
  source.next(cacheSet({ reason: 'add', key: 'a', current: undefined }));
  source.next(cacheSet({ reason: 'refresh', key: 'a', current: undefined }));
  source.next(cacheSet({ reason: 'remove', key: 'a', current: undefined }));
  assert.deepEqual(values.map(value => value.hasValue), [false, true, false]);
  assert.equal(values[1].value, undefined); source.complete();
  assert.equal(capture(of(cacheSet()).pipe(toObservableOptional('a', true))).values[0].hasValue, false);
  const populated = capture(of(cacheSet({ reason: 'add', key: 'a', current: 2 })).pipe(toObservableOptional('a', true))).values;
  assert.deepEqual(populated.map(value => value.value), [2]);
});

test('move rewriting preserves application order and sorted snapshots', () => {
  const input = listSet({ reason: 'move', current: 'c', previousIndex: 2, currentIndex: 0 });
  const output = capture(of(input).pipe(treatMovesAsRemoveAdd())).values[0];
  const state = ['a', 'b', 'c']; applyChanges(state, output);
  assert.deepEqual(state, ['c', 'a', 'b']);
  assert.deepEqual(output.map(c => c.reason), ['remove', 'add']);
  input.items = ['c', 'a', 'b']; input.sortedItems = { items: input.items };
  assert.equal(capture(of(input).pipe(treatMovesAsRemoveAdd())).values[0].sortedItems, input.sortedItems);
});

test('buffer flattening concatenates batches and keeps the last authoritative snapshot', () => {
  const first = listSet({ reason: 'add', current: 1, currentIndex: 0 });
  const last = listSet({ reason: 'add', current: 2, currentIndex: 1 }); last.items = [1, 2];
  const values = capture(of([], [first, last]).pipe(flattenBufferResult())).values;
  assert.equal(values.length, 1); assert.equal(values[0].length, 2); assert.deepEqual(values[0].items, [1, 2]);
});

test('evaluate hooks run only for refreshes; reference equality and refresh filters compose', () => {
  const item = { count: 0, evaluate() { this.count++; } };
  const source = of(cacheSet({ reason: 'add', key: 1, current: item }, { reason: 'update', key: 1, current: item, previous: item }, { reason: 'refresh', key: 1, current: item }));
  const values = capture(source.pipe(invokeEvaluate(), ignoreSameReferenceUpdate(), suppressRefresh())).values;
  assert.equal(item.count, 1); assert.equal(values[0].length, 1); assert.equal(values[0][0].reason, 'add');
});

test('populateFrom accepts batches and items; startWithItem extracts object keys', () => {
  const destination = new SourceCache(x => x.id), source = new Subject();
  const subscription = populateFrom(destination, source);
  source.next([{ id: 1 }, { id: 2 }]); source.next({ id: 1, updated: true });
  assert.equal(destination.size, 2); assert.equal(destination.lookup(1).value.updated, true);
  const values = capture(of(cacheSet()).pipe(startWithItem({ key: 'hello' }))).values;
  assert.equal(values[0][0].key, 'hello'); subscription.unsubscribe(); destination.dispose();
});

test('mergeChangeSets prioritizes first key arrival, falls back and retains completed source values', () => {
  const a = new Subject(), b = new Subject(), merged = new Map(); let done = false;
  const records = [];
  const sub = mergeChangeSets([a, b]).subscribe({ next(changes) { records.push(changes); applyChanges(merged, changes); }, complete() { done = true; } });
  b.next(cacheSet({ reason: 'add', key: 1, current: 'b' }));
  a.next(cacheSet({ reason: 'add', key: 1, current: 'a' }));
  assert.equal(merged.get(1), 'b'); assert.equal(records.length, 1);
  b.next(cacheSet({ reason: 'remove', key: 1, current: 'b' }));
  assert.equal(merged.get(1), 'a'); assert.equal(records.at(-1)[0].reason, 'update');
  a.complete(); assert.equal(done, false); assert.equal(merged.get(1), 'a');
  b.complete(); assert.equal(done, true); assert.equal(sub.closed, true);
});

test('mergeChangeSets comparer changes winners on refresh and equality suppresses updates', () => {
  const a = new SourceCache(x => x.id), b = new SourceCache(x => x.id), merged = new Map();
  const values = [];
  const sub = a.connect().pipe(mergeChangeSets(b.connect(), { comparer: (x, y) => x.rank - y.rank, equalityComparer: (x, y) => x.rank === y.rank })).subscribe(changes => { values.push(changes); applyChanges(merged, changes); });
  const x = { id: 1, rank: 3 }, y = { id: 1, rank: 2 };
  a.addOrUpdate(x); b.addOrUpdate(y); assert.equal(merged.get(1), y);
  x.rank = 1; a.refreshKey(1); assert.equal(merged.get(1), x);
  const previousCount = values.length;
  a.addOrUpdate({ id: 1, rank: 1 }); assert.equal(values.length, previousCount);
  sub.unsubscribe(); a.dispose(); b.dispose();
});

test('mergeManyChangeSets removes child ownership and unsubscribes on parent replacement/removal', () => {
  const parents = new SourceCache(x => x.id), a = new SourceCache(x => x.id), b = new SourceCache(x => x.id), merged = new Map();
  a.addOrUpdate({ id: 1, name: 'a' }); b.addOrUpdate([{ id: 1, name: 'b' }, { id: 2, name: 'other' }]);
  let selected = 0;
  const sub = parents.connect().pipe(mergeManyChangeSets(parent => { selected++; return parent.children.connect(); })).subscribe(changes => applyChanges(merged, changes));
  parents.addOrUpdate([{ id: 'p', children: a }, { id: 'q', children: b }]);
  assert.equal(merged.get(1).name, 'a'); assert.equal(selected, 2);
  parents.refreshKey('p'); assert.equal(selected, 2);
  parents.removeKey('p'); assert.equal(merged.get(1).name, 'b');
  a.addOrUpdate({ id: 3, name: 'detached' }); assert.equal(merged.has(3), false);
  parents.removeKey('q'); assert.equal(merged.size, 0);
  sub.unsubscribe(); parents.dispose(); a.dispose(); b.dispose();
});

test('mergeManyChangeSets list children preserve duplicates, offsets and parent removal ranges', () => {
  const parents = new SourceList(), a = new SourceList(), b = new SourceList(), output = [];
  const records = [];
  const sub = parents.connect().pipe(mergeManyChangeSets(parent => parent.connect())).subscribe(changes => { records.push(changes); applyChanges(output, changes); });
  parents.addRange([a, b]); a.addRange(['a', 'a']); b.addRange(['b', 'c']);
  assert.deepEqual(output, ['a', 'a', 'b', 'c']);
  b.move(1, 0); assert.deepEqual(output, ['a', 'a', 'c', 'b']);
  assert.equal(records.at(-1)[0].previousIndex, 3); assert.equal(records.at(-1)[0].currentIndex, 2);
  a.removeAt(0); assert.deepEqual(output, ['a', 'c', 'b']);
  parents.removeAt(0); assert.deepEqual(output, ['c', 'b']);
  b.replaceAt(0, 'd'); assert.deepEqual(output, ['d', 'b']);
  parents.clear(); assert.deepEqual(output, []);
  sub.unsubscribe(); parents.dispose(); a.dispose(); b.dispose();
});

test('merge dynamic streams waits for active children and supports noncompleting output', () => {
  const outer = new Subject(), a = new Subject(); let completed = false;
  const sub = outer.pipe(mergeChangeSets()).subscribe({ complete() { completed = true; } });
  outer.next(a); outer.complete(); assert.equal(completed, false); a.complete(); assert.equal(completed, true);
  assert.equal(sub.closed, true);
  let staticDone = false;
  const live = mergeChangeSets([of(cacheSet())], { completable: false }).subscribe({ complete() { staticDone = true; } });
  assert.equal(staticDone, false); live.unsubscribe();
});

test('merge child errors tear down siblings and collection subscriptions', () => {
  const parents = new Subject(), a = new Subject(), b = new Subject(); let error;
  const sub = parents.pipe(mergeManyChangeSets(value => value)).subscribe({ error(value) { error = value; } });
  parents.next(listSet({ reason: 'addRange', range: { items: [a, b], index: 0 } }));
  a.error(new Error('child failed'));
  assert.match(error.message, /child failed/); assert.equal(sub.closed, true);
  assert.equal(b.observers.length, 0); assert.equal(parents.observers.length, 0);
});
