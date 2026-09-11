import test from 'node:test';
import assert from 'node:assert/strict';
import { of } from 'rxjs';
import { SourceCache, SourceList, ObservableCache, ObservableList, ChangeAwareCache, ChangeAwareList, IntermediateCache, Change, ListChange, ChangeSet, Optional, ChangeReason, ListChangeReason, applyChanges, snapshotChanges } from '../src/core.js';

test('cache editing, Optional, Pascal aliases, counts and incremental changes', () => {
  const source = new SourceCache(item => item.id), batches = [], counts = [], state = new Map();
  source.CountChanged.subscribe(value => counts.push(value));
  source.Connect().subscribe(changes => { batches.push(changes); applyChanges(state, changes); });
  source.Edit(updater => { updater.AddOrUpdate({ id: 1, name: 'one' }); updater.AddOrUpdate([{ id: 2 }, { id: 3 }]); });
  assert.equal(batches.length, 1); assert.equal(batches[0].Adds, 3); assert.equal(batches[0].items, undefined);
  source.AddOrUpdate({ id: 1, name: 'updated' });
  assert.equal(batches[1][0].Previous.Value.name, 'one'); assert.equal(batches[1][0].previous.name, 'one');
  source.RefreshKey(2); source.RemoveKeys([2, 3]);
  assert.deepEqual(counts, [0, 3, 1]); assert.equal(source.Count, 1);
  assert.equal(source.Lookup(1).Value.name, 'updated'); assert.equal(source.Lookup(9).HasValue, false);
  assert.deepEqual([...state], [...source.KeyValues]); assert.equal(source.KeySelector({ id: 4 }), 4);
  assert.equal(Optional.some(undefined).HasValue, true); assert.throws(() => Optional.none().Value);
  assert.equal(ChangeReason.Add, 'add'); assert.equal(ListChangeReason.AddRange, 'addRange');
});

test('a failed transaction rolls back data, emits nothing, and remains usable', () => {
  const cache = new SourceCache(x => x.id), list = new SourceList();
  cache.addOrUpdate({ id: 1 }); list.addRange(['a', 'b', 'c']);
  let cacheBatches = 0, listBatches = 0;
  cache.connect().subscribe(() => cacheBatches++); list.connect().subscribe(() => listBatches++);
  assert.throws(() => cache.edit(c => { c.removeKey(1); c.addOrUpdate({ id: 2 }); throw Error('abort'); }), /abort/);
  assert.deepEqual(cache.keys, [1]); assert.equal(cacheBatches, 1);
  assert.throws(() => list.edit(l => { l.move(2, 0); l.removeRange(1, 2); l.add('x'); throw Error('abort'); }), /abort/);
  assert.deepEqual(list.items, ['a', 'b', 'c']); assert.equal(listBatches, 1);
  cache.addOrUpdate({ id: 3 }); list.add('d'); assert.equal(cacheBatches, 2); assert.equal(listBatches, 2);
});

test('nested edits form one batch and preview observes the pre-edit state', () => {
  const source = new SourceCache(x => x.id), previews = [], batches = [];
  source.addOrUpdate({ id: 1 });
  source.preview().subscribe(changes => previews.push({ keys: source.keys, adds: changes.adds, removes: changes.removes }));
  source.connect().subscribe(changes => batches.push(changes));
  source.edit(s => { s.removeKey(1); s.edit(nested => nested.addOrUpdate({ id: 2 })); });
  assert.deepEqual(previews, [{ keys: [1], adds: 1, removes: 1 }]); assert.equal(batches.length, 2);
  assert.deepEqual(source.keys, [2]);
});

test('reentrant writes are delivered to all observers in the same order', () => {
  const source = new SourceCache(x => x.id), first = [], second = [];
  source.connect().subscribe(changes => { first.push(changes[0].key); if (changes[0].key < 2500) source.addOrUpdate({ id: changes[0].key + 1 }); });
  source.connect().subscribe(changes => second.push(changes[0].key));
  source.addOrUpdate({ id: 1 });
  assert.equal(source.count, 2500); assert.deepEqual(first, second);
  assert.deepEqual(second.slice(0, 4), [1, 2, 3, 4]);
});

test('connect does not lose changes produced by its initial snapshot subscriber', () => {
  const source = new SourceCache(x => x.id); source.addOrUpdate({ id: 1 });
  const values = [];
  source.connect().subscribe(changes => { values.push(changes.map(c => c.key)); if (values.length === 1) source.addOrUpdate({ id: 2 }); });
  assert.deepEqual(values, [[1], [2]]);
});

test('notification suspension coalesces changes and new subscribers do not replay prior edits', () => {
  const source = new SourceCache(x => x.id), oldBatches = [], newBatches = [], counts = [];
  source.connect().subscribe(changes => oldBatches.push(changes.map(c => c.key)));
  source.countChanged.subscribe(value => counts.push(value));
  const outer = source.suspendNotifications(), inner = source.suspendNotifications();
  source.addOrUpdate({ id: 1 });
  source.connect().subscribe(changes => newBatches.push(changes.map(c => c.key)));
  source.addOrUpdate({ id: 2 }); inner.dispose();
  assert.deepEqual(oldBatches, []); assert.deepEqual(newBatches, [[1]]);
  outer.Dispose(); outer.Dispose();
  assert.deepEqual(oldBatches, [[1, 2]]); assert.deepEqual(newBatches, [[1], [2]]); assert.deepEqual(counts, [0, 2]);
});

test('cache predicates track membership changes on replacement and refresh; watch retains exact key', () => {
  const source = new SourceCache(x => x.id), filtered = new Map(), watched = [], events = [];
  source.connect(x => x.enabled).subscribe(changes => { applyChanges(filtered, changes); events.push(changes.map(c => c.reason)); });
  source.watch(1).subscribe(change => watched.push(change.reason));
  const one = { id: 1, enabled: false }, two = { id: 2, enabled: true };
  source.addOrUpdate([one, two]); one.enabled = true; source.refresh(one);
  source.addOrUpdate({ id: 2, enabled: false }); source.removeKey(1);
  assert.deepEqual(events, [['add'], ['add'], ['remove'], ['remove']]);
  assert.deepEqual(watched, ['add', 'refresh', 'remove']); assert.equal(filtered.size, 0);
});

test('list every mutation replays accurately, with duplicates and both insertRange overloads', () => {
  const source = new SourceList(), mirror = [], snapshots = [];
  source.connect().subscribe(changes => { applyChanges(mirror, changes); snapshots.push(mirror.slice()); });
  source.addRange(['a', 'b', 'a']); source.insert(1, 'c'); source.insertRange(['d', 'e'], 2);
  source.insertRange(0, ['f']); source.move(6, 0); source.replaceAt(2, 'x'); source.replace('e', 'z');
  source.remove('a'); source.removeMany(['d']); source.removeRange(0, 2); source.refresh();
  assert.deepEqual(mirror, source.items);
  source.editDiff(['z', 'new', 'z']); assert.deepEqual(mirror, ['z', 'new', 'z']);
  assert.deepEqual(source.items, mirror); source.clear(); assert.deepEqual(mirror, []);
  assert.throws(() => source.removeAt(0), RangeError); assert.throws(() => source.insert(-1, 'x'), RangeError);
});

test('list predicate deltas preserve order through moves, ranges, replacements and refresh', () => {
  const a = { name: 'a', yes: true }, b = { name: 'b', yes: false }, c = { name: 'c', yes: true };
  const source = new SourceList(), mirror = [];
  source.connect(x => x.yes).subscribe(changes => applyChanges(mirror, changes));
  const check = () => assert.deepEqual(mirror, source.items.filter(x => x.yes));
  source.addRange([a, b, c, a]); check(); source.move(3, 1); check(); source.move(0, 3); check();
  b.yes = true; source.refresh(b); check(); source.replaceAt(0, { name: 'off', yes: false }); check();
  source.removeRange(0, 2); check(); source.insertRange([b, c], 1); check(); source.clear(); check();
});

test('readonly observable views retain completed snapshots and own their subscriptions', () => {
  const cache = new ObservableCache(of(snapshotChanges([{ id: 1 }]))), list = new ObservableList(of(snapshotChanges(['a', 'b'], 'list')));
  let cacheInitial, listInitial, completed = 0;
  cache.connect().subscribe({ next: changes => { cacheInitial = changes; }, complete: () => completed++ });
  list.connect().subscribe({ next: changes => { listInitial = changes; }, complete: () => completed++ });
  assert.equal(cacheInitial.adds, 1); assert.equal(listInitial.adds, 2); assert.equal(completed, 2);
  assert.equal(cache.edit, undefined); assert.equal(list.add, undefined);
  const source = new SourceCache(x => x.id), view = source.asObservableCache(); source.addOrUpdate({ id: 2 });
  assert.equal(view.count, 1); view.dispose(); source.addOrUpdate({ id: 3 }); assert.equal(view.count, 1); assert.equal(source.count, 2);
});

test('dispose completes subscribers and rejects mutations', () => {
  const source = new SourceCache(x => x.id); let completed = 0;
  source.connect().subscribe({ complete: () => completed++ }); source.countChanged.subscribe({ complete: () => completed++ });
  source.dispose(); source.dispose(); assert.equal(completed, 2); assert.throws(() => source.addOrUpdate({ id: 1 }), /disposed/);
});

test('range imports and ChangeSet statistics support 100000 items without argument overflows', () => {
  const values = Array.from({ length: 100000 }, (_, i) => i), source = new SourceList(), mirror = [];
  let additions;
  source.connect().subscribe(changes => { additions = changes.adds; applyChanges(mirror, changes); });
  source.addRange(values); assert.equal(additions, values.length); assert.deepEqual(mirror, values);
  const changes = new ChangeSet([new Change('add', 1, 'a'), new ListChange('addRange', ['b', 'c'], 1), new ListChange('removeRange', ['d'], 0), new ListChange('refresh', 'b', 0)]);
  assert.equal(changes.TotalChanges, 5); assert.equal(changes.Removes, 1); assert.equal(changes.map(c => c.reason) instanceof ChangeSet, false);
});

test('change-aware collections capture and clear deltas; intermediate cache accepts explicit keys', () => {
  const cache = new ChangeAwareCache(new Map([['initial', { name: 'initial' }]]));
  assert.equal(cache.CaptureChanges().length, 0);
  cache.Add({ name: 'one' }, 'one'); cache.AddOrUpdate({ name: 'updated' }, 'one'); cache.RemoveKey('initial');
  const changes = cache.CaptureChanges(); assert.equal(changes.Adds, 1); assert.equal(changes.Updates, 1); assert.equal(changes.Removes, 1);
  assert.equal(cache.CaptureChanges().length, 0); assert.equal(cache.Count, 1); assert.throws(() => cache.Add({}, 'one'), /already exists/);
  const list = new ChangeAwareList(['a', 'b']); assert.equal(list.CaptureChanges().Adds, 2);
  list.Move(1, 0); list.Add('c'); const copy = new ChangeAwareList(list, true);
  assert.deepEqual(copy.Items, ['b', 'a', 'c']); assert.equal(copy.CaptureChanges().Moves, 1); assert.equal(list.CaptureChanges().Adds, 1);
  const intermediate = new IntermediateCache(); intermediate.Edit(c => c.AddOrUpdate({ name: 'explicit' }, 'key'));
  assert.equal(intermediate.Lookup('key').Value.name, 'explicit');
});

test('constructed and appended operator records expose Pascal fields and typed list ranges', () => {
  const cache = new ChangeSet([{ reason: 'add', key: 'a', current: 1, custom: true }]);
  cache.push({ reason: 'update', key: 'a', current: 2, previous: 1 });
  assert.equal(cache[0].Current, 1); assert.equal(cache[0].custom, true); assert.equal(cache[1].Previous.Value, 1);
  const list = new ChangeSet([{ reason: 'addRange', range: { items: ['a', 'b'], index: 0 } }], 'list');
  assert.equal(list[0].Range.Count, 2); assert.equal(list[0].Range.Index, 0);
  assert.equal(list.keys, undefined);
});
