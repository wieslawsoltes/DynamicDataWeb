import test from 'node:test';
import assert from 'node:assert/strict';
import { Observable, Subject, firstValueFrom } from 'rxjs';
import { ChangeSet, SourceCache, SourceList } from '../src/core.js';
import { autoRefresh, whenPropertyChanged, whenAnyPropertyChanged, observeProperty, filterOnProperty, createObservableObject, notifyPropertyChanged, subscribeMany, disposeMany, asyncDisposeMany, ObservableChangeSet } from '../src/lifecycle.js';
import { bind, sortAndBind } from '../src/operators.js';

// The public ReactiveWeb ReactiveObject protocol, without importing it or creating a cycle.
class ReactiveFixture {
  constructor(properties = {}, alias = 'all') {
    Object.assign(this, properties);
    this.events = new Subject(); this.active = 0; this.connections = 0;
    const stream = new Observable(observer => {
      this.active++; this.connections++;
      const inner = this.events.subscribe(observer);
      return () => { this.active--; inner.unsubscribe(); };
    });
    if (alias === 'all') this.PropertyChanged = this.Changed = this.changed = stream;
    else this[alias] = stream;
  }
  SetValue(PropertyName, Value) {
    const OldValue = this[PropertyName]; this[PropertyName] = Value;
    this.events.next({ Sender: this, PropertyName, Value, OldValue });
  }
  Dispose() { this.events.complete(); }
}

test('native ReactiveObject aliases share one connection and normalize PascalCase events', () => {
  const item = new ReactiveFixture({ Name: 'Ada' });
  const first = [], second = [];
  const a = whenPropertyChanged(item, 'Name').subscribe(event => first.push(event));
  const b = observeProperty(item, 'Name').subscribe(value => second.push(value));
  assert.equal(item.active, 1); assert.equal(item.connections, 1);
  item.SetValue('Name', 'Grace');
  assert.deepEqual(first.map(event => [event.sender, event.propertyName, event.value, event.previous]), [[item, 'Name', 'Ada', undefined], [item, 'Name', 'Grace', 'Ada']]);
  assert.deepEqual(second, ['Ada', 'Grace']);
  a.unsubscribe(); assert.equal(item.active, 1);
  b.unsubscribe(); assert.equal(item.active, 0);
  const c = observeProperty(item, 'Name').subscribe(); assert.equal(item.active, 1);
  item.Dispose(); assert.equal(c.closed, true); assert.equal(item.active, 0);
});

test('Changed-only, camelCase and all-property notifications integrate without proxy wrapping', () => {
  for (const alias of ['Changed', 'PropertyChanged', 'changed', 'propertyChanged']) {
    const item = new ReactiveFixture({ Count: 1 }, alias), values = [], any = [];
    assert.equal(createObservableObject(item), item);
    const a = observeProperty(item, 'Count').subscribe(value => values.push(value));
    const b = whenAnyPropertyChanged(item, 'Count').subscribe(value => any.push(value));
    item.Count = 2; item.events.next({ sender: item, propertyName: 'Count', value: 2, previous: 1 });
    for (const PropertyName of ['', null, undefined]) { item.Count++; item.events.next({ Sender: item, PropertyName }); }
    item.events.next({ PropertyName: 'Unrelated' });
    assert.deepEqual(values, [1, 2, 3, 4, 5]); assert.equal(any.length, 4);
    a.unsubscribe(); b.unsubscribe(); assert.equal(item.active, 0);
  }
});

test('plain-object explicit all-property notifications and Symbol properties remain observable', () => {
  const token = Symbol('state'), item = { value: 1, [token]: 0 }, values = [], symbols = [];
  const a = observeProperty(item, 'value').subscribe(value => values.push(value));
  const b = observeProperty(item, token).subscribe(value => symbols.push(value));
  item.value = 2; notifyPropertyChanged(item, '');
  item[token] = 3; notifyPropertyChanged(item, token, 0);
  assert.deepEqual(values, [1, 2]); assert.deepEqual(symbols, [0, 3]);
  a.unsubscribe(); b.unsubscribe();
});

test('native nested paths replace and detach children while preserving parent subscriptions', () => {
  const first = new ReactiveFixture({ City: 'Paris' }), second = new ReactiveFixture({ City: 'Oslo' });
  const item = new ReactiveFixture({ Address: first }), seen = [];
  const subscription = whenPropertyChanged(item, 'Address.City').subscribe(event => seen.push([event.value, event.previous]));
  first.SetValue('City', 'London');
  assert.equal(item.connections, 1); assert.equal(first.connections, 1);
  item.SetValue('Address', second);
  assert.equal(first.active, 0); assert.equal(second.active, 1); assert.equal(item.connections, 1);
  first.SetValue('City', 'Stale'); second.SetValue('City', 'Rome');
  item.SetValue('Address', null); assert.equal(second.active, 0);
  item.SetValue('Address', first); first.City = 'Warsaw'; first.events.next({ PropertyName: '' });
  assert.deepEqual(seen, [['Paris', undefined], ['London', 'Paris'], ['Oslo', 'London'], ['Rome', 'Oslo'], [undefined, 'Rome'], ['Stale', undefined], ['Warsaw', 'Stale']]);
  item.Dispose(); assert.equal(subscription.closed, true); assert.equal(first.active, 0); assert.equal(item.active, 0);
});

test('nested paths through the same object subscribe once and emit once per native event', () => {
  const item = new ReactiveFixture({ Name: 'A' }); item.Self = item;
  const values = [], subscription = observeProperty(item, 'Self.Name').subscribe(value => values.push(value));
  item.SetValue('Name', 'B');
  assert.deepEqual(values, ['A', 'B']); assert.equal(item.active, 1); assert.equal(item.connections, 1);
  subscription.unsubscribe(); assert.equal(item.active, 0);
});

test('AutoRefresh retains duplicate occurrences, removes listeners and replaces keyed objects', () => {
  const item = new ReactiveFixture({ Id: 1, Name: 'A' }), list = new SourceList(), batches = [];
  const subscription = list.connect().pipe(autoRefresh('Name')).subscribe(changes => batches.push(changes));
  list.addRange([item, item]); assert.equal(item.active, 1);
  item.SetValue('Name', 'B');
  assert.deepEqual(batches.flatMap(batch => batch.filter(change => change.reason === 'refresh').map(change => change.currentIndex)), [0, 1]);
  list.removeAt(0); assert.equal(item.active, 1);
  const before = batches.length; item.SetValue('Name', 'C'); assert.equal(batches.length, before + 1);
  list.clear(); assert.equal(item.active, 0);
  const removed = batches.length; item.SetValue('Name', 'D'); assert.equal(batches.length, removed);
  subscription.unsubscribe(); list.dispose();
  const cache = new SourceCache(value => value.Id), replacement = new ReactiveFixture({ Id: 1, Name: 'New' });
  const cached = cache.connect().pipe(autoRefresh()).subscribe();
  cache.addOrUpdate(item); cache.addOrUpdate(replacement);
  assert.equal(item.active, 0); assert.equal(replacement.active, 1);
  cached.unsubscribe(); assert.equal(replacement.active, 0); cache.dispose();
});

test('FilterOnProperty follows native nested properties and shared children across items', () => {
  const shared = new ReactiveFixture({ Enabled: false }), other = new ReactiveFixture({ Enabled: true });
  const first = new ReactiveFixture({ Id: 1, Options: shared }), second = new ReactiveFixture({ Id: 2, Options: shared });
  const cache = new SourceCache(item => item.Id), rows = [];
  const subscription = cache.connect().pipe(filterOnProperty('Options.Enabled', item => item.Options.Enabled), bind(rows)).subscribe();
  cache.addOrUpdate([first, second]); assert.deepEqual(rows, []); assert.equal(shared.active, 1);
  shared.SetValue('Enabled', true); assert.deepEqual(rows, [first, second]);
  first.SetValue('Options', other); shared.SetValue('Enabled', false); assert.deepEqual(rows, [first]);
  cache.removeKey(2); assert.equal(shared.active, 0); assert.equal(second.active, 0);
  subscription.unsubscribe(); assert.equal(first.active, 0); assert.equal(other.active, 0); cache.dispose();
});

test('native errors and property getter errors reach observers and detach subscriptions', () => {
  const leaf = new ReactiveFixture({ Value: 1 }), item = new ReactiveFixture({ Child: leaf }), errors = [];
  const subscription = whenPropertyChanged(item, 'Child.Value').subscribe({ error: error => errors.push(error) });
  const error = new Error('native source failed'); leaf.events.error(error);
  assert.deepEqual(errors, [error]); assert.equal(subscription.closed, true); assert.equal(item.active, 0); assert.equal(leaf.active, 0);
  const failing = new ReactiveFixture({ Name: 'first' }), failures = [];
  const simple = observeProperty(failing, 'Name').subscribe({ error: error => failures.push(error) });
  Object.defineProperty(failing, 'Name', { get() { throw error; } });
  failing.events.next({ PropertyName: 'Name' });
  assert.equal(simple.closed, true); assert.deepEqual(failures, [error]); assert.equal(failing.active, 0);
});

test('Bind delta contract preserves batches, ranges, refreshes and sorting metadata', () => {
  for (const method of ['ApplyChanges', 'applyChanges']) {
    const source = new Subject(), received = [], forwarded = [];
    const target = { [method](changes) { assert.equal(this, target); received.push(changes); }, edit() { throw new Error('delta target must take priority'); } };
    const subscription = source.pipe(bind(target)).subscribe(changes => forwarded.push(changes));
    const batch = new ChangeSet([{ reason: 'addRange', range: { items: ['a', 'a'], index: 0 } }, { reason: 'refresh', current: 'a', currentIndex: 1 }], 'list');
    batch.items = ['a', 'a']; batch.keys = [1, 2]; source.next(batch);
    assert.equal(received[0], batch); assert.equal(forwarded[0], batch);
    assert.equal(received[0].kind, 'list'); assert.equal(received[0].items, batch.items); subscription.unsubscribe();
  }
  const source = new SourceCache(item => item.Id), batches = [];
  const sorted = source.connect().pipe(sortAndBind((a, b) => a.Order - b.Order, { ApplyChanges(changes) { batches.push(changes); } })).subscribe();
  source.addOrUpdate([{ Id: 'b', Order: 2 }, { Id: 'a', Order: 1 }]);
  assert.deepEqual(batches.at(-1).keys, ['a', 'b']); sorted.unsubscribe(); source.dispose();
});

test('Bind accepts PascalCase collection edits, preserves this and forwards target errors', () => {
  const source = new SourceList(), rows = [];
  const target = { Edit(action) { assert.equal(this, target); action(this); }, Clear() { assert.equal(this, target); rows.length = 0; }, AddRange(items) { assert.equal(this, target); rows.push(...items); } };
  const subscription = source.connect().pipe(bind(target)).subscribe();
  source.addRange([1, 2]); source.move(0, 1); assert.deepEqual(rows, [2, 1]);
  source.removeAt(0); assert.deepEqual(rows, [1]); subscription.unsubscribe(); source.dispose();
  const events = new Subject(), expected = new Error('target failed'), errors = [];
  const broken = events.pipe(bind({ ApplyChanges() { throw expected; } })).subscribe({ error: error => errors.push(error) });
  events.next(new ChangeSet([], 'list'));
  assert.deepEqual(errors, [expected]); assert.equal(events.observed, false); assert.equal(broken.closed, true);
});

test('SubscribeMany, DisposeMany and ObservableChangeSet accept Dispose-only resources', () => {
  const source = new SourceList(), disposed = [], own = [];
  const first = { Dispose() { disposed.push('first'); } }, second = { Dispose() { disposed.push('second'); } };
  const subscription = source.connect().pipe(subscribeMany(item => ({ Dispose() { own.push(item); } })), disposeMany()).subscribe();
  source.addRange([first, second]); source.remove(first);
  assert.deepEqual(disposed, ['first']); assert.deepEqual(own, [first]);
  subscription.unsubscribe(); subscription.unsubscribe();
  assert.deepEqual(disposed, ['first', 'second']); assert.deepEqual(own, [first, second]); source.dispose();
  let cleaned = 0;
  const created = ObservableChangeSet.createList(list => { list.add(1); return { Dispose() { cleaned++; } }; }).subscribe();
  created.unsubscribe(); assert.equal(cleaned, 1);
});

test('AsyncDisposeMany waits for PascalCase DisposeAsync completion', async () => {
  const source = new SourceList(); let resolve, completed, calls = 0;
  const resource = { DisposeAsync() { calls++; return new Promise(done => { resolve = done; }); } };
  const subscription = source.connect().pipe(asyncDisposeMany(completion => { completed = firstValueFrom(completion); })).subscribe();
  source.add(resource); subscription.unsubscribe();
  assert.equal(calls, 1); let finished = false; completed.then(() => { finished = true; });
  await Promise.resolve(); assert.equal(finished, false); resolve(); await completed; assert.equal(finished, true); source.dispose();
});

test('helpers and kernel share ItemWithIndex identity for optional lookups', async () => {
  const helpers = await import('../src/helpers.js'), kernel = await import('../src/kernel.js');
  assert.equal(helpers.ItemWithIndex, kernel.ItemWithIndex);
  const item = {}, found = helpers.indexOfOptional([item], item).value;
  assert.equal(found instanceof kernel.ItemWithIndex, true);
  assert.equal(found.Equals(new kernel.ItemWithIndex(item, 99)), true);
});

test('synchronous native errors stop attaching later items in the same change batch', () => {
  const expected = new Error('already failed'), failed = new ReactiveFixture(), live = new ReactiveFixture(), source = new SourceList(), errors = [];
  failed.events.error(expected);
  const subscription = source.connect().pipe(autoRefresh()).subscribe({ error: error => errors.push(error) });
  source.addRange([failed, live]);
  assert.equal(subscription.closed, true); assert.deepEqual(errors, [expected]);
  assert.equal(failed.active, 0); assert.equal(live.active, 0); assert.equal(live.connections, 0);
  source.dispose();
});

test('SubscribeMany releases every resource even if one Dispose method throws', () => {
  const source = new SourceList(), disposed = [], expected = new Error('cleanup failed');
  const subscription = source.connect().pipe(subscribeMany(item => ({ Dispose() { disposed.push(item); if (item === 1) throw expected; } }))).subscribe();
  source.addRange([1, 2, 3]);
  assert.throws(() => subscription.unsubscribe(), /cleanup failed/);
  assert.deepEqual(disposed, [1, 2, 3]);
  source.add(4); assert.deepEqual(disposed, [1, 2, 3]); source.dispose();
});

test('DisposeMany attempts every item cleanup when a Dispose method throws', () => {
  const source = new SourceList(), disposed = [];
  const subscription = source.connect().pipe(disposeMany()).subscribe();
  source.addRange([1, 2, 3].map(id => ({ Dispose() { disposed.push(id); if (id !== 2) throw new Error(`cleanup ${id}`); } })));
  assert.throws(() => subscription.unsubscribe(), /Items failed to dispose/);
  assert.deepEqual(disposed, [1, 2, 3]); source.dispose();
});
