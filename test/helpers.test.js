import test from 'node:test';
import assert from 'node:assert/strict';
import { Subject, VirtualTimeScheduler, defer, of, throwError } from 'rxjs';
import { ChangeSet, SourceCache, SourceList } from '../src/core.js';
import { addOrInsertRange, asAggregator, asWatcher, binarySearch, getChangeType, indexOfOptional, replaceOrAdd, retryWithBackOff, scheduleRecurringAction, yieldWithoutIndex } from '../src/helpers.js';
import { whereReasonsAre } from '../src/extras.js';

test('binarySearch matches negative complement insertion and heterogeneous comparison', () => {
  const values = [1, 3, 5, 8];
  assert.equal(binarySearch(values, 3), 1);
  for (const [value, position] of [[0, 0], [2, 1], [4, 2], [6, 3], [9, 4]]) assert.equal(binarySearch(values, value), ~position);
  assert.equal(binarySearch([], 1), -1);
  assert.equal(binarySearch([{ id: 2 }, { id: 5 }], 5, (id, item) => id - item.id), 1);
  assert.equal(binarySearch(new Uint8Array([1, 3, 7]), 6), ~2);
});

test('list helpers append on negative indices, insert ranges atomically and replace or append', () => {
  const source = new SourceList(), messages = [];
  source.connect().subscribe(changes => messages.push(changes));
  addOrInsertRange(source, [1, 4], -1); addOrInsertRange(source, [2, 3], 1);
  assert.deepEqual(source.items, [1, 2, 3, 4]); assert.equal(messages.length, 2);
  replaceOrAdd(source, 2, 8); replaceOrAdd(source, 99, 9);
  assert.deepEqual(source.items, [1, 8, 3, 4, 9]);
  const array = [2]; addOrInsertRange(array, [1], 0); addOrInsertRange(array, [3, 4], -1);
  replaceOrAdd(array, 2, 9); assert.deepEqual(array, [1, 9, 3, 4]);
  assert.throws(() => addOrInsertRange(array, [0], 100), RangeError);
  source.dispose();
});

test('indexOfOptional returns the sought item with matched index and Pascal properties', () => {
  const sought = { id: 2 }, values = [{ id: 1 }, { id: 2 }];
  const found = indexOfOptional(values, sought, (a, b) => a.id === b.id);
  assert.equal(found.HasValue, true); assert.equal(found.Value.Item, sought); assert.equal(found.Value.Index, 1);
  assert.equal(indexOfOptional(values, sought).hasValue, false);
  assert.equal(indexOfOptional([undefined], undefined).value.index, 0);
});

test('yieldWithoutIndex discards moves and reason filters strip stale list indices', () => {
  const changes = new ChangeSet([{ reason: 'move', current: 2, previousIndex: 0, currentIndex: 1 }, { reason: 'add', current: 3, currentIndex: 5 }, { reason: 'addRange', range: { items: [4, 5], index: 6 } }], 'list');
  const result = [...yieldWithoutIndex(changes)];
  assert.equal(result.length, 2); assert.equal(result[0].CurrentIndex, -1); assert.equal(result[1].Range.index, -1);
  let filtered; of(changes).pipe(whereReasonsAre('add')).subscribe(value => { filtered = value; });
  assert.equal(filtered[0].currentIndex, -1);
  assert.equal(getChangeType('move'), 'item'); assert.equal(getChangeType('clear'), 'range'); assert.throws(() => getChangeType('nonsense'), RangeError);
});

test('aggregator captures cache messages, current data, errors and cumulative statistics once', () => {
  const source = new Subject(); let subscriptions = 0;
  const aggregated = asAggregator(defer(() => { subscriptions++; return source; }));
  source.next(new ChangeSet([{ reason: 'add', key: 1, current: 'a' }], 'cache'));
  source.next(new ChangeSet([{ reason: 'update', key: 1, current: 'b', previous: 'a' }], 'cache'));
  assert.equal(subscriptions, 1); assert.equal(aggregated.Messages.length, 2); assert.equal(aggregated.Data.lookup(1).value, 'b');
  assert.equal(aggregated.Summary.Overall.Adds, 1); assert.equal(aggregated.Summary.Overall.Updates, 1);
  const error = new Error('failure'); source.error(error); assert.equal(aggregated.Error, error); assert.equal(aggregated.IsCompleted, false);
  aggregated.Dispose(); assert.equal(aggregated.isDisposed, true);
});

test('aggregator detects list kind and preserves duplicates/completion', () => {
  const source = new SourceList(), aggregate = asAggregator(source.connect());
  source.addRange(['x', 'x', 'y']); source.removeAt(0);
  assert.deepEqual(aggregate.data.items, ['x', 'y']);
  source.dispose(); assert.equal(aggregate.isCompleted, true); assert.equal(aggregate.summary.overall.adds, 3);
  aggregate.dispose();
});

test('watcher shares source subscription and schedules key changes and initial state', () => {
  const source = new SourceCache(x => x.id), scheduler = new VirtualTimeScheduler(); let subscriptions = 0;
  source.addOrUpdate({ id: 1, value: 'initial' });
  const watcher = asWatcher(defer(() => { subscriptions++; return source.connect(); }), scheduler);
  const first = [], second = [];
  const a = watcher.watch(1).subscribe(change => first.push(change));
  const b = watcher.Watch(1).subscribe(change => second.push(change));
  assert.equal(first.length, 0); scheduler.flush(); assert.equal(first[0].current.value, 'initial');
  source.addOrUpdate({ id: 1, value: 'changed' }); source.removeKey(1); scheduler.flush();
  assert.deepEqual(first.map(c => c.reason), ['add', 'update', 'remove']); assert.deepEqual(second.map(c => c.reason), ['add', 'update', 'remove']);
  assert.equal(subscriptions, 1);
  a.unsubscribe(); b.unsubscribe(); watcher.dispose(); source.dispose();
});

test('retryWithBackOff uses zero-based strategy counts and virtual millisecond scheduling', () => {
  const scheduler = new VirtualTimeScheduler(); let attempts = 0; const counts = [], times = [], values = [];
  defer(() => { attempts++; times.push(scheduler.now()); return attempts < 3 ? throwError(() => new Error('again')) : of('ready'); }).pipe(retryWithBackOff({ scheduler, backOffStrategy(error, failureCount) { counts.push(failureCount); return (failureCount + 1) * 10; } })).subscribe(value => values.push(value));
  assert.equal(attempts, 1); scheduler.flush();
  assert.deepEqual(counts, [0, 1]); assert.deepEqual(times, [0, 10, 30]); assert.deepEqual(values, ['ready']);
});

test('retryWithBackOff stops on null, respects maxRetries and cancels timers', () => {
  const scheduler = new VirtualTimeScheduler(); let attempts = 0, error;
  const source = defer(() => { attempts++; return throwError(() => new Error('failed')); });
  source.pipe(retryWithBackOff({ scheduler, count: 2, initialDelay: 5 })).subscribe({ error(value) { error = value; } });
  scheduler.flush(); assert.equal(attempts, 3); assert.match(error.message, /failed/);
  attempts = 0;
  const cancelled = source.pipe(retryWithBackOff({ scheduler, initialDelay: 50 })).subscribe({ error() {} });
  cancelled.unsubscribe(); scheduler.flush(); assert.equal(attempts, 1);
  source.pipe(retryWithBackOff(() => null)).subscribe({ error(value) { error = value; } });
  assert.equal(attempts, 2); assert.match(error.message, /failed/);
});

test('recurring scheduling reevaluates intervals, delays first call and stops from action', () => {
  const scheduler = new VirtualTimeScheduler(); const times = []; let nextInterval = 10;
  const scheduled = scheduleRecurringAction(scheduler, () => nextInterval, () => {
    times.push(scheduler.now()); nextInterval += 5;
    if (times.length === 3) scheduled.Dispose();
  });
  assert.deepEqual(times, []); scheduler.flush();
  assert.deepEqual(times, [10, 25, 45]); assert.equal(scheduled.closed, true);
  assert.throws(() => scheduleRecurringAction(scheduler, -1, () => {}), RangeError);
});
