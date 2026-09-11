import test from 'node:test';
import assert from 'node:assert/strict';
import { Subject, of } from 'rxjs';
import { Optional } from '../src/core.js';
import { asArray, asList, duplicates, indexOfMany, firstOrOptional, toOptional, createOptional, fromOptional, convertOptional, convertOr, orElse, valueOr, valueOrDefault, valueOrThrow, onHasValue, onHasNoValue, selectValues, lookup, removeIfContained, getValueOrDefault, ifHasValue, OptionElse, ItemWithIndex, ItemWithValue, ErrorInfo, Error as DynamicDataError, SortExpression } from '../src/kernel.js';

test('nullable construction and explicit undefined presence remain distinguishable', () => {
  assert.equal(toOptional(null).hasValue, false); assert.equal(toOptional(undefined).hasValue, false); assert.equal(createOptional(undefined).hasValue, false);
  const present = Optional.some(undefined), absent = Optional.none(); let calls = 0;
  assert.equal(valueOr(present, () => ++calls), undefined); assert.equal(calls, 0);
  assert.equal(valueOr(absent, () => ++calls), 1); assert.equal(calls, 1);
  assert.equal(fromOptional(present), undefined); assert.throws(() => fromOptional(absent), /no value/);
  assert.equal(valueOrThrow(present, () => new Error('absent')), undefined); assert.throws(() => valueOrThrow(absent, () => new Error('absent')), /absent/);
  assert.equal(valueOrDefault(absent), undefined); assert.equal(valueOr(null, 5), 5); assert.equal(valueOr(0, 5), 0);
});

test('optional conversion maps and flat-maps while evaluating only the chosen branch', () => {
  let converted = 0, fallback = 0;
  assert.equal(convertOptional(Optional.some(3), x => { converted++; return x * 2; }).value, 6);
  assert.equal(convertOptional(Optional.none(), x => { converted++; return x; }).hasValue, false); assert.equal(converted, 1);
  const none = Optional.none(); assert.equal(convertOptional(Optional.some(3), () => none), none);
  assert.equal(convertOptional(Optional.some(3), () => undefined).hasValue, true);
  assert.equal(convertOr(Optional.some(undefined), x => x, () => ++fallback), undefined); assert.equal(fallback, 0);
  assert.equal(convertOr(Optional.none(), x => x, () => ++fallback), 1);
  const some = Optional.some(undefined); assert.equal(orElse(some, () => { throw new Error('must be lazy'); }), some);
  assert.equal(orElse(none, () => Optional.some(7)).value, 7); assert.throws(() => orElse(none, () => 7), /Expected Optional/);
});

test('pure side-effect helpers preserve optional identity and support Else continuation', () => {
  const undefinedValue = Optional.some(undefined), none = Optional.none(); const events = [];
  assert.equal(onHasValue(undefinedValue, value => events.push(['some', value]), () => events.push(['none'])), undefinedValue);
  assert.equal(onHasNoValue(none, () => events.push(['none']), value => events.push(['some', value])), none);
  ifHasValue(undefinedValue, value => events.push(['if', value])).Else(() => events.push(['unexpected']));
  ifHasValue(none, () => events.push(['unexpected'])).else(() => events.push(['else']));
  assert.deepEqual(events, [['some', undefined], ['none'], ['if', undefined], ['else']]);
  assert.ok(ifHasValue(none, () => {}) instanceof OptionElse);
});

test('observable optional pipelines preserve present undefined and clean up after error', () => {
  const source = new Subject(), events = [], values = []; let receivedError;
  source.pipe(onHasValue(x => events.push(x), () => events.push('none')), convertOptional(x => x), valueOrThrow(() => new Error('missing'))).subscribe({ next: value => values.push(value), error: error => receivedError = error });
  source.next(Optional.some(undefined)); source.next(Optional.some(4)); source.next(Optional.none());
  assert.deepEqual(values, [undefined, 4]); assert.deepEqual(events, [undefined, 4, 'none']); assert.equal(receivedError.message, 'missing'); assert.equal(source.observed, false);
});

test('all optional observable forms remain cold and support explicit unsubscription', () => {
  const source = new Subject(); let invoked = 0;
  const pipeline = source.pipe(convertOr(x => x * 2, () => { invoked++; return 9; })); assert.equal(source.observed, false);
  const values = [], subscription = pipeline.subscribe(x => values.push(x)); source.next(Optional.none()); source.next(Optional.some(2));
  subscription.unsubscribe(); source.next(Optional.none()); assert.deepEqual(values, [9, 4]); assert.equal(invoked, 1); assert.equal(source.observed, false);
  const defaults = []; valueOrDefault(of(Optional.some(undefined), Optional.none())).subscribe(x => defaults.push(x)); assert.deepEqual(defaults, [undefined, undefined]);
  const extracted = []; fromOptional(of(Optional.some(5))).subscribe(x => extracted.push(x)); assert.deepEqual(extracted, [5]);
  const fallback = []; of(Optional.none(), Optional.some(8)).pipe(orElse(() => Optional.some(3)), valueOr(0)).subscribe(x => fallback.push(x)); assert.deepEqual(fallback, [3, 8]);
});

test('observable side effects propagate exceptions and retain completion semantics', () => {
  const source = new Subject(); let error;
  source.pipe(onHasNoValue(() => { throw new Error('effect failed'); })).subscribe({ error: e => error = e }); source.next(Optional.none());
  assert.equal(error.message, 'effect failed'); assert.equal(source.observed, false);
  let completed = false; const values = [];
  of(Optional.none(), Optional.some(undefined), Optional.some(null), Optional.some(2)).pipe(selectValues()).subscribe({ next: x => values.push(x), complete: () => completed = true });
  assert.deepEqual(values, [undefined, null, 2]); assert.equal(completed, true);
  assert.deepEqual(selectValues([Optional.none(), Optional.some(undefined), Optional.some(1)]), [undefined, 1]);
});

test('toOptional/fromOptional compose as observable operators', () => {
  const values = []; of(null, undefined, 0, false, '').pipe(toOptional(), selectValues()).subscribe(x => values.push(x)); assert.deepEqual(values, [0, false, '']);
  const extracted = []; of(Optional.some(0), Optional.some(undefined)).pipe(fromOptional()).subscribe(x => extracted.push(x)); assert.deepEqual(extracted, [0, undefined]);
});

test('iterable array and list adapters retain arrays and materialize iterable contents', () => {
  const source = [1, 2]; assert.equal(asArray(source), source); assert.equal(asList(source), source);
  assert.deepEqual(asArray(new Set([2, 1])), [2, 1]); assert.deepEqual(asList(new Uint8Array([3, 4])), [3, 4]);
  assert.throws(() => asArray(null), /iterable/);
});

test('duplicates returns every member of each repeated group in upstream grouping order', () => {
  const a1 = { group: 'A', n: 1 }, b1 = { group: 'B', n: 1 }, a2 = { group: 'A', n: 2 }, b2 = { group: 'B', n: 2 };
  assert.deepEqual(duplicates([a1, b1, a2, { group: 'C' }, b2], x => x.group), [a1, a2, b1, b2]);
  assert.deepEqual(duplicates([1, 2, 1, 3, 2]), [1, 1, 2, 2]);
});

test('indexOfMany joins every matching source occurrence in requested order', () => {
  const results = indexOfMany(['a', 'b', 'a'], ['a', 'missing', 'b', 'a']);
  assert.deepEqual(results.map(x => [x.Item, x.Index]), [['a', 0], ['a', 2], ['b', 1], ['a', 0], ['a', 2]]);
  assert.deepEqual(indexOfMany([1, 2, 1], [1], (item, index) => `${item}@${index}`), ['1@0', '1@2']);
});

test('firstOrOptional stops iteration at the first match and preserves present undefined', () => {
  let finalized = false; function* source() { try { yield 1; yield undefined; throw new Error('must not consume after match'); } finally { finalized = true; } }
  const value = firstOrOptional(source(), x => x === undefined); assert.equal(value.hasValue, true); assert.equal(value.value, undefined); assert.equal(finalized, true);
  assert.equal(firstOrOptional([1, 2], x => x > 3).hasValue, false);
});

test('dictionary helpers distinguish absent keys from keys storing undefined and ignore prototypes', () => {
  const source = new Map([['present', undefined]]); assert.equal(lookup(source, 'present').hasValue, true); assert.equal(lookup(source, 'absent').hasValue, false);
  assert.equal(getValueOrDefault(source, 'present', 7), undefined); assert.equal(getValueOrDefault(source, 'absent', 7), 7);
  assert.equal(removeIfContained(source, 'present'), true); assert.equal(removeIfContained(source, 'present'), false);
  const object = Object.create({ inherited: 1 }); object.own = undefined; assert.equal(lookup(object, 'own').hasValue, true); assert.equal(lookup(object, 'inherited').hasValue, false);
  assert.equal(removeIfContained(object, 'inherited'), false); assert.equal(getValueOrDefault(Optional.none(), 8), 8); assert.equal(getValueOrDefault(Optional.some(undefined), 8), undefined);
});

test('kernel wrappers expose Pascal members and upstream equality semantics', () => {
  const item = {}; assert.equal(new ItemWithIndex(item, 1).Equals(new ItemWithIndex(item, 9)), true); assert.equal(new ItemWithIndex(item, 1).Equals(new ItemWithIndex({}, 1)), false);
  const value = new ItemWithValue(item, 3); assert.equal(value.Item, item); assert.equal(value.Value, 3); assert.equal(value.Equals(new ItemWithValue(item, 4)), false);
  const exception = new Error('failure'), info = new ErrorInfo(exception, item, 1); assert.equal(info.Exception, exception); assert.equal(info.Key, 1); assert.equal(info.Equals(new DynamicDataError(exception, item, 1)), true);
  assert.equal(globalThis.Error, Error); assert.equal(Object.isFrozen(value), true);
});

test('sort expressions provide reusable directional comparisons', () => {
  const descending = new SortExpression(x => x.rank, 'Descending'); const input = [{ rank: 1 }, { rank: 3 }, { rank: 2 }];
  assert.deepEqual(input.sort((a, b) => descending.Compare(a, b)).map(x => x.rank), [3, 2, 1]); assert.equal(descending.Direction, 'descending'); assert.equal(descending.Expression(input[0]), 3);
  const dates = new SortExpression(x => x); assert.equal(dates.compare(new Date(0), new Date(0)), 0); assert.equal(dates.compare(NaN, NaN), 0);
});
