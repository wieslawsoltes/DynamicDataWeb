/** Optional-value and iterable helpers adapted from DynamicData.Kernel. */
import { isObservable, map, filter, tap } from 'rxjs';
import { Optional } from './core.js';

const identity = value => value;
const requireFunction = (fn, name) => { if (typeof fn !== 'function') throw new TypeError(`${name} must be a function`); return fn; };
const isOptional = value => value instanceof Optional;
const requireOptional = value => { if (!isOptional(value)) throw new TypeError('Expected Optional.some(value) or Optional.none()'); return value; };
const optionalMap = (source, project) => isObservable(source) ? source.pipe(map(value => project(requireOptional(value)))) : project(requireOptional(source));
const fallbackValue = value => typeof value === 'function' ? value() : value;
const equal = (a, b) => a === b || (Number.isNaN(a) && Number.isNaN(b));

export function asArray(source) {
  if (source == null || typeof source[Symbol.iterator] !== 'function') throw new TypeError('Expected an iterable');
  return Array.isArray(source) ? source : Array.from(source);
}
/** JavaScript arrays implement both the list and array contracts. Existing arrays are retained. */
export const asList = asArray;

/** Includes every member of duplicate groups, grouped in first-key-appearance order. */
export function duplicates(source, valueSelector = identity) {
  requireFunction(valueSelector, 'valueSelector');
  const groups = new Map();
  for (const item of asArray(source)) { const key = valueSelector(item); if (!groups.has(key)) groups.set(key, []); groups.get(key).push(item); }
  return [...groups.values()].filter(items => items.length > 1).flat();
}

/** Each requested item joins all equal source occurrences; missing items produce no entry. */
export function indexOfMany(source, itemsToFind, resultSelector = (item, index) => new ItemWithIndex(item, index)) {
  requireFunction(resultSelector, 'resultSelector');
  const index = new Map();
  asArray(source).forEach((item, i) => { if (!index.has(item)) index.set(item, []); index.get(item).push([item, i]); });
  const results = [];
  for (const item of asArray(itemsToFind)) for (const [found, i] of index.get(item) || []) results.push(resultSelector(found, i));
  return results;
}

export function firstOrOptional(source, predicate = () => true) {
  requireFunction(predicate, 'predicate');
  if (source == null || typeof source[Symbol.iterator] !== 'function') throw new TypeError('Expected an iterable');
  for (const item of source) if (predicate(item)) return Optional.some(item);
  return Optional.none();
}

/** Nullable conversion: null and undefined map to None; use Optional.some for explicit presence. */
export function toOptional(source) {
  if (!arguments.length) return input => isObservable(input) ? input.pipe(map(value => Optional.of(value))) : Optional.of(input);
  return isObservable(source) ? source.pipe(map(value => Optional.of(value))) : Optional.of(source);
}
export const createOptional = source => Optional.of(source);
/** Explicit extraction throws on None, just as upstream Optional.FromOptional does. */
export function fromOptional(source) {
  if (!arguments.length) return input => optionalMap(input, value => value.value);
  return optionalMap(source, value => value.value);
}

/** Map or flat-map an Optional. Omit the Optional argument to create an RxJS operator. */
export function convertOptional(sourceOrConverter, maybeConverter) {
  const direct = arguments.length > 1, converter = requireFunction(direct ? maybeConverter : sourceOrConverter, 'converter');
  const project = value => { if (!value.hasValue) return Optional.none(); const result = converter(value.value); return isOptional(result) ? result : Optional.some(result); };
  return direct ? optionalMap(sourceOrConverter, project) : source => optionalMap(source, project);
}

export function convertOr(sourceOrConverter, converterOrFallback, maybeFallback) {
  const direct = arguments.length >= 3;
  const converter = requireFunction(direct ? converterOrFallback : sourceOrConverter, 'converter');
  const fallback = requireFunction(direct ? maybeFallback : converterOrFallback, 'fallbackConverter');
  const project = value => value.hasValue ? converter(value.value) : fallback();
  return direct ? optionalMap(sourceOrConverter, project) : source => optionalMap(source, project);
}

export function orElse(sourceOrFallback, maybeFallback) {
  const direct = arguments.length > 1, fallback = requireFunction(direct ? maybeFallback : sourceOrFallback, 'fallbackOperation');
  const project = value => value.hasValue ? value : requireOptional(fallback());
  return direct ? optionalMap(sourceOrFallback, project) : source => optionalMap(source, project);
}

/** Fallback may be a value or a lazy factory; stored undefined never triggers it. */
export function valueOr(sourceOrFallback, maybeFallback) {
  const direct = arguments.length > 1, fallback = direct ? maybeFallback : sourceOrFallback;
  const project = value => value.hasValue ? value.value : fallbackValue(fallback);
  if (direct && !isOptional(sourceOrFallback) && !isObservable(sourceOrFallback)) return sourceOrFallback ?? fallbackValue(fallback);
  return direct ? optionalMap(sourceOrFallback, project) : source => optionalMap(source, project);
}

/** No-argument form is pipeable; direct Optional form returns undefined when absent. */
export function valueOrDefault(source) {
  const project = value => value.hasValue ? value.value : undefined;
  return arguments.length ? optionalMap(source, project) : input => optionalMap(input, project);
}

export function valueOrThrow(sourceOrException, maybeException) {
  const direct = isOptional(sourceOrException) || isObservable(sourceOrException);
  const exception = requireFunction((direct ? maybeException : sourceOrException) ?? (() => new globalThis.Error('Optional has no value')), 'exceptionGenerator');
  const project = value => { if (value.hasValue) return value.value; throw exception(); };
  return direct ? optionalMap(sourceOrException, project) : source => optionalMap(source, project);
}

function optionalEffect(onPresent, sourceOrAction, actionOrElse, maybeElse) {
  const direct = isOptional(sourceOrAction) || isObservable(sourceOrAction);
  const action = requireFunction(direct ? actionOrElse : sourceOrAction, 'action'), otherwise = direct ? maybeElse : actionOrElse;
  if (otherwise != null) requireFunction(otherwise, 'elseAction');
  const effect = value => {
    requireOptional(value);
    if (onPresent) { if (value.hasValue) action(value.value); else otherwise?.(); }
    else { if (!value.hasValue) action(); else otherwise?.(value.value); }
  };
  const run = source => { if (isObservable(source)) return source.pipe(tap(effect)); effect(source); return source; };
  return direct ? run(sourceOrAction) : run;
}
export function onHasValue(sourceOrAction, actionOrElse, maybeElse) { return optionalEffect(true, sourceOrAction, actionOrElse, maybeElse); }
export function onHasNoValue(sourceOrAction, actionOrElse, maybeElse) { return optionalEffect(false, sourceOrAction, actionOrElse, maybeElse); }

/** None values are omitted; explicit Some(undefined) and Some(null) are retained. */
export function selectValues(source) {
  const run = input => {
    if (isObservable(input)) return input.pipe(filter(value => requireOptional(value).hasValue), map(value => value.value));
    const values = []; for (const optional of asArray(input)) if (requireOptional(optional).hasValue) values.push(optional.value); return values;
  };
  return arguments.length ? run(source) : run;
}

export function lookup(source, key) {
  if (source instanceof Map) return source.has(key) ? Optional.some(source.get(key)) : Optional.none();
  if (typeof source?.lookup === 'function') return source.lookup(key);
  if (source && typeof source === 'object') return Object.hasOwn(source, key) ? Optional.some(source[key]) : Optional.none();
  throw new TypeError('lookup requires a Map, cache or object');
}
export function removeIfContained(source, key) {
  if (source instanceof Map) return source.delete(key);
  if (source && typeof source === 'object') { if (!Object.hasOwn(source, key)) return false; return Reflect.deleteProperty(source, key); }
  throw new TypeError('removeIfContained requires a Map or object');
}
export function getValueOrDefault(source, key, fallback) {
  if (isOptional(source)) return source.hasValue ? source.value : key;
  const value = lookup(source, key); return value.hasValue ? value.value : fallback;
}

export class OptionElse {
  constructor(shouldRunAction = true) { this.shouldRunAction = shouldRunAction; Object.freeze(this); }
  else(action) { requireFunction(action, 'action'); if (this.shouldRunAction) action(); }
  Else(action) { return this.else(action); }
}
export function ifHasValue(source, action) {
  requireFunction(action, 'action');
  if (source == null) return new OptionElse();
  requireOptional(source); if (!source.hasValue) return new OptionElse();
  action(source.value); return new OptionElse(false);
}

export class ItemWithIndex {
  constructor(item, index) { if (!Number.isInteger(index)) throw new TypeError('index must be an integer'); this.item = item; this.index = index; Object.freeze(this); }
  get Item() { return this.item; } get Index() { return this.index; }
  // Upstream equality deliberately compares the item, not its index.
  equals(other) { return other instanceof ItemWithIndex && equal(this.item, other.item); }
  Equals(other) { return this.equals(other); }
  toString() { return `${String(this.item)} (${this.index})`; } ToString() { return this.toString(); }
}
export class ItemWithValue {
  constructor(item, value) { this.item = item; this.value = value; Object.freeze(this); }
  get Item() { return this.item; } get Value() { return this.value; }
  equals(other) { return other instanceof ItemWithValue && equal(this.item, other.item) && equal(this.value, other.value); }
  Equals(other) { return this.equals(other); }
  toString() { return `${String(this.item)} (${String(this.value)})`; } ToString() { return this.toString(); }
}
export class ErrorInfo {
  constructor(exception, value, key) { this.exception = exception; this.value = value; this.key = key; Object.freeze(this); }
  get Exception() { return this.exception; } get Value() { return this.value; } get Key() { return this.key; }
  equals(other) { return other instanceof ErrorInfo && equal(this.exception, other.exception) && equal(this.value, other.value) && equal(this.key, other.key); }
  Equals(other) { return this.equals(other); }
  toString() { return `Key: ${String(this.key)}, Value: ${String(this.value)}, Exception: ${String(this.exception)}`; } ToString() { return this.toString(); }
}
// Namespace import users can use DynamicData.Error without replacing JavaScript's global Error.
export { ErrorInfo as Error };
export class SortExpression {
  constructor(expression, direction = 'ascending') {
    this.expression = requireFunction(expression, 'expression'); this.selector = expression;
    if (direction === 0) direction = 'ascending'; else if (direction === 1) direction = 'descending';
    direction = String(direction).toLowerCase(); if (direction !== 'ascending' && direction !== 'descending') throw new TypeError('direction must be ascending or descending');
    this.direction = direction; Object.freeze(this);
  }
  get Expression() { return this.expression; } get Direction() { return this.direction; }
  compare(left, right) { const a = this.expression(left), b = this.expression(right), comparison = equal(a, b) ? 0 : a == null ? -1 : b == null ? 1 : Number.isNaN(a) ? -1 : Number.isNaN(b) ? 1 : a < b ? -1 : a > b ? 1 : 0; return this.direction === 'descending' ? -comparison : comparison; }
  Compare(left, right) { return this.compare(left, right); }
}
