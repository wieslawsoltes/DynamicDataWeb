/** Per-item observable lifetimes, property notifications and time-aware change-set operators. */
import { Observable, Subject, ReplaySubject, Subscription, asyncScheduler, from, of, isObservable, filter as rxFilter, map, distinctUntilChanged, debounceTime, bufferTime, skip, finalize, combineLatest, fromEvent } from 'rxjs';
import { ChangeSet, SourceCache, SourceList, applyChanges, snapshotChanges } from './core.js';
import { filter, transformMany } from './operators.js';

const cs = (changes, kind = 'cache') => new ChangeSet(changes, kind);
const release = resource => { if (typeof resource === 'function') resource(); else if (resource?.unsubscribe) resource.unsubscribe(); else if (resource?.dispose) resource.dispose(); else if (resource?.[Symbol.dispose]) resource[Symbol.dispose](); };
const getChanges = changes => changes?.changes ?? changes;

/** Tracks list occurrences independently, so duplicate references retain distinct lifetimes. */
function tracker(hooks = {}) {
  const state = { kind: 'cache', entries: [], byKey: new Map() };
  const add = (item, key, index = state.entries.length, previous, prior) => {
    if (prior) prior.prior = undefined;
    const entry = { item, key, prior, active: true, subscription: new Subscription() };
    if (state.kind === 'cache') { const old = state.byKey.get(key); if (old) { index = state.entries.indexOf(old); remove(old); } state.byKey.set(key, entry); }
    state.entries.splice(Math.max(0, index), 0, entry); hooks.add?.(entry, previous); return entry;
  };
  const remove = entry => { if (!entry) return; entry.active = false; const index = state.entries.indexOf(entry); if (index >= 0) state.entries.splice(index, 1); if (state.kind === 'cache' && state.byKey.get(entry.key) === entry) state.byKey.delete(entry.key); entry.subscription.unsubscribe(); hooks.remove?.(entry, index); };
  const at = change => state.kind === 'cache' ? state.byKey.get(change.key) : state.entries[change.currentIndex >= 0 ? change.currentIndex : change.previousIndex >= 0 ? change.previousIndex : state.entries.findIndex(entry => entry.item === change.current)];
  state.apply = changes => {
    state.kind = changes.kind ?? state.kind;
    for (const change of getChanges(changes)) {
      const reason = change.reason;
      if (reason === 'add') add(change.current, change.key, change.currentIndex >= 0 ? change.currentIndex : state.entries.length);
      else if (reason === 'addRange') { const range = change.range ?? { items: change.current ?? [], index: change.currentIndex }; let index = range.index >= 0 ? range.index : state.entries.length; for (const item of range.items) add(item, undefined, index++); }
      else if (reason === 'update' || reason === 'replace') { const old = at(change); const index = old ? state.entries.indexOf(old) : change.currentIndex; const previous = old?.item ?? change.previous; remove(old); add(change.current, change.key, index >= 0 ? index : state.entries.length, previous, old); }
      else if (reason === 'remove') remove(at(change));
      else if (reason === 'removeRange') { const range = change.range ?? { items: change.current ?? [], index: change.currentIndex }; if (range.index >= 0) { for (let i = 0; i < range.items.length; i++) remove(state.entries[range.index]); } else for (const item of range.items) remove(state.entries.find(entry => entry.item === item)); }
      else if (reason === 'clear') { for (const entry of [...state.entries]) remove(entry); }
      else if (reason === 'refresh') { const entry = at(change); if (entry) hooks.refresh?.(entry); }
      else if (reason === 'moved' || reason === 'move') { const entry = state.entries.splice(change.previousIndex, 1)[0]; if (entry) { state.entries.splice(change.currentIndex, 0, entry); hooks.move?.(entry); } }
    }
    if (state.kind === 'cache' && Array.isArray(changes.keys)) { const ordered = changes.keys.map(key => state.byKey.get(key)).filter(Boolean); const included = new Set(ordered); state.entries = [...ordered, ...state.entries.filter(entry => !included.has(entry))]; }
  };
  state.erase = remove;
  state.dispose = () => { for (const entry of [...state.entries]) remove(entry); };
  return state;
}

const notifications = new WeakMap();
const proxyTargets = new WeakMap();
const objectProxies = new WeakMap();
function propertyEvents(item) { const target = proxyTargets.get(item) ?? item; if (target == null || (typeof target !== 'object' && typeof target !== 'function')) throw new TypeError('Property notifications require an object'); let subject = notifications.get(target); if (!subject) notifications.set(target, subject = new Subject()); return subject; }
function readProperty(item, property) { if (typeof property === 'function') return property(item); if (property == null) return item; return String(property).split('.').reduce((value, part) => value?.[part], item); }
export function notifyPropertyChanged(item, propertyName, previous) { propertyEvents(item).next({ sender: item, propertyName, value: readProperty(item, propertyName), previous }); }
/** Shallow property notification Proxy. Keep and mutate the returned object. */
export function createObservableObject(item) {
  if (proxyTargets.has(item)) return item;
  if (objectProxies.has(item)) return objectProxies.get(item);
  const proxy = new Proxy(item, {
    set(target, property, value) { const previous = target[property]; const result = Reflect.set(target, property, value); if (result && !Object.is(previous, value)) notifyPropertyChanged(proxy, property, previous); return result; },
    deleteProperty(target, property) { const existed = Reflect.has(target, property); const previous = target[property]; const result = Reflect.deleteProperty(target, property); if (result && existed) notifyPropertyChanged(proxy, property, previous); return result; }
  });
  proxyTargets.set(proxy, item); objectProxies.set(item, proxy); return proxy;
}
export const observableObject = createObservableObject;
function changedProperty(item, property, notifyInitial = true) {
  if (typeof property === 'string' && property.includes('.')) {
    const parts = property.split('.');
    return new Observable(observer => {
      let connections = new Subscription(), currentValue = readProperty(item, property);
      const rewire = () => {
        connections.unsubscribe(); connections = new Subscription();
        let current = item;
        for (let depth = 0; depth < parts.length; depth++) {
          if (current == null || typeof current !== 'object' && typeof current !== 'function') break;
          const part = parts[depth], remaining = parts.slice(depth).join('.');
          connections.add(propertyEvents(current).subscribe({
            next(event) {
              if (event.propertyName != null && event.propertyName !== part && event.propertyName !== remaining) return;
              const previous = currentValue;
              try { currentValue = readProperty(item, property); rewire(); observer.next({ sender: item, propertyName: property, value: currentValue, previous }); } catch(error) { observer.error(error); }
            }, error: error => observer.error(error)
          }));
          current = current[part];
        }
      };
      try { rewire(); if (notifyInitial) observer.next({ sender: item, propertyName: property, value: currentValue, previous: undefined }); } catch(error) { observer.error(error); }
      return () => connections.unsubscribe();
    });
  }
  return new Observable(observer => {
    const sub = propertyEvents(item).subscribe({ next(event) { if (typeof property === 'function' || property == null || event.propertyName === property) observer.next({ sender: item, propertyName: property, value: readProperty(item, property), previous: event.previous }); }, error: error => observer.error(error), complete: () => observer.complete() });
    if (notifyInitial) observer.next({ sender: item, propertyName: property, value: readProperty(item, property), previous: undefined });
    return sub;
  });
}

export function whenPropertyChanged(itemOrChanges, property, notifyInitial = true) { return isObservable(itemOrChanges) ? itemOrChanges.pipe(mergeMany(item => changedProperty(item, property, notifyInitial))) : changedProperty(itemOrChanges, property, notifyInitial); }
export function observeProperty(itemOrChanges, property, notifyInitial = true) { return whenPropertyChanged(itemOrChanges, property, notifyInitial).pipe(map(event => event.value), distinctUntilChanged()); }
export const whenValueChanged = observeProperty;

export function autoRefreshOnObservable(selector, options = {}) {
  if (typeof options === 'number') options = { buffer: options };
  return source => new Observable(observer => {
    let state, processing = false, pending = [];
    const flush = () => { const live = pending.filter(entry => entry.active); pending = []; if (live.length) observer.next(cs([...new Set(live)].map(entry => ({ reason: 'refresh', current: entry.item, key: entry.key, currentIndex: state.kind === 'list' ? state.entries.indexOf(entry) : -1 })), state.kind)); };
    const resources = new Subscription(); let timer;
    const refresh = entry => { pending.push(entry); if (processing) return; if (options.buffer > 0) { if (!timer) { timer = (options.scheduler ?? asyncScheduler).schedule(() => { timer = null; flush(); }, options.buffer); resources.add(timer); } } else flush(); };
    state = tracker({ add(entry) { entry.subscription.add(from(selector(entry.item, entry.key)).subscribe({ next: () => refresh(entry), error: error => observer.error(error) })); } });
    resources.add(() => state.dispose());
    resources.add(source.subscribe({ next(changes) { try { processing = true; state.apply(changes); observer.next(changes); processing = false; if (pending.length) { if (options.buffer > 0) { const entry = pending.pop(); refresh(entry); } else flush(); } } catch (error) { observer.error(error); } }, error: error => observer.error(error), complete() { flush(); observer.complete(); } }));
    return resources;
  });
}
export function autoRefresh(property, options = {}) {
  if (property && typeof property === 'object') { options = property; property = undefined; }
  return autoRefreshOnObservable(item => { const events = changedProperty(item, property, false); return options.throttle > 0 ? events.pipe(debounceTime(options.throttle, options.scheduler ?? asyncScheduler)) : events; }, options);
}

/** Reconciles observable-derived values with each cache key or list occurrence. */
function observableProjection(selector, mode, options = {}) {
  return source => new Observable(observer => {
    const resources = new Subscription(); let state, processing = false, done = false, output = []; const pending = new Set();
    const reconcile = () => {
      if (processing || observer.closed) return;
      const desired = state.entries.filter(entry => mode === 'filter' ? entry.value === true : entry.hasValue).map(entry => ({ entry, value: mode === 'filter' ? entry.item : entry.value }));
      const changes = [];
      if (state.kind === 'cache') {
        const next = new Map(desired.map(record => [record.entry.key, record])); const old = new Map(output.map(record => [record.entry.key, record]));
        for (const record of output) if (!next.has(record.entry.key)) changes.push({ reason: 'remove', key: record.entry.key, current: record.value });
        const work = output.filter(record => next.has(record.entry.key)).map(record => record.entry.key);
        for (let index = 0; index < desired.length; index++) {
          const record = desired[index], key = record.entry.key, previous = old.get(key);
          if (!previous) { changes.push({ reason: 'add', key, current: record.value, currentIndex: index }); work.splice(index, 0, key); }
          else {
            const previousIndex = work.indexOf(key);
            if (previousIndex !== index) { changes.push({ reason: 'move', key, current: previous.value, previousIndex, currentIndex: index }); work.splice(index, 0, work.splice(previousIndex, 1)[0]); }
            if (!Object.is(previous.value, record.value)) changes.push({ reason: 'update', key, current: record.value, previous: previous.value, currentIndex: index, previousIndex: index });
            else if (record.entry.refreshed) changes.push({ reason: 'refresh', key, current: record.value, currentIndex: index });
          }
        }
      } else {
        const replacements = new Map(desired.filter(record => record.entry.prior).map(record => [record.entry.prior, record.entry]));
        const work = output.map(record => replacements.has(record.entry) ? { ...record, entry: replacements.get(record.entry) } : record); const wanted = new Set(desired.map(record => record.entry));
        for (let index = work.length - 1; index >= 0; index--) if (!wanted.has(work[index].entry)) { changes.push({ reason: 'remove', current: work[index].value, currentIndex: index }); work.splice(index, 1); }
        for (let index = 0; index < desired.length; index++) { const record = desired[index]; let found = work.findIndex(value => value.entry === record.entry); if (found < 0) { changes.push({ reason: 'add', current: record.value, currentIndex: index }); work.splice(index, 0, record); } else { if (found !== index) { changes.push({ reason: 'move', current: work[found].value, previousIndex: found, currentIndex: index }); work.splice(index, 0, work.splice(found, 1)[0]); } if (!Object.is(work[index].value, record.value)) { changes.push({ reason: 'replace', current: record.value, previous: work[index].value, currentIndex: index, previousIndex: index }); work[index] = record; } else if (record.entry.refreshed) changes.push({ reason: 'refresh', current: record.value, currentIndex: index }); } }
      }
      output = desired; for (const entry of state.entries) entry.refreshed = false;
      if (changes.length) { const result = cs(changes, state.kind); if (state.kind === 'cache') { result.items = desired.map(record => record.value); result.keys = desired.map(record => record.entry.key); } observer.next(result); }
      if (done && (!options.waitForCompletion || !pending.size)) observer.complete();
    };
    const start = (entry, previous) => {
        if (mode === 'transform' && entry.prior?.hasValue) { entry.value = entry.prior.value; entry.hasValue = true; }
        pending.add(entry);
        entry.subscription.add(() => { pending.delete(entry); entry.controller?.abort(); });
        entry.controller = new AbortController();
        let observable;
        try { const selected = selector(entry.item, entry.key, previous, entry.controller.signal); observable = isObservable(selected) || selected?.then ? from(selected) : of(selected); } catch (error) { if (options.onError) { options.onError({ error, item: entry.item, key: entry.key }); pending.delete(entry); return; } throw error; }
        entry.subscription.add(observable.subscribe({ next(value) { if (entry.active) { entry.value = value; entry.hasValue = true; reconcile(); } }, error(error) { pending.delete(entry); if (options.onError) { options.onError({ error, item: entry.item, key: entry.key }); reconcile(); } else observer.error(error); }, complete() { pending.delete(entry); reconcile(); } }));
      };
    state = tracker({
      add: start,
      refresh(entry) { if (options.transformOnRefresh) { entry.subscription.unsubscribe(); entry.subscription = new Subscription(); start(entry, entry.item); } else entry.refreshed = true; },
      remove() { /* reconciliation occurs at transaction end */ }
    });
    resources.add(() => state.dispose());
    resources.add(source.subscribe({ next(changes) { try { processing = true; state.apply(changes); processing = false; reconcile(); } catch (error) { observer.error(error); } }, error: error => observer.error(error), complete() { done = true; reconcile(); } }));
    return resources;
  });
}
export function filterOnObservable(selector, options = {}) { return observableProjection(selector, 'filter', options); }
export function transformOnObservable(selector, options = {}) { return observableProjection(selector, 'transform', options); }
/** factory(item, key, previous, AbortSignal); stale results never enter the output. */
export function transformAsync(factory, options = {}) {
  const maximum = options.maximumConcurrency ?? options.MaximumConcurrency ?? Infinity;
  if (!(maximum === Infinity || Number.isInteger(maximum) && maximum > 0)) throw new RangeError('Maximum concurrency must be a positive integer');
  return source => new Observable(observer => {
    let running = 0, draining = false; const waiting = [];
    const drain = () => { if (draining) return; draining = true; try { while (running < maximum && waiting.length) { const job = waiting.shift(); if (!job.closed) job.start(); } } finally { draining = false; } };
    const queued = (...arguments_) => new Observable(destination => {
      let inner, started = false, finished = false;
      const finish = () => { if (finished) return; finished = true; if (started) running--; drain(); };
      const job = { closed: false, start() { started = true; running++; try { const value = factory(...arguments_); inner = (isObservable(value) || value?.then ? from(value) : of(value)).subscribe({ next: value => destination.next(value), error(error) { finish(); destination.error(error); }, complete() { finish(); destination.complete(); } }); } catch(error) { finish(); destination.error(error); } } };
      waiting.push(job); drain();
      return () => { job.closed = true; inner?.unsubscribe(); finish(); };
    });
    return source.pipe(observableProjection(queued, 'transform', { ...options, transformOnRefresh: options.transformOnRefresh ?? options.TransformOnRefresh, waitForCompletion: true })).subscribe(observer);
  });
}
export function transformSafeAsync(factory, onError, options = {}) { return transformAsync(factory, { ...options, onError }); }

export function mergeMany(selector) { return source => new Observable(observer => {
  const resources = new Subscription(); const state = tracker({ add(entry) { entry.subscription.add(from(selector(entry.item, entry.key)).subscribe({ next: value => observer.next(value), error: error => observer.error(error) })); } });
  resources.add(() => state.dispose()); resources.add(source.subscribe({ next(changes) { try { state.apply(changes); } catch (error) { observer.error(error); } }, error: error => observer.error(error), complete: () => observer.complete() })); return resources;
}); }
export function mergeManyItems(selector) { return mergeMany((item, key) => from(selector(item, key)).pipe(map(value => ({ item, key, value })))); }
export function subscribeMany(selector) { return source => new Observable(observer => {
  const resources = new Subscription(); const state = tracker({ add(entry) { const subscription = selector(entry.item, entry.key); if (subscription) entry.subscription.add(() => release(subscription)); } });
  resources.add(() => state.dispose()); resources.add(source.subscribe({ next(changes) { try { state.apply(changes); observer.next(changes); } catch (error) { observer.error(error); } }, error: error => observer.error(error), complete: () => observer.complete() })); return resources;
}); }
export function disposeMany(disposer = release) { return disposalOperator(disposer); }

function onItem(reason, action) { return source => new Observable(observer => source.subscribe({ next(changes) { try { for (const change of getChanges(changes)) { if (change.reason === reason || (reason === 'update' && change.reason === 'replace')) { if (reason === 'update') action(change.current, change.previous, change.key); else action(change.current, change.key); } else if (reason === 'add' && change.reason === 'addRange' || reason === 'remove' && (change.reason === 'removeRange' || change.reason === 'clear')) { for (const item of change.range?.items ?? change.current ?? []) action(item); } } observer.next(changes); } catch (error) { observer.error(error); } }, error: error => observer.error(error), complete: () => observer.complete() })); }
export const onItemAdded = action => onItem('add', action);
export const onItemUpdated = action => onItem('update', action);
export const onItemRemoved = action => onItem('remove', action);
export const onItemRefreshed = action => onItem('refresh', action);

export function notEmpty() { return rxFilter(changes => getChanges(changes).length > 0); }
export function skipInitial() { return skip(1); }
export function deferUntilLoaded() { return source => new Observable(observer => { let loaded = false; return source.subscribe({ next(changes) { if (getChanges(changes).length > 0) loaded = true; if (loaded) observer.next(changes); }, error: error => observer.error(error), complete: () => observer.complete() }); }); }
export function watch(key) { return source => source.pipe(map(changes => [...getChanges(changes)].filter(change => Object.is(change.key, key))), rxFilter(changes => changes.length > 0), mergeChangeRecords()); }
function mergeChangeRecords() { return source => new Observable(observer => source.subscribe({ next(changes) { for (const change of changes) observer.next(change); }, error: error => observer.error(error), complete: () => observer.complete() })); }
export function batch(duration = 16, scheduler = asyncScheduler) { if (!(duration > 0)) throw new RangeError('Batch duration must be positive'); return source => source.pipe(bufferTime(duration, scheduler), rxFilter(sets => sets.some(changes => getChanges(changes).length)), map(sets => cs(sets.flatMap(changes => [...getChanges(changes)]), sets[0]?.kind))); }
export function bufferIf(pause, options = {}) {
  if (typeof options === 'boolean') options = { initialPauseState: options };
  return source => new Observable(observer => {
    let paused = options.initialPauseState ?? false, sets = [], timeout; const resources = new Subscription();
    const flush = () => { timeout?.unsubscribe(); timeout = undefined; if (sets.length) { const current = sets; sets = []; observer.next(cs(current.flatMap(changes => [...getChanges(changes)]), current[0].kind)); } };
    const armTimeout = () => { if (paused && options.timeout != null && !timeout) { timeout = (options.scheduler ?? asyncScheduler).schedule(() => { paused = false; flush(); }, options.timeout); resources.add(timeout); } };
    armTimeout(); resources.add(pause.subscribe({ next(value) { paused = !!value; if (!paused) flush(); else armTimeout(); }, error: error => observer.error(error) }));
    resources.add(source.subscribe({ next(changes) { if (paused) sets.push(changes); else observer.next(changes); }, error: error => observer.error(error), complete() { flush(); observer.complete(); } })); return resources;
  });
}

/** Stream expiry affects downstream state; it never mutates the upstream cache or list. */
export function expireAfter(selector, options = {}, sourceOptions = {}) {
  if (selector?.connect && typeof selector.edit === 'function') return expireSource(selector, options, sourceOptions);
  if (options?.schedule) options = { scheduler: options };
  const scheduler = options.scheduler ?? asyncScheduler;
  return source => new Observable(observer => {
    const resources = new Subscription(); let state, processing = false, output = [], done = false;
    const reconcile = () => { if (processing) return; const desired = state.entries.filter(entry => !entry.expired); const changes = [];
      if (state.kind === 'cache') { const next = new Map(desired.map(entry => [entry.key, entry])); const old = new Map(output.map(entry => [entry.key, entry])); for (const entry of output) if (!next.has(entry.key)) changes.push({ reason: 'remove', key: entry.key, current: entry.item }); for (const entry of desired) { const previous = old.get(entry.key); if (!previous) changes.push({ reason: 'add', key: entry.key, current: entry.item }); else if (previous !== entry) changes.push({ reason: 'update', key: entry.key, current: entry.item, previous: previous.item }); else if (entry.refreshed) changes.push({ reason: 'refresh', key: entry.key, current: entry.item }); } }
      else { const work = output.slice(); const keep = new Set(desired); for (let index = work.length - 1; index >= 0; index--) if (!keep.has(work[index])) { changes.push({ reason: 'remove', current: work[index].item, currentIndex: index }); work.splice(index, 1); } for (let index = 0; index < desired.length; index++) { const entry = desired[index]; const previousIndex = work.indexOf(entry); if (previousIndex < 0) { changes.push({ reason: 'add', current: entry.item, currentIndex: index }); work.splice(index, 0, entry); } else if (previousIndex !== index) { changes.push({ reason: 'move', current: entry.item, previousIndex, currentIndex: index }); work.splice(index, 0, work.splice(previousIndex, 1)[0]); } else if (entry.refreshed) changes.push({ reason: 'refresh', current: entry.item, currentIndex: index }); } }
      output = desired; for (const entry of state.entries) entry.refreshed = false; if (changes.length) observer.next(cs(changes, state.kind));
      if (done) observer.complete();
    };
    state = tracker({ add(entry) { const duration = typeof selector === 'function' ? selector(entry.item, entry.key) : selector; if (duration != null && Number.isFinite(duration)) { entry.timer = scheduler.schedule(() => { entry.expired = true; reconcile(); if (state.kind === 'cache') state.erase(entry); }, Math.max(0, duration)); entry.subscription.add(entry.timer); } }, refresh(entry) { entry.refreshed = true; } });
    resources.add(() => state.dispose()); resources.add(source.subscribe({ next(changes) { try { processing = true; state.apply(changes); processing = false; reconcile(); } catch (error) { observer.error(error); } }, error: error => observer.error(error), complete() { done = true; reconcile(); } })); return resources;
  });
}

/** FIFO eviction of the oldest retained entries. Updates retain their insertion order. */
export function limitSizeTo(size, sourceSize, sourceOptions = {}) {
  if (size?.connect && typeof size.edit === 'function') return limitSourceSize(size, sourceSize, sourceOptions);
  if (!Number.isInteger(size) || size <= 0) throw new RangeError('Size limit must be a positive integer');
  return source => new Observable(observer => {
    let state, output = [], order = 0;
    state = tracker({ add(entry) { entry.order = entry.prior?.order ?? order++; }, refresh(entry) { entry.refreshed = true; } });
    const sub = source.subscribe({ next(changes) { try { state.apply(changes); const live = state.entries.filter(entry => !entry.evicted); const selected = new Set([...live].sort((a,b) => b.order-a.order).slice(0,size)); for (const entry of live) if (!selected.has(entry)) { entry.evicted = true; if (state.kind === 'cache') state.erase(entry); } const desired = state.entries.filter(entry => selected.has(entry)); const result=[];
      if (state.kind==='cache') { const next=new Map(desired.map(entry=>[entry.key,entry])); const old=new Map(output.map(entry=>[entry.key,entry])); for(const entry of output) if(!next.has(entry.key)) result.push({reason:'remove',key:entry.key,current:entry.item}); for(const entry of desired) {const previous=old.get(entry.key); if(!previous) result.push({reason:'add',key:entry.key,current:entry.item}); else if(previous!==entry) result.push({reason:'update',key:entry.key,current:entry.item,previous:previous.item}); else if(entry.refreshed) result.push({reason:'refresh',key:entry.key,current:entry.item}); } }
      else {const work=output.slice();for(let index=work.length-1;index>=0;index--)if(!selected.has(work[index])) {result.push({reason:'remove',current:work[index].item,currentIndex:index});work.splice(index,1);}for(let index=0;index<desired.length;index++){const entry=desired[index];const previousIndex=work.indexOf(entry);if(previousIndex<0){result.push({reason:'add',current:entry.item,currentIndex:index});work.splice(index,0,entry);}else if(previousIndex!==index){result.push({reason:'move',current:entry.item,previousIndex,currentIndex:index});work.splice(index,0,work.splice(previousIndex,1)[0]);}} }
      output=desired;for(const entry of state.entries) entry.refreshed=false;if(result.length)observer.next(cs(result,state.kind));
    }catch(error){observer.error(error);} },error:error=>observer.error(error),complete:()=>observer.complete()});return()=>{sub.unsubscribe();state.dispose();};
  });
}

/** Mutating source overload: expired cache [key, item] pairs, or expired list items. */
function expireSource(source, selector, options = {}) {
  if (options?.schedule) options = { scheduler: options };
  const scheduler = options.scheduler ?? asyncScheduler;
  if (options.pollingInterval != null && !(options.pollingInterval > 0)) throw new RangeError('Polling interval must be positive');
  return new Observable(observer => {
    const resources = new Subscription(); let state;
    const expireEntries = candidates => {
      if (observer.closed || source.isDisposed) return;
      try { source.edit(updater => {
        const live = candidates.filter(entry => entry.active && state.entries.includes(entry));
        if (!live.length) return;
        if (source.kind === 'cache') {
          const expired = live.map(entry => [entry.key, entry.item]);
          updater.removeKeys(expired.map(([key]) => key)); observer.next(expired);
        } else {
          const expired = live.map(entry => ({ entry, index: state.entries.indexOf(entry) })).sort((a,b) => b.index-a.index);
          for (const record of expired) updater.removeAt(record.index);
          observer.next(expired.reverse().map(record => record.entry.item));
        }
      }); } catch (error) { observer.error(error); }
    };
    state = tracker({ add(entry) {
      const duration = typeof selector === 'function' ? selector(entry.item, entry.key) : selector;
      if (duration == null || duration === Infinity) return;
      if (!Number.isFinite(duration)) throw new RangeError('Expiry duration must be finite, null or Infinity');
      entry.expiresAt = scheduler.now() + Math.max(0, duration);
      if (options.pollingInterval == null) entry.subscription.add(scheduler.schedule(() => expireEntries([entry]), Math.max(0, duration)));
    } });
    resources.add(() => state.dispose());
    if (options.pollingInterval != null) resources.add(scheduler.schedule(function poll() { expireEntries(state.entries.filter(entry => entry.expiresAt <= scheduler.now())); if (!observer.closed) this.schedule(undefined, options.pollingInterval); }, options.pollingInterval));
    resources.add(source.connect().subscribe({ next(changes) { try { state.apply(changes); } catch(error) { observer.error(error); } }, error: error => observer.error(error), complete: () => observer.complete() }));
    return resources;
  });
}

/** Mutating source overload: removes the oldest entries and emits the removed values/pairs. */
function limitSourceSize(source, size, options = {}) {
  if (!Number.isInteger(size) || size <= 0) throw new RangeError('Size limit must be a positive integer');
  if (options?.schedule) options = { scheduler: options };
  return new Observable(observer => {
    const resources = new Subscription(); let scheduled;
    const enforce = () => {
      scheduled = undefined;
      if (observer.closed || source.isDisposed || source.count <= size) return;
      try { source.edit(updater => {
        const excess = source.count - size;
        if (excess <= 0) return;
        if (source.kind === 'cache') {
          const removed = source.keys.slice(0, excess).map(key => [key, source.lookup(key).value]);
          updater.removeKeys(removed.map(([key]) => key)); observer.next(removed);
        } else {
          const removed = source.items.slice(0, excess); updater.removeRange(0, excess); observer.next(removed);
        }
      }); } catch(error) { observer.error(error); }
    };
    resources.add(source.connect().subscribe({ next() { if (source.count <= size) return; if (options.scheduler) { if (!scheduled) { scheduled = options.scheduler.schedule(enforce); resources.add(scheduled); } } else enforce(); }, error: error => observer.error(error), complete: () => observer.complete() }));
    return resources;
  });
}

/** Converts item/iterable emissions; configured retention removes backing source entries. */
export function toObservableChangeSet(keySelectorOrOptions, maybeOptions = {}) {
  const keySelector = typeof keySelectorOrOptions === 'function' ? keySelectorOrOptions : keySelectorOrOptions?.keySelector;
  const options = typeof keySelectorOrOptions === 'function' ? maybeOptions : keySelectorOrOptions ?? {};
  return source => new Observable(observer => {
    const collection = keySelector ? new SourceCache(keySelector) : new SourceList();
    const resources = new Subscription();
    resources.add(() => collection.dispose());
    resources.add(collection.connect().subscribe(observer));
    if (options.limitSize != null) resources.add(limitSourceSize(collection, options.limitSize).subscribe({ error: error => observer.error(error) }));
    if (options.expireAfter != null) resources.add(expireSource(collection, options.expireAfter, { scheduler: options.scheduler, pollingInterval: options.pollingInterval }).subscribe({ error: error => observer.error(error) }));
    resources.add(source.subscribe({
      next(value) { try {
        const items = !options.singleItem && value != null && typeof value !== 'string' && typeof value[Symbol.iterator] === 'function' ? [...value] : [value];
        if (keySelector) collection.addOrUpdate(items); else collection.addRange(items);
      } catch(error) { observer.error(error); } },
      error: error => observer.error(error), complete: () => collection.dispose()
    }));
    return resources;
  });
}

function createChangeSet(subscribe, keySelector) {
  return new Observable(observer => {
    const source = keySelector ? new SourceCache(keySelector) : new SourceList(); const resources=new Subscription(); resources.add(source.connect().subscribe(observer)); resources.add(()=>source.dispose());
    try { const cleanup = subscribe(source); if(cleanup?.then) { let disposed=false; resources.add(()=>{disposed=true;}); Promise.resolve(cleanup).then(value=>{if(disposed)release(value);else if(value)resources.add(()=>release(value));},error=>observer.error(error)); } else if(cleanup) resources.add(()=>release(cleanup)); }catch(error){observer.error(error);}return resources;
  });
}
export const ObservableChangeSet = Object.freeze({ create:createChangeSet, Create:createChangeSet, createCache:(subscribe,keySelector)=>createChangeSet(subscribe,keySelector), CreateCache:(subscribe,keySelector)=>createChangeSet(subscribe,keySelector), createList:subscribe=>createChangeSet(subscribe), CreateList:subscribe=>createChangeSet(subscribe) });


function disposalOperator(disposer, completedAccessor) {
  return source => new Observable(observer => {
    const completion = new ReplaySubject(1); const resources = new Subscription(); let queue = [], pending = 0, ended = false;
    completedAccessor?.(completion.asObservable());
    const check = () => { if (ended && pending === 0 && !completion.isStopped) { completion.next(undefined); completion.complete(); } };
    const dispose = item => { try { const result = disposer(item); if (result?.then) { pending++; Promise.resolve(result).then(() => { pending--; check(); }, error => { pending--; completion.error(error); }); } } catch (error) { if (completedAccessor) completion.error(error); else throw error; } };
    const state = tracker({ remove: entry => queue.push(entry) });
    const flush = () => { const removed = queue; queue = []; for (const entry of removed) if (!state.entries.some(current => current.prior === entry && Object.is(current.item, entry.item))) dispose(entry.item); };
    resources.add(() => { state.dispose(); flush(); ended = true; check(); });
    resources.add(source.subscribe({ next(changes) { try { state.apply(changes); observer.next(changes); flush(); } catch(error) { observer.error(error); } }, error:error => observer.error(error), complete:() => observer.complete() }));
    return resources;
  });
}
/** Completion accessor receives a separate observable that awaits async cleanup, even on unsubscribe. */
export function asyncDisposeMany(completedAccessor, disposer = item => item?.[Symbol.asyncDispose] ? item[Symbol.asyncDispose]() : item?.disposeAsync ? item.disposeAsync() : release(item)) { return disposalOperator(disposer, completedAccessor); }
export const batchIf = bufferIf;
export function bufferInitial(duration, scheduler = asyncScheduler) {
  if (!(duration >= 0)) throw new RangeError('Initial duration must be non-negative');
  return source => new Observable(observer => {
    let started = false, ready = false, sets = []; const resources = new Subscription();
    const flush = () => { ready = true; if (sets.length) { const current=sets;sets=[];observer.next(cs(current.flatMap(changes=>[...getChanges(changes)]),current[0].kind)); } };
    resources.add(source.subscribe({next(changes) { if (ready) observer.next(changes); else if (getChanges(changes).length) {sets.push(changes);if(!started){started=true;resources.add(scheduler.schedule(flush,duration));}} },error:error=>observer.error(error),complete(){flush();observer.complete();}}));return resources;
  });
}
export function filterOnProperty(property, predicate, options = {}) { return source => source.pipe(autoRefresh(property, options), filter(predicate)); }
/** Uses the RxJS terminal/unsubscribe finalizer contract. */
export const finallySafe = action => finalize(action);
export const ConnectionStatus = Object.freeze({ Pending:'pending', Loaded:'loaded', Errored:'errored', Completed:'completed' });
export function monitorStatus() { return source => new Observable(observer => {let loaded=false;observer.next(ConnectionStatus.Pending);return source.subscribe({next(){if(!loaded){loaded=true;observer.next(ConnectionStatus.Loaded);}},error(){observer.next(ConnectionStatus.Errored);observer.complete();},complete(){observer.next(ConnectionStatus.Completed);observer.complete();}});}); }
export const watchValue = key => source => (typeof source.connect === 'function' ? source.connect() : source).pipe(watch(key),map(change=>change.current));
export function whenAnyPropertyChanged(itemOrChanges, ...propertyNames) { const observe=item=>propertyEvents(item).pipe(rxFilter(event=>!propertyNames.length||propertyNames.includes(event.propertyName)),map(()=>item));return isObservable(itemOrChanges)?itemOrChanges.pipe(mergeMany(observe)):observe(itemOrChanges); }
/** whenChanged(object, [property names/selectors], (object, ...values) => result). */
export function whenChanged(item, properties, resultSelector) {
  const fields=Array.isArray(properties)?properties:[properties];
  return combineLatest(fields.map(property=>observeProperty(item,property))).pipe(map(values=>resultSelector?resultSelector(item,...values):values.length===1?values[0]:values));
}
/** Native EventTarget events, or SourceList/SourceCache connections, become sender/eventArgs pairs. */
export function observeCollectionChanges(collection, eventName = 'collectionchange') {const events=typeof collection.connect==='function'?collection.connect():isObservable(collection)?collection:fromEvent(collection,eventName);return events.pipe(map(event=>({sender:collection,eventArgs:event?.detail??event})));}

function trueFor(selector, condition, all) { return source=>new Observable(observer=>{
  let state,processing=false,previous;const resources=new Subscription();
  const evaluate=()=>{if(processing)return;try{const accepts=entry=>entry.hasValue&&!!(condition.length>=2?condition(entry.item,entry.value):condition(entry.value));const result=all?state.entries.every(accepts):state.entries.some(accepts);if(result!==previous){previous=result;observer.next(result);}}catch(error){observer.error(error);}};
  state=tracker({add(entry){entry.subscription.add(from(selector(entry.item,entry.key)).subscribe({next(value){entry.value=value;entry.hasValue=true;evaluate();},error:error=>observer.error(error)}));}});
  resources.add(()=>state.dispose());resources.add(source.subscribe({next(changes){try{processing=true;state.apply(changes);processing=false;evaluate();}catch(error){observer.error(error);}},error:error=>observer.error(error),complete:()=>observer.complete()}));return resources;
});}
export const trueForAll = (selector, condition = Boolean) => trueFor(selector,condition,true);
export const trueForAny = (selector, condition = Boolean) => trueFor(selector,condition,false);
export function transformManyAsync(factory, keySelector, options = {}) { return source=>source.pipe(transformAsync(factory,options),transformMany(items=>items,keySelector)); }
export function transformManySafeAsync(factory, keySelector, onError, options = {}) { return transformManyAsync(factory,keySelector,{...options,onError}); }

/** Shares a single upstream connection and replays the current collection, not its last delta. */
export function refCount() { return source=>{
  let session;
  return new Observable(observer=>{
    if(!session)session={subject:new Subject(),references:0,state:undefined,kind:'cache',upstream:new Subscription()};
    const current=session;current.references++;
    const subscription=current.subject.subscribe(observer);
    if(current.state) {const initial=snapshotChanges(current.state,current.kind);if(initial.length)observer.next(initial);}
    if(current.references===1&&!current.started){current.started=true;current.upstream.add(source.subscribe({next(changes){current.kind=changes.kind??current.kind;if(!current.state)current.state=current.kind==='list'?[]:new Map();applyChanges(current.state,changes);current.subject.next(changes);},error:error=>current.subject.error(error),complete:()=>current.subject.complete()}));}
    return()=>{subscription.unsubscribe();if(--current.references===0){current.upstream.unsubscribe();if(session===current)session=undefined;}};
  });
};}

/** Changes to the active inner source first remove all items supplied by its predecessor. */
export function switchLatest() { return sources=>new Observable(observer=>{
  const resources=new Subscription();let inner,kind='cache',state,outerDone=false,innerDone=true,generation=0;
  const clear=()=>{if(!state)return;const removed=kind==='cache'?[...state].map(([key,current])=>({reason:'remove',key,current})):state.length?[{reason:'clear',range:{items:state.slice(),index:0}}]:[];if(removed.length)observer.next(cs(removed,kind));state=undefined;};
  resources.add(()=>inner?.unsubscribe());resources.add(sources.subscribe({next(source){const current=++generation;inner?.unsubscribe();clear();innerDone=false;const incoming=typeof source.connect==='function'?source.connect():source;inner=from(incoming).subscribe({next(changes){if(current!==generation)return;kind=changes.kind??kind;if(!state)state=kind==='list'?[]:new Map();applyChanges(state,changes);if(getChanges(changes).length)observer.next(changes);},error:error=>observer.error(error),complete(){if(current!==generation)return;innerDone=true;if(outerDone)observer.complete();}});},error:error=>observer.error(error),complete(){outerDone=true;if(innerDone)observer.complete();}}));return resources;
});}
export { switchLatest as switch };
