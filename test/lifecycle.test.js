import test from 'node:test';
import assert from 'node:assert/strict';
import { Subject, BehaviorSubject, Observable, of, firstValueFrom } from 'rxjs';
import { TestScheduler } from 'rxjs/testing';
import { ChangeSet, SourceCache, SourceList, applyChanges } from '../src/core.js';
import { autoRefreshOnObservable, autoRefresh, createObservableObject, notifyPropertyChanged, observeProperty, whenPropertyChanged, filterOnObservable, transformOnObservable, transformAsync, transformSafeAsync, mergeMany, mergeManyItems, subscribeMany, disposeMany, onItemAdded, onItemUpdated, onItemRemoved, onItemRefreshed, expireAfter, limitSizeTo, batch, bufferIf, notEmpty, deferUntilLoaded, skipInitial, watch, toObservableChangeSet, ObservableChangeSet, asyncDisposeMany, bufferInitial, filterOnProperty, finallySafe, monitorStatus, whenAnyPropertyChanged, whenChanged, observeCollectionChanges, trueForAll, trueForAny, transformManyAsync, refCount, switchLatest, watchValue } from '../src/lifecycle.js';
const changes = (items, kind = 'cache') => new ChangeSet(items, kind);
const add = (key, current) => ({ reason: 'add', key, current });
const update = (key, current, previous) => ({ reason: 'update', key, current, previous });
const remove = (key, current) => ({ reason: 'remove', key, current });
const scheduler = () => new TestScheduler((actual, expected) => assert.deepEqual(actual, expected));
const snapshots = (stream, kind='cache') => { const state=kind==='cache'?new Map():[]; const history=[];const subscription=stream.subscribe(value=>{applyChanges(state,value);history.push(kind==='cache'?[...state.values()]:state.slice());}); return {state,history,subscription}; };

test('autoRefresh forwards original transaction before synchronous per-item notifications', () => {
  const source=new Subject();const trigger=new BehaviorSubject(0);const seen=[];
  const sub=source.pipe(autoRefreshOnObservable(()=>trigger)).subscribe(set=>seen.push(set.map(item=>item.reason)));
  source.next(changes([add('a',{id:'a'})]));assert.deepEqual(seen,[['add'],['refresh']]);
  source.next(changes([remove('a')]));trigger.next(1);assert.deepEqual(seen,[['add'],['refresh'],['remove']]);sub.unsubscribe();assert.equal(trigger.observed,false);
});

test('property bridge emits actual changes, supports selectors and stops removed item notifications', () => {
  const item=createObservableObject({id:'a',price:1});const values=[];const sub=observeProperty(item,'price').subscribe(value=>values.push(value));
  item.price=2;item.price=2;item.price=3;assert.deepEqual(values,[1,2,3]);
  const source=new Subject();const reasons=[];const stream=source.pipe(autoRefresh('price')).subscribe(set=>reasons.push(set[0].reason));
  source.next(changes([add('a',item)]));item.price=4;source.next(changes([remove('a',item)]));item.price=5;assert.deepEqual(reasons,['add','refresh','remove']);
  const normal={count:0};const fields=[];const manual=whenPropertyChanged(normal,'count',false).subscribe(event=>fields.push([event.sender,event.value,event.previous]));normal.count=2;notifyPropertyChanged(normal,'count',0);assert.deepEqual(fields,[[normal,2,0]]);manual.unsubscribe();sub.unsubscribe();stream.unsubscribe();
});

test('list duplicate item occurrences have separate subscriptions and moving preserves them', () => {
  const source=new Subject();const trigger=new Subject();let alive=0;
  const sub=source.pipe(mergeMany(()=>new Observable(observer=>{alive++;const inner=trigger.subscribe(observer);return()=>{alive--;inner.unsubscribe();};}))).subscribe();
  const item={};source.next(changes([{reason:'addRange',range:{items:[item,item],index:0}}],'list'));assert.equal(alive,2);
  source.next(changes([{reason:'moved',previousIndex:0,currentIndex:1}],'list'));assert.equal(alive,2);
  source.next(changes([{reason:'remove',current:item,currentIndex:0}],'list'));assert.equal(alive,1);sub.unsubscribe();assert.equal(alive,0);
});

test('observable filter responds to flags and replacement tears down previous observable', () => {
  const source=new Subject();const first={id:'a',flag:new BehaviorSubject(false)},second={id:'b',flag:new BehaviorSubject(true)};
  const result=snapshots(source.pipe(filterOnObservable(item=>item.flag)));
  source.next(changes([add('a',first),add('b',second)]));assert.deepEqual([...result.state.keys()],['b']);first.flag.next(true);assert.deepEqual([...result.state.keys()],['a','b']);
  const replacement={id:'a',flag:new BehaviorSubject(false)};source.next(changes([update('a',replacement,first)]));assert.equal(first.flag.observed,false);assert.deepEqual([...result.state.keys()],['b']);result.subscription.unsubscribe();assert.equal(second.flag.observed,false);
});

test('observable list filter remaps indices and preserves moves', () => {
  const source=new Subject();const items=[1,2,3].map(id=>({id,flag:new BehaviorSubject(id!==2)}));const result=snapshots(source.pipe(filterOnObservable(item=>item.flag)),'list');
  source.next(changes([{reason:'addRange',range:{items,index:0}}],'list'));assert.deepEqual(result.state.map(item=>item.id),[1,3]);
  items[1].flag.next(true);assert.deepEqual(result.state.map(item=>item.id),[1,2,3]);
  source.next(changes([{reason:'moved',previousIndex:2,currentIndex:0}],'list'));assert.deepEqual(result.state.map(item=>item.id),[3,1,2]);result.subscription.unsubscribe();
});

test('observable transform emits add/update/removal and ignores replaced streams', () => {
  const source=new Subject();const first=new BehaviorSubject('one');const second=new BehaviorSubject('two');const result=snapshots(source.pipe(transformOnObservable(item=>item)));
  source.next(changes([add('a',first)]));first.next('ONE');assert.equal(result.state.get('a'),'ONE');source.next(changes([update('a',second,first)]));first.next('stale');assert.equal(result.state.get('a'),'two');source.next(changes([remove('a',second)]));assert.equal(result.state.size,0);assert.equal(second.observed,false);result.subscription.unsubscribe();
});

test('async transform cancels replaced work, ignores out-of-order results and waits for current work on completion', async () => {
  const source=new Subject();const work=[];const values=[];let completed=false;
  source.pipe(transformAsync((item,key,previous,signal)=>new Promise(resolve=>work.push({item,signal,resolve})))).subscribe({next:set=>values.push(...set),complete:()=>{completed=true;}});
  source.next(changes([add('a',1)]));source.next(changes([update('a',2,1)]));assert.equal(work[0].signal.aborted,true);
  source.complete();assert.equal(completed,false);work[0].resolve('stale');await Promise.resolve();assert.equal(values.length,0);work[1].resolve('fresh');await Promise.resolve();await Promise.resolve();assert.equal(completed,true);assert.equal(values[0].current,'fresh');
});

test('async transform aborts on removal and unsubscribe', async () => {
  const source=new Subject();const jobs=[];const seen=[];const sub=source.pipe(transformAsync((item,key,previous,signal)=>new Promise(resolve=>jobs.push({signal,resolve})))).subscribe(set=>seen.push(set));
  source.next(changes([add('a',1)]));source.next(changes([remove('a',1)]));assert.equal(jobs[0].signal.aborted,true);jobs[0].resolve(5);await Promise.resolve();assert.equal(seen.length,0);
  source.next(changes([add('b',2)]));sub.unsubscribe();assert.equal(jobs[1].signal.aborted,true);
});

test('safe async transform reports failures and continues other keys', async () => {
  const errors=[];const result=snapshots(of(changes([add('bad',0),add('good',2)])).pipe(transformSafeAsync(async item=>{if(!item)throw new Error('invalid');return item*2;},error=>errors.push(error))));
  await Promise.resolve();await Promise.resolve();assert.equal(errors[0].key,'bad');assert.equal(result.state.get('good'),4);
});

test('mergeManyItems associates notifications with item and key and releases on removal', () => {
  const source=new Subject(),trigger=new Subject();const item={trigger};const seen=[];const sub=source.pipe(mergeManyItems(item=>item.trigger)).subscribe(value=>seen.push(value));
  source.next(changes([add('a',item)]));trigger.next(4);source.next(changes([remove('a',item)]));trigger.next(5);assert.deepEqual(seen,[{item,key:'a',value:4}]);assert.equal(trigger.observed,false);sub.unsubscribe();
});

test('subscribeMany and disposeMany clean up replacements, removal and unsubscribe once', () => {
  const source=new Subject();const disposed=[];const item=id=>({id,dispose(){disposed.push(id);}});const a=item(1),b=item(2),c=item(3);const sub=source.pipe(disposeMany()).subscribe();
  source.next(changes([add('a',a)]));source.next(changes([update('a',b,a),add('c',c)]));source.next(changes([remove('a',b)]));sub.unsubscribe();assert.deepEqual(disposed,[1,2,3]);
  let count=0;of(changes([add('x',{})])).pipe(subscribeMany(()=>()=>count++)).subscribe();assert.equal(count,1);
});

test('item callbacks cover keyed updates, refreshes and list ranges', () => {
  const source=new Subject();const seen=[];source.pipe(onItemAdded((item,key)=>seen.push(['add',item,key])),onItemUpdated((item,previous,key)=>seen.push(['update',item,previous,key])),onItemRefreshed((item,key)=>seen.push(['refresh',item,key])),onItemRemoved((item,key)=>seen.push(['remove',item,key]))).subscribe();
  source.next(changes([add('x',1),update('x',2,1),{reason:'refresh',key:'x',current:2},remove('x',2)]));assert.deepEqual(seen,[['add',1,'x'],['update',2,1,'x'],['refresh',2,'x'],['remove',2,'x']]);
  source.next(changes([{reason:'addRange',range:{items:[3,4],index:0}},{reason:'clear',range:{items:[3,4],index:0}}],'list'));assert.equal(seen.length,8);
});

test('expiry resets updates, never expires null durations, and cancels on source completion', () => {
  const clock=scheduler();const source=new Subject();const observed=[];source.pipe(expireAfter(item=>item.ttl,clock)).subscribe({next:set=>observed.push([clock.frame,set.map(change=>[change.reason,change.key])]),complete:()=>observed.push([clock.frame,'complete'])});
  clock.schedule(()=>source.next(changes([add('a',{ttl:10}),add('b',{ttl:null})])),0);
  clock.schedule(()=>source.next(changes([update('a',{ttl:10})])),5);
  clock.schedule(()=>source.next(changes([add('c',{ttl:20})])),16);
  clock.schedule(()=>source.complete(),20);clock.flush();
  assert.deepEqual(observed,[[0,[['add','a'],['add','b']]],[5,[['update','a']]],[15,[['remove','a']]],[16,[['add','c']]],[20,'complete']]);
});

test('expiry filters subsequent source removals and remaps list indices', () => {
  const clock=scheduler(),source=new Subject();const result=snapshots(source.pipe(expireAfter(item=>item.ttl,clock)),'list');const a={id:'a',ttl:5},b={id:'b',ttl:10},c={id:'c',ttl:null};
  clock.schedule(()=>source.next(changes([{reason:'addRange',range:{items:[a,b,c],index:0}}],'list')),0);
  clock.schedule(()=>assert.deepEqual(result.state.map(item=>item.id),['b','c']),6);
  clock.schedule(()=>source.next(changes([{reason:'remove',current:b,currentIndex:1}],'list')),7);
  clock.schedule(()=>assert.deepEqual(result.state.map(item=>item.id),['c']),11);clock.flush();result.subscription.unsubscribe();
});

test('FIFO size limit does not resurrect evicted entries or reorder updated entries', () => {
  const source=new Subject();const result=snapshots(source.pipe(limitSizeTo(2)));
  source.next(changes([add('a',1),add('b',2)]));source.next(changes([update('a',10,1)]));source.next(changes([add('c',3)]));assert.deepEqual([...result.state.keys()],['b','c']);
  source.next(changes([remove('c',3)]));assert.deepEqual([...result.state.keys()],['b']);source.next(changes([update('a',11,10)]));assert.deepEqual([...result.state.keys()],['b','a']);result.subscription.unsubscribe();
});

test('bufferIf concatenates sequential change records and flushes on resume and completion', () => {
  const source=new Subject(),pause=new BehaviorSubject(true);const seen=[];source.pipe(bufferIf(pause)).subscribe(set=>seen.push([...set].map(change=>change.current)));
  source.next(changes([add('a',1)]));source.next(changes([update('a',2,1)]));assert.deepEqual(seen,[]);pause.next(false);assert.deepEqual(seen,[[1,2]]);pause.next(true);source.next(changes([add('b',3)]));source.complete();assert.deepEqual(seen,[[1,2],[3]]);assert.equal(pause.observed,false);
});

test('bufferIf timeout resumes with scheduler and batch flushes on completion', () => {
  const clock=scheduler(),source=new Subject(),pause=new BehaviorSubject(true);const seen=[];source.pipe(bufferIf(pause,{timeout:5,scheduler:clock})).subscribe(set=>seen.push([clock.frame,set.length]));
  clock.schedule(()=>source.next(changes([add('a',1)])),0);clock.schedule(()=>source.next(changes([add('b',2)])),7);clock.schedule(()=>source.complete(),9);clock.flush();assert.deepEqual(seen,[[5,1],[7,1]]);
  const clock2=scheduler(),input=new Subject(),batches=[];input.pipe(batch(10,clock2)).subscribe(set=>batches.push([clock2.frame,set.length]));clock2.schedule(()=>input.next(changes([add('a',1)])),1);clock2.schedule(()=>input.next(changes([add('b',2)])),4);clock2.schedule(()=>input.complete(),8);clock2.flush();assert.deepEqual(batches,[[8,2]]);
});

test('small flow operators retain the intended empty and initial semantics', () => {
  const input=of(changes([]),changes([add('a',1)]),changes([]),changes([update('a',2,1)]));const normal=[],loaded=[],skipped=[],watched=[];
  input.pipe(notEmpty()).subscribe(set=>normal.push(set.length));input.pipe(deferUntilLoaded()).subscribe(set=>loaded.push(set.length));input.pipe(skipInitial()).subscribe(set=>skipped.push(set.length));input.pipe(watch('a')).subscribe(change=>watched.push(change.reason));assert.deepEqual(normal,[1,1]);assert.deepEqual(loaded,[1,0,1]);assert.deepEqual(skipped,[1,0,1]);assert.deepEqual(watched,['add','update']);
});

test('toObservableChangeSet supports item and iterable emissions with cache and list retention', () => {
  const cache=snapshots(of([{id:1,value:'a'},{id:2,value:'b'}],{id:1,value:'A'}).pipe(toObservableChangeSet(item=>item.id)));assert.deepEqual([...cache.state.values()],[{id:1,value:'A'},{id:2,value:'b'}]);
  const list=snapshots(of([1,2],3,[4,5]).pipe(toObservableChangeSet({limitSize:2})),'list');assert.deepEqual(list.state,[4,5]);
});

test('ObservableChangeSet factory creates independent sources and disposes resources per subscription', () => {
  let disposed=0;const stream=ObservableChangeSet.createList(source=>{source.addRange([1,2]);return()=>disposed++;});const first=snapshots(stream,'list');const second=snapshots(stream,'list');assert.deepEqual(first.state,[1,2]);assert.deepEqual(second.state,[1,2]);first.subscription.unsubscribe();second.subscription.unsubscribe();assert.equal(disposed,2);
});


test('disposal occurs after downstream removal and same-reference update retains the item', () => {
  const source=new Subject(),events=[];const item={dispose:()=>events.push('disposed')};const sub=source.pipe(disposeMany()).subscribe(set=>events.push(set[0].reason));source.next(changes([add('a',item)]));source.next(changes([update('a',item,item)]));source.next(changes([remove('a',item)]));assert.deepEqual(events,['add','update','remove','disposed']);sub.unsubscribe();
});

test('asyncDisposeMany completion channel waits for all disposal promises after unsubscribe', async () => {
  const source=new Subject(),jobs=[];let complete=false;const item=id=>({disposeAsync:()=>new Promise(resolve=>jobs.push({id,resolve}))});const a=item('a'),b=item('b');const sub=source.pipe(asyncDisposeMany(signal=>signal.subscribe(()=>{complete=true;}))).subscribe();source.next(changes([add('a',a),add('b',b)]));source.next(changes([remove('a',a)]));sub.unsubscribe();assert.equal(jobs.length,2);assert.equal(complete,false);jobs[1].resolve();await Promise.resolve();assert.equal(complete,false);jobs[0].resolve();await Promise.resolve();assert.equal(complete,true);
});

test('bufferInitial starts with first nonempty input, then passes live changes through', () => {
  const clock=scheduler(),source=new Subject(),seen=[];source.pipe(bufferInitial(5,clock)).subscribe(set=>seen.push([clock.frame,set.length]));clock.schedule(()=>source.next(changes([])),0);clock.schedule(()=>source.next(changes([add('a',1)])),10);clock.schedule(()=>source.next(changes([add('b',2)])),12);clock.schedule(()=>source.next(changes([add('c',3)])),17);clock.schedule(()=>source.complete(),20);clock.flush();assert.deepEqual(seen,[[15,2],[17,1]]);
});

test('filterOnProperty tracks mutable objects through the property bridge', () => {
  const source=new Subject(),item=createObservableObject({id:'a',active:false});const result=snapshots(source.pipe(filterOnProperty('active',item=>item.active)));source.next(changes([add('a',item)]));assert.equal(result.state.size,0);item.active=true;assert.equal(result.state.size,1);item.active=false;assert.equal(result.state.size,0);result.subscription.unsubscribe();
});

test('status monitor translates terminal events while finalizer runs on unsubscribe', () => {
  const states=[];of(1,2).pipe(monitorStatus()).subscribe(status=>states.push(status));assert.deepEqual(states,['pending','loaded','completed']);const source=new Subject(),failures=[];source.pipe(monitorStatus()).subscribe(status=>failures.push(status));source.error(new Error('network'));assert.deepEqual(failures,['pending','errored']);let count=0;const sub=new Subject().pipe(finallySafe(()=>count++)).subscribe();sub.unsubscribe();assert.equal(count,1);
});

test('multi-property selectors and any-property observations emit current values', () => {
  const item=createObservableObject({first:'Ada',last:'Lovelace',age:30}),values=[],changed=[];const sub=whenChanged(item,['first','last'],(person,first,last)=>`${first} ${last}`).subscribe(value=>values.push(value));const any=whenAnyPropertyChanged(item,'first','last').subscribe(value=>changed.push(value));item.age=31;item.first='Grace';item.last='Hopper';assert.deepEqual(values,['Ada Lovelace','Grace Lovelace','Grace Hopper']);assert.equal(changed.length,2);assert.equal(changed[0],item);sub.unsubscribe();any.unsubscribe();
});

test('collection changes work with native event targets and source collections', () => {
  const target=new EventTarget(),events=[];const sub=observeCollectionChanges(target).subscribe(event=>events.push(event));target.dispatchEvent(new Event('collectionchange'));assert.equal(events[0].sender,target);assert.equal(events[0].eventArgs.type,'collectionchange');sub.unsubscribe();const list=new SourceList();const records=[];const connection=observeCollectionChanges(list).subscribe(event=>records.push(event));list.addRange([1,2]);assert.equal(records.at(-1).eventArgs.kind,'list');connection.unsubscribe();list.dispose();
});

test('observable all/any predicates handle not-yet-emitted values and unsubscriptions', () => {
  const source=new Subject(),a=new Subject(),b=new BehaviorSubject(true),all=[],any=[];const first=source.pipe(trueForAll(item=>item)).subscribe(value=>all.push(value)),second=source.pipe(trueForAny(item=>item)).subscribe(value=>any.push(value));source.next(changes([]));source.next(changes([add('a',a),add('b',b)]));a.next(true);b.next(false);source.next(changes([remove('b',b)]));assert.deepEqual(all,[true,false,true,false,true]);assert.deepEqual(any,[false,true]);first.unsubscribe();second.unsubscribe();assert.equal(a.observed,false);assert.equal(b.observed,false);
});

test('async transformMany produces keyed children and removes obsolete parent output', async () => {
  const source=new Subject();const result=snapshots(source.pipe(transformManyAsync(async item=>item.children,child=>child.id)));source.next(changes([add('p',{children:[{id:1},{id:2}]})]));await Promise.resolve();assert.deepEqual([...result.state.keys()],[1,2]);source.next(changes([update('p',{children:[{id:2},{id:3}]})]));await Promise.resolve();assert.deepEqual([...result.state.keys()],[2,3]);source.next(changes([remove('p')]));assert.equal(result.state.size,0);result.subscription.unsubscribe();
});

test('refCount shares one connection and late subscribers receive the full current state', () => {
  const source=new Subject();let connections=0,disconnects=0;const stream=new Observable(observer=>{connections++;const sub=source.subscribe(observer);return()=>{disconnects++;sub.unsubscribe();};}).pipe(refCount());const first=snapshots(stream);source.next(changes([add('a',1),add('b',2)]));source.next(changes([update('a',3,1)]));const second=snapshots(stream);assert.equal(connections,1);assert.deepEqual([...second.state],[['a',3],['b',2]]);first.subscription.unsubscribe();assert.equal(disconnects,0);second.subscription.unsubscribe();assert.equal(disconnects,1);const third=snapshots(stream);assert.equal(connections,2);assert.equal(third.state.size,0);third.subscription.unsubscribe();
});

test('switchLatest removes previous source state, releases it and waits for active completion', () => {
  const outer=new Subject(),a=new Subject(),b=new Subject();const result=snapshots(outer.pipe(switchLatest()));let completed=false;const sub=outer.pipe(switchLatest()).subscribe({complete:()=>{completed=true;}});outer.next(a);a.next(changes([add('a',1)]));outer.next(b);assert.equal(a.observed,false);assert.equal(result.state.size,0);b.next(changes([add('b',2)]));assert.deepEqual([...result.state],[['b',2]]);outer.complete();assert.equal(completed,false);b.complete();assert.equal(completed,true);sub.unsubscribe();
});

test('watchValue emits a removed item as well as add/update values', () => { const seen=[];of(changes([add('a',1),update('a',2,1),remove('a',2)])).pipe(watchValue('a')).subscribe(value=>seen.push(value));assert.deepEqual(seen,[1,2,2]); });

test('async transform maximumConcurrency queues work and removing queued entries prevents factory calls', async () => {
  const source=new Subject(),jobs=[];const result=snapshots(source.pipe(transformAsync(item=>new Promise(resolve=>jobs.push({item,resolve})),{maximumConcurrency:1})));
  source.next(changes([add('a',1),add('b',2),add('c',3)]));assert.deepEqual(jobs.map(job=>job.item),[1]);source.next(changes([remove('b',2)]));jobs[0].resolve(10);await Promise.resolve();assert.deepEqual(jobs.map(job=>job.item),[1,3]);jobs[1].resolve(30);await Promise.resolve();assert.deepEqual([...result.state],[['a',10],['c',30]]);result.subscription.unsubscribe();
});

test('async transform can reevaluate refreshes and keeps the last value until completion', async () => {
  const source=new Subject(),item={value:1},jobs=[];const result=snapshots(source.pipe(transformAsync(item=>new Promise(resolve=>jobs.push({value:item.value,resolve})),{transformOnRefresh:true})));
  source.next(changes([add('a',item)]));jobs[0].resolve(jobs[0].value);await Promise.resolve();assert.equal(result.state.get('a'),1);item.value=2;source.next(changes([{reason:'refresh',key:'a',current:item}]));assert.equal(result.state.get('a'),1);jobs[1].resolve(jobs[1].value);await Promise.resolve();assert.equal(result.state.get('a'),2);result.subscription.unsubscribe();
});

test('nested property paths subscribe at every segment and rewire on parent replacement', () => {
  const first=createObservableObject({name:'Ada'}),second=createObservableObject({name:'Grace'}),container=createObservableObject({person:first}),root=createObservableObject({container});const values=[];
  const sub=observeProperty(root,'container.person.name').subscribe(value=>values.push(value));first.name='ADA';container.person=second;first.name='stale';second.name='GRACE';root.container=null;second.name='detached';root.container=createObservableObject({person:createObservableObject({name:'Katherine'})});root.container.person.name='KATHERINE';assert.deepEqual(values,['Ada','ADA','Grace','GRACE',undefined,'Katherine','KATHERINE']);sub.unsubscribe();root.container.person.name='unobserved';assert.equal(values.length,7);
});

test('nested autoRefresh follows leaf proxies, parent replacement, and removed collection items', () => {
  const source=new SourceCache(item=>item.id),old=createObservableObject({score:1}),item=createObservableObject({id:1,profile:old}),sets=[];const sub=source.connect().pipe(autoRefresh('profile.score')).subscribe(set=>sets.push(set[0].reason));source.addOrUpdate(item);old.score=2;item.profile=createObservableObject({score:3});old.score=4;item.profile.score=5;source.removeKey(1);item.profile.score=6;assert.deepEqual(sets,['add','refresh','refresh','refresh','remove']);sub.unsubscribe();source.dispose();
});

test('direct source FIFO limits remove backing entries and expose accurate new subscriber snapshots', () => {
  const cache=new SourceCache(item=>item.id),list=new SourceList(),removed=[];const a=limitSizeTo(cache,3).subscribe(items=>removed.push(...items));const b=limitSizeTo(list,2).subscribe();
  cache.addOrUpdate(Array.from({length:1000},(_,id)=>({id})));list.addRange(Array.from({length:1000},(_,id)=>id));assert.equal(cache.count,3);assert.deepEqual(cache.keys,[997,998,999]);assert.deepEqual(list.items,[998,999]);assert.equal(removed.length,997);const snapshot=[];const late=cache.connect().subscribe(set=>snapshot.push(...set.map(change=>change.key)));assert.deepEqual(snapshot,[997,998,999]);a.unsubscribe();b.unsubscribe();late.unsubscribe();cache.dispose();list.dispose();
});

test('direct source expiry resets timers, deletes actual cache entries and cleans resources', () => {
  const clock=scheduler(),cache=new SourceCache(item=>item.id),removed=[];const sub=expireAfter(cache,item=>item.ttl,{scheduler:clock}).subscribe(items=>removed.push([clock.frame,...items.map(([key])=>key)]));cache.addOrUpdate([{id:1,ttl:5},{id:2,ttl:null}]);clock.schedule(()=>cache.addOrUpdate({id:1,ttl:10}),3);clock.schedule(()=>assert.equal(cache.count,2),6);clock.schedule(()=>assert.equal(cache.count,1),14);clock.flush();assert.deepEqual(removed,[[13,1]]);assert.deepEqual(cache.keys,[2]);cache.addOrUpdate({id:3,ttl:50});assert.equal(clock.actions.length,1);sub.unsubscribe();assert.equal(clock.actions.length,0);cache.dispose();
});

test('direct list expiry distinguishes duplicate occurrences and optional polling batches deadlines', () => {
  const clock=scheduler(),list=new SourceList(),duplicate={},removed=[];let calls=0;const sub=expireAfter(list,()=>++calls===1?10:5,{scheduler:clock}).subscribe(items=>removed.push([clock.frame,items.length]));list.addRange([duplicate,duplicate]);clock.schedule(()=>assert.equal(list.count,1),6);clock.flush();assert.equal(list.count,0);assert.deepEqual(removed,[[5,1],[10,1]]);sub.unsubscribe();list.dispose();
  const clock2=scheduler(),cache=new SourceCache(item=>item.id),batches=[];const polling=expireAfter(cache,()=>5,{scheduler:clock2,pollingInterval:10}).subscribe(items=>batches.push([clock2.frame,items.length]));cache.addOrUpdate([{id:1},{id:2}]);clock2.schedule(()=>polling.unsubscribe(),11);clock2.flush();assert.deepEqual(batches,[[10,2]]);assert.equal(cache.count,0);cache.dispose();
});

test('conversion keeps backing caches and lists bounded under long-lived large inputs', () => {
  const cacheOriginal=SourceCache.prototype.addOrUpdate,listOriginal=SourceList.prototype.addRange;let capturedCache,capturedList;
  SourceCache.prototype.addOrUpdate=function(...args){capturedCache=this;return cacheOriginal.apply(this,args);};SourceList.prototype.addRange=function(...args){capturedList=this;return listOriginal.apply(this,args);};
  const input=new Subject(),clock=scheduler();const cacheSub=input.pipe(toObservableChangeSet(item=>item.id,{limitSize:32,expireAfter:100,scheduler:clock})).subscribe();const listSub=input.pipe(toObservableChangeSet({limitSize:16,expireAfter:100,scheduler:clock})).subscribe();
  try {for(let batch=0;batch<100;batch++)input.next(Array.from({length:100},(_,index)=>({id:batch*100+index})));assert.equal(capturedCache.count,32);assert.equal(capturedList.count,16);assert.equal(clock.actions.length,48);const current=[];const late=capturedCache.connect().subscribe(set=>current.push(...set.map(change=>change.key)));assert.equal(current.length,32);assert.equal(current[0],9968);late.unsubscribe();clock.flush();assert.equal(capturedCache.count,0);assert.equal(capturedList.count,0);assert.equal(clock.actions.length,0);}finally{cacheSub.unsubscribe();listSub.unsubscribe();SourceCache.prototype.addOrUpdate=cacheOriginal;SourceList.prototype.addRange=listOriginal;}
});
