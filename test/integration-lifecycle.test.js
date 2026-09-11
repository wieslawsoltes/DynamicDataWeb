import test from 'node:test';
import assert from 'node:assert/strict';
import { BehaviorSubject } from 'rxjs';
import { SourceCache } from '../src/core.js';
import { createObservableObject, autoRefresh, transformAsync, filterOnObservable, transformOnObservable, refCount, expireAfter } from '../src/lifecycle.js';
import { filter, sort, page, bind, toCollection } from '../src/operators.js';
import { innerJoin } from '../src/advanced.js';

test('property notifications compose through filter, sorted paging and array binding', () => {
  const cache=new SourceCache(item=>item.id);const requests=new BehaviorSubject({page:1,size:2});const rows=[];const subscription=cache.connect().pipe(autoRefresh(),filter(item=>item.active),sort((a,b)=>b.score-a.score),page(requests),bind(rows)).subscribe();
  const a=createObservableObject({id:'a',score:10,active:true}),b=createObservableObject({id:'b',score:20,active:true}),c=createObservableObject({id:'c',score:30,active:true}),d=createObservableObject({id:'d',score:40,active:false});cache.addOrUpdate([a,b,c,d]);assert.deepEqual(rows.map(item=>item.id),['c','b']);a.score=50;assert.deepEqual(rows.map(item=>item.id),['a','c']);d.active=true;assert.deepEqual(rows.map(item=>item.id),['a','d']);requests.next({page:2,size:2});assert.deepEqual(rows.map(item=>item.id),['c','b']);cache.removeKey('a');requests.next({page:1,size:2});assert.deepEqual(rows.map(item=>item.id),['d','c']);subscription.unsubscribe();cache.dispose();
});

test('async derived cache joins another live cache, binds rows and rejects stale transformed revisions', async () => {
  const people=new SourceCache(item=>item.id),tasks=new SourceCache(item=>item.id);const jobs=[],rows=[];
  const subscription=people.connect().pipe(transformAsync((item,key,previous,signal)=>new Promise(resolve=>jobs.push({item,signal,resolve}))),innerJoin(tasks.connect(),task=>task.owner,(key,person,task)=>({name:person.name,task:task.title})),bind(rows)).subscribe();
  people.addOrUpdate({id:1,name:'Ada'});tasks.addOrUpdate({id:'t',owner:1,title:'Review'});assert.equal(rows.length,0);jobs[0].resolve({id:1,name:'ADA'});await Promise.resolve();assert.deepEqual(rows,[{name:'ADA',task:'Review'}]);
  people.addOrUpdate({id:1,name:'Grace'});assert.deepEqual(rows,[{name:'ADA',task:'Review'}]);people.addOrUpdate({id:1,name:'Katherine'});assert.equal(jobs[1].signal.aborted,true);jobs[1].resolve({id:1,name:'STALE'});await Promise.resolve();assert.deepEqual(rows,[{name:'ADA',task:'Review'}]);jobs[2].resolve({id:1,name:'KATHERINE'});await Promise.resolve();assert.deepEqual(rows,[{name:'KATHERINE',task:'Review'}]);
  tasks.addOrUpdate({id:'t',owner:1,title:'Approve'});assert.deepEqual(rows,[{name:'KATHERINE',task:'Approve'}]);people.removeKey(1);assert.equal(rows.length,0);subscription.unsubscribe();people.dispose();tasks.dispose();
});

test('observable projections preserve upstream sorted order when item observables change', () => {
  const cache=new SourceCache(item=>item.id),rows=[];
  const subscription=cache.connect().pipe(autoRefresh('rank'),sort((a,b)=>a.rank-b.rank),filterOnObservable(item=>item.visible),transformOnObservable(item=>item.label),bind(rows)).subscribe();
  const a=createObservableObject({id:'a',rank:1,visible:new BehaviorSubject(true),label:new BehaviorSubject('A')}),b=createObservableObject({id:'b',rank:2,visible:new BehaviorSubject(true),label:new BehaviorSubject('B')});cache.addOrUpdate([a,b]);assert.deepEqual(rows,['A','B']);b.rank=0;assert.deepEqual(rows,['B','A']);a.visible.next(false);assert.deepEqual(rows,['B']);a.visible.next(true);assert.deepEqual(rows,['B','A']);subscription.unsubscribe();cache.dispose();
});
