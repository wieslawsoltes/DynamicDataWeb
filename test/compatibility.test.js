import test from 'node:test';
import assert from 'node:assert/strict';
import { Observable, of } from 'rxjs';
import {SourceCache,SourceList,SortExpressionComparer,PageRequest,VirtualRequest,ObservableCollectionExtended,filter,sort,bind,fluent,editDiff,Change,Optional,ChangeSet,transform} from '../src/index.js';

test('Pascal fluent operators preserve RxJS identity and do not change global Observable prototype',()=>{
 const source=new SourceCache(x=>x.id);const values=[];
 const connection=source.Connect();assert.ok(connection instanceof Observable);assert.equal(Observable.prototype.Filter,undefined);
 const sub=connection.Filter(x=>x.score>1).Transform(x=>x.score*10).ToCollection().Subscribe(x=>values.push(x));
 source.AddOrUpdate([{id:1,score:1},{id:2,score:2}]);assert.deepEqual(values.at(-1),[20]);
 source.AddOrUpdate({id:1,score:3});assert.deepEqual(values.at(-1),[20,30]);sub.Dispose();source.Dispose();
});
test('multi-expression comparer, Pascal change metadata and ordered observable binding compose',()=>{
 const source=new SourceCache(x=>x.id),bound=new ObservableCollectionExtended();let output;
 const comparer=SortExpressionComparer.Ascending(x=>x.group).ThenByDescending(x=>x.score);
 const sub=source.Connect().Sort(comparer).Bind(bound).Subscribe(c=>output=c);
 source.AddOrUpdate([{id:1,group:'A',score:1},{id:2,group:'A',score:3},{id:3,group:'B',score:2}]);
 assert.deepEqual(bound.items.map(x=>x.id),[2,1,3]);assert.ok(output[0] instanceof Change);assert.equal(output[0].Reason,'add');assert.equal(output[0].Current.id,2);
 source.AddOrUpdate({id:2,group:'B',score:0});assert.deepEqual(bound.items.map(x=>x.id),[1,3,2]);
 sub.unsubscribe();source.dispose();bound.dispose();
});
test('request validation, edit diff, independent decorated inputs and empty optional behavior',()=>{
 assert.throws(()=>new PageRequest(0,10));assert.throws(()=>new VirtualRequest(-1,5));
 assert.equal(new PageRequest(2,10).Page,2);assert.equal(new VirtualRequest(4,5).StartIndex,4);
 const source=new SourceCache(x=>x.id);source.addOrUpdate([{id:1,a:1},{id:2,a:2}]);editDiff(source,[{id:2,a:3},{id:3,a:4}],(a,b)=>a.a===b.a);assert.deepEqual(source.items,[{id:2,a:3},{id:3,a:4}]);
 let result;fluent(of(new ChangeSet([{reason:'add',key:1,current:2}]))).Transform(x=>x+2).ToCollection().Subscribe(x=>result=x);assert.deepEqual(result,[4]);assert.equal(Optional.some(undefined).hasValue,true);assert.throws(()=>Optional.none().value);source.dispose();
});
