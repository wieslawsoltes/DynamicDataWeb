/** Web-specific compatibility helpers and C#-style fluent instance decoration. */
import { Observable, Subscription, animationFrameScheduler, asyncScheduler, observeOn as rxObserveOn, subscribeOn as rxSubscribeOn, map, distinctUntilChanged } from 'rxjs';
import { SourceCache, SourceList, Optional, ChangeSet, setObservableDecorator } from './core.js';
import { sort, bind } from './operators.js';
import { count, min, max } from './advanced.js';

const compare = (a,b) => a == null ? b == null ? 0 : -1 : b == null ? 1 : a < b ? -1 : a > b ? 1 : 0;
export const SortDirection = Object.freeze({Ascending:'ascending',Descending:'descending'});
export class SortExpressionComparer {
  constructor(expressions=[]) { this.expressions=Array.from(expressions); }
  compare(a,b) { for(const {selector,direction} of this.expressions){const n=compare(selector(a),selector(b));if(n)return direction==='descending'?-n:n;}return 0; }
  Compare(a,b){return this.compare(a,b);}
  thenBy(selector,direction='ascending'){return new SortExpressionComparer([...this.expressions,{selector,direction}]);}
  thenByAscending(selector){return this.thenBy(selector);}
  thenByDescending(selector){return this.thenBy(selector,'descending');}
  ThenBy(selector,direction){return this.thenBy(selector,direction);}
  ThenByAscending(selector){return this.thenByAscending(selector);}
  ThenByDescending(selector){return this.thenByDescending(selector);}
  static ascending(selector){return new SortExpressionComparer([{selector,direction:'ascending'}]);}
  static descending(selector){return new SortExpressionComparer([{selector,direction:'descending'}]);}
  static Ascending(selector){return this.ascending(selector);}
  static Descending(selector){return this.descending(selector);}
}
export class PageRequest {
  constructor(page=1,size=25){if(!Number.isInteger(page)||page<1||!Number.isInteger(size)||size<1)throw new RangeError('Page and size must be positive integers');this.page=page;this.size=size;Object.freeze(this);}
  get Page(){return this.page;} get Size(){return this.size;}
  equals(other){return other?.page===this.page&&other?.size===this.size;}
  static get Default(){return new PageRequest();}
}
export class VirtualRequest {
  constructor(startIndex=0,size=25){if(!Number.isInteger(startIndex)||startIndex<0||!Number.isInteger(size)||size<1)throw new RangeError('Start index must be nonnegative and size positive');this.startIndex=startIndex;this.size=size;Object.freeze(this);}
  get StartIndex(){return this.startIndex;}get Size(){return this.size;}
  equals(other){return other?.startIndex===this.startIndex&&other?.size===this.size;}
  static get Default(){return new VirtualRequest();}
}
export class BindingOptions { constructor(options={}){this.resetThreshold=25;Object.assign(this,options);} static neverFireReset(useReplaceForUpdates=true){return new BindingOptions({resetThreshold:Infinity,useReplaceForUpdates});} static NeverFireReset(value){return this.neverFireReset(value);} }
export class SortAndBindOptions extends BindingOptions { constructor(options={}){super(options);} }
export const SortOptions=Object.freeze({None:0,UseBinarySearch:1,ComparesImmutableValuesOnly:2});
export const DynamicDataOptions=Object.freeze({binding:new BindingOptions(),scheduler:asyncScheduler});
/** Framework-neutral observable ordered collection. Bind via replaceAll or .items snapshots. */
export class ObservableCollectionExtended extends SourceList {
  constructor(items=[]){super();if(items.length)this.addRange(items);}
  replaceAll(items){return this.edit(list=>{list.clear();list.addRange(items);});}
  ReplaceAll(items){return this.replaceAll(items);}
  suspendCountNotifications(){return this.suspendCount();}
}
export const minimum=min;
export const maximum=max;
export const isEmpty=()=>source=>source.pipe(count(),map(n=>n===0),distinctUntilChanged());
export const isNotEmpty=()=>source=>source.pipe(count(),map(n=>n!==0),distinctUntilChanged());
export const addOrUpdate=(source,...args)=>source.addOrUpdate(...args);
export const clear=source=>source.clear();
export const refresh=(source,...args)=>source.refresh(...args);
export const remove=(source,...args)=>source.remove(...args);
export const removeKeys=(source,...args)=>source.removeKeys(...args);
export function editDiff(source,items,equality=Object.is){
 const incoming=Array.from(items);if(source instanceof SourceCache){const byKey=new Map(incoming.map(x=>[source.getKey(x),x]));return source.edit(c=>{c.removeKeys(c.keys.filter(k=>!byKey.has(k)));for(const [key,value]of byKey){const old=c.lookup(key);if(!old.hasValue||!equality(old.value,value))c.addOrUpdate(value);}});}
 return source.edit(list=>{for(let i=list.count-1;i>=incoming.length;i--)list.removeAt(i);for(let i=0;i<incoming.length;i++){if(i>=list.count)list.add(incoming[i]);else if(!equality(list.items[i],incoming[i]))list.replaceAt(i,incoming[i]);}});
}
export const observeOn=scheduler=>rxObserveOn(scheduler);
export const subscribeOn=scheduler=>rxSubscribeOn(scheduler);
export const observeOnDispatcher=()=>rxObserveOn(typeof requestAnimationFrame==='function'?animationFrameScheduler:asyncScheduler);

const decorated = new WeakSet();
let operatorRegistry={};
export function fluent(observable){
 if(!(observable instanceof Observable)&&!observable?.subscribe)return observable;
 if(decorated.has(observable))return observable;decorated.add(observable);
 const originalPipe=observable.pipe.bind(observable);
 Object.defineProperty(observable,'pipe',{value:(...operators)=>fluent(originalPipe(...operators)),configurable:true});
 Object.defineProperty(observable,'Pipe',{value:(...operators)=>fluent(originalPipe(...operators)),configurable:true});
 Object.defineProperty(observable,'Subscribe',{value:(...args)=>{const sub=observable.subscribe(...args);if(!sub.Dispose)Object.defineProperty(sub,'Dispose',{value:()=>sub.unsubscribe()});return sub;},configurable:true});
 for(const [name,operator] of Object.entries(operatorRegistry)){
   if(typeof operator!=='function'||!/^[A-Z]/.test(name)||name in observable)continue;
   Object.defineProperty(observable,name,{value:(...args)=>{const result=operator(...args);return typeof result==='function'?fluent(result(observable)):result;},configurable:true});
 }
 return observable;
}
export function installFluentOperators(registry){operatorRegistry=registry;setObservableDecorator(fluent);}
