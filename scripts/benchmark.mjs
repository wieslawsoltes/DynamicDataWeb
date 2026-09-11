import { performance } from 'node:perf_hooks';
import { SourceCache, SourceList, filter, transform, sort, virtualise } from '../src/index.js';
const scenarios=[];
for(const size of [1000,10000,100000]){
 const source=new SourceCache(x=>x.id);let batches=0;const subscription=source.connect().subscribe(()=>batches++);
 const measure=fn=>{const start=performance.now();fn();return +(performance.now()-start).toFixed(3);};
 const ingestMs=measure(()=>source.addOrUpdate(Array.from({length:size},(_,id)=>({id,value:id}))));
 const editMs=measure(()=>source.edit(s=>{for(let i=0;i<1000;i++)s.addOrUpdate({id:i%size,value:i*2});}));
 let projectorCalls=0;
 const projection=source.connect().pipe(filter(x=>x.value%2===0),transform(x=>{projectorCalls++;return x.value;})).subscribe();projectorCalls=0;
 const projectionDeltaMs=measure(()=>source.edit(s=>{for(let i=0;i<100;i++)s.addOrUpdate({id:i%size,value:i*4});}));
 projection.unsubscribe();let ordered;
 const sortInitialMs=measure(()=>{ordered=source.connect().pipe(sort((a,b)=>a.value-b.value),virtualise({startIndex:10,size:30})).subscribe();});
 const sortBatchDeltaMs=measure(()=>source.edit(s=>{for(let i=0;i<100;i++)s.addOrUpdate({id:i%size,value:size+i});}));
 scenarios.push({size,ingestMs,edit1000Ms:editMs,projection100DeltaMs:projectionDeltaMs,projectionFactoryCalls:projectorCalls,sortInitialMs,sort100BatchDeltaMs:sortBatchDeltaMs,batches});
 ordered.unsubscribe();subscription.unsubscribe();source.dispose();
}
console.log(JSON.stringify({runtime:process.version,platform:process.platform,date:new Date().toISOString(),notes:'Single runs; process warm-up and local machine contention affect results. Sorted snapshots and arrays require linear copy/shift work. This is not a cross-library benchmark.',scenarios},null,2));
