import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import { Window } from 'happy-dom';

test('sample app runs all seven workspaces and 73 executable recipes in a DOM environment',async()=>{
 const window=new Window({url:'http://localhost:4173'});
 const document=window.document;
 for(const [name,value]of Object.entries({window,document,location:window.location,localStorage:window.localStorage,navigator:window.navigator}))Object.defineProperty(globalThis,name,{value,configurable:true});
 document.write((await readFile(new URL('../demo/index.html',import.meta.url),'utf8')).replace(/<script[\s\S]*?<\/script>/g,''));
 const errors=[];const savedError=console.error;console.error=(...args)=>errors.push(args.map(String).join(' '));
 const sleep=ms=>new Promise(r=>setTimeout(r,ms));
 const $=selector=>document.querySelector(selector);
 const click=selector=>{assert.ok($(selector),selector+' exists');$(selector).click();};
 const route=async view=>{window.location.hash=view;window.dispatchEvent(new window.HashChangeEvent('hashchange'));await sleep(15);};
 try{
  await import('../demo/app.js');
  assert.equal($('#source-count').textContent,'48');assert.equal(document.querySelectorAll('#data-rows tr').length,8);
  click('#add-item');assert.equal($('#source-count').textContent,'49');
  click('#next-page');assert.match($('#page-label').textContent,/2 \/ 7/);
  $('#search').value='NSTR';$('#search').dispatchEvent(new window.Event('input'));assert.equal($('#match-count').textContent,'4');
  click('#sort-price');click('#refresh-batch');assert.notEqual($('#latency').textContent,'—');
  await route('list');assert.equal($('#list-count').textContent,'4');click('#append');assert.equal($('#list-count').textContent,'5');click('[data-down="0"]');click('[data-up="1"]');click('[data-replace="0"]');assert.match($('#list-items').textContent,/Done/);click('#batch-list');assert.equal($('#list-count').textContent,'6');click('[data-delete="0"]');assert.equal($('#list-count').textContent,'5');
  await route('groups');assert.equal(document.querySelectorAll('#groups .group-card').length,5);assert.equal(document.querySelectorAll('#joined tr').length,6);click('#change-owner');assert.match($('#joined').textContent,/Jamie Park/);click('#move-group');await sleep(1);click('#add-group-item');
  await route('lifecycle');$('#property-price').value='200';$('#property-price').dispatchEvent(new window.Event('input'));assert.equal($('#property-result').textContent,'200.00');click('#add-expiring');assert.match($('#expiring-items').textContent,/Notification 1/);click('#async-race');await sleep(250);assert.match($('#runtime-output').textContent,/Resolved Version 2/);assert.doesNotMatch($('#runtime-output').textContent,/Resolved Version 1/);
  await route('recipes');const recipes=[...document.querySelectorAll('[data-recipe]')];assert.ok(recipes.length===73);for(const recipe of recipes){recipe.click();click('#run-recipe');await sleep(170);assert.notEqual($('#recipe-status').textContent,'ERROR',recipe.textContent);assert.notEqual($('#runtime-output').textContent,'',recipe.textContent+' emitted');}
  await route('performance');$('#workload').value='10000';click('#run-benchmark');await sleep(600);assert.equal($('#bench-size').textContent,'10,000');assert.equal($('#bench-notify').textContent,'3');assert.equal($('#bench-rows').textContent,'30');assert.equal(document.querySelectorAll('.bench-bar').length,4);
  await route('api');await sleep(50);assert.ok(document.querySelectorAll('.api-item').length>250);$('#api-search').value='SourceCache';$('#api-search').dispatchEvent(new window.Event('input'));assert.equal(document.querySelectorAll('.api-item').length,1);
  click('#theme');assert.equal(document.documentElement.dataset.theme,'light');
  assert.deepEqual(errors,[]);
 }finally{await route('api');console.error=savedError;window.happyDOM.abort();}
});
