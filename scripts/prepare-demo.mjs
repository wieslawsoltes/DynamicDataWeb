import { copyFile, mkdir, cp, readFile, writeFile } from 'node:fs/promises';
await mkdir('demo',{recursive:true});
for(const name of ['dynamicdata.js','dynamicdata.js.map'])await copyFile(`dist/${name}`,`demo/${name}`);
await copyFile('README.md','demo/README.md');
await copyFile('docs/COMPATIBILITY.md','demo/COMPATIBILITY.md');
await cp('docs','demo/docs',{recursive:true});
const dd=await import('../src/index.js');
await writeFile('demo/api-catalog.json',JSON.stringify({exports:Object.keys(dd)},null,2));
console.log('Demo assets synchronized.');
