import { build } from 'esbuild';
import './generate-index.mjs';
import { mkdir, copyFile } from 'node:fs/promises';
await mkdir('dist',{recursive:true});
await build({entryPoints:['src/index.js'],outfile:'dist/index.cjs',bundle:true,platform:'node',format:'cjs',external:['rxjs','rxjs/*'],sourcemap:true});
await build({entryPoints:['src/index.js'],outfile:'dist/index.js',bundle:true,format:'esm',external:['rxjs','rxjs/*'],sourcemap:true});
await build({entryPoints:['src/browser.js'],outfile:'dist/dynamicdata.js',bundle:true,format:'esm',minify:true,sourcemap:true,legalComments:'eof',target:'es2022'});
await build({entryPoints:['src/browser.js'],outfile:'dist/dynamicdata.global.js',globalName:'DynamicData',bundle:true,format:'iife',minify:true,sourcemap:true,legalComments:'eof',target:'es2022'});
console.log('Built ESM, CommonJS, standalone ESM, and browser global bundles.');
