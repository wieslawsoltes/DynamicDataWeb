import { build } from 'esbuild';
import './generate-index.mjs';
import { mkdir, cp, readdir, writeFile } from 'node:fs/promises';
await mkdir('dist/cjs', { recursive: true });
const entries = (await readdir('src')).filter(name => name.endsWith('.js') && name !== 'browser.js').map(name => `src/${name}`);
// Preserve the module graph: independently bundled subpaths would duplicate cache
// classes and the per-instance fluent registration state within CommonJS consumers.
await build({ entryPoints: entries, outdir: 'dist/cjs', bundle: false, platform: 'node', format: 'cjs', sourcemap: true, target: 'es2022' });
await cp('types', 'dist/cjs', { recursive: true });
await writeFile('dist/cjs/package.json', '{"type":"commonjs"}\n');
await writeFile('dist/index.cjs', "module.exports = require('./cjs/index.js');\n");
await build({ entryPoints: ['src/index.js'], outfile: 'dist/index.js', bundle: true, format: 'esm', external: ['rxjs', 'rxjs/*'], sourcemap: true });
await build({ entryPoints: ['src/browser.js'], outfile: 'dist/dynamicdata.js', bundle: true, format: 'esm', minify: true, sourcemap: true, legalComments: 'eof', target: 'es2022' });
await build({ entryPoints: ['src/browser.js'], outfile: 'dist/dynamicdata.global.js', globalName: 'DynamicData', bundle: true, format: 'iife', minify: true, sourcemap: true, legalComments: 'eof', target: 'es2022' });
console.log('Built ESM, shared CommonJS modules, standalone ESM, and browser global bundles.');
