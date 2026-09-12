import assert from 'node:assert/strict';
import { mkdir, mkdtemp, readFile, rm, symlink, writeFile } from 'node:fs/promises';
import { resolve, join, dirname } from 'node:path';
import { spawnSync } from 'node:child_process';
import { createRequire } from 'node:module';

const project = resolve('.');
const pkg = JSON.parse(await readFile(join(project, 'package.json'), 'utf8'));
const args = process.argv.slice(2);
assert(args.length === 0 || (args.length === 2 && args[0] === '--tarball'), 'Usage: node scripts/package-test.mjs [--tarball path]');
const suppliedTarball = args.length ? resolve(args[1]) : undefined;
const npm = process.platform === 'win32' ? 'npm.cmd' : 'npm';
function run(command, args, cwd, label) {
  const result = spawnSync(command, args, { cwd, encoding: 'utf8', timeout: 90_000, env: { ...process.env, npm_config_update_notifier: 'false' } });
  assert.equal(result.status, 0, `${label} failed:\n${result.stdout ?? ''}${result.stderr ?? ''}${result.error ?? ''}`);
  return result.stdout;
}
await mkdir(join(project, 'test-results'), { recursive: true });
const temporary = await mkdtemp(join(project, 'test-results/package-'));
try {
  const result = JSON.parse(run(npm, ['pack', ...(suppliedTarball ? [suppliedTarball] : []), '--ignore-scripts', '--json', '--pack-destination', temporary], project, 'npm pack'))[0];
  assert.equal(result.name, pkg.name); assert.equal(result.version, pkg.version);
  const tarball = join(temporary, result.filename);
  const files = new Set(result.files.map(file => file.path));
  const subpaths = Object.keys(pkg.exports).filter(key => key !== './package.json' && key !== './browser');
  for (const file of ['src/index.js', 'types/index.d.ts', 'dist/cjs/package.json', 'dist/dynamicdata.js', 'dist/dynamicdata.global.js', 'LICENSE', 'NOTICE']) assert(files.has(file), `Missing ${file}`);
  for (const file of files) assert(!file.startsWith('node_modules/') && !file.startsWith('test-results/'), `Unexpected package file ${file}`);
  for (const entry of subpaths) for (const kind of ['import', 'require']) {
    const target = pkg.exports[entry][kind];
    for (const path of [target.default, target.types]) assert(files.has(path.slice(2)), `Missing ${entry} ${kind} export ${path}`);
  }
  const consumer = join(temporary, 'consumer'); await mkdir(consumer);
  await writeFile(join(consumer, 'package.json'), JSON.stringify({ name: 'dynamicdataweb-consumer', version: '1.0.0', private: true, type: 'module' }));
  run(npm, ['install', '--offline', '--ignore-scripts', '--no-audit', '--no-fund', '--package-lock=false', '--legacy-peer-deps', tarball], consumer, 'isolated tarball installation');
  await symlink(join(project, 'node_modules/rxjs'), join(consumer, 'node_modules/rxjs'), process.platform === 'win32' ? 'junction' : 'dir');
  const name = JSON.stringify(pkg.name);
  const behavior = `
const before = Reflect.ownKeys(Observable.prototype);
const root = LOAD(${name});
assert.deepEqual(Reflect.ownKeys(Observable.prototype), before, 'Import must not patch global RxJS prototypes');
for (const subpath of ${JSON.stringify(subpaths.slice(1))}) {
  const module = LOAD(${name} + subpath.slice(1));
  assert(Object.keys(module).length > 0, 'Subpath exports must be usable');
  for (const [key, value] of Object.entries(module)) if (key in root) assert.equal(value, root[key], 'Shared module identity: ' + subpath + '/' + key);
}
const source = new root.SourceCache(item => item.id);
assert(source.connect() instanceof Observable, 'RxJS must remain an external peer');
const visible = [];
const snapshots = [];
const subscription = source.Connect().Filter(item => item.active).Transform(item => ({ id: item.id, label: item.name.toUpperCase() })).Sort((a, b) => a.label.localeCompare(b.label)).Bind(visible).Subscribe();
const snapshotsSubscription = source.connect().pipe(root.toCollection()).subscribe(items => snapshots.push(items));
source.edit(updater => { updater.addOrUpdate({ id: 1, name: 'Beta', active: true }); updater.addOrUpdate({ id: 2, name: 'Alpha', active: false }); });
assert.deepEqual(visible.map(x => x.label), ['BETA']);
source.addOrUpdate({ id: 2, name: 'Alpha', active: true });
assert.deepEqual(visible.map(x => x.label), ['ALPHA', 'BETA']);
assert.equal(snapshots.at(-1).length, 2);
subscription.Dispose(); snapshotsSubscription.unsubscribe(); source.Dispose();
const list = new root.SourceList(); const lists = [];
const listSubscription = list.connect().pipe(root.toCollection()).subscribe(items => lists.push(items));
list.addRange(['a', 'b', 'a']); list.move(2, 0);
assert.deepEqual(lists.at(-1), ['a', 'a', 'b']);
listSubscription.unsubscribe(); list.dispose();
const { Subject } = LOAD('rxjs');
const model = { id: 1, Score: 1, Changed: new Subject() };
const native = new root.SourceCache(item => item.id); const nativeValues = [];
const nativeSubscription = native.Connect().pipe(root.AutoRefresh('Score'), root.Filter(item => item.Score >= 2), root.Bind(nativeValues)).subscribe();
native.AddOrUpdate(model); assert.equal(nativeValues.length, 0);
model.Score = 2; model.Changed.next({ Sender: model, PropertyName: 'Score', Value: 2, OldValue: 1 });
assert.equal(nativeValues[0], model, 'Native property streams must update cache pipelines');
let disposed = 0;
const lifetime = native.Connect().pipe(root.SubscribeMany(() => ({ Dispose() { disposed++; } }))).subscribe();
native.RemoveKey(1); assert.equal(disposed, 1); assert.equal(nativeValues.length, 0);
lifetime.unsubscribe(); nativeSubscription.unsubscribe(); native.Dispose();
`;
  await writeFile(join(consumer, 'consumer.mjs'), `import assert from 'node:assert/strict';\nimport { Observable } from 'rxjs';\n${behavior.replaceAll('LOAD(', 'await import(')}`);
  await writeFile(join(consumer, 'consumer.cjs'), `const assert = require('node:assert/strict');\nconst { Observable } = require('rxjs');\n${behavior.replaceAll('LOAD(', 'require(')}`);
  run(process.execPath, ['consumer.mjs'], consumer, 'ESM installed consumers');
  run(process.execPath, ['consumer.cjs'], consumer, 'CommonJS installed consumers');
  console.log('ESM/CommonJS exports, subpath identity, real RxJS, cache/list/fluent behavior: passed');
  const esmTypes = `import { SourceCache, ChangeSet, filter, transform, toCollection } from ${name};\nimport { Observable } from 'rxjs';\n${subpaths.slice(1).map((p, i) => `import * as module${i} from ${JSON.stringify(pkg.name + p.slice(1))};\nvoid module${i};`).join('\n')}\ninterface Item { id: number; name: string; }\nconst cache = new SourceCache<Item, number>(item => item.id);\nconst changes: Observable<ChangeSet<Item, number>> = cache.connect();\nconst values: Observable<string[]> = changes.pipe(filter(item => item.id > 0), transform(item => item.name), toCollection());\ncache.addOrUpdate({ id: 1, name: 'Ada' });\n// @ts-expect-error Key and item types must be preserved.\ncache.addOrUpdate({ id: 'bad', name: 'Invalid' });\nvoid values; cache.dispose();\n`;
  const cjsTypes = `import dd = require(${name});\n${subpaths.slice(1).map((p, i) => `import module${i} = require(${JSON.stringify(pkg.name + p.slice(1))});\nvoid module${i};`).join('\n')}\nconst source = new dd.SourceCache<{id: number}, number>(item => item.id);\nconst value: dd.Optional<{id: number}> = source.lookup(1);\nvoid value; source.dispose();\n`;
  await writeFile(join(consumer, 'consumer.ts'), esmTypes); await writeFile(join(consumer, 'consumer.cts'), cjsTypes);
  const require = createRequire(join(project, 'package.json'));
  const typescriptJson = require.resolve('typescript/package.json');
  const compiler = join(dirname(typescriptJson), JSON.parse(await readFile(typescriptJson, 'utf8')).bin.tsc);
  run(process.execPath, [compiler, '--ignoreConfig', '--noEmit', '--strict', '--target', 'ES2022', '--module', 'NodeNext', '--moduleResolution', 'NodeNext', '--skipLibCheck', 'consumer.ts', 'consumer.cts'], consumer, 'strict TypeScript ESM/CommonJS consumers');
  console.log(`Verified ${pkg.name}@${pkg.version}: installed tarball, all module exports, behavior and strict TypeScript consumers.`);
} finally { await rm(temporary, { recursive: true, force: true }); }
