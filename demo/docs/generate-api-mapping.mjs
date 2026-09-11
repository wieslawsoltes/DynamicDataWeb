#!/usr/bin/env node
/** Runtime-name availability audit; this deliberately does not claim overload parity. */
import { readFile, writeFile, readdir } from 'node:fs/promises';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { dirname, resolve, join } from 'node:path';

const directory = dirname(fileURLToPath(import.meta.url));
const root = resolve(directory, '..');
const upstream = JSON.parse(await readFile(join(directory, 'upstream-api.json'), 'utf8'));
const exportsByName = new Map();
const moduleErrors = [];
const modules = {};
const files = (await readdir(join(root, 'src'))).filter(name => name.endsWith('.js') && !['browser.js'].includes(name));
for (const filename of files) {
  try {
    const module = await import(pathToFileURL(join(root, 'src', filename)).href);
    modules[filename] = module;
    for (const [name, value] of Object.entries(module)) {
      if (!exportsByName.has(name)) exportsByName.set(name, []);
      exportsByName.get(name).push({ module: filename, kind: typeof value });
    }
  } catch (error) { moduleErrors.push({ module: filename, error: String(error) }); }
}
let typesText = '';
const inspectedTypeDeclarations = [];
for (const folder of ['src', 'types', '.']) {
  let entries;
  try { entries = await readdir(join(root, folder)); } catch { continue; }
  for (const name of entries.filter(name => name.endsWith('.d.ts'))) {
    const filename = folder === '.' ? name : `${folder}/${name}`;
    inspectedTypeDeclarations.push(filename);
    typesText += '\n' + await readFile(join(root, filename), 'utf8');
  }
}
const declarationNames = new Set([...typesText.matchAll(/(?:export\s+)?(?:declare\s+)?(?:interface|type|class|enum|namespace|function|const)\s+(\w+)/g)].map(match => match[1]));
const camel = name => name[0].toLowerCase() + name.slice(1);
const equivalentNames = {
  SortAndVirtualize: ['sortAndVirtualize', 'sortAndVirtualise'], Switch: ['switchLatest'], Maximum: ['maximum', 'max'], Minimum: ['minimum', 'min'],
  IgnoreUpdateWhen: ['ignoreUpdateWhen', 'excludeUpdateWhen'],
  Where: ['where', 'filter'], Select: ['select', 'transform'], SelectMany: ['selectMany', 'transformMany'],
  SelectSafe: ['selectSafe', 'transformSafe'], SelectTree: ['selectTree', 'transformToTree'],
  AsObservableChangeSet: ['asObservableChangeSet', 'toObservableChangeSet'],
};
const editing = new Set(['AddOrUpdate','Remove','RemoveKey','RemoveKeys','Refresh','Clear','EditDiff','Add','AddRange','Insert','InsertRange','Move','RemoveAt','RemoveMany','RemoveRange','Replace','ReplaceAt','Clone','IndexOf']);
const limitation = name => {
  if (['Avg','Sum','StdDev','Maximum','Minimum'].includes(name)) return 'JavaScript numeric contract; C# decimal/long/nullable numeric overloads are not independently represented or certified.';
  if (/WhenChanged|WhenPropertyChanged|WhenValueChanged|WhenAnyPropertyChanged|AutoRefresh|FilterOnProperty|GroupOnProperty/.test(name)) return 'Property notification requires the documented observable object/property adapter; arbitrary C# expression trees and plain-object mutation notifications are not equivalent.';
  if (/Bind/.test(name)) return 'Explicit target arrays/collections adapt C# out arguments and .NET collection event types; overload and reset-option equivalence require separate verification.';
  if (/Transform.*Async|AsyncDisposeMany/.test(name)) return 'Promise/Observable and JavaScript cleanup contracts adapt Task/cancellation/IAsyncDisposable; all upstream overload, scheduler and failure semantics are not proven by name availability.';
  if (['SourceCache','SourceList','ChangeAwareCache','ChangeAwareList'].includes(name)) return 'JavaScript collection/event-loop contract; review transaction, replay, preview, duplicate and equality behavior independently.';
  return 'A callable web counterpart is present. Exact C# overload selection, selector shapes, defaults, change traces and lifecycle parity are not established by this name audit.';
};
const findExport = name => [camel(name), name, ...(equivalentNames[name] ?? [])].find(candidate => exportsByName.has(candidate));
const methodGroups = new Map();
for (const method of upstream.methods) {
  const id = `${method.namespace}.${method.owner}.${method.name}`;
  if (!methodGroups.has(id)) methodGroups.set(id, { id, name: method.name, owner: method.owner, family: method.family, upstreamOverloads: [], source: method.url });
  methodGroups.get(id).upstreamOverloads.push(method.signature);
}
const methodMapping = [...methodGroups.values()].map(method => {
  if (method.family === 'Platforms') return { ...method, status:'missing', overloadParity:'not-implemented-as-a-platform-contract', note:'Legacy .NET parallel execution overloads have no audited worker/thread-pool counterpart. The identically named sequential web operator is not counted as implementing this platform contract.' };
  const exportName = method.name === 'Convert' && method.owner.startsWith('Option') && exportsByName.has('convertOptional') ? 'convertOptional' : findExport(method.name);
  if (exportName) return { ...method, status: 'adapted', webExport: exportName, locations: exportsByName.get(exportName), overloadParity: 'not-certified', note: limitation(method.name) };
  const members = [];
  for (const [filename,module] of Object.entries(modules)) {
    for (const [name, value] of Object.entries(module)) {
      if (value && name === method.owner && (typeof value === 'object' || typeof value === 'function')) {
        const member = [camel(method.name), method.name].find(candidate => typeof value[candidate] === 'function');
        if (member) members.push({ module:filename,type:name,member,kind:'static' });
      }
      if (typeof value !== 'function') continue;
      const member = camel(method.name);
      if ((editing.has(method.name) && ['SourceCache','SourceList','ChangeAwareCache','ChangeAwareList'].includes(name)) || method.owner.startsWith('Option')) {
        if (value.prototype && typeof value.prototype[member] === 'function') members.push({ module: filename, type: name, member, kind: 'instance' });
        else if (typeof value[member] === 'function') members.push({ module: filename, type: name, member, kind: 'static' });
      }
      if (['Ascending','Descending'].includes(method.name) && name === 'SortExpressionComparer' && typeof value[method.name] === 'function') members.push({module: filename,type:name,member:method.name,kind:'static'});
    }
  }
  if (members.length) return { ...method, status: 'adapted', webMembers: members, overloadParity: 'not-certified', note: 'Available through the listed collection/helper members; the upstream extension-call form is adapted. '+limitation(method.name) };
  return { ...method, status: 'missing', overloadParity: 'not-implemented-as-a-named-counterpart', note: 'No matching runtime export or audited collection/helper member was found in the built source modules.' };
});
const typeAlternatives = { 'DynamicData.Change`1': ['ListChange'], 'DynamicData.Change`2': ['Change'], 'DynamicData.ChangeSet`1':['ChangeSet'], 'DynamicData.ChangeSet`2':['ChangeSet'], 'DynamicData.ChangeSet`3':['ChangeSet'], 'DynamicData.ChangeAwareList`1':['ChangeAwareList'], 'DynamicData.ChangeAwareCache`2':['ChangeAwareCache'] };
const typeMapping = upstream.types.map(type => {
  const candidates = [...(typeAlternatives[type.id]??[]),type.name];
  const runtimeName = candidates.find(name => exportsByName.has(name));
  const declarationName = candidates.find(name => declarationNames.has(name));
  return { upstreamId:type.id, name:type.name, kind:type.kind, genericArity:type.genericArity, status: runtimeName||declarationName?'adapted':'missing', ...(runtimeName ? {webRuntimeExport:runtimeName,locations:exportsByName.get(runtimeName)} : {}), ...(declarationName ? {webTypeDeclaration:declarationName}:{}), memberParity:'not-certified', note: runtimeName||declarationName?'Representation exists; complete upstream member, inheritance and generic-arity compatibility is not established by this declaration audit.':'No same-name or explicitly mapped runtime/type declaration found.' };
});
const countStatus = array => array.reduce((counts,item)=>(counts[item.status]=(counts[item.status]??0)+1,counts),{});
const report = {
  schemaVersion:1, upstreamCommit:upstream.upstream.commit,
  policy:'This is a runtime-export/type-declaration availability map, not a behavior conformance score. adapted means a web counterpart exists; missing means this scanner found no named counterpart. Overload and member parity are not certified. Re-run after source changes.',
  inspectedModules:files, inspectedTypeDeclarations, moduleErrors, packageEntrypoint: { imported: !!modules['index.js'], exports: modules['index.js'] ? Object.keys(modules['index.js']).length : 0 }, summary:{runtimeExportNames:exportsByName.size, methodOwnerNamePairs:methodMapping.length, methods:countStatus(methodMapping), types:countStatus(typeMapping)}, methods:methodMapping,types:typeMapping
};
await writeFile(join(directory,'api-mapping.json'),JSON.stringify(report,null,2)+'\n');
const coreMissing = methodMapping.filter(m => ['ObservableCacheEx','ObservableListEx'].includes(m.owner)&&m.status==='missing');
const lines = ['# Web API availability map','',report.policy,'',`Upstream revision: \`${report.upstreamCommit}\`.`, '', 'Recreate this report with `node docs/generate-api-mapping.mjs` after building or editing the source. The inventory source is [`upstream-api.json`](upstream-api.json), and the full per-name results are in [`api-mapping.json`](api-mapping.json).', '', '| Item | Count |','| --- | ---: |',`| Source-module runtime export names (including aliases) | ${report.summary.runtimeExportNames} |`,`| Upstream method owner/name pairs | ${methodMapping.length} |`,`| Methods with adapted counterparts | ${report.summary.methods.adapted??0} |`,`| Methods without named counterparts | ${report.summary.methods.missing??0} |`,`| Public types with mapped representations | ${report.summary.types.adapted??0} |`,`| Public types without mapped representations | ${report.summary.types.missing??0} |`,'', '## Cache/list operator names without named counterparts',''];
if(coreMissing.length) {lines.push('| Owner | Name | Overloads |','| --- | --- | ---: |');for(const m of coreMissing) lines.push(`| \`${m.owner}\` | [\`${m.name}\`](${m.source}) | ${m.upstreamOverloads.length} |`);}
else lines.push('All cache/list operator names have a mapped web counterpart. This is name-level availability; C# overload, type-member and behavioral compatibility must be assessed separately.');
lines.push('','## Other methods without named counterparts','','| Owner | Name | Overloads |','| --- | --- | ---: |');
for(const m of methodMapping.filter(m=>!['ObservableCacheEx','ObservableListEx'].includes(m.owner)&&m.status==='missing'))lines.push(`| \`${m.owner}\` | [\`${m.name}\`](${m.source}) | ${m.upstreamOverloads.length} |`);
lines.push('','## Adaptations needing per-contract assessment','','- C# overload families are folded into JavaScript functions and options. Numeric aggregation, source-vs-stream overloads, selectors, optional values, and comparator streams need overload-specific validation.','- .NET property/collection notifications, expression trees, binding out parameters, decimal arithmetic, thread synchronization, and Task cancellation have explicit web contracts.','- Matching final collection contents is insufficient for change-trace parity. Use the fixture-oriented checklist in [`UPSTREAM-API.md`](UPSTREAM-API.md).','- Interface/type declarations and convenience aliases do not prove full runtime inheritance, member signatures, or platform-specific API support.','');
if(moduleErrors.length)lines.push('Module import errors occurred during this audit; inspect `moduleErrors` in the JSON before using the counts. Source-module availability does not prove that the package entrypoint imports successfully.','');
else lines.push(`Package entrypoint imports successfully with ${report.packageEntrypoint.exports} exports. Type declarations inspected: ${inspectedTypeDeclarations.map(name=>'\`'+name+'\`').join(', ') || 'none'}.`,'');
await writeFile(join(directory,'API-MAPPING.md'),lines.join('\n'));
console.log(JSON.stringify(report.summary,null,2));
if(moduleErrors.length) console.error(JSON.stringify(moduleErrors,null,2));
