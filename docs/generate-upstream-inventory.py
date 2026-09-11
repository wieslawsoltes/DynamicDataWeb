#!/usr/bin/env python3
"""Recreate the source-declaration API inventory from a checked-out DynamicData tree.

Usage: python docs/generate-upstream-inventory.py /path/to/DynamicData
This is a declaration scanner, not a C# compiler. It unions conditional branches
and explicitly reports that policy rather than claiming a target-specific API.
"""
import collections
import datetime
import json
from pathlib import Path
import re
import subprocess
import sys

repo = Path(sys.argv[1]).resolve()
source = repo / 'src/DynamicData'
out = Path(__file__).parent

def mask(text):
    # Keep character positions, including newlines, so declarations retain source locations.
    pattern = r'//[^\n]*|/\*[\s\S]*?\*/|@"(?:[^"]|"")*"|"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\''
    def blank(match):
        return ''.join('\n' if c == '\n' else ' ' for c in match.group())
    return re.sub(pattern, blank, text)

def without_attributes(text):
    # C# attributes can contain parentheses, including in generic type arguments.
    chars = list(text)
    depth = 0
    for i,c in enumerate(text):
        if c == '[': depth += 1
        if depth and c != '\n': chars[i] = ' '
        if c == ']': depth -= 1
    return ''.join(chars)

def close_pair(text, start, left='(', right=')'):
    depth = 0
    for i in range(start, len(text)):
        if text[i] == left: depth += 1
        elif text[i] == right:
            depth -= 1
            if depth == 0: return i
    raise ValueError('Unclosed declaration')

def generic_arity(text):
    return len([x for x in text.strip('<> ').split(',') if x.strip()])

def source_url(rel, line):
    return f'https://github.com/reactivemarbles/DynamicData/blob/{commit}/src/DynamicData/{rel}#L{line}'

commit = subprocess.check_output(['git','-C',str(repo),'rev-parse','HEAD'], text=True).strip()
commit_date = subprocess.check_output(['git','-C',str(repo),'log','-1','--format=%cI'], text=True).strip()
version_text = (repo/'version.json').read_text()
version_pattern = re.search(r'"version"\s*:\s*"([^"]+)"',version_text).group(1)
methods = []
types = {}
file_data = []
missed_static = []

for path in sorted(source.rglob('*.cs')):
    text = path.read_text(encoding='utf-8-sig')
    clean = without_attributes(mask(text))
    rel = str(path.relative_to(source))
    ns_match = re.search(r'\bnamespace\s+([\w.]+)', clean)
    namespace = ns_match.group(1) if ns_match else ''
    type_matches = list(re.finditer(r'(?m)^[ \t]*public\s+(?:(?:static|partial|sealed|abstract|readonly|ref|unsafe)\s+)*(class|interface|enum|struct|record(?:\s+struct|\s+class)?)\s+(\w+)(\s*<[^\n{(]+>)?',clean))
    namespace_declaration = re.search(r'\bnamespace\s+[\w.]+\s*([;{])', clean)
    namespace_depth = 1 if namespace_declaration and namespace_declaration.group(1) == '{' else 0
    type_matches = [m for m in type_matches if clean[:m.start()].count('{') - clean[:m.start()].count('}') == namespace_depth]
    if not type_matches:
        continue
    file_types = []
    for m in type_matches:
        arity = generic_arity(m.group(3) or '')
        key = f'{namespace}.{m.group(2)}' + (f'`{arity}' if arity else '')
        line = text.count('\n',0,m.start())+1
        entry = types.setdefault(key, dict(id=key,name=m.group(2),genericArity=arity,kind=m.group(1),namespace=namespace,area=rel.split('/')[0] if '/' in rel else 'Root',declarations=[]))
        entry['declarations'].append(dict(file=rel,line=line,url=source_url(rel,line)))
        file_types.append(m.group(2))
    for m in re.finditer(r'\bpublic\s+static\s+',clean):
        tail = clean[m.end():]
        # First body/assignment/semicolon is a hard boundary. Generic constraints
        # occur after parameters and are retained in the readable declaration.
        boundary = re.search(r'[;={}]',tail)
        header = tail[:boundary.start()] if boundary else tail
        angle_depth = 0
        first_open = -1
        for pos,c in enumerate(header):
            if c == '<': angle_depth += 1
            elif c == '>': angle_depth -= 1
            elif c == '(' and angle_depth == 0:
                first_open = pos
                break
        prefix = header[:first_open].rstrip() if first_open >= 0 else ''
        method_generic = ''
        if prefix.endswith('>'):
            angle_depth = 0
            for pos in range(len(prefix)-1, -1, -1):
                if prefix[pos] == '>': angle_depth += 1
                elif prefix[pos] == '<':
                    angle_depth -= 1
                    if angle_depth == 0:
                        method_generic = prefix[pos:]
                        prefix = prefix[:pos].rstrip()
                        break
        mm = re.search(r'([A-Za-z_]\w*)$',prefix)
        if not mm:
            if not re.match(r'(?:partial\s+)?class\b',tail):
                missed_static.append(dict(file=rel,line=text.count('\n',0,m.start())+1,declaration=text[m.start():m.start()+130].split('\n')[0]))
            continue
        name = mm.group(1)
        if name in ('operator','class') or re.search(r'\boperator\b',prefix[:mm.start()]):
            continue
        open_index = m.end()+first_open
        end_index = close_pair(clean,open_index)
        parameters = text[open_index+1:end_index]
        after = clean[end_index+1:]
        body = re.search(r'=>|[;{]',after)
        signature_end = end_index+1+(body.start() if body else 0)
        signature = re.sub(r'\s+',' ',text[m.start():signature_end]).strip()
        line = text.count('\n',0,m.start())+1
        owner = next((tm.group(2) for tm in reversed(type_matches) if tm.start()<m.start()),file_types[0])
        area = rel.split('/')[0] if '/' in rel else 'Root'
        if owner == 'ObservableCacheEx': family='Cache'
        elif owner == 'ObservableListEx': family='List'
        else: family=area
        method = dict(name=name,owner=owner,namespace=namespace,family=family,genericArity=generic_arity(method_generic),extension=bool(re.match(r'\s*this\s',clean[open_index+1:end_index])),file=rel,line=line,signature=signature,url=source_url(rel,line))
        methods.append(method)
    file_data.append(dict(file=rel,types=file_types))

families={}
for method in methods:
    fam=families.setdefault(method['family'],{})
    fam.setdefault(method['name'],[]).append(method)
summary=[]
for family,names in sorted(families.items()):
    summary.append(dict(family=family,uniqueMethodNames=len(names),overloadDeclarations=sum(map(len,names.values())),methods=[dict(name=name,overloadDeclarations=len(entries),extensionDeclarations=sum(e['extension'] for e in entries),owners=sorted(set(e['owner'] for e in entries)),files=sorted(set(e['file'] for e in entries))) for name,entries in sorted(names.items())]))
core=[m for m in methods if m['owner'] in ('ObservableCacheEx','ObservableListEx')]
result=dict(schemaVersion=1,upstream=dict(repository='https://github.com/reactivemarbles/DynamicData',commit=commit,commitDate=commit_date,versionPattern=version_pattern,license='MIT',licenseCopyright='Copyright (c) Roland Pheasant 2011-2022',sourceHeaderCopyright='Copyright (c) 2011-2025 Roland Pheasant',licenseUrl=f'https://github.com/reactivemarbles/DynamicData/blob/{commit}/LICENSE',systemReactiveVersion='6.1.0'),methodology=dict(scope='All src/DynamicData C# public top-level types and public static named methods in those files; includes public types in Internal namespaces.',counting='One overload declaration per public static named method. Extension receivers and overload signatures are retained. Partial type declarations are deduplicated by namespace, name and generic arity. Operator overloads, constructors, properties, instance methods and non-public declarations are not counted as operators.',conditionalCompilation='Source union of all branches and platforms, not an evaluated target-framework API. AsyncDisposeMany is conditionally included by DynamicData.csproj. No assumptions are made that every public source declaration ships for every target.',limitations='Declaration scanner, not Roslyn. Name-level coverage is not behavioral or overload compatibility. The inventory does not count members inherited from framework types.'),totals=dict(publicTypes=len(types),publicTypeDeclarations=sum(len(t['declarations']) for t in types.values()),publicStaticMethodDeclarations=len(methods),publicStaticUniqueNames=len(set(m['name'] for m in methods)),extensionMethodDeclarations=sum(m['extension'] for m in methods),coreOperatorDeclarations=len(core),coreOperatorUniqueNames=len(set(m['name'] for m in core))),families=summary,types=sorted(types.values(),key=lambda t:t['id']),methods=sorted(methods,key=lambda m:(m['family'],m['name'],m['file'],m['line'])))
(out/'upstream-api.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result['totals'],indent=2))
print('Families:',json.dumps([{k:v for k,v in f.items() if k!='methods'} for f in summary],indent=2))
print('Non-method public static declarations:',json.dumps(missed_static,indent=2))

# Human-readable companion. Exact overload declarations stay in JSON to keep
# this document navigable; every name links to an immutable source revision.
lines = [
'# DynamicData upstream API inventory',
'',
f'This inventory is pinned to DynamicData commit [`{commit}`](https://github.com/reactivemarbles/DynamicData/tree/{commit}), committed {commit_date}. The checkout declares version pattern `{version_pattern}`; it does not identify an exact published NuGet package version. Its System.Reactive dependency is 6.1.0. The web implementation uses RxJS.',
'',
'[`upstream-api.json`](upstream-api.json) is the machine-readable baseline: exact public static method signatures, overload declaration counts, generic arity, defining files, and immutable source links. [`generate-upstream-inventory.py`](generate-upstream-inventory.py) regenerates it from an upstream checkout.',
'',
'## What the counts mean',
'',
'An operator name is counted once per extension owner, even when C# has many overloads. A declaration is one C# overload, including nullable numeric variants and overloads whose first argument is a different observable kind. Neither an exported JavaScript function nor a PascalCase alias proves that all corresponding C# overloads and behaviors have been implemented.',
'',
'Public types are deduplicated by namespace, name, and generic arity. The inventory includes public types in an `Internal` namespace because they are public declarations. It unions conditional source branches; it does not pretend that every declaration is compiled into every .NET target. In particular, the project conditionally removes `AsyncDisposeMany` on targets without `IAsyncDisposable`.',
'',
'Constructors, instance methods, properties, C# operator overloads, non-public types, framework-inherited members, benchmarks, samples, and separate test projects are excluded from the static-operator declaration counts. Public type names are inventoried separately. Public test-support helpers under `src/DynamicData/Cache/Tests` and `List/Tests` remain included because they belong to the library source tree. All public static named declarations in every `Cache/ObservableCacheEx*.cs` and `List/ObservableListEx*.cs` file were cross-checked against declaration counts in those source files.',
'',
'| Scope | Unique method names | Overload declarations |',
'| --- | ---: | ---: |',
]
for owner in ('ObservableCacheEx','ObservableListEx'):
    entries=[m for m in methods if m['owner']==owner]
    lines.append(f'| `{owner}` | {len(set(m["name"] for m in entries))} | {len(entries)} |')
lines += [f'| Cache/list operator union | {len(set(m["name"] for m in core))} | {len(core)} |',f'| All inventoried public static methods | {result["totals"]["publicStaticUniqueNames"]} | {len(methods)} |','',f'There are **{len(types)} distinct public types** across **{result["totals"]["publicTypeDeclarations"]} declarations**. Static methods include **{result["totals"]["extensionMethodDeclarations"]} extension-method overload declarations**.','',
'## Cache and list operator names','',
'Counts below are per extension owner, avoiding ambiguity between cache/list methods that share a name. A dash means that owner does not declare the name at this revision. Auxiliary extensions may still supply a similarly named method.','',
'| Name | Cache overloads | List overloads |','| --- | ---: | ---: |']
for name in sorted(set(m['name'] for m in core)):
    selected=[m for m in core if m['name']==name]
    cache=sum(m['owner']=='ObservableCacheEx' for m in selected)
    listing=sum(m['owner']=='ObservableListEx' for m in selected)
    lines.append(f'| [`{name}`]({selected[0]["url"]}) | {cache or "—"} | {listing or "—"} |')
lines += ['','## Additional public static API','',
'These include aggregation, query aliases, property/collection binding, cache/list editing helpers, optional values, diagnostics, and observable factories. Counts here exclude methods already listed in `ObservableCacheEx` and `ObservableListEx`.','',
'| Area / owner | Method | Overloads |','| --- | --- | ---: |']
extra=collections.defaultdict(list)
for m in methods:
    if m['owner'] not in ('ObservableCacheEx','ObservableListEx'):
        extra[(m['family'],m['owner'],m['name'])].append(m)
for (family,owner,name),entries in sorted(extra.items()):
    lines.append(f'| {family} / `{owner}` | [`{name}`]({entries[0]["url"]}) | {len(entries)} |')
lines += ['','## Public types','',
'The number following a backtick in the JSON `id` is CLR generic arity. For example, `Change` with one generic parameter is the list type; `Change` with two generic parameters is the keyed cache type. A JavaScript export name alone cannot preserve this distinction.','',
'| Namespace | Public type | Kind | Defining area |','| --- | --- | --- | --- |']
for t in sorted(types.values(),key=lambda t:t['id']):
    display=t['name']+(f'〈{t["genericArity"]} type parameters〉' if t['genericArity'] else '')
    lines.append(f'| `{t["namespace"]}` | [`{display}`]({t["declarations"][0]["url"]}) | {t["kind"]} | {t["area"]} |')
lines += ['','## API adaptation contract','',
'| C# construct | JavaScript / RxJS requirement | Compatibility consequence |',
'| --- | --- | --- |',
'| `IObservable<T>` and System.Reactive | Actual RxJS `Observable<T>` and RxJS subscriptions/schedulers | Downstream streams must work with ordinary RxJS operators, marble testing, errors, completion, and unsubscription. |',
'| Extension methods | Pipeable functions; PascalCase source-first wrappers where documented | `source.pipe(Filter(predicate))` and `ObservableCacheEx.Filter(source, predicate)` are different call contracts and require explicit examples. |',
'| Generic overloads | Generic TypeScript declarations plus documented runtime dispatch | Cache/list routing, selector argument order, optional predicates, comparer streams, error handlers, and options objects need individual coverage. |',
'| `Change<T>` / `Change<T,K>` and `ChangeSet<T>` / `ChangeSet<T,K>` | Distinct named list/cache shapes or a discriminated generic representation | List duplicate identity and indexes must survive; a map keyed by the item cannot represent all list operations. |',
'| `out` collection arguments | Explicit target arrays/collections or returned binding objects | The return contract differs from C# and should be described per bind overload. |',
'| `IComparer<T>` / `IEqualityComparer<T>` | Comparator/equality/key-selector functions or adapters | JS `Map` uses SameValueZero equality; C# structural equality and custom hash comparers do not transfer automatically. |',
'| `Optional<T>` | A tagged presence/value object | Missing must remain distinguishable from `undefined`, `null`, `0`, and `false`. Join optional arguments and update previous-values require the same distinction. |',
'| `INotifyPropertyChanged` and expression trees | Explicit observable properties, observable model wrappers, property names/paths, or selector subscriptions | Plain JS object mutation emits no event. Nested property replacement must detach old subscriptions and attach new ones. |',
'| `INotifyCollectionChanged`, `BindingList`, read-only collections | Observable collection adapter and target mutation protocol | Browser DOM binding is an additional adapter; it does not replicate .NET collection event types by name. |',
'| `Task<T>` / cancellation / async disposal | Promise or Observable factories, cancellation signals when supported, awaited async cleanup | Stale completions must not resurrect removed items; unsubscription must release pending subscriptions and resources. |',
'| `TimeSpan`, `DateTimeOffset`, `IScheduler` | Millisecond durations and RxJS `SchedulerLike` | Virtual scheduler tests must establish expiration, batching boundaries, and rescheduling semantics. |',
'| `int`, `long`, `float`, `double`, `decimal` and nullable variants | Documented JS numeric contract; explicit bigint/decimal integration when offered | One `number` aggregator does not preserve 64-bit integer precision, decimal arithmetic, or every overflow rule. |',
'| Locks, scheduler synchronization, `Interlocked` | Serialized event-loop delivery and explicit reentrancy rules | Single-threaded JS avoids some races but nested observer edits and synchronous inner observables still require queueing tests. |',
'| Target-specific parallel operators | Explicit worker or concurrency API if supplied | Promise concurrency is not CPU parallelism or a faithful replacement for .NET thread-pool operators. |',
'',
'## Parity verification that distinguishes implementations',
'',
'For each operator family, compare both the resulting collection and the emitted change trace. A final-state-only check misses duplicate removals, incorrect previous values, lost refresh events, bad indexes, extra emissions, and incorrect completion or teardown.',
'',
'1. **Source transactions and preview.** Check initial connection snapshots, empty suppression, one output for a multi-edit transaction, add/update/remove of the same key inside one transaction, nested edits, callback exceptions, subscription-time edits, and preview observing the pre-edit state. Use `Cache/SourceCacheFixture.cs`, `Cache/FilterOnConnectFixture.cs`, `List/SourceListFixture.cs`, and `List/SourceListPreviewFixture.cs` as the starting fixtures.',
'2. **List identity and indexes.** Begin with repeated identical object references and equal primitive values. Insert/remove ranges, replace at an index, move both directions, filter, reverse, flatten, and clear. Replay every emitted list change into an independent array; compare after every batch. Preserve each occurrence, even if key-based cache operations would collapse it.',
'3. **Filter transitions.** Cover false→true, true→false, true→true update, mutable refresh, dynamic predicate changes, no initial predicate, predicate-state streams, re-filter triggers, predicate exceptions, and completion of auxiliary streams. Inspect `Cache/FilterFixture.DynamicPredicateState.UnitTests.cs` and `List/FilterFixture.WithPredicateState.cs`.',
'4. **Projection and async ordering.** Record factory calls and identities for refresh enabled/disabled, force-transform predicates, inline update, failed transforms, removal while pending, update while pending, inner completion, and immediate synchronous Observable emissions. Inspect `Cache/TransformWithInlineUpdateFixture.cs`, `Cache/TransformSafeAsyncFixture.cs`, `Cache/TransformOnObservableFixture.cs`, and `List/TransformAsyncFixture.cs`.',
'5. **Flattening and groups.** Test parent removal while a child stream remains active, duplicate child keys from multiple parents, child key changes, mutable regrouping, dynamic group selector changes, disappearance/reappearance of the final group member, immutable group snapshots, and group disposal. Fixtures include `Cache/TransformManyObservableCacheFixture.cs`, `List/TransformManyRefreshFixture.cs`, `Cache/GroupOnObservableFixture.cs`, and `Cache/GroupOnPropertyWithImmutableStateFixture.cs`.',
'6. **Joins and set combinations.** Exercise duplicate foreign keys, right-side moves between left keys, missing left/right rows, update of the preserved side, add/remove of whole input streams, and different arrival orders. `InnerJoin` uses composite left/right keys upstream, while the outer join selectors carry optional values. Inspect `Cache/InnerJoinFixture.cs`, `Cache/InnerJoinFixtureRaceCondition.cs`, and all four `*JoinManyFixture.cs` files.',
'7. **Sort, page, virtual windows, and binding.** Test stable ties, refresh-driven movement, comparer changes, unchanged windows, out-of-range requests, remove-induced backfill, target-array identity, reset thresholds, response totals, and transitions between empty/nonempty windows. The new `SortAndVirtualize` contract emits no data until the request observable has produced a value. Use `Cache/SortAndVirtualizeFixture.cs`, `Cache/SortAndPageAndBindFixture.cs`, and `List/VirtualisationFixture.cs`.',
'8. **Time and lifecycle.** Use RxJS `TestScheduler`, not wall-clock delays, for expiration, batch windows, timer replacement after updates, buffer gates, and disposal ordering. Count per-item subscriptions and disposal calls for add/update/remove, duplicate list references, source errors, completion, and early unsubscription. Compare against `Cache/ExpireAfterFixture.ForStream.cs`, `Cache/ExpireAfterFixture.ForSource.cs`, and `Cache/AsyncDisposeManyFixture.UnitTests.cs`.',
'9. **Aggregates and diagnostics.** Include empty collections, nullable values, replacements, refresh after in-place mutation, negative values, repeated values, reset-to-empty, and initial/changed emission counts. Validate the selected standard-deviation formula against the pinned upstream implementation, since matching a familiar statistical name is insufficient. Validate cumulative diagnostic counters against the change trace, not only the final size.',
'10. **Scale and resource bounds.** Benchmark cold initial population separately from one-item edits on 1k/10k/100k collections. Count selector/comparer invocations to expose whole-collection recomputation. Measure listener counts after removal/unsubscription and distinguish data windowing from DOM row virtualization. Record environment, seeds, inputs, and elapsed units with each result.',
'',
'The cited fixture paths are relative to [`src/DynamicData.Tests`](https://github.com/reactivemarbles/DynamicData/tree/'+commit+'/src/DynamicData.Tests). A parity claim should identify which fixture behaviors were ported, which upstream overloads were folded into one JS contract, and which remain unavailable.',
'',
'## License and attribution','',
'DynamicData is MIT licensed. Its root license attributes **Copyright (c) Roland Pheasant 2011-2022**; the inspected source headers use **Copyright (c) 2011-2025 Roland Pheasant**. Preserve the upstream notice and complete MIT license when redistributing substantial portions or derived code. The immutable [upstream license](https://github.com/reactivemarbles/DynamicData/blob/'+commit+'/LICENSE) supplies the exact text. RxJS is a separate dependency with its own license; distribute its license alongside vendored or bundled RxJS code.',
'']
(out/'UPSTREAM-API.md').write_text('\n'.join(lines))
