# DynamicData upstream API inventory

This inventory is pinned to DynamicData commit [`ef790af138811c3268c9bb687886c2acbd0b66c0`](https://github.com/reactivemarbles/DynamicData/tree/ef790af138811c3268c9bb687886c2acbd0b66c0), committed 2026-09-03T22:07:13-05:00. The checkout declares version pattern `10.0-preview.{height}`; it does not identify an exact published NuGet package version. Its System.Reactive dependency is 6.1.0. The web implementation uses RxJS.

[`upstream-api.json`](upstream-api.json) is the machine-readable baseline: exact public static method signatures, overload declaration counts, generic arity, defining files, and immutable source links. [`generate-upstream-inventory.py`](generate-upstream-inventory.py) regenerates it from an upstream checkout.

## What the counts mean

An operator name is counted once per extension owner, even when C# has many overloads. A declaration is one C# overload, including nullable numeric variants and overloads whose first argument is a different observable kind. Neither an exported JavaScript function nor a PascalCase alias proves that all corresponding C# overloads and behaviors have been implemented.

Public types are deduplicated by namespace, name, and generic arity. The inventory includes public types in an `Internal` namespace because they are public declarations. It unions conditional source branches; it does not pretend that every declaration is compiled into every .NET target. In particular, the project conditionally removes `AsyncDisposeMany` on targets without `IAsyncDisposable`.

Constructors, instance methods, properties, C# operator overloads, non-public types, framework-inherited members, benchmarks, samples, and separate test projects are excluded from the static-operator declaration counts. Public type names are inventoried separately. Public test-support helpers under `src/DynamicData/Cache/Tests` and `List/Tests` remain included because they belong to the library source tree. All public static named declarations in every `Cache/ObservableCacheEx*.cs` and `List/ObservableListEx*.cs` file were cross-checked against declaration counts in those source files.

| Scope | Unique method names | Overload declarations |
| --- | ---: | ---: |
| `ObservableCacheEx` | 109 | 313 |
| `ObservableListEx` | 63 | 124 |
| Cache/list operator union | 118 | 437 |
| All inventoried public static methods | 183 | 680 |

There are **144 distinct public types** across **320 declarations**. Static methods include **656 extension-method overload declarations**.

## Cache and list operator names

Counts below are per extension owner, avoiding ambiguity between cache/list methods that share a name. A dash means that owner does not declare the name at this revision. Auxiliary extensions may still supply a similarly named method.

| Name | Cache overloads | List overloads |
| --- | ---: | ---: |
| [`Adapt`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.Adapt.cs#L46) | 2 | 1 |
| [`AddKey`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/ObservableListEx.AddKey.cs#L42) | — | 1 |
| [`AddOrUpdate`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.AddOrUpdate.cs#L49) | 5 | — |
| [`And`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.And.cs#L39) | 5 | 5 |
| [`AsObservableCache`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.AsObservableCache.cs#L37) | 2 | — |
| [`AsObservableList`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/ObservableListEx.AsObservableList.cs#L33) | — | 2 |
| [`AsyncDisposeMany`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.AsyncDisposeMany.cs#L72) | 1 | — |
| [`AutoRefresh`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.AutoRefresh.cs#L39) | 2 | 2 |
| [`AutoRefreshOnObservable`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.AutoRefreshOnObservable.cs#L40) | 2 | 1 |
| [`Batch`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.Batch.cs#L56) | 1 | — |
| [`BatchIf`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.BatchIf.cs#L30) | 6 | — |
| [`Bind`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.Bind.cs#L39) | 20 | 5 |
| [`BufferIf`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/ObservableListEx.BufferIf.cs#L31) | — | 4 |
| [`BufferInitial`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.BufferInitial.cs#L47) | 1 | 1 |
| [`Cast`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.Cast.cs#L49) | 1 | 2 |
| [`CastToObject`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/ObservableListEx.CastToObject.cs#L33) | — | 1 |
| [`ChangeKey`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.ChangeKey.cs#L48) | 2 | — |
| [`Clear`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.Clear.cs#L44) | 3 | — |
| [`Clone`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.Clone.cs#L46) | 1 | 1 |
| [`Convert`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.Convert.cs#L38) | 1 | 1 |
| [`DeferUntilLoaded`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.DeferUntilLoaded.cs#L40) | 2 | 2 |
| [`DisposeMany`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.DisposeMany.cs#L63) | 1 | 1 |
| [`DistinctValues`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.DistinctValues.cs#L43) | 1 | 1 |
| [`EditDiff`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.EditDiff.cs#L36) | 4 | — |
| [`EnsureUniqueKeys`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.EnsureUniqueKeys.cs#L46) | 1 | — |
| [`Except`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.Except.cs#L43) | 5 | 5 |
| [`ExpireAfter`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.ExpireAfter.cs#L51) | 5 | 1 |
| [`Filter`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.Filter.cs#L51) | 4 | 3 |
| [`FilterImmutable`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.FilterImmutable.cs#L56) | 1 | — |
| [`FilterOnObservable`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.FilterOnObservable.cs#L72) | 2 | 1 |
| [`FilterOnProperty`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/ObservableListEx.FilterOnProperty.cs#L43) | — | 1 |
| [`FinallySafe`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.FinallySafe.cs#L36) | 1 | — |
| [`Flatten`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.Flatten.cs#L38) | 1 | — |
| [`FlattenBufferResult`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.FlattenBufferResult.cs#L37) | 1 | 1 |
| [`ForEachChange`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.ForEachChange.cs#L53) | 1 | 1 |
| [`ForEachItemChange`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/ObservableListEx.ForEachItemChange.cs#L42) | — | 1 |
| [`FullJoin`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.FullJoin.cs#L34) | 2 | — |
| [`FullJoinMany`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.FullJoinMany.cs#L34) | 2 | — |
| [`Group`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.Group.cs#L42) | 5 | — |
| [`GroupOn`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/ObservableListEx.GroupOn.cs#L54) | — | 1 |
| [`GroupOnObservable`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.GroupOnObservable.cs#L75) | 2 | — |
| [`GroupOnProperty`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.GroupOnProperty.cs#L40) | 1 | 1 |
| [`GroupOnPropertyWithImmutableState`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.GroupOnPropertyWithImmutableState.cs#L40) | 1 | 1 |
| [`GroupWithImmutableState`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.GroupWithImmutableState.cs#L56) | 1 | 1 |
| [`IgnoreSameReferenceUpdate`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.IgnoreSameReferenceUpdate.cs#L35) | 1 | — |
| [`IgnoreUpdateWhen`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.IgnoreUpdateWhen.cs#L37) | 1 | — |
| [`IncludeUpdateWhen`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.IncludeUpdateWhen.cs#L37) | 1 | — |
| [`InnerJoin`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.InnerJoin.cs#L34) | 2 | — |
| [`InnerJoinMany`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.InnerJoinMany.cs#L34) | 2 | — |
| [`InvokeEvaluate`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.InvokeEvaluate.cs#L45) | 1 | — |
| [`LeftJoin`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.LeftJoin.cs#L34) | 2 | — |
| [`LeftJoinMany`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.LeftJoinMany.cs#L34) | 2 | — |
| [`LimitSizeTo`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.LimitSizeTo.cs#L48) | 2 | 1 |
| [`MergeChangeSets`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.MergeChangeSets.cs#L75) | 16 | 10 |
| [`MergeMany`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.MergeMany.cs#L61) | 2 | 1 |
| [`MergeManyChangeSets`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.MergeManyChangeSets.cs#L43) | 14 | 3 |
| [`MergeManyItems`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.MergeManyItems.cs#L40) | 2 | — |
| [`MonitorStatus`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.MonitorStatus.cs#L38) | 1 | — |
| [`NotEmpty`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.NotEmpty.cs#L37) | 1 | 1 |
| [`OfType`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.OfType.cs#L48) | 1 | — |
| [`OnItemAdded`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.OnItemAdded.cs#L56) | 2 | 1 |
| [`OnItemRefreshed`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.OnItemRefreshed.cs#L54) | 2 | 1 |
| [`OnItemRemoved`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.OnItemRemoved.cs#L68) | 2 | 1 |
| [`OnItemUpdated`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.OnItemUpdated.cs#L55) | 2 | — |
| [`Or`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.Or.cs#L55) | 5 | 5 |
| [`Page`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.VirtualiseAndPage.cs#L326) | 1 | 1 |
| [`PopulateFrom`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.PopulateFrom.cs#L42) | 2 | — |
| [`PopulateInto`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.PopulateInto.cs#L54) | 3 | 1 |
| [`QueryWhenChanged`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.QueryWhenChanged.cs#L45) | 3 | 2 |
| [`RefCount`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.RefCount.cs#L37) | 1 | 1 |
| [`Refresh`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.Refresh.cs#L41) | 3 | — |
| [`Remove`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.Remove.cs#L42) | 6 | — |
| [`RemoveIndex`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/ObservableListEx.RemoveIndex.cs#L37) | — | 1 |
| [`RemoveKey`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.RemoveKey.cs#L38) | 2 | — |
| [`RemoveKeys`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.RemoveKeys.cs#L36) | 1 | — |
| [`Reverse`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/ObservableListEx.Reverse.cs#L37) | — | 1 |
| [`RightJoin`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.RightJoin.cs#L34) | 2 | — |
| [`RightJoinMany`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.RightJoinMany.cs#L34) | 2 | — |
| [`SkipInitial`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.SkipInitial.cs#L39) | 1 | 1 |
| [`Sort`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.Sort.cs#L45) | 4 | 2 |
| [`SortAndBind`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.SortAndBind.cs#L174) | 12 | — |
| [`SortAndPage`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.VirtualiseAndPage.cs#L209) | 4 | — |
| [`SortAndVirtualize`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.VirtualiseAndPage.cs#L17) | 4 | — |
| [`SortBy`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.SortBy.cs#L41) | 1 | — |
| [`StartWithEmpty`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.StartWithEmpty.cs#L37) | 7 | 1 |
| [`StartWithItem`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.StartWithItem.cs#L32) | 2 | — |
| [`SubscribeMany`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.SubscribeMany.cs#L62) | 2 | 1 |
| [`SuppressRefresh`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.SuppressRefresh.cs#L35) | 1 | 1 |
| [`Switch`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.Switch.cs#L31) | 2 | 2 |
| [`ToCollection`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.ToCollection.cs#L36) | 1 | 1 |
| [`ToObservableChangeSet`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.ToObservableChangeSet.cs#L42) | 2 | 8 |
| [`ToObservableOptional`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.ToObservableOptional.cs#L55) | 2 | — |
| [`ToSortedCollection`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.ToSortedCollection.cs#L39) | 2 | 2 |
| [`Top`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.VirtualiseAndPage.cs#L167) | 2 | 1 |
| [`Transform`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.Transform.cs#L31) | 9 | 4 |
| [`TransformAsync`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.TransformAsync.cs#L32) | 8 | 5 |
| [`TransformImmutable`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.TransformImmutable.cs#L54) | 1 | — |
| [`TransformMany`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.TransformMany.cs#L55) | 4 | 4 |
| [`TransformManyAsync`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.TransformManyAsync.cs#L55) | 6 | — |
| [`TransformManySafeAsync`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.TransformManySafeAsync.cs#L47) | 6 | — |
| [`TransformOnObservable`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.TransformOnObservable.cs#L71) | 2 | — |
| [`TransformSafe`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.TransformSafe.cs#L30) | 6 | — |
| [`TransformSafeAsync`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.TransformSafeAsync.cs#L31) | 8 | — |
| [`TransformToTree`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.TransformToTree.cs#L50) | 1 | — |
| [`TransformWithInlineUpdate`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.TransformWithInlineUpdate.cs#L30) | 4 | — |
| [`TreatMovesAsRemoveAdd`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.TreatMovesAsRemoveAdd.cs#L35) | 1 | — |
| [`TrueForAll`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.TrueForAll.cs#L51) | 2 | — |
| [`TrueForAny`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.TrueForAny.cs#L51) | 2 | — |
| [`UpdateIndex`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.UpdateIndex.cs#L36) | 1 | — |
| [`Virtualise`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.VirtualiseAndPage.cs#L137) | 1 | 1 |
| [`Watch`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.Watch.cs#L47) | 1 | — |
| [`WatchValue`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.WatchValue.cs#L54) | 2 | — |
| [`WhenAnyPropertyChanged`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.WhenAnyPropertyChanged.cs#L57) | 1 | 1 |
| [`WhenPropertyChanged`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.WhenPropertyChanged.cs#L55) | 1 | 1 |
| [`WhenValueChanged`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.WhenValueChanged.cs#L58) | 1 | 1 |
| [`WhereReasonsAre`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.WhereReasonsAre.cs#L41) | 1 | 1 |
| [`WhereReasonsAreNot`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.WhereReasonsAreNot.cs#L41) | 1 | 1 |
| [`Xor`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.Xor.cs#L56) | 5 | 5 |

## Additional public static API

These include aggregation, query aliases, property/collection binding, cache/list editing helpers, optional values, diagnostics, and observable factories. Counts here exclude methods already listed in `ObservableCacheEx` and `ObservableListEx`.

| Area / owner | Method | Overloads |
| --- | --- | ---: |
| Aggregation / `AggregationEx` | [`ForAggregation`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Aggregation/AggregationEx.cs#L22) | 2 |
| Aggregation / `AggregationEx` | [`InvalidateWhen`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Aggregation/AggregationEx.cs#L51) | 2 |
| Aggregation / `AvgEx` | [`Avg`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Aggregation/AvgEx.cs#L25) | 30 |
| Aggregation / `CountEx` | [`Count`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Aggregation/CountEx.cs#L21) | 4 |
| Aggregation / `CountEx` | [`IsEmpty`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Aggregation/CountEx.cs#L59) | 2 |
| Aggregation / `CountEx` | [`IsNotEmpty`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Aggregation/CountEx.cs#L81) | 2 |
| Aggregation / `MaxEx` | [`Maximum`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Aggregation/MaxEx.cs#L32) | 2 |
| Aggregation / `MaxEx` | [`Minimum`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Aggregation/MaxEx.cs#L62) | 2 |
| Aggregation / `StdDevEx` | [`StdDev`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Aggregation/StdDevEx.cs#L22) | 15 |
| Aggregation / `SumEx` | [`Sum`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Aggregation/SumEx.cs#L20) | 30 |
| Alias / `ObservableCacheAlias` | [`Select`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Alias/ObservableCacheAlias.cs#L29) | 4 |
| Alias / `ObservableCacheAlias` | [`SelectMany`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Alias/ObservableCacheAlias.cs#L118) | 1 |
| Alias / `ObservableCacheAlias` | [`SelectSafe`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Alias/ObservableCacheAlias.cs#L143) | 4 |
| Alias / `ObservableCacheAlias` | [`SelectTree`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Alias/ObservableCacheAlias.cs#L250) | 1 |
| Alias / `ObservableCacheAlias` | [`Where`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Alias/ObservableCacheAlias.cs#L268) | 3 |
| Alias / `ObservableListAlias` | [`Select`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Alias/ObservableListAlias.cs#L25) | 1 |
| Alias / `ObservableListAlias` | [`SelectMany`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Alias/ObservableListAlias.cs#L44) | 1 |
| Alias / `ObservableListAlias` | [`Where`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Alias/ObservableListAlias.cs#L62) | 2 |
| Binding / `BindingListEx` | [`ObserveCollectionChanges`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Binding/BindingListEx.cs#L23) | 1 |
| Binding / `BindingListEx` | [`ToObservableChangeSet`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Binding/BindingListEx.cs#L34) | 3 |
| Binding / `BindingOptions` | [`NeverFireReset`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Binding/BindingOptions.cs#L35) | 1 |
| Binding / `IObservableListEx` | [`BindToObservableList`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Binding/IObservableListEx.cs#L26) | 3 |
| Binding / `NotifyPropertyChangedEx` | [`WhenAnyPropertyChanged`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Binding/NotifyPropertyChangedEx.cs#L23) | 1 |
| Binding / `NotifyPropertyChangedEx` | [`WhenChanged`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Binding/NotifyPropertyChangedEx.cs#L45) | 6 |
| Binding / `NotifyPropertyChangedEx` | [`WhenPropertyChanged`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Binding/NotifyPropertyChangedEx.cs#L248) | 1 |
| Binding / `NotifyPropertyChangedEx` | [`WhenValueChanged`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Binding/NotifyPropertyChangedEx.cs#L270) | 1 |
| Binding / `ObservableCollectionEx` | [`ObserveCollectionChanges`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Binding/ObservableCollectionEx.cs#L22) | 1 |
| Binding / `ObservableCollectionEx` | [`ToObservableChangeSet`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Binding/ObservableCollectionEx.cs#L32) | 5 |
| Binding / `SortExpressionComparer` | [`Ascending`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Binding/SortExpressionComparer.cs#L18) | 1 |
| Binding / `SortExpressionComparer` | [`Descending`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Binding/SortExpressionComparer.cs#L25) | 1 |
| Cache / `SourceCacheEx` | [`Cast`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/SourceCacheEx.cs#L23) | 1 |
| Cache / `TestEx` | [`AsAggregator`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/Tests/TestEx.cs#L20) | 7 |
| Diagnostics / `DiagnosticOperators` | [`CollectUpdateStats`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Diagnostics/DiagnosticOperators.cs#L22) | 2 |
| Experimental / `ExperimentalEx` | [`AsWatcher`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Experimental/ExperimentalEx.cs#L23) | 1 |
| Kernel / `EnumerableEx` | [`AsArray`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Kernel/EnumerableEx.cs#L18) | 1 |
| Kernel / `EnumerableEx` | [`AsList`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Kernel/EnumerableEx.cs#L31) | 1 |
| Kernel / `EnumerableEx` | [`Duplicates`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Kernel/EnumerableEx.cs#L46) | 1 |
| Kernel / `EnumerableEx` | [`IndexOfMany`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Kernel/EnumerableEx.cs#L63) | 2 |
| Kernel / `InternalEx` | [`RetryWithBackOff`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Kernel/InternalEx.cs#L35) | 1 |
| Kernel / `InternalEx` | [`ScheduleRecurringAction`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Kernel/InternalEx.cs#L65) | 2 |
| Kernel / `OptionExtensions` | [`Convert`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Kernel/OptionExtensions.cs#L21) | 2 |
| Kernel / `OptionExtensions` | [`ConvertOr`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Kernel/OptionExtensions.cs#L62) | 1 |
| Kernel / `OptionExtensions` | [`FirstOrOptional`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Kernel/OptionExtensions.cs#L100) | 1 |
| Kernel / `OptionExtensions` | [`IfHasValue`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Kernel/OptionExtensions.cs#L121) | 2 |
| Kernel / `OptionExtensions` | [`Lookup`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Kernel/OptionExtensions.cs#L171) | 1 |
| Kernel / `OptionExtensions` | [`OrElse`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Kernel/OptionExtensions.cs#L83) | 1 |
| Kernel / `OptionExtensions` | [`RemoveIfContained`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Kernel/OptionExtensions.cs#L188) | 1 |
| Kernel / `OptionExtensions` | [`SelectValues`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Kernel/OptionExtensions.cs#L202) | 1 |
| Kernel / `OptionExtensions` | [`ValueOr`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Kernel/OptionExtensions.cs#L212) | 2 |
| Kernel / `OptionExtensions` | [`ValueOrDefault`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Kernel/OptionExtensions.cs#L237) | 1 |
| Kernel / `OptionExtensions` | [`ValueOrThrow`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Kernel/OptionExtensions.cs#L256) | 1 |
| Kernel / `OptionObservableExtensions` | [`Convert`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Kernel/OptionObservableExtensions.cs#L25) | 2 |
| Kernel / `OptionObservableExtensions` | [`ConvertOr`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Kernel/OptionObservableExtensions.cs#L73) | 1 |
| Kernel / `OptionObservableExtensions` | [`OnHasNoValue`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Kernel/OptionObservableExtensions.cs#L131) | 1 |
| Kernel / `OptionObservableExtensions` | [`OnHasValue`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Kernel/OptionObservableExtensions.cs#L114) | 1 |
| Kernel / `OptionObservableExtensions` | [`OrElse`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Kernel/OptionObservableExtensions.cs#L96) | 1 |
| Kernel / `OptionObservableExtensions` | [`SelectValues`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Kernel/OptionObservableExtensions.cs#L148) | 1 |
| Kernel / `OptionObservableExtensions` | [`ValueOr`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Kernel/OptionObservableExtensions.cs#L161) | 1 |
| Kernel / `OptionObservableExtensions` | [`ValueOrDefault`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Kernel/OptionObservableExtensions.cs#L176) | 1 |
| Kernel / `OptionObservableExtensions` | [`ValueOrThrow`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Kernel/OptionObservableExtensions.cs#L194) | 1 |
| Kernel / `Optional` | [`Create`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Kernel/Optional.cs#L96) | 1 |
| Kernel / `Optional` | [`FromOptional`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Kernel/Optional.cs#L103) | 1 |
| Kernel / `Optional` | [`None`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Kernel/Optional.cs#L186) | 1 |
| Kernel / `Optional` | [`Some`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Kernel/Optional.cs#L196) | 1 |
| Kernel / `Optional` | [`ToOptional`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Kernel/Optional.cs#L110) | 1 |
| List / `ChangeSetEx` | [`Flatten`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/ChangeSetEx.cs#L23) | 1 |
| List / `ChangeSetEx` | [`GetChangeType`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/ChangeSetEx.cs#L36) | 1 |
| List / `ChangeSetEx` | [`Transform`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/ChangeSetEx.cs#L56) | 1 |
| List / `ChangeSetEx` | [`YieldWithoutIndex`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/ChangeSetEx.cs#L83) | 1 |
| List / `ListEx` | [`Add`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/ListEx.cs#L26) | 1 |
| List / `ListEx` | [`AddOrInsertRange`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/ListEx.cs#L41) | 1 |
| List / `ListEx` | [`AddRange`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/ListEx.cs#L88) | 2 |
| List / `ListEx` | [`BinarySearch`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/ListEx.cs#L141) | 3 |
| List / `ListEx` | [`Clone`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/ListEx.cs#L208) | 3 |
| List / `ListEx` | [`IndexOf`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/ListEx.cs#L257) | 2 |
| List / `ListEx` | [`IndexOfOptional`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/ListEx.cs#L294) | 1 |
| List / `ListEx` | [`Remove`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/ListEx.cs#L312) | 1 |
| List / `ListEx` | [`RemoveMany`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/ListEx.cs#L326) | 1 |
| List / `ListEx` | [`Replace`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/ListEx.cs#L368) | 2 |
| List / `ListEx` | [`ReplaceOrAdd`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/ListEx.cs#L420) | 1 |
| List / `ListTextEx` | [`AsAggregator`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/Tests/ListTextEx.cs#L19) | 1 |
| List / `SourceListEditConvenienceEx` | [`Add`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/SourceListEditConvenienceEx.cs#L21) | 1 |
| List / `SourceListEditConvenienceEx` | [`AddRange`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/SourceListEditConvenienceEx.cs#L35) | 1 |
| List / `SourceListEditConvenienceEx` | [`Clear`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/SourceListEditConvenienceEx.cs#L48) | 1 |
| List / `SourceListEditConvenienceEx` | [`EditDiff`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/SourceListEditConvenienceEx.cs#L64) | 1 |
| List / `SourceListEditConvenienceEx` | [`Insert`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/SourceListEditConvenienceEx.cs#L81) | 1 |
| List / `SourceListEditConvenienceEx` | [`InsertRange`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/SourceListEditConvenienceEx.cs#L96) | 1 |
| List / `SourceListEditConvenienceEx` | [`Move`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/SourceListEditConvenienceEx.cs#L111) | 1 |
| List / `SourceListEditConvenienceEx` | [`Remove`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/SourceListEditConvenienceEx.cs#L126) | 1 |
| List / `SourceListEditConvenienceEx` | [`RemoveAt`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/SourceListEditConvenienceEx.cs#L142) | 1 |
| List / `SourceListEditConvenienceEx` | [`RemoveMany`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/SourceListEditConvenienceEx.cs#L156) | 1 |
| List / `SourceListEditConvenienceEx` | [`RemoveRange`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/SourceListEditConvenienceEx.cs#L173) | 1 |
| List / `SourceListEditConvenienceEx` | [`Replace`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/SourceListEditConvenienceEx.cs#L188) | 1 |
| List / `SourceListEditConvenienceEx` | [`ReplaceAt`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/SourceListEditConvenienceEx.cs#L203) | 1 |
| List / `SourceListEx` | [`Cast`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/SourceListEx.cs#L22) | 1 |
| Platforms / `ParallelOperators` | [`Filter`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Platforms/net45/ParallelOperators.cs#L25) | 1 |
| Platforms / `ParallelOperators` | [`SubscribeMany`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Platforms/net45/ParallelOperators.cs#L49) | 2 |
| Platforms / `ParallelOperators` | [`Transform`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Platforms/net45/ParallelOperators.cs#L101) | 2 |
| Platforms / `ParallelOperators` | [`TransformSafe`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Platforms/net45/ParallelOperators.cs#L152) | 2 |
| Root / `EnumerableEx` | [`AsObservableChangeSet`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/EnumerableEx.cs#L28) | 2 |
| Root / `ObservableChangeSet` | [`Create`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/ObservableChangeSet.cs#L23) | 16 |

## Public types

The number following a backtick in the JSON `id` is CLR generic arity. For example, `Change` with one generic parameter is the list type; `Change` with two generic parameters is the keyed cache type. A JavaScript export name alone cannot preserve this distinction.

| Namespace | Public type | Kind | Defining area |
| --- | --- | --- | --- |
| `DynamicData.Aggregation` | [`AggregateItem〈1 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Aggregation/AggregateItem.cs#L16) | struct | Aggregation |
| `DynamicData.Aggregation` | [`AggregateType`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Aggregation/AggregateType.cs#L10) | enum | Aggregation |
| `DynamicData.Aggregation` | [`AggregationEx`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Aggregation/AggregationEx.cs#L13) | class | Aggregation |
| `DynamicData.Aggregation` | [`AvgEx`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Aggregation/AvgEx.cs#L12) | class | Aggregation |
| `DynamicData.Aggregation` | [`CountEx`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Aggregation/CountEx.cs#L12) | class | Aggregation |
| `DynamicData.Aggregation` | [`IAggregateChangeSet〈1 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Aggregation/IAggregateChangeSet.cs#L11) | interface | Aggregation |
| `DynamicData.Aggregation` | [`MaxEx`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Aggregation/MaxEx.cs#L12) | class | Aggregation |
| `DynamicData.Aggregation` | [`StdDevEx`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Aggregation/StdDevEx.cs#L12) | class | Aggregation |
| `DynamicData.Aggregation` | [`SumEx`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Aggregation/SumEx.cs#L10) | class | Aggregation |
| `DynamicData.Alias` | [`ObservableCacheAlias`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Alias/ObservableCacheAlias.cs#L12) | class | Alias |
| `DynamicData.Alias` | [`ObservableListAlias`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Alias/ObservableListAlias.cs#L10) | class | Alias |
| `DynamicData.Binding` | [`AbstractNotifyPropertyChanged`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Binding/AbstractNotifyPropertyChanged.cs#L15) | class | Binding |
| `DynamicData.Binding` | [`BindingListAdaptor〈1 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Binding/BindingListAdaptor.cs#L22) | class | Binding |
| `DynamicData.Binding` | [`BindingListAdaptor〈2 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Binding/BindingListAdaptor.cs#L59) | class | Binding |
| `DynamicData.Binding` | [`BindingListEx`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Binding/BindingListEx.cs#L16) | class | Binding |
| `DynamicData.Binding` | [`BindingOptions`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Binding/BindingOptions.cs#L13) | record struct | Binding |
| `DynamicData.Binding` | [`IEvaluateAware`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Binding/IEvaluateAware.cs#L11) | interface | Binding |
| `DynamicData.Binding` | [`IIndexAware`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Binding/IIndexAware.cs#L11) | interface | Binding |
| `DynamicData.Binding` | [`INotifyCollectionChangedSuspender`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Binding/INotifyCollectionChangedSuspender.cs#L10) | interface | Binding |
| `DynamicData.Binding` | [`IObservableCollectionAdaptor〈2 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Binding/IObservableCollectionAdaptor.cs#L13) | interface | Binding |
| `DynamicData.Binding` | [`IObservableCollection〈3 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Binding/IObservableCollection.cs#L14) | interface | Binding |
| `DynamicData.Binding` | [`IObservableListEx`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Binding/IObservableListEx.cs#L13) | class | Binding |
| `DynamicData.Binding` | [`ISortedObservableCollectionAdaptor〈2 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Binding/ISortedObservableCollectionAdaptor.cs#L13) | interface | Binding |
| `DynamicData.Binding` | [`NotifyPropertyChangedEx`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Binding/NotifyPropertyChangedEx.cs#L14) | class | Binding |
| `DynamicData.Binding` | [`ObservableCollectionAdaptor〈1 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Binding/ObservableCollectionAdaptor.cs#L23) | class | Binding |
| `DynamicData.Binding` | [`ObservableCollectionAdaptor〈2 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Binding/ObservableCollectionAdaptor.cs#L90) | class | Binding |
| `DynamicData.Binding` | [`ObservableCollectionEx`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Binding/ObservableCollectionEx.cs#L15) | class | Binding |
| `DynamicData.Binding` | [`ObservableCollectionExtended〈3 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Binding/ObservableCollectionExtended.cs#L16) | class | Binding |
| `DynamicData.Binding` | [`PropertyValue〈3 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Binding/PropertyValue.cs#L12) | class | Binding |
| `DynamicData.Binding` | [`SortAndBindOptions`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Binding/SortAndBindOptions.cs#L12) | record struct | Binding |
| `DynamicData.Binding` | [`SortDirection`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Binding/SortDirection.cs#L10) | enum | Binding |
| `DynamicData.Binding` | [`SortExpressionComparer〈2 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Binding/SortExpressionComparer.cs#L11) | class | Binding |
| `DynamicData.Binding` | [`SortExpression〈1 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Binding/SortExpression.cs#L16) | class | Binding |
| `DynamicData.Binding` | [`SortedBindingListAdaptor〈2 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Binding/SortedBindingListAdaptor.cs#L22) | class | Binding |
| `DynamicData.Binding` | [`SortedObservableCollectionAdaptor〈2 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Binding/SortedObservableCollectionAdaptor.cs#L19) | class | Binding |
| `DynamicData.Cache.Internal` | [`CombineOperator`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/Internal/CombineOperator.cs#L10) | enum | Cache |
| `DynamicData.Cache.Internal` | [`KeySelectorException`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/Internal/KeySelectorException.cs#L11) | class | Cache |
| `DynamicData.Cache.Internal` | [`LockFreeObservableCache〈3 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/Internal/LockFreeObservableCache.cs#L19) | class | Cache |
| `DynamicData` | [`ChangeAwareCache〈3 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ChangeAwareCache.cs#L17) | class | Cache |
| `DynamicData` | [`ChangeAwareList〈1 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/ChangeAwareList.cs#L15) | class | List |
| `DynamicData` | [`ChangeReason`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ChangeReason.cs#L12) | enum | Cache |
| `DynamicData` | [`ChangeSetEx`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/ChangeSetEx.cs#L14) | class | List |
| `DynamicData` | [`ChangeSet〈2 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/ChangeSet.cs#L12) | class | List |
| `DynamicData` | [`ChangeSet〈5 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ChangeSet.cs#L51) | class | Cache |
| `DynamicData` | [`ChangeSet〈7 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ChangeSet.cs#L14) | class | Cache |
| `DynamicData` | [`ChangeType`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/ChangeType.cs#L10) | enum | List |
| `DynamicData` | [`Change〈1 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/Change.cs#L12) | class | List |
| `DynamicData` | [`Change〈3 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/Change.cs#L13) | struct | Cache |
| `DynamicData.Diagnostics` | [`ChangeStatistics`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Diagnostics/ChangeStatistics.cs#L10) | class | Diagnostics |
| `DynamicData.Diagnostics` | [`ChangeSummary`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Diagnostics/ChangeSummary.cs#L10) | class | Diagnostics |
| `DynamicData.Diagnostics` | [`DiagnosticOperators`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Diagnostics/DiagnosticOperators.cs#L12) | class | Diagnostics |
| `DynamicData` | [`DynamicDataOptions`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/DynamicDataOptions.cs#L12) | class | Root |
| `DynamicData` | [`EnumerableEx`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/EnumerableEx.cs#L13) | class | Root |
| `DynamicData.Experimental` | [`ExperimentalEx`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Experimental/ExperimentalEx.cs#L12) | class | Experimental |
| `DynamicData.Experimental` | [`IWatcher〈2 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Experimental/IWatcher.cs#L12) | interface | Experimental |
| `DynamicData` | [`ICacheUpdater〈3 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ICacheUpdater.cs#L20) | interface | Cache |
| `DynamicData` | [`ICache〈3 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ICache.cs#L15) | interface | Cache |
| `DynamicData` | [`IChangeSet`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/IChangeSet.cs#L10) | interface | Root |
| `DynamicData` | [`IChangeSetAdaptor〈1 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/IChangeSetAdaptor.cs#L11) | interface | List |
| `DynamicData` | [`IChangeSetAdaptor〈2 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/IChangeSetAdaptor.cs#L13) | interface | Cache |
| `DynamicData` | [`IChangeSet〈1 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/IChangeSet.cs#L13) | interface | List |
| `DynamicData` | [`IChangeSet〈4 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/IChangeSet.cs#L15) | interface | Cache |
| `DynamicData` | [`IConnectableCache〈2 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/IConnectableCache.cs#L13) | interface | Cache |
| `DynamicData` | [`IDistinctChangeSet〈2 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/IDistinctChangeSet.cs#L12) | interface | Cache |
| `DynamicData` | [`IExtendedList〈1 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/IExtendedList.cs#L12) | interface | List |
| `DynamicData` | [`IGroupChangeSet〈6 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/IGroupChangeSet.cs#L14) | interface | Cache |
| `DynamicData` | [`IGroup〈2 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/IGroup.cs#L12) | interface | List |
| `DynamicData` | [`IGroup〈3 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/IGroup.cs#L14) | interface | Cache |
| `DynamicData` | [`IGrouping〈3 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/IGrouping.cs#L14) | interface | Cache |
| `DynamicData` | [`IImmutableGroupChangeSet〈6 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/IImmutableGroupChangeSet.cs#L13) | interface | Cache |
| `DynamicData` | [`IIntermediateCache〈3 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/IIntermediateCache.cs#L14) | interface | Cache |
| `DynamicData` | [`IKeyValueCollection〈3 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/IKeyValueCollection.cs#L13) | interface | Cache |
| `DynamicData` | [`IKeyValue〈2 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/IKeyValue.cs#L12) | interface | Cache |
| `DynamicData` | [`IKey〈1 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/IKey.cs#L11) | interface | Cache |
| `DynamicData` | [`IObservableCache〈3 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/IObservableCache.cs#L13) | interface | Cache |
| `DynamicData` | [`IObservableList〈1 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/IObservableList.cs#L13) | interface | List |
| `DynamicData` | [`IPageChangeSet〈1 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/IPageChangeSet.cs#L15) | interface | List |
| `DynamicData` | [`IPageRequest`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/IPageRequest.cs#L10) | interface | Cache |
| `DynamicData` | [`IPagedChangeSet〈3 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/IPagedChangeSet.cs#L15) | interface | Cache |
| `DynamicData` | [`IQuery〈2 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/IQuery.cs#L13) | interface | Cache |
| `DynamicData` | [`ISortedChangeSetAdaptor〈2 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ISortedChangeSetAdaptor.cs#L12) | interface | Cache |
| `DynamicData` | [`ISortedChangeSet〈3 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ISortedChangeSet.cs#L12) | interface | Cache |
| `DynamicData` | [`ISourceCache〈3 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ISourceCache.cs#L14) | interface | Cache |
| `DynamicData` | [`ISourceList〈1 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/ISourceList.cs#L13) | interface | List |
| `DynamicData` | [`ISourceUpdater〈3 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ISourceUpdater.cs#L20) | interface | Cache |
| `DynamicData` | [`IVirtualChangeSet〈1 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/IVirtualChangeSet.cs#L12) | interface | List |
| `DynamicData` | [`IVirtualChangeSet〈3 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/IVirtualChangeSet.cs#L12) | interface | Cache |
| `DynamicData` | [`IVirtualRequest`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/IVirtualRequest.cs#L10) | interface | Cache |
| `DynamicData` | [`IVirtualResponse`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/IVirtualResponse.cs#L10) | interface | Cache |
| `DynamicData` | [`IndexedItem〈2 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/IndexedItem.cs#L19) | class | Cache |
| `DynamicData` | [`IntermediateCache〈3 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/IntermediateCache.cs#L18) | class | Cache |
| `DynamicData` | [`ItemChange〈1 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/ItemChange.cs#L12) | struct | List |
| `DynamicData.Kernel` | [`ConnectionStatus`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Kernel/ConnectionStatus.cs#L10) | enum | Kernel |
| `DynamicData.Kernel` | [`EnumerableEx`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Kernel/EnumerableEx.cs#L10) | class | Kernel |
| `DynamicData.Kernel` | [`Error〈2 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Kernel/Error.cs#L19) | class | Kernel |
| `DynamicData.Kernel` | [`InternalEx`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Kernel/InternalEx.cs#L15) | class | Kernel |
| `DynamicData.Kernel` | [`ItemWithIndex〈1 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Kernel/ItemWithIndex.cs#L17) | struct | Kernel |
| `DynamicData.Kernel` | [`ItemWithValue〈2 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Kernel/ItemWithValue.cs#L18) | struct | Kernel |
| `DynamicData.Kernel` | [`OptionElse`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Kernel/OptionElse.cs#L10) | class | Kernel |
| `DynamicData.Kernel` | [`OptionExtensions`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Kernel/OptionExtensions.cs#L10) | class | Kernel |
| `DynamicData.Kernel` | [`OptionObservableExtensions`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Kernel/OptionObservableExtensions.cs#L12) | class | Kernel |
| `DynamicData.Kernel` | [`Optional`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Kernel/Optional.cs#L179) | class | Kernel |
| `DynamicData.Kernel` | [`Optional〈1 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Kernel/Optional.cs#L15) | struct | Kernel |
| `DynamicData.List` | [`IGrouping〈2 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/IGrouping.cs#L12) | interface | List |
| `DynamicData` | [`ListChangeReason`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/ListChangeReason.cs#L11) | enum | List |
| `DynamicData` | [`ListEx`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/ListEx.cs#L13) | class | List |
| `DynamicData` | [`ListFilterPolicy`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/ListFilterPolicy.cs#L10) | enum | List |
| `DynamicData` | [`MissingKeyException`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/MissingKeyException.cs#L12) | class | Cache |
| `DynamicData` | [`Node〈4 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/Node.cs#L15) | class | Cache |
| `DynamicData` | [`ObservableCacheEx`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/ObservableCacheEx.Adapt.cs#L26) | class | Cache |
| `DynamicData` | [`ObservableChangeSet`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/ObservableChangeSet.cs#L13) | class | Root |
| `DynamicData` | [`ObservableListEx`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/ObservableListEx.Adapt.cs#L24) | class | List |
| `DynamicData.Operators` | [`IPageResponse`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/IPageResponse.cs#L10) | interface | Cache |
| `DynamicData.PLinq` | [`ParallelOperators`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Platforms/net45/ParallelOperators.cs#L13) | class | Platforms |
| `DynamicData.PLinq` | [`ParallelType`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Platforms/net45/ParallelType.cs#L12) | enum | Platforms |
| `DynamicData.PLinq` | [`ParallelisationOptions`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Platforms/net45/ParallelisationOptions.cs#L18) | class | Platforms |
| `DynamicData` | [`PageContext〈1 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/PageContext.cs#L16) | record | Cache |
| `DynamicData` | [`PageRequest`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/PageRequest.cs#L11) | class | Cache |
| `DynamicData` | [`RangeChange〈1 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/RangeChange.cs#L14) | class | List |
| `DynamicData` | [`SortAndPageOptions`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/SortAndPageOptions.cs#L12) | record struct | Cache |
| `DynamicData` | [`SortAndVirtualizeOptions`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/SortAndVirtualizeOptions.cs#L12) | record struct | Cache |
| `DynamicData` | [`SortException`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/SortException.cs#L12) | class | List |
| `DynamicData` | [`SortOptimisations`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/SortOptimisations.cs#L12) | enum | Cache |
| `DynamicData` | [`SortOptions`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/SortOptions.cs#L10) | enum | List |
| `DynamicData` | [`SortReason`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/SortReason.cs#L10) | enum | Cache |
| `DynamicData` | [`SourceCacheEx`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/SourceCacheEx.cs#L11) | class | Cache |
| `DynamicData` | [`SourceCache〈2 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/SourceCache.cs#L23) | class | Cache |
| `DynamicData` | [`SourceListEditConvenienceEx`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/SourceListEditConvenienceEx.cs#L13) | class | List |
| `DynamicData` | [`SourceListEx`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/SourceListEx.cs#L11) | class | List |
| `DynamicData` | [`SourceList〈1 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/SourceList.cs#L20) | class | List |
| `DynamicData.Tests` | [`ChangeSetAggregator〈1 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/Tests/ChangeSetAggregator.cs#L15) | class | List |
| `DynamicData.Tests` | [`ChangeSetAggregator〈2 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/Tests/ChangeSetAggregator.cs#L99) | class | Cache |
| `DynamicData.Tests` | [`ChangeSetAggregator〈3 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/Tests/ChangeSetAggregator.cs#L18) | class | Cache |
| `DynamicData.Tests` | [`DistinctChangeSetAggregator〈1 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/Tests/DistinctChangeSetAggregator.cs#L17) | class | Cache |
| `DynamicData.Tests` | [`GroupChangeSetAggregator〈3 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/Tests/GroupChangeSetAggregator.cs#L18) | class | Cache |
| `DynamicData.Tests` | [`ListTextEx`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/Tests/ListTextEx.cs#L11) | class | List |
| `DynamicData.Tests` | [`PagedChangeSetAggregator〈2 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/Tests/PagedChangeSetAggregator.cs#L18) | class | Cache |
| `DynamicData.Tests` | [`SortedChangeSetAggregator〈2 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/Tests/SortedChangeSetAggregator.cs#L18) | class | Cache |
| `DynamicData.Tests` | [`TestEx`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/Tests/TestEx.cs#L11) | class | Cache |
| `DynamicData.Tests` | [`VirtualChangeSetAggregator〈2 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/Tests/VirtualChangeSetAggregator.cs#L18) | class | Cache |
| `DynamicData` | [`TransformAsyncOptions`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/TransformAsyncOptions.cs#L12) | record struct | Cache |
| `DynamicData` | [`UnspecifiedIndexException`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/List/UnspecifiedIndexException.cs#L12) | class | List |
| `DynamicData` | [`VirtualContext〈1 type parameters〉`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/VirtualContext.cs#L14) | record | Cache |
| `DynamicData` | [`VirtualRequest`](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData/Cache/VirtualRequest.cs#L11) | class | Cache |

## API adaptation contract

| C# construct | JavaScript / RxJS requirement | Compatibility consequence |
| --- | --- | --- |
| `IObservable<T>` and System.Reactive | Actual RxJS `Observable<T>` and RxJS subscriptions/schedulers | Downstream streams must work with ordinary RxJS operators, marble testing, errors, completion, and unsubscription. |
| Extension methods | Pipeable functions; PascalCase source-first wrappers where documented | `source.pipe(Filter(predicate))` and `ObservableCacheEx.Filter(source, predicate)` are different call contracts and require explicit examples. |
| Generic overloads | Generic TypeScript declarations plus documented runtime dispatch | Cache/list routing, selector argument order, optional predicates, comparer streams, error handlers, and options objects need individual coverage. |
| `Change<T>` / `Change<T,K>` and `ChangeSet<T>` / `ChangeSet<T,K>` | Distinct named list/cache shapes or a discriminated generic representation | List duplicate identity and indexes must survive; a map keyed by the item cannot represent all list operations. |
| `out` collection arguments | Explicit target arrays/collections or returned binding objects | The return contract differs from C# and should be described per bind overload. |
| `IComparer<T>` / `IEqualityComparer<T>` | Comparator/equality/key-selector functions or adapters | JS `Map` uses SameValueZero equality; C# structural equality and custom hash comparers do not transfer automatically. |
| `Optional<T>` | A tagged presence/value object | Missing must remain distinguishable from `undefined`, `null`, `0`, and `false`. Join optional arguments and update previous-values require the same distinction. |
| `INotifyPropertyChanged` and expression trees | Explicit observable properties, observable model wrappers, property names/paths, or selector subscriptions | Plain JS object mutation emits no event. Nested property replacement must detach old subscriptions and attach new ones. |
| `INotifyCollectionChanged`, `BindingList`, read-only collections | Observable collection adapter and target mutation protocol | Browser DOM binding is an additional adapter; it does not replicate .NET collection event types by name. |
| `Task<T>` / cancellation / async disposal | Promise or Observable factories, cancellation signals when supported, awaited async cleanup | Stale completions must not resurrect removed items; unsubscription must release pending subscriptions and resources. |
| `TimeSpan`, `DateTimeOffset`, `IScheduler` | Millisecond durations and RxJS `SchedulerLike` | Virtual scheduler tests must establish expiration, batching boundaries, and rescheduling semantics. |
| `int`, `long`, `float`, `double`, `decimal` and nullable variants | Documented JS numeric contract; explicit bigint/decimal integration when offered | One `number` aggregator does not preserve 64-bit integer precision, decimal arithmetic, or every overflow rule. |
| Locks, scheduler synchronization, `Interlocked` | Serialized event-loop delivery and explicit reentrancy rules | Single-threaded JS avoids some races but nested observer edits and synchronous inner observables still require queueing tests. |
| Target-specific parallel operators | Explicit worker or concurrency API if supplied | Promise concurrency is not CPU parallelism or a faithful replacement for .NET thread-pool operators. |

## Parity verification that distinguishes implementations

For each operator family, compare both the resulting collection and the emitted change trace. A final-state-only check misses duplicate removals, incorrect previous values, lost refresh events, bad indexes, extra emissions, and incorrect completion or teardown.

1. **Source transactions and preview.** Check initial connection snapshots, empty suppression, one output for a multi-edit transaction, add/update/remove of the same key inside one transaction, nested edits, callback exceptions, subscription-time edits, and preview observing the pre-edit state. Use `Cache/SourceCacheFixture.cs`, `Cache/FilterOnConnectFixture.cs`, `List/SourceListFixture.cs`, and `List/SourceListPreviewFixture.cs` as the starting fixtures.
2. **List identity and indexes.** Begin with repeated identical object references and equal primitive values. Insert/remove ranges, replace at an index, move both directions, filter, reverse, flatten, and clear. Replay every emitted list change into an independent array; compare after every batch. Preserve each occurrence, even if key-based cache operations would collapse it.
3. **Filter transitions.** Cover false→true, true→false, true→true update, mutable refresh, dynamic predicate changes, no initial predicate, predicate-state streams, re-filter triggers, predicate exceptions, and completion of auxiliary streams. Inspect `Cache/FilterFixture.DynamicPredicateState.UnitTests.cs` and `List/FilterFixture.WithPredicateState.cs`.
4. **Projection and async ordering.** Record factory calls and identities for refresh enabled/disabled, force-transform predicates, inline update, failed transforms, removal while pending, update while pending, inner completion, and immediate synchronous Observable emissions. Inspect `Cache/TransformWithInlineUpdateFixture.cs`, `Cache/TransformSafeAsyncFixture.cs`, `Cache/TransformOnObservableFixture.cs`, and `List/TransformAsyncFixture.cs`.
5. **Flattening and groups.** Test parent removal while a child stream remains active, duplicate child keys from multiple parents, child key changes, mutable regrouping, dynamic group selector changes, disappearance/reappearance of the final group member, immutable group snapshots, and group disposal. Fixtures include `Cache/TransformManyObservableCacheFixture.cs`, `List/TransformManyRefreshFixture.cs`, `Cache/GroupOnObservableFixture.cs`, and `Cache/GroupOnPropertyWithImmutableStateFixture.cs`.
6. **Joins and set combinations.** Exercise duplicate foreign keys, right-side moves between left keys, missing left/right rows, update of the preserved side, add/remove of whole input streams, and different arrival orders. `InnerJoin` uses composite left/right keys upstream, while the outer join selectors carry optional values. Inspect `Cache/InnerJoinFixture.cs`, `Cache/InnerJoinFixtureRaceCondition.cs`, and all four `*JoinManyFixture.cs` files.
7. **Sort, page, virtual windows, and binding.** Test stable ties, refresh-driven movement, comparer changes, unchanged windows, out-of-range requests, remove-induced backfill, target-array identity, reset thresholds, response totals, and transitions between empty/nonempty windows. The new `SortAndVirtualize` contract emits no data until the request observable has produced a value. Use `Cache/SortAndVirtualizeFixture.cs`, `Cache/SortAndPageAndBindFixture.cs`, and `List/VirtualisationFixture.cs`.
8. **Time and lifecycle.** Use RxJS `TestScheduler`, not wall-clock delays, for expiration, batch windows, timer replacement after updates, buffer gates, and disposal ordering. Count per-item subscriptions and disposal calls for add/update/remove, duplicate list references, source errors, completion, and early unsubscription. Compare against `Cache/ExpireAfterFixture.ForStream.cs`, `Cache/ExpireAfterFixture.ForSource.cs`, and `Cache/AsyncDisposeManyFixture.UnitTests.cs`.
9. **Aggregates and diagnostics.** Include empty collections, nullable values, replacements, refresh after in-place mutation, negative values, repeated values, reset-to-empty, and initial/changed emission counts. Validate the selected standard-deviation formula against the pinned upstream implementation, since matching a familiar statistical name is insufficient. Validate cumulative diagnostic counters against the change trace, not only the final size.
10. **Scale and resource bounds.** Benchmark cold initial population separately from one-item edits on 1k/10k/100k collections. Count selector/comparer invocations to expose whole-collection recomputation. Measure listener counts after removal/unsubscription and distinguish data windowing from DOM row virtualization. Record environment, seeds, inputs, and elapsed units with each result.

The cited fixture paths are relative to [`src/DynamicData.Tests`](https://github.com/reactivemarbles/DynamicData/tree/ef790af138811c3268c9bb687886c2acbd0b66c0/src/DynamicData.Tests). A parity claim should identify which fixture behaviors were ported, which upstream overloads were folded into one JS contract, and which remain unavailable.

## License and attribution

DynamicData is MIT licensed. Its root license attributes **Copyright (c) Roland Pheasant 2011-2022**; the inspected source headers use **Copyright (c) 2011-2025 Roland Pheasant**. Preserve the upstream notice and complete MIT license when redistributing substantial portions or derived code. The immutable [upstream license](https://github.com/reactivemarbles/DynamicData/blob/ef790af138811c3268c9bb687886c2acbd0b66c0/LICENSE) supplies the exact text. RxJS is a separate dependency with its own license; distribute its license alongside vendored or bundled RxJS code.
