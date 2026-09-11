import { BehaviorSubject, Observable, of } from 'rxjs';
import {
  SourceCache, SourceList, Optional, ChangeSet, Change, Group, ObservableCache,
  Filter, Transform, Sort, Page, ToCollection, SortExpressionComparer, PageRequest,
  filter, transform, groupOn, innerJoin, leftJoin, transformAsync, autoRefresh,
  toObservableChangeSet, toObservableOptional, queryWhenChanged, sum, virtualise,
  addKey, transformToTree, mergeManyChangeSets, asAggregator, asWatcher,
  ChangeAwareCache, ChangeAwareList, IntermediateCache, applyChanges, fluent,
  valueOr, convertOptional, firstOrOptional, indexOfMany, retryWithBackOff,
  scheduleRecurringAction, type DataObservable, type Node,
  expireAfter, limitSizeTo, top, sortAndBind,
} from 'dynamicdata-rxjs';
import { SourceCache as CoreCache } from 'dynamicdata-rxjs/core';
import { filter as moduleFilter, Transform as ModuleTransform } from 'dynamicdata-rxjs/operators';
import { groupOn as moduleGroup } from 'dynamicdata-rxjs/advanced';
import { autoRefresh as moduleRefresh } from 'dynamicdata-rxjs/lifecycle';
import { queryWhenChanged as moduleQuery } from 'dynamicdata-rxjs/extras';
import { asAggregator as moduleAggregate } from 'dynamicdata-rxjs/helpers';
import { firstOrOptional as moduleOptional } from 'dynamicdata-rxjs/kernel';

interface Person { id: number; name: string; active: boolean; score: number; parentId?: number; }
interface View { id: number; label: string; }
const people = new SourceCache<Person, number>(person => person.id);
const subpathCache = new CoreCache<Person, number>(person => person.id);
const subpathNames: Observable<string[]> = subpathCache.connect().pipe(moduleFilter(person => person.active), ModuleTransform(person => person.name), ToCollection());
const subpathGroups = subpathCache.connect().pipe(moduleRefresh('active'), moduleGroup(person => person.active));
const subpathQuery = subpathCache.connect().pipe(moduleQuery(query => query.count));
const subpathAggregate = moduleAggregate(subpathCache);
const subpathOptional: Optional<Person> = moduleOptional(subpathCache.items);
void [subpathNames, subpathGroups, subpathQuery, subpathAggregate, subpathOptional];
people.Edit(cache => {
  cache.AddOrUpdate({ id: 1, name: 'Ada', active: true, score: 42 });
  cache.AddOrUpdate([{ id: 2, name: 'Grace', active: false, score: 100 }]);
  cache.RefreshKey(1); cache.RemoveKeys([2]);
});
const person: Person = people.Lookup(1).Value;
const keys: number[] = people.Keys;
const counts: Observable<number> = people.CountChanged;

const projected: Observable<View[]> = people.connect().pipe(
  filter(person => person.active),
  transform(person => ({ id: person.id, label: person.name.toUpperCase() })),
  Sort((left, right) => left.label.localeCompare(right.label)),
  Page(new PageRequest(1, 20)),
  ToCollection(),
);
const pascal: DataObservable<View, number> = people.Connect()
  .Filter(person => person.active)
  .Transform(person => ({ id: person.id, label: person.name }))
  .Sort((left, right) => left.label.localeCompare(right.label));
pascal.ToCollection().Subscribe(views => { const value: string = views[0].label; void value; });

const directOperators: Observable<View[]> = people.connect().pipe(
  Filter((person: Person) => person.score > 0),
  Transform((person: Person) => ({ id: person.id, label: person.name })),
  ToCollection(),
);
const requests = new BehaviorSubject({ startIndex: 0, size: 10 });
people.connect().pipe(virtualise(requests), autoRefresh('active')).subscribe();
const grouped: Observable<ChangeSet<Group<Person, number, boolean>, boolean>> = people.connect().pipe(groupOn(person => person.active));
const readonly = new ObservableCache(people.connect());
const lookup: Optional<Person> = readonly.lookup(1);

const list = new SourceList<Person>();
list.AddRange([person]); list.InsertRange(0, [person]); list.InsertRange([person], 0);
list.Move(0, 1); list.ReplaceAt(0, person); list.RemoveRange(0, 1);
const keyed: Observable<ChangeSet<Person, number>> = list.connect().pipe(addKey(person => person.id));
const total: Observable<number> = people.connect().pipe(sum(person => person.score));
const names: Observable<string[]> = people.connect().pipe(queryWhenChanged(query => query.items.map(person => person.name)));
const selected: Observable<Optional<Person>> = people.connect().pipe(toObservableOptional(1));
const tree: Observable<ChangeSet<Node<Person, number>, number>> = people.connect().pipe(transformToTree(person => person.parentId));
const asyncViews: Observable<ChangeSet<View, number>> = people.connect().pipe(transformAsync(async (person, key, previous, signal) => {
  const numericKey: number = key; const aborted: boolean = signal.aborted;
  void previous; void numericKey; void aborted;
  return { id: person.id, label: person.name };
}));

const ratings = new SourceCache<{ id: string; personId: number; rating: number }, string>(x => x.id);
const joined = people.connect().pipe(innerJoin(ratings.connect(), rating => rating.personId,
  (left: Person, right) => ({ name: left.name, rating: right.rating })));
const leftJoined = people.connect().pipe(leftJoin(ratings.connect(), rating => rating.personId,
  (left: Person, right) => ({ name: left.name, rating: right.getOrElse({ id: '', personId: 0, rating: 0 }).rating })));
const many = people.connect().pipe(mergeManyChangeSets(person => of(new ChangeSet([{ reason: 'add', key: person.id, current: person }]))));
const input: Observable<ChangeSet<Person, number>> = of([person]).pipe(toObservableChangeSet((person: Person) => person.id));
const comparer = SortExpressionComparer.Ascending((person: Person) => person.name).ThenByDescending(person => person.score);
people.connect().pipe(Sort(comparer)).subscribe();
const expiredCache: Observable<Array<[number, Person]>> = expireAfter(people, person => person.score * 100, { pollingInterval: 50 });
const expiredList: Observable<Person[]> = expireAfter(list, 1000, { pollingInterval: 100 });
const limitedCache: Observable<Array<[number, Person]>> = limitSizeTo(people, 10);
const limitedList: Observable<Person[]> = limitSizeTo(list, 10);
const bound: Person[] = [];
people.connect().pipe(top(comparer, 5), sortAndBind(comparer, bound)).subscribe();
people.connect().pipe(sortAndBind(bound, comparer)).subscribe();
people.Connect().Top(comparer, 5).SortAndBind(comparer, bound).Subscribe();
void [expiredCache, expiredList, limitedCache, limitedList];
const aggregate = asAggregator(people); aggregate.Messages.forEach(set => { const item: Person = set[0].Current; void item; });
const watcher = asWatcher(people); watcher.Watch(1).subscribe(change => { const value: number = change.Key; void value; });

const aware = new ChangeAwareCache<Person, number>(new Map()); aware.Add(person, 1); aware.CaptureChanges();
const awareList = new ChangeAwareList([person]); awareList.CaptureChanges();
const intermediate = new IntermediateCache<Person, number>(); intermediate.AddOrUpdate(person, 1);
const map = applyChanges(new Map<number, Person>(), new ChangeSet([new Change('add', 1, person)]));
const optional = firstOrOptional([person]);
const optionalName: Optional<string> = convertOptional(optional, value => value.name);
const defaulted: Person | string = valueOr(optional, 'none');
const indexes = indexOfMany([person], [person]); const index: number = indexes[0].Index;
const fluentObservable = fluent(people.connect()); fluentObservable.Filter(person => person.active);
const safe: Observable<number> = of(1).pipe(retryWithBackOff({ maxRetries: 3, initialDelay: 100 }));
const recurring = scheduleRecurringAction(100, () => {}); recurring.Dispose();

// Negative assertions catch accidental erasure of keys, items or transformation output.
// @ts-expect-error numeric cache keys reject strings
people.Lookup('wrong');
// @ts-expect-error a Person has no nonexistent property
people.Connect().Filter(person => person.nonexistent);
// @ts-expect-error transformed output is View, not Person
const wrongProjection: Observable<Person[]> = projected;
// @ts-expect-error list members must be Person
list.Add({ id: 'bad' });
// @ts-expect-error cache updates require complete Person shape
people.AddOrUpdate({ id: 3 });
// @ts-expect-error readonly cache does not expose mutation
readonly.addOrUpdate(person);

void [person, keys, counts, projected, pascal, directOperators, grouped, lookup, keyed, total, names, selected, tree, asyncViews, joined, leftJoined, many, input, map, optionalName, defaulted, index, safe];
