import { SourceList, SourceCache, type ChangeSet, type DisposableLike, type ChangeSetBindingTarget, type PascalCaseCollectionTarget, subscribeMany, bind, autoRefresh, filterOnProperty, whenPropertyChanged, observeProperty, notifyPropertyChanged, ObservableChangeSet } from '@wieslawsoltes/dynamicdataweb';
import type { PropertyValue, ReactivePropertyNotification } from '@wieslawsoltes/dynamicdataweb/lifecycle';
import { Subject, type Observable } from 'rxjs';

class Item {
  readonly Changed = new Subject<ReactivePropertyNotification<Item>>();
  constructor(public Id: string, public Name: string, public Address: { City: string }) {}
}
const source = new SourceCache((item: Item) => item.Id);
const target: ChangeSetBindingTarget<Item, string> = { ApplyChanges(changes: ChangeSet<Item, string>) { changes.forEach(change => { const key: string = change.key; void key; }); } };
source.connect().pipe(bind(target));
source.Connect().Bind(target).AutoRefresh('Address.City');
source.connect().pipe(autoRefresh<Item, string>('Address.City'), filterOnProperty<Item, string>('Address.City', item => item.Address.City !== ''));
const cleanup: DisposableLike = { Dispose() {} };
source.connect().pipe(subscribeMany<Item, string>(() => cleanup));
source.Connect().SubscribeMany(() => ({ Dispose() {} }));
ObservableChangeSet.createList((list: SourceList<Item>) => { list.add(new Item('a', 'Ada', { City: 'London' })); return cleanup; });
const collection: PascalCaseCollectionTarget<Item> = { Edit(action) { action({ Clear() {}, AddRange(items: Iterable<Item>) { Array.from(items); } }); } };
source.connect().pipe(bind<Item, string>(collection));
const item = new Item('b', 'Grace', { City: 'Paris' });
const property: Observable<PropertyValue<Item, string>> = whenPropertyChanged<Item, string>(item, 'Address.City');
const projected: Observable<string> = observeProperty(item, value => value.Name);
const nested: Observable<string> = observeProperty<Item, string>(item, 'Address.City');
notifyPropertyChanged(item, ''); notifyPropertyChanged(item, null); notifyPropertyChanged(item);
void property; void projected; void nested;
// @ts-expect-error a delta target receives keyed ChangeSets, not item arrays.
const invalid: ChangeSetBindingTarget<Item, string> = { ApplyChanges(changes: Item[]) {} };
void invalid;
