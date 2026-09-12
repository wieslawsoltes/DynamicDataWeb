import type { Observable, Observer, ObservableInput, OperatorFunction, MonoTypeOperatorFunction, PartialObserver, SchedulerLike, Subscription, TeardownLogic } from 'rxjs';
export type CollectionKind = 'cache' | 'list';
export type ChangeReasonValue = 'add' | 'update' | 'remove' | 'refresh' | 'move' | 'addRange' | 'removeRange' | 'clear' | 'replace';
export type KeySelector<T, K> = (item: T) => K;
export type Selector<T, R, K = unknown> = (item: T, key: K) => R;
export type Predicate<T, K = unknown> = Selector<T, boolean, K>;
export type Comparer<T> = ((left: T, right: T) => number) | { compare(left: T, right: T): number } | { Compare(left: T, right: T): number };
export type EqualityComparer<T> = ((left: T, right: T) => boolean) | { equals(left: T, right: T): boolean } | { Equals(left: T, right: T): boolean };
export type ChangeOperator<T, R = T, K = unknown, RK = K> = OperatorFunction<ChangeSet<T, K>, ChangeSet<R, RK>>;
export type ChangeSource<T, K = unknown> = Observable<ChangeSet<T, K>> | ReadonlyCache<T, K> | ReadonlyList<T>;
export type DisposableLike = TeardownLogic | { dispose(): void } | { Dispose(): void };
export type PropertyPath<T> = keyof T | `${Extract<keyof T, string>}.${string}`;
export interface Disposable { dispose(): void; Dispose(): void; unsubscribe(): void; readonly closed?: boolean; }
export interface FluentObservable<T> extends Observable<T> {
  Subscribe: Observable<T>['subscribe']; Pipe: Observable<T>['pipe'];
}
export interface DataObservable<T, K = unknown> extends FluentObservable<ChangeSet<T, K>> {
  Filter(predicate: Predicate<T, K> | Observable<Predicate<T, K>>, options?: FilterOptions | Observable<unknown>): DataObservable<T, K>;
  Transform<R>(factory: (item: T, key: K, previous?: T | R) => R, options?: TransformOptions<T, R, K>): DataObservable<R, K>;
  TransformMany<R, RK = unknown>(selector: Selector<T, Iterable<R>, K>, keySelector?: KeySelector<R, RK>): DataObservable<R, RK>;
  TransformAsync<R>(factory: AsyncSelector<T, R, K>, options?: AsyncOptions<T, K>): DataObservable<R, K>;
  FilterOnObservable(selector: Selector<T, ObservableInput<boolean>, K>, options?: AsyncOptions<T, K>): DataObservable<T, K>;
  TransformOnObservable<R>(selector: AsyncSelector<T, R, K>, options?: AsyncOptions<T, K>): DataObservable<R, K>;
  AutoRefresh(property?: PropertyPath<T> | ((item: T) => unknown), options?: RefreshOptions): DataObservable<T, K>;
  AutoRefreshOnObservable(selector: Selector<T, ObservableInput<unknown>, K>, options?: RefreshOptions): DataObservable<T, K>;
  Sort(comparer?: Comparer<T> | Observable<Comparer<T>>, options?: SortConfiguration | Observable<unknown>): DataObservable<T, K>;
  SortBy<R>(selector: KeySelector<T, R>, direction?: SortDirection, options?: SortConfiguration): DataObservable<T, K>;
  Page(requests: PageRequestLike | Observable<PageRequestLike>): DataObservable<T, K>;
  Virtualise(requests: VirtualRequestLike | Observable<VirtualRequestLike>): DataObservable<T, K>;
  Virtualize(requests: VirtualRequestLike | Observable<VirtualRequestLike>): DataObservable<T, K>;
  Top(size: number): DataObservable<T, K>; Top(comparer: Comparer<T> | Observable<Comparer<T>>, size: number): DataObservable<T, K>; Reverse(): DataObservable<T, K>;
  SortAndBind(target: BindingTarget<T, K>, comparer?: Comparer<T>, options?: SortConfiguration): DataObservable<T, K>;
  SortAndBind(comparer: Comparer<T> | Observable<Comparer<T>>, target: BindingTarget<T, K>, options?: SortConfiguration): DataObservable<T, K>;
  Bind(target: BindingTarget<T, K>): DataObservable<T, K>;
  ToCollection(): FluentObservable<T[]>;
  ToSortedCollection<R>(selectorOrComparer?: KeySelector<T, R> | Comparer<T>, direction?: SortDirection): FluentObservable<T[]>;
  AsObservableCache(keySelector?: KeySelector<T, K>): ObservableCache<T, K>;
  AsObservableList(): ObservableList<T>;
  GroupOn<G>(selector: Selector<T, G, K> | Observable<Selector<T, G, K>>, regrouper?: Observable<unknown>): DataObservable<Group<T, K, G>, G>;
  Group<G>(selector: Selector<T, G, K>, regrouper?: Observable<unknown>): DataObservable<Group<T, K, G>, G>;
  GroupOnImmutable<G>(selector: Selector<T, G, K>, regrouper?: Observable<unknown>): DataObservable<ImmutableGroup<T, K, G>, G>;
  ChangeKey<RK>(selector: Selector<T, RK, K>): DataObservable<T, RK>;
  AddKey<RK>(selector: KeySelector<T, RK>): DataObservable<T, RK>;
  RemoveKey(): DataObservable<T, unknown>; RemoveIndex(): DataObservable<T, K>;
  DistinctValues<R>(selector?: Selector<T, R, K>): DataObservable<R, R>;
  Watch(key: K): FluentObservable<Change<T, K>>;
  ToObservableOptional(key: K, initial?: boolean, comparer?: EqualityComparer<T>): FluentObservable<Optional<T>>;
  QueryWhenChanged<R = QuerySnapshot<T, K>>(selector?: (query: QuerySnapshot<T, K>) => R): FluentObservable<R>;
  Count(predicate?: Predicate<T, K>): FluentObservable<number>;
  Sum(selector?: Selector<T, number, K>): FluentObservable<number>;
  Average(selector?: Selector<T, number, K>, fallback?: number): FluentObservable<number>;
  Avg(selector?: Selector<T, number, K>, fallback?: number): FluentObservable<number>;
  Min(selector?: Selector<T, number, K>, fallback?: number): FluentObservable<number>;
  Max(selector?: Selector<T, number, K>, fallback?: number): FluentObservable<number>;
  StdDev(selector?: Selector<T, number, K>, fallback?: number): FluentObservable<number>;
  IsEmpty(): FluentObservable<boolean>; IsNotEmpty(): FluentObservable<boolean>;
  DisposeMany(disposer?: (item: T) => void): DataObservable<T, K>;
  SubscribeMany(selector: Selector<T, DisposableLike, K>): DataObservable<T, K>;
  MergeMany<R>(selector: Selector<T, ObservableInput<R>, K>): FluentObservable<R>;
  OnItemAdded(action: Selector<T, void, K>): DataObservable<T, K>;
  OnItemUpdated(action: (current: T, previous: T, key: K) => void): DataObservable<T, K>;
  OnItemRemoved(action: Selector<T, void, K>): DataObservable<T, K>;
  OnItemRefreshed(action: Selector<T, void, K>): DataObservable<T, K>;
  NotEmpty(): DataObservable<T, K>; SkipInitial(): DataObservable<T, K>;
  SuppressRefresh(): DataObservable<T, K>; DeferUntilLoaded(): DataObservable<T, K>;
  Batch(duration?: number, scheduler?: SchedulerLike): DataObservable<T, K>;
  BufferIf(pause: Observable<boolean>, options?: BufferOptions): DataObservable<T, K>;
  ExpireAfter(selector: Selector<T, number | null | undefined, K>, options?: TimerOptions): DataObservable<T, K>;
  LimitSizeTo(size: number): DataObservable<T, K>;
  TransformToTree(pivot: KeySelector<T, K | null | undefined>, predicate?: Predicate<Node<T, K>, K> | Observable<Predicate<Node<T, K>, K>>): DataObservable<Node<T, K>, K>;
}

export class Optional<T> {
  constructor(value?: T, hasValue?: boolean);
  static some<T>(value: T): Optional<T>; static Some: typeof Optional.some;
  static none<T = never>(): Optional<T>; static None: typeof Optional.none;
  static of<T>(value: T): Optional<NonNullable<T>>; static Of: typeof Optional.of;
  static create: typeof Optional.of; static Create: typeof Optional.of; static toOptional: typeof Optional.of; static ToOptional: typeof Optional.of;
  static fromOptional<T>(optional: Optional<T>): T; static FromOptional: typeof Optional.fromOptional;
  readonly hasValue: boolean; readonly HasValue: boolean; readonly value: T; readonly Value: T;
  readonly valueOrDefault: T | undefined; readonly ValueOrDefault: T | undefined;
  getOrElse<R>(fallback: R | (() => R)): T | R; GetOrElse: Optional<T>['getOrElse'];
  map<R>(project: (value: T) => R): Optional<R>; Map: Optional<T>['map'];
  ifHasValue(action: (value: T) => void): this; IfHasValue: Optional<T>['ifHasValue'];
  equals(other: unknown): boolean; Equals: Optional<T>['equals']; toString(): string;
}
export const ChangeReason: Readonly<Record<string, ChangeReasonValue> & { Add: 'add'; Update: 'update'; Remove: 'remove'; Refresh: 'refresh'; Moved: 'move'; Move: 'move' }>;
export const ListChangeReason: Readonly<Record<string, ChangeReasonValue> & { Add: 'add'; AddRange: 'addRange'; Replace: 'replace'; Remove: 'remove'; RemoveRange: 'removeRange'; Clear: 'clear'; Refresh: 'refresh'; Moved: 'move' }>;
export const ChangeType: Readonly<{ Item: 'item'; Range: 'range' }>;
export interface ChangeRecord<T, K = unknown> {
  reason: ChangeReasonValue; key?: K; current?: T; previous?: T; currentIndex?: number; previousIndex?: number;
  range?: RangeChange<T> | { items: T[]; index: number } | T[];
}
export class Change<T, K = unknown> {
  constructor(change: ChangeRecord<T, K>);
  constructor(reason: ChangeReasonValue, key: K, current: T, previous?: T | Optional<T>, currentIndex?: number, previousIndex?: number);
  reason: ChangeReasonValue; key: K; current: T; previous: T | undefined; currentIndex: number; previousIndex: number;
  readonly Reason: ChangeReasonValue; readonly Key: K; readonly Current: T; readonly Previous: Optional<T>;
  readonly CurrentIndex: number; readonly PreviousIndex: number; range?: RangeChange<T> | T[];
  equals(other: unknown): boolean; Equals: Change<T, K>['equals']; toString(): string;
}
export class RangeChange<T> implements Iterable<T> {
  constructor(items?: Iterable<T>, index?: number); items: T[]; index: number; readonly count: number; readonly Count: number; readonly Index: number;
  [Symbol.iterator](): Iterator<T>;
}
export class ListChange<T> extends Change<T, unknown> {
  constructor(change: ChangeRecord<T>);
  constructor(reason: ChangeReasonValue, current: T | Iterable<T>, currentIndex?: number, previous?: T | Optional<T>, previousIndex?: number);
  readonly type: 'item' | 'range'; readonly Type: 'item' | 'range'; readonly item: this; readonly Item: this; readonly Range: RangeChange<T> | undefined;
}
export const ItemChange: typeof ListChange;
export interface SortedItems<T, K> extends Iterable<readonly [K, T]> {
  items: T[]; keys: K[]; keyValues: [K, T][]; count: number; comparer: (a: T, b: T) => number; sortReason: string;
  Items: T[]; Keys: K[]; Count: number; Comparer: (a: T, b: T) => number; SortReason: string;
}
/** Array-compatible; keys is ordered cache metadata instead of Array.keys(). */
export interface ChangeSet<T, K = unknown> extends Omit<Array<Change<T, K>>, 'keys'> {
  push(...changes: ChangeRecord<T, K>[]): number; Push: ChangeSet<T, K>['push'];
  kind: CollectionKind; items?: T[]; keys?: K[]; sortedItems?: SortedItems<T, K>; SortedItems?: SortedItems<T, K>;
  response?: PageResponse | VirtualResponse; Response?: PageResponse | VirtualResponse;
  readonly size: number; readonly count: number; readonly Count: number; readonly Size: number;
  readonly adds: number; readonly updates: number; readonly removes: number; readonly refreshes: number; readonly moves: number; readonly totalChanges: number;
  readonly Adds: number; readonly Updates: number; readonly Replaced: number; readonly Removes: number; readonly Refreshes: number; readonly Moves: number; readonly TotalChanges: number;
}
export const ChangeSet: {
  new<T, K = unknown>(changes?: Iterable<ChangeRecord<T, K>> | number, kind?: CollectionKind, items?: Iterable<T>): ChangeSet<T, K>;
  readonly empty: ChangeSet<never>; readonly Empty: ChangeSet<never>;
};
export function snapshotChanges<T, K = unknown>(items: Iterable<T> | Map<K, T>, kind?: CollectionKind, keySelector?: KeySelector<T, K>): ChangeSet<T, K>;
export function applyChanges<T, K>(state: Map<K, T>, changes: ChangeSet<T, K> | Iterable<ChangeRecord<T, K>>): Map<K, T>;
export function applyChanges<T>(state: T[], changes: ChangeSet<T> | Iterable<ChangeRecord<T>>): T[];
export function setObservableDecorator(decorator: <T>(observable: Observable<T>) => Observable<T>): void;

export interface ReadonlyCollection<T, K = unknown> extends Iterable<T> {
  readonly count: number; readonly Count: number; readonly size: number; readonly Size: number; readonly items: T[]; readonly Items: T[];
  readonly countChanged: FluentObservable<number>; readonly CountChanged: FluentObservable<number>; readonly isDisposed: boolean;
  connect(predicate?: Predicate<T, K>, suppressEmptyChangeSets?: boolean): DataObservable<T, K>;
  Connect: ReadonlyCollection<T, K>['connect']; preview(predicate?: Predicate<T, K>): DataObservable<T, K>; Preview: ReadonlyCollection<T, K>['preview'];
  dispose(): void; Dispose(): void; unsubscribe(): void;
}
export interface ReadonlyCache<T, K> extends ReadonlyCollection<T, K> {
  readonly keys: K[]; readonly Keys: K[]; readonly keyValues: Map<K, T>; readonly KeyValues: Map<K, T>;
  lookup(key: K): Optional<T>; Lookup: ReadonlyCache<T, K>['lookup']; watch(key: K): FluentObservable<Change<T, K>>; Watch: ReadonlyCache<T, K>['watch'];
}
export interface ReadonlyList<T> extends ReadonlyCollection<T> {
  get(index: number): T; Get: ReadonlyList<T>['get']; at(index: number): T | undefined; At: ReadonlyList<T>['at'];
  indexOf(item: T): number; IndexOf: ReadonlyList<T>['indexOf']; contains(item: T): boolean; Contains: ReadonlyList<T>['contains'];
}
export class SourceCache<T, K> implements ReadonlyCache<T, K> {
  constructor(keySelector: KeySelector<T, K>);
  readonly keySelector: KeySelector<T, K>; readonly KeySelector: KeySelector<T, K>;
  readonly count: number; readonly Count: number; readonly size: number; readonly Size: number; readonly items: T[]; readonly Items: T[];
  readonly keys: K[]; readonly Keys: K[]; readonly keyValues: Map<K, T>; readonly KeyValues: Map<K, T>;
  readonly countChanged: FluentObservable<number>; readonly CountChanged: FluentObservable<number>; readonly isDisposed: boolean;
  connect(predicate?: Predicate<T, K>, suppressEmptyChangeSets?: boolean): DataObservable<T, K>; Connect: SourceCache<T, K>['connect'];
  preview(predicate?: Predicate<T, K>): DataObservable<T, K>; Preview: SourceCache<T, K>['preview'];
  lookup(key: K): Optional<T>; Lookup: SourceCache<T, K>['lookup']; watch(key: K): FluentObservable<Change<T, K>>; Watch: SourceCache<T, K>['watch'];
  edit(action: (updater: SourceCache<T, K>) => void): this; Edit: SourceCache<T, K>['edit'];
  addOrUpdate(itemOrItems: T | Iterable<T> | Map<K, T>, keyOrComparer?: K | EqualityComparer<T>): this; AddOrUpdate: SourceCache<T, K>['addOrUpdate'];
  addOrUpdatePairs(pairs: Iterable<readonly [K, T] | { key: K; value: T }>): this; AddOrUpdatePairs: SourceCache<T, K>['addOrUpdatePairs'];
  removeKey(key: K): this; RemoveKey: SourceCache<T, K>['removeKey']; removeKeys(keys: Iterable<K>): this; RemoveKeys: SourceCache<T, K>['removeKeys'];
  remove(itemOrItems: T | K | Iterable<T | K>): this; Remove: SourceCache<T, K>['remove']; removeMany(items: Iterable<T>): this; RemoveMany: SourceCache<T, K>['removeMany'];
  removeWhere(predicate: Predicate<T, K>): this; RemoveWhere: SourceCache<T, K>['removeWhere'];
  refresh(itemOrItems?: T | K | Iterable<T | K>): this; Refresh: SourceCache<T, K>['refresh'];
  refreshKey(key: K): this; RefreshKey: SourceCache<T, K>['refreshKey']; refreshKeys(keys: Iterable<K>): this; RefreshKeys: SourceCache<T, K>['refreshKeys'];
  clear(): this; Clear: SourceCache<T, K>['clear']; load(items: Iterable<T>): this; Load: SourceCache<T, K>['load'];
  editDiff(items: Iterable<T>, comparer?: EqualityComparer<T>): this; EditDiff: SourceCache<T, K>['editDiff'];
  clone(changes: Iterable<ChangeRecord<T, K>>): this; Clone: SourceCache<T, K>['clone']; update(changes: Iterable<ChangeRecord<T, K>>): this; Update: SourceCache<T, K>['update'];
  getKey(item: T): K; GetKey: SourceCache<T, K>['getKey']; getKeyValues(items: Iterable<T>): [K, T][]; GetKeyValues: SourceCache<T, K>['getKeyValues'];
  asObservableCache(): ObservableCache<T, K>; AsObservableCache: SourceCache<T, K>['asObservableCache'];
  suspendNotifications(): Disposable; SuspendNotifications: SourceCache<T, K>['suspendNotifications']; suspendCount(): Disposable; SuspendCount: SourceCache<T, K>['suspendCount'];
  dispose(): void; Dispose(): void; unsubscribe(): void; [Symbol.iterator](): Iterator<T>;
}
export class SourceList<T> implements ReadonlyList<T> {
  constructor(source?: Observable<ChangeSet<T>> | Iterable<T>);
  readonly count: number; readonly Count: number; readonly size: number; readonly Size: number; readonly items: T[]; readonly Items: T[];
  readonly countChanged: FluentObservable<number>; readonly CountChanged: FluentObservable<number>; readonly isDisposed: boolean;
  connect(predicate?: Predicate<T>, suppressEmptyChangeSets?: boolean): DataObservable<T>; Connect: SourceList<T>['connect'];
  preview(predicate?: Predicate<T>): DataObservable<T>; Preview: SourceList<T>['preview'];
  edit(action: (updater: SourceList<T>) => void): this; Edit: SourceList<T>['edit'];
  get(index: number): T; Get: SourceList<T>['get']; at(index: number): T | undefined; At: SourceList<T>['at'];
  indexOf(item: T): number; IndexOf: SourceList<T>['indexOf']; contains(item: T): boolean; Contains: SourceList<T>['contains'];
  add(item: T): this; Add: SourceList<T>['add']; addRange(items: Iterable<T>): this; AddRange: SourceList<T>['addRange'];
  insert(index: number, item: T): this; Insert: SourceList<T>['insert'];
  insertRange(items: Iterable<T>, index: number): this; insertRange(index: number, items: Iterable<T>): this; InsertRange: SourceList<T>['insertRange'];
  remove(item: T): boolean; Remove: SourceList<T>['remove']; removeAt(index: number): this; RemoveAt: SourceList<T>['removeAt'];
  removeMany(items: Iterable<T>): this; RemoveMany: SourceList<T>['removeMany']; removeWhere(predicate: Predicate<T, number>): this; RemoveWhere: SourceList<T>['removeWhere'];
  removeRange(index: number, count: number): this; RemoveRange: SourceList<T>['removeRange'];
  clear(): this; Clear: SourceList<T>['clear']; load(items: Iterable<T>): this; Load: SourceList<T>['load'];
  replaceAt(index: number, item: T): this; ReplaceAt: SourceList<T>['replaceAt']; set(index: number, item: T): this; Set: SourceList<T>['set'];
  replace(original: T, replacement: T): this; Replace: SourceList<T>['replace']; move(previousIndex: number, currentIndex: number): this; Move: SourceList<T>['move'];
  refresh(item?: T): this; Refresh: SourceList<T>['refresh']; refreshAt(index: number): this; RefreshAt: SourceList<T>['refreshAt']; refreshMany(items: Iterable<T>): this; RefreshMany: SourceList<T>['refreshMany'];
  editDiff(items: Iterable<T>, comparer?: EqualityComparer<T>): this; EditDiff: SourceList<T>['editDiff']; clone(changes: ChangeSet<T> | Iterable<ChangeRecord<T>>): this; Clone: SourceList<T>['clone'];
  asObservableList(): ObservableList<T>; AsObservableList: SourceList<T>['asObservableList'];
  suspendNotifications(): Disposable; SuspendNotifications: SourceList<T>['suspendNotifications']; suspendCount(): Disposable; SuspendCount: SourceList<T>['suspendCount'];
  dispose(): void; Dispose(): void; unsubscribe(): void; [Symbol.iterator](): Iterator<T>;
}
export interface ObservableCache<T, K> extends ReadonlyCache<T, K> {}
export class ObservableCache<T, K> { constructor(source: Observable<ChangeSet<T, K>> | ReadonlyCache<T, K>, keySelector?: KeySelector<T, K>); readonly keySelector: KeySelector<T, K>; }
export interface ObservableList<T> extends ReadonlyList<T> {}
export class ObservableList<T> { constructor(source: Observable<ChangeSet<T>> | ReadonlyList<T>); }
export class IntermediateCache<T, K> extends SourceCache<T, K> { constructor(source?: Observable<ChangeSet<T, K>> | ReadonlyCache<T, K>); }
export class ChangeAwareCache<T, K> extends SourceCache<T, K> {
  constructor(data?: Map<K, T> | KeySelector<T, K> | number, keySelector?: KeySelector<T, K>);
  captureChanges(): ChangeSet<T, K>; CaptureChanges: ChangeAwareCache<T, K>['captureChanges']; add(item: T, key?: K): this; Add: ChangeAwareCache<T, K>['add'];
}
export class ChangeAwareList<T> extends SourceList<T> {
  constructor(items?: Iterable<T> | ChangeAwareList<T> | number, copyChanges?: boolean); readonly isReadOnly: false; readonly IsReadOnly: false;
  captureChanges(): ChangeSet<T>; CaptureChanges: ChangeAwareList<T>['captureChanges']; copyTo(array: T[], arrayIndex?: number): void; CopyTo: ChangeAwareList<T>['copyTo'];
}

export interface FilterOptions { suppressEmptyChangeSets?: boolean; reapply?: Observable<unknown>; reapplyFilter?: Observable<unknown>; }
export interface TransformError<T, K> { error: unknown; value: T; key: K; }
export interface TransformOptions<T, R, K> { transformOnRefresh?: boolean; suppressEmptyChangeSets?: boolean; inlineUpdate?: (destination: R, source: T) => void; errorHandler?: (error: TransformError<T, K>) => void; forceTransform?: Observable<Predicate<T, K>>; }
export interface SortConfiguration { resetThreshold?: number; suppressEmptyChangeSets?: boolean; resort?: Observable<unknown>; resorter?: Observable<unknown>; }
export type SortDirection = 'ascending' | 'descending' | 'Ascending' | 'Descending' | 1 | -1;
export interface PageRequestLike { page: number; size: number; }
export interface VirtualRequestLike { startIndex: number; size: number; }
export interface PageResponse { page: number; size: number; totalSize: number; pages: number; }
export interface VirtualResponse { startIndex: number; size: number; totalSize: number; }
/** A delta target consumes the original batch, including sorting and range metadata. */
export type ChangeSetBindingTarget<T, K = unknown> = { ApplyChanges(changes: ChangeSet<T, K>): unknown } | { applyChanges(changes: ChangeSet<T, K>): unknown };
export interface PascalCaseListUpdater<T> { Clear(): unknown; AddRange(items: Iterable<T>): unknown; }
export interface PascalCaseCollectionTarget<T> { Edit(action: (collection: PascalCaseListUpdater<T>) => void): unknown; }
export type BindingTarget<T, K = unknown> = T[] | SourceList<T> | SourceCache<T, K> | ChangeSetBindingTarget<T, K> | PascalCaseCollectionTarget<T> | ((items: T[], changes: ChangeSet<T, K>) => void) | { load(items: T[]): unknown } | { next(items: T[]): void };
export function filter<T, K = unknown>(predicate: Predicate<T, K> | Observable<Predicate<T, K>>, reapplyOrOptions?: Observable<unknown> | FilterOptions | boolean, suppressEmptyChangeSets?: boolean): ChangeOperator<T, T, K>;
export function filter<T, S, K = unknown>(state: Observable<S>, predicate: (state: S, item: T, key: K) => boolean, suppressEmptyChangeSets?: boolean): ChangeOperator<T, T, K>;
export const filterImmutable: typeof filter;
export function filterWithState<T, S, K = unknown>(state: Observable<S>, predicate: (state: S, item: T, key: K) => boolean, suppressEmpty?: boolean): ChangeOperator<T, T, K>;
export function transform<T, R, K = unknown>(factory: (item: T, key: K, previous?: T | R) => R, options?: TransformOptions<T, R, K>): ChangeOperator<T, R, K>;
export const transformImmutable: typeof transform; export const convert: typeof transform;
export function cast<T, R = T, K = unknown>(converter?: Selector<T, R, K>): ChangeOperator<T, R, K>;
export function castToObject<T, K = unknown>(): ChangeOperator<T, unknown, K>;
export function ofType<T, R extends T, K = unknown>(type: abstract new (...args: never[]) => R): ChangeOperator<T, R, K>;
export function ofType<T, K = unknown>(type: string | Predicate<T, K>): ChangeOperator<T, T, K>;
export function transformSafe<T, R, K = unknown>(factory: Selector<T, R, K>, errorHandler: (error: TransformError<T, K>) => void, options?: TransformOptions<T, R, K>): ChangeOperator<T, R, K>;
export function transformWithInlineUpdate<T, R, K = unknown>(factory: Selector<T, R, K>, updateAction: (destination: R, source: T) => void, errorHandler?: (error: TransformError<T, K>) => void, transformOnRefresh?: boolean): ChangeOperator<T, R, K>;
export function transformMany<T, R, K = unknown, RK = unknown>(selector: Selector<T, Iterable<R> | ReadonlyList<R> | ReadonlyCache<R, RK>, K>, keySelector?: KeySelector<R, RK>): ChangeOperator<T, R, K, RK>;
export function distinctValues<T, R = T, K = unknown>(selector?: Selector<T, R, K>): ChangeOperator<T, R, K, R>;
export function sort<T, K = unknown>(comparer?: Comparer<T> | Observable<Comparer<T>>, resortOrOptions?: Observable<unknown> | SortConfiguration): ChangeOperator<T, T, K>;
export function page<T, K = unknown>(requests: PageRequestLike | Observable<PageRequestLike>): ChangeOperator<T, T, K>;
export function virtualise<T, K = unknown>(requests: VirtualRequestLike | Observable<VirtualRequestLike>): ChangeOperator<T, T, K>;
export const virtualize: typeof virtualise;
export function top<T, K = unknown>(size: number): ChangeOperator<T, T, K>;
export function top<T, K = unknown>(comparer: Comparer<T> | Observable<Comparer<T>>, size: number): ChangeOperator<T, T, K>;
export function reverse<T, K = unknown>(): ChangeOperator<T, T, K>;
export function sortAndPage<T, K = unknown>(comparer: Comparer<T> | Observable<Comparer<T>>, requests: PageRequestLike | Observable<PageRequestLike>, options?: SortConfiguration): ChangeOperator<T, T, K>;
export function sortAndVirtualise<T, K = unknown>(comparer: Comparer<T> | Observable<Comparer<T>>, requests: VirtualRequestLike | Observable<VirtualRequestLike>, options?: SortConfiguration): ChangeOperator<T, T, K>;
export const sortAndVirtualize: typeof sortAndVirtualise;
export function sortBy<T, R, K = unknown>(selector: KeySelector<T, R>, direction?: SortDirection, options?: SortConfiguration): ChangeOperator<T, T, K>;
export function sortBy<T, K = unknown>(property: keyof T, direction?: SortDirection, options?: SortConfiguration): ChangeOperator<T, T, K>;
export function toCollection<T, K = unknown>(): OperatorFunction<ChangeSet<T, K>, T[]>;
export function bind<T, K = unknown>(target: BindingTarget<T, K>): ChangeOperator<T, T, K>;
export const bindToObservableList: typeof bind; export const bindToObservableCollection: typeof bind;
export function sortAndBind<T, K = unknown>(target: BindingTarget<T, K>, comparer?: Comparer<T>, options?: SortConfiguration): ChangeOperator<T, T, K>;
export function sortAndBind<T, K = unknown>(comparer: Comparer<T> | Observable<Comparer<T>>, target: BindingTarget<T, K>, options?: SortConfiguration): ChangeOperator<T, T, K>;
export function asObservableCache<T, K>(source: Observable<ChangeSet<T, K>>, keySelector?: KeySelector<T, K>): ObservableCache<T, K>;
export function asObservableCache<T, K>(keySelector?: KeySelector<T, K>): (source: Observable<ChangeSet<T, K>>) => ObservableCache<T, K>;
export function asObservableList<T>(source: Observable<ChangeSet<T>>): ObservableList<T>;
export function asObservableList<T>(): (source: Observable<ChangeSet<T>>) => ObservableList<T>;
export function changeKey<T, RK, K = unknown>(selector: Selector<T, RK, K>): ChangeOperator<T, T, K, RK>;
export function removeKey<T, K = unknown>(): ChangeOperator<T, T, K, unknown>;

export interface TimerOptions { scheduler?: SchedulerLike; }
export interface ExpiryOptions extends TimerOptions { pollingInterval?: number; }
export interface RefreshOptions extends TimerOptions { throttle?: number; buffer?: number; }
export interface BufferOptions extends TimerOptions { initialPauseState?: boolean; timeout?: number; }
export interface AsyncOptions<T = unknown, K = unknown> { onError?: (error: { error: unknown; item: T; key: K }) => void; waitForCompletion?: boolean; maximumConcurrency?: number; transformOnRefresh?: boolean; }
export type AsyncSelector<T, R, K = unknown> = (item: T, key: K, previous: R | undefined, signal: AbortSignal) => ObservableInput<R> | R;
export interface PropertyValue<T, V = unknown> { sender: T; propertyName: PropertyKey | ((item: T) => V) | null | undefined; value: V; previous?: V; }
/** PropertyChanged/Changed observables are discovered structurally; no ReactiveWeb dependency is required. */
export interface ReactivePropertyNotification<T, V = unknown> { Sender: T; PropertyName: PropertyKey | null | undefined; Value?: V; OldValue?: V; }
export function notifyPropertyChanged<T, P extends keyof T>(item: T, propertyName: P, previous?: T[P]): void;
export function notifyPropertyChanged<T>(item: T, propertyName?: null | ''): void;
export function createObservableObject<T extends object>(item: T): T;
export const observableObject: typeof createObservableObject;
export function whenPropertyChanged<T, P extends keyof T>(itemOrChanges: T | Observable<ChangeSet<T>>, property: P, notifyInitial?: boolean): Observable<PropertyValue<T, T[P]>>;
export function whenPropertyChanged<T, V>(itemOrChanges: T | Observable<ChangeSet<T>>, property: (item: T) => V, notifyInitial?: boolean): Observable<PropertyValue<T, V>>;
export function whenPropertyChanged<T, V = unknown>(itemOrChanges: T | Observable<ChangeSet<T>>, property: `${Extract<keyof T, string>}.${string}`, notifyInitial?: boolean): Observable<PropertyValue<T, V>>;
export function observeProperty<T, V>(itemOrChanges: T | Observable<ChangeSet<T>>, property: (item: T) => V, notifyInitial?: boolean): Observable<V>;
export function observeProperty<T, V = unknown>(itemOrChanges: T | Observable<ChangeSet<T>>, property: `${Extract<keyof T, string>}.${string}`, notifyInitial?: boolean): Observable<V>;
export function observeProperty<T, P extends keyof T>(itemOrChanges: T | Observable<ChangeSet<T>>, property: P, notifyInitial?: boolean): Observable<T[P]>;
export const whenValueChanged: typeof observeProperty;
export function autoRefreshOnObservable<T, K = unknown>(selector: Selector<T, ObservableInput<unknown>, K>, options?: RefreshOptions): ChangeOperator<T, T, K>;
export function autoRefresh<T, K = unknown>(property?: PropertyPath<T> | ((item: T) => unknown), options?: RefreshOptions): ChangeOperator<T, T, K>;
export function filterOnObservable<T, K = unknown>(selector: Selector<T, ObservableInput<boolean> | boolean, K>, options?: AsyncOptions<T, K>): ChangeOperator<T, T, K>;
export function transformOnObservable<T, R, K = unknown>(selector: AsyncSelector<T, R, K>, options?: AsyncOptions<T, K>): ChangeOperator<T, R, K>;
export function transformAsync<T, R, K = unknown>(factory: AsyncSelector<T, R, K>, options?: AsyncOptions<T, K>): ChangeOperator<T, R, K>;
export function transformSafeAsync<T, R, K = unknown>(factory: AsyncSelector<T, R, K>, onError: NonNullable<AsyncOptions<T, K>['onError']>, options?: AsyncOptions<T, K>): ChangeOperator<T, R, K>;
export function mergeMany<T, R, K = unknown>(selector: Selector<T, ObservableInput<R>, K>): OperatorFunction<ChangeSet<T, K>, R>;
export function mergeManyItems<T, R, K = unknown>(selector: Selector<T, ObservableInput<R>, K>): OperatorFunction<ChangeSet<T, K>, { item: T; key: K; value: R }>;
export function subscribeMany<T, K = unknown>(selector: Selector<T, DisposableLike, K>): ChangeOperator<T, T, K>;
export function disposeMany<T, K = unknown>(disposer?: (item: T) => void): ChangeOperator<T, T, K>;
export function onItemAdded<T, K = unknown>(action: Selector<T, void, K>): ChangeOperator<T, T, K>;
export function onItemUpdated<T, K = unknown>(action: (current: T, previous: T, key: K) => void): ChangeOperator<T, T, K>;
export function onItemRemoved<T, K = unknown>(action: Selector<T, void, K>): ChangeOperator<T, T, K>;
export function onItemRefreshed<T, K = unknown>(action: Selector<T, void, K>): ChangeOperator<T, T, K>;
export function notEmpty<T, K = unknown>(): ChangeOperator<T, T, K>;
export function skipInitial<T, K = unknown>(): ChangeOperator<T, T, K>;
export function deferUntilLoaded<T, K = unknown>(): ChangeOperator<T, T, K>;
export function watch<T, K>(key: K): OperatorFunction<ChangeSet<T, K>, Change<T, K>>;
export function batch<T, K = unknown>(duration?: number, scheduler?: SchedulerLike): ChangeOperator<T, T, K>;
export function bufferIf<T, K = unknown>(pause: Observable<boolean>, options?: BufferOptions): ChangeOperator<T, T, K>;
export const batchIf: typeof bufferIf;
export function expireAfter<T, K = unknown>(selector: Selector<T, number | null | undefined, K> | number, options?: TimerOptions): ChangeOperator<T, T, K>;
export function expireAfter<T, K>(source: SourceCache<T, K>, selector: Selector<T, number | null | undefined, K> | number, options?: ExpiryOptions | SchedulerLike): Observable<Array<[K, T]>>;
export function expireAfter<T>(source: SourceList<T>, selector: Selector<T, number | null | undefined> | number, options?: ExpiryOptions | SchedulerLike): Observable<T[]>;
export function limitSizeTo<T, K = unknown>(size: number): ChangeOperator<T, T, K>;
export function limitSizeTo<T, K>(source: SourceCache<T, K>, size: number, options?: TimerOptions | SchedulerLike): Observable<Array<[K, T]>>;
export function limitSizeTo<T>(source: SourceList<T>, size: number, options?: TimerOptions | SchedulerLike): Observable<T[]>;
export interface ChangeSetConversionOptions<T, K = unknown> extends ExpiryOptions { singleItem?: boolean; limitSize?: number; expireAfter?: Selector<T, number | null | undefined, K> | number; keySelector?: KeySelector<T, K>; }
export function toObservableChangeSet<T, K>(keySelector: KeySelector<T, K>, options?: ChangeSetConversionOptions<T, K>): OperatorFunction<T | Iterable<T>, ChangeSet<T, K>>;
export function toObservableChangeSet<T>(options?: ChangeSetConversionOptions<T>): OperatorFunction<T | Iterable<T>, ChangeSet<T>>;
export function toObservableChangeSet<T, K>(options: ChangeSetConversionOptions<T, K> & { keySelector: KeySelector<T, K> }): OperatorFunction<T | Iterable<T>, ChangeSet<T, K>>;
export const ObservableChangeSet: Readonly<{
  create<T, K>(subscribe: (source: SourceCache<T, K>) => DisposableLike, keySelector: KeySelector<T, K>): Observable<ChangeSet<T, K>>;
  create<T>(subscribe: (source: SourceList<T>) => DisposableLike): Observable<ChangeSet<T>>;
  createCache<T, K>(subscribe: (source: SourceCache<T, K>) => DisposableLike, keySelector: KeySelector<T, K>): Observable<ChangeSet<T, K>>;
  createList<T>(subscribe: (source: SourceList<T>) => DisposableLike): Observable<ChangeSet<T>>;
}>;
export function asyncDisposeMany<T, K = unknown>(completedAccessor?: (completion: Observable<unknown>) => void, disposer?: (item: T) => PromiseLike<void> | void): ChangeOperator<T, T, K>;
export function bufferInitial<T, K = unknown>(duration: number, scheduler?: SchedulerLike): ChangeOperator<T, T, K>;
export function filterOnProperty<T, K = unknown>(property: PropertyPath<T> | ((item: T) => unknown), predicate: Predicate<T, K>, options?: RefreshOptions): ChangeOperator<T, T, K>;
export function finallySafe<T>(action: () => void): MonoTypeOperatorFunction<T>;
export const ConnectionStatus: Readonly<{ Pending: 'pending'; Loaded: 'loaded'; Errored: 'errored'; Completed: 'completed' }>;
export type ConnectionStatusValue = typeof ConnectionStatus[keyof typeof ConnectionStatus];
export function monitorStatus<T>(): OperatorFunction<T, ConnectionStatusValue>;
export function watchValue<T, K>(key: K): (source: ChangeSource<T, K>) => Observable<T>;
export function whenAnyPropertyChanged<T>(itemOrChanges: T | Observable<ChangeSet<T>>, ...propertyNames: Array<keyof T>): Observable<T>;
export function whenChanged<T, P extends readonly (keyof T)[], R>(item: T, properties: P, resultSelector: (...values: { [I in keyof P]: T[P[I]] }) => R): Observable<R>;
export function observeCollectionChanges<T>(collection: ReadonlyCollection<T> | Observable<ChangeSet<T>> | EventTarget, eventName?: string): Observable<{ sender: typeof collection; eventArgs: ChangeSet<T> | Event }>;
export function trueForAll<T, V, K = unknown>(selector: Selector<T, ObservableInput<V>, K>, condition?: (value: V) => boolean): OperatorFunction<ChangeSet<T, K>, boolean>;
export const trueForAny: typeof trueForAll;
export function transformManyAsync<T, R, K = unknown, RK = unknown>(factory: AsyncSelector<T, Iterable<R>, K>, keySelector?: KeySelector<R, RK>, options?: AsyncOptions<T, K>): ChangeOperator<T, R, K, RK>;
export function transformManySafeAsync<T, R, K = unknown, RK = unknown>(factory: AsyncSelector<T, Iterable<R>, K>, keySelector: KeySelector<R, RK>, onError: NonNullable<AsyncOptions<T, K>['onError']>, options?: AsyncOptions<T, K>): ChangeOperator<T, R, K, RK>;
export function refCount<T, K = unknown>(): ChangeOperator<T, T, K>;
export function switchLatest<T, K = unknown>(): OperatorFunction<ChangeSource<T, K>, ChangeSet<T, K>>;
export { switchLatest as switch };

export class Group<T, K = unknown, G = unknown> implements Iterable<T> {
  constructor(key: G, kind?: CollectionKind); readonly key: G; readonly Key: G; readonly kind: CollectionKind;
  readonly cache: this; readonly Cache: this; readonly list: this; readonly List: this; readonly items: T[]; readonly Items: T[];
  readonly keys: K[]; readonly keyValues: [K, T][]; readonly size: number; readonly count: number; readonly Count: number; readonly isDisposed: boolean;
  lookup(key: K): Optional<T>; Lookup: Group<T, K, G>['lookup']; connect(): Observable<ChangeSet<T, K>>; Connect: Group<T, K, G>['connect'];
  dispose(): void; Dispose(): void; [Symbol.iterator](): Iterator<T>;
}
export class ImmutableGroup<T, K = unknown, G = unknown> implements Iterable<T> {
  constructor(key: G, entries: Iterable<readonly [K, T]>, kind?: CollectionKind); readonly key: G; readonly Key: G; readonly kind: CollectionKind;
  readonly cache: this; readonly Cache: this; readonly list: this; readonly List: this; readonly items: readonly T[]; readonly Items: readonly T[];
  readonly keys: K[]; readonly keyValues: [K, T][]; readonly size: number; readonly count: number; readonly Count: number;
  lookup(key: K): Optional<T>; Lookup: ImmutableGroup<T, K, G>['lookup']; connect(): Observable<ChangeSet<T, K>>; Connect: ImmutableGroup<T, K, G>['connect'];
  [Symbol.iterator](): Iterator<T>;
}
export function groupOn<T, G, K = unknown>(selector: Selector<T, G, K> | Observable<Selector<T, G, K>>, regrouper?: Observable<unknown>): ChangeOperator<T, Group<T, K, G>, K, G>;
export function group<T, G, K = unknown>(selector: Selector<T, G, K> | Observable<Selector<T, G, K>>, regrouperOrOptions?: Observable<unknown> | { resultGroupSource: ChangeSource<G> }): ChangeOperator<T, Group<T, K, G>, K, G>;
export function groupOnImmutable<T, G, K = unknown>(selector: Selector<T, G, K> | Observable<Selector<T, G, K>>, regrouper?: Observable<unknown>): ChangeOperator<T, ImmutableGroup<T, K, G>, K, G>;
export const groupWithImmutableState: typeof groupOnImmutable;
export function groupOnObservable<T, G, K = unknown>(selector: Selector<T, Observable<G>, K>): ChangeOperator<T, Group<T, K, G>, K, G>;
export function groupOnProperty<T, P extends keyof T, K = unknown>(property: P, throttleOrOptions?: number | RefreshOptions, scheduler?: SchedulerLike): ChangeOperator<T, Group<T, K, T[P]>, K, T[P]>;
export function groupOnPropertyWithImmutableState<T, P extends keyof T, K = unknown>(property: P, throttleOrOptions?: number | RefreshOptions, scheduler?: SchedulerLike): ChangeOperator<T, ImmutableGroup<T, K, T[P]>, K, T[P]>;
export function groupWithSpecifiedGroups<T, G, K = unknown>(selector: Selector<T, G, K>, resultGroupSource: ChangeSource<G>): ChangeOperator<T, Group<T, K, G>, K, G>;
export type JoinKey<LK, RK> = readonly [LK, RK] & { readonly leftKey: LK; readonly rightKey: RK };
export type JoinSelector<L, R, O, K> = ((left: L, right: R) => O) | ((key: K, left: L, right: R) => O);
export function innerJoin<L, R, O, LK, RK>(right: ChangeSource<R, RK>, foreignKey: Selector<R, LK, RK>, selector: (left: L, right: R) => O): ChangeOperator<L, O, LK, JoinKey<LK, RK>>;
export function innerJoin<L, R, O, LK, RK>(right: ChangeSource<R, RK>, foreignKey: Selector<R, LK, RK>, selector: (key: JoinKey<LK, RK>, left: L, right: R) => O): ChangeOperator<L, O, LK, JoinKey<LK, RK>>;
export function leftJoin<L, R, O, LK, RK>(right: ChangeSource<R, RK>, foreignKey: Selector<R, LK, RK>, selector: (left: L, right: Optional<R>) => O): ChangeOperator<L, O, LK>;
export function leftJoin<L, R, O, LK, RK>(right: ChangeSource<R, RK>, foreignKey: Selector<R, LK, RK>, selector: (key: LK, left: L, right: Optional<R>) => O): ChangeOperator<L, O, LK>;
export function rightJoin<L, R, O, LK, RK>(right: ChangeSource<R, RK>, foreignKey: Selector<R, LK, RK>, selector: (left: Optional<L>, right: R) => O): ChangeOperator<L, O, LK, RK>;
export function rightJoin<L, R, O, LK, RK>(right: ChangeSource<R, RK>, foreignKey: Selector<R, LK, RK>, selector: (key: RK, left: Optional<L>, right: R) => O): ChangeOperator<L, O, LK, RK>;
export function fullJoin<L, R, O, LK, RK>(right: ChangeSource<R, RK>, foreignKey: Selector<R, LK, RK>, selector: (left: Optional<L>, right: Optional<R>) => O): ChangeOperator<L, O, LK>;
export function fullJoin<L, R, O, LK, RK>(right: ChangeSource<R, RK>, foreignKey: Selector<R, LK, RK>, selector: (key: LK, left: Optional<L>, right: Optional<R>) => O): ChangeOperator<L, O, LK>;
export function innerJoinMany<L, R, O, LK, RK>(right: ChangeSource<R, RK>, foreignKey: Selector<R, LK, RK>, selector: JoinSelector<L, ImmutableGroup<R, RK, LK>, O, LK>): ChangeOperator<L, O, LK>;
export const leftJoinMany: typeof innerJoinMany;
export function rightJoinMany<L, R, O, LK, RK>(right: ChangeSource<R, RK>, foreignKey: Selector<R, LK, RK>, selector: JoinSelector<Optional<L>, ImmutableGroup<R, RK, LK>, O, LK>): ChangeOperator<L, O, LK>;
export const fullJoinMany: typeof rightJoinMany;
export const CombineOperator: Readonly<Record<'And' | 'Or' | 'Xor' | 'Except' | 'and' | 'or' | 'xor' | 'except', 'and' | 'or' | 'xor' | 'except'>>;
export function combine<T, K = unknown>(sources: ChangeSource<T, K>[] | Observable<ChangeSet<ChangeSource<T, K>>> | SourceList<ChangeSource<T, K>> | Observable<ChangeSource<T, K>[]>, operator?: 'and' | 'or' | 'xor' | 'except'): Observable<ChangeSet<T, K>>;
export function and<T, K = unknown>(...others: Array<ChangeSource<T, K> | ChangeSource<T, K>[]>): ChangeOperator<T, T, K>;
export const or: typeof and; export const xor: typeof and; export const except: typeof and;
export function count<T, K = unknown>(predicate?: Predicate<T, K> | null): OperatorFunction<ChangeSet<T, K>, number>;
export function sum<T, K = unknown>(selector?: Selector<T, number, K>): OperatorFunction<ChangeSet<T, K>, number>;
export function avg<T, K = unknown>(selector?: Selector<T, number, K>, fallback?: number): OperatorFunction<ChangeSet<T, K>, number>;
export const average: typeof avg; export const min: typeof avg; export const max: typeof avg; export const stdDev: typeof avg; export const standardDeviation: typeof avg;
export function sumMany<T, C, K = unknown>(childrenSelector?: Selector<T, Iterable<C>, K>, valueSelector?: (child: C) => number): OperatorFunction<ChangeSet<T, K>, number>;
export const AggregateType: Readonly<{ Add: 'add'; Remove: 'remove'; add: 'add'; remove: 'remove' }>;
export interface AggregateItem<T> { type: 'add' | 'remove'; item: T; }
export function forAggregation<T, K = unknown>(): OperatorFunction<ChangeSet<T, K>, AggregateItem<T>[]>;
export function invalidateWhen<T>(invalidate: Observable<unknown>): MonoTypeOperatorFunction<T>;

export function whereReasonsAre<T, K = unknown>(...reasons: Array<ChangeReasonValue | ChangeReasonValue[]>): ChangeOperator<T, T, K>;
export function whereReasonsAreNot<T, K = unknown>(...reasons: Array<ChangeReasonValue | ChangeReasonValue[]>): ChangeOperator<T, T, K>;
export function includeUpdateWhen<T, K = unknown>(predicate: (current: T, previous: T) => boolean): ChangeOperator<T, T, K>;
export const excludeUpdateWhen: typeof includeUpdateWhen; export const ignoreUpdateWhen: typeof includeUpdateWhen;
export function ignoreSameReferenceUpdate<T, K = unknown>(): ChangeOperator<T, T, K>;
export function suppressRefresh<T, K = unknown>(): ChangeOperator<T, T, K>;
export function invokeEvaluate<T, K = unknown>(): ChangeOperator<T, T, K>;
export function flattenBufferResult<T, K = unknown>(): OperatorFunction<ChangeSet<T, K>[], ChangeSet<T, K>>;
export function treatMovesAsRemoveAdd<T, K = unknown>(): ChangeOperator<T, T, K>;
export function forEachChange<T, K = unknown>(action: (change: Change<T, K>) => void): ChangeOperator<T, T, K>;
export function itemChanges<T, K = unknown>(changes: ChangeSet<T, K>): Generator<Change<T, K>>;
export function forEachItemChange<T, K = unknown>(action: (change: Change<T, K>) => void): ChangeOperator<T, T, K>;
export function flattenChanges<T, K = unknown>(): OperatorFunction<ChangeSet<T, K>, Change<T, K>>;
export const flatten: typeof flattenChanges;
export function startWithEmpty<T, K = unknown>(kind?: CollectionKind): ChangeOperator<T, T, K>;
export function startWithItem<T, K = unknown>(item: T, key?: K): ChangeOperator<T, T, K>;
export function removeIndex<T, K = unknown>(): ChangeOperator<T, T, K>;
export function addKey<T, K>(keySelector: KeySelector<T, K>): ChangeOperator<T, T, unknown, K>;
export function ensureUniqueKeys<T, K = unknown>(): ChangeOperator<T, T, K>;
export type PopulationTarget<T, K = unknown> = SourceCache<T, K> | SourceList<T> | Map<K, T> | Set<T> | T[];
export function clone<T, K = unknown>(target: PopulationTarget<T, K>): ChangeOperator<T, T, K>;
export function populateInto<T, K = unknown>(source: Observable<ChangeSet<T, K>>, destination: PopulationTarget<T, K>, observer?: PartialObserver<ChangeSet<T, K>>): Subscription;
export function populateFrom<T, K>(destination: SourceCache<T, K>, observable: Observable<T | Iterable<T>>, observer?: PartialObserver<T | Iterable<T>>): Subscription;
export function toObservableOptional<T, K>(key: K, initialOptionalWhenMissing?: boolean | EqualityComparer<T>, comparer?: EqualityComparer<T>): OperatorFunction<ChangeSet<T, K>, Optional<T>>;
export function adapt<T, K = unknown>(adapter: ((changes: ChangeSet<T, K>) => void) | { adapt(changes: ChangeSet<T, K>): void } | { Adapt(changes: ChangeSet<T, K>): void }): ChangeOperator<T, T, K>;
export class QuerySnapshot<T, K = unknown> implements Iterable<T> {
  constructor(state: Map<K, T> | T[]); readonly kind: CollectionKind; readonly count: number; readonly Count: number; readonly size: number;
  readonly items: T[]; readonly Items: T[]; readonly keys: K[]; readonly Keys: K[]; readonly keyValues: [K, T][]; readonly KeyValues: [K, T][];
  lookup(key: K): Optional<T>; Lookup: QuerySnapshot<T, K>['lookup']; get(key: K): T | undefined; has(key: K): boolean; [Symbol.iterator](): Iterator<T>;
}
export function queryWhenChanged<T, K = unknown, R = QuerySnapshot<T, K>>(selector?: (query: QuerySnapshot<T, K>) => R): OperatorFunction<ChangeSet<T, K>, R>;
export function toSortedCollection<T, R = T, K = unknown>(selectorOrComparer?: KeySelector<T, R> | Comparer<T>, direction?: SortDirection): OperatorFunction<ChangeSet<T, K>, T[]>;
export function updateIndex<T, K = unknown>(setter?: (item: T, index: number) => void): ChangeOperator<T, T, K>;
export class ChangeStatistics {
  constructor(index?: number, adds?: number, updates?: number, removes?: number, refreshes?: number, moves?: number, count?: number);
  index: number; adds: number; updates: number; removes: number; refreshes: number; moves: number; count: number; lastUpdated: Date;
  readonly Index: number; readonly Adds: number; readonly Updates: number; readonly Removes: number; readonly Refreshes: number; readonly Moves: number; readonly Count: number; readonly LastUpdated: Date;
}
export class ChangeSummary {
  constructor(index?: number, latest?: ChangeStatistics, overall?: ChangeStatistics); index: number; latest: ChangeStatistics; overall: ChangeStatistics;
  readonly Latest: ChangeStatistics; readonly Overall: ChangeStatistics; static readonly empty: ChangeSummary; static readonly Empty: ChangeSummary;
}
export function collectUpdateStats<T, K = unknown>(): OperatorFunction<ChangeSet<T, K>, ChangeSummary>;
export class Node<T, K = unknown> {
  constructor(item: T, key: K); readonly item: T; readonly Item: T; readonly key: K; readonly Key: K; readonly parent: Optional<Node<T, K>>; readonly Parent: Optional<Node<T, K>>;
  readonly children: SourceCache<Node<T, K>, K>; readonly Children: SourceCache<Node<T, K>, K>; readonly isRoot: boolean; readonly IsRoot: boolean;
  readonly depth: number; readonly Depth: number; readonly isDisposed: boolean; equals(other: unknown): boolean; Equals: Node<T, K>['equals']; dispose(): void; Dispose(): void;
}
export function transformToTree<T, K>(pivotOn: KeySelector<T, K | null | undefined>, predicateChanged?: Predicate<Node<T, K>, K> | Observable<Predicate<Node<T, K>, K>>): ChangeOperator<T, Node<T, K>, K>;

export interface MergeOptions<T> { comparer?: Comparer<T>; equalityComparer?: EqualityComparer<T>; suppressEmptyChangeSets?: boolean; completable?: boolean; }
export function mergeChangeSets<T, K = unknown>(sources: ChangeSource<T, K>[] | Observable<ChangeSource<T, K>>, options?: MergeOptions<T>): Observable<ChangeSet<T, K>>;
export function mergeChangeSets<T, K = unknown>(options?: MergeOptions<T>): OperatorFunction<ChangeSource<T, K>, ChangeSet<T, K>>;
export function mergeManyChangeSets<T, R, K = unknown, RK = unknown>(selector: Selector<T, ChangeSource<R, RK>, K>, options?: MergeOptions<R>): ChangeOperator<T, R, K, RK>;
export function asArray<T>(source: Iterable<T>): T[]; export const asList: typeof asArray;
export function duplicates<T, V = T>(source: Iterable<T>, valueSelector?: KeySelector<T, V>): T[];
export function indexOfMany<T, R = ItemWithIndex<T>>(source: Iterable<T>, itemsToFind: Iterable<T>, resultSelector?: (item: T, index: number) => R): R[];
export function firstOrOptional<T>(source: Iterable<T>, predicate?: (item: T) => boolean): Optional<T>;
export function toOptional<T>(source: Observable<T>): Observable<Optional<NonNullable<T>>>;
export function toOptional<T>(source: T): Optional<NonNullable<T>>;
export function toOptional<T>(): OperatorFunction<T, Optional<NonNullable<T>>>;
export function createOptional<T>(source: T): Optional<NonNullable<T>>;
export function fromOptional<T>(source: Optional<T>): T;
export function fromOptional<T>(source: Observable<Optional<T>>): Observable<T>;
export function fromOptional<T>(): OperatorFunction<Optional<T>, T>;
export function convertOptional<T, R>(source: Optional<T>, converter: (value: T) => R | Optional<R>): Optional<R>;
export function convertOptional<T, R>(source: Observable<Optional<T>>, converter: (value: T) => R | Optional<R>): Observable<Optional<R>>;
export function convertOptional<T, R>(converter: (value: T) => R | Optional<R>): OperatorFunction<Optional<T>, Optional<R>>;
export function convertOr<T, R>(source: Optional<T>, converter: (value: T) => R, fallback: () => R): R;
export function convertOr<T, R>(source: Observable<Optional<T>>, converter: (value: T) => R, fallback: () => R): Observable<R>;
export function convertOr<T, R>(converter: (value: T) => R, fallback: () => R): OperatorFunction<Optional<T>, R>;
export function orElse<T>(source: Optional<T>, fallback: () => Optional<T>): Optional<T>;
export function orElse<T>(source: Observable<Optional<T>>, fallback: () => Optional<T>): Observable<Optional<T>>;
export function orElse<T>(fallback: () => Optional<T>): MonoTypeOperatorFunction<Optional<T>>;
export function valueOr<T, R>(source: Optional<T> | T | null | undefined, fallback: R | (() => R)): T | R;
export function valueOr<T, R>(source: Observable<Optional<T>>, fallback: R | (() => R)): Observable<T | R>;
export function valueOr<T, R>(fallback: R | (() => R)): OperatorFunction<Optional<T>, T | R>;
export function valueOrDefault<T>(source: Optional<T>): T | undefined;
export function valueOrDefault<T>(source: Observable<Optional<T>>): Observable<T | undefined>;
export function valueOrDefault<T>(): OperatorFunction<Optional<T>, T | undefined>;
export function valueOrThrow<T>(source: Optional<T>, exception?: () => unknown): T;
export function valueOrThrow<T>(source: Observable<Optional<T>>, exception?: () => unknown): Observable<T>;
export function valueOrThrow<T>(exception?: () => unknown): OperatorFunction<Optional<T>, T>;
export function onHasValue<T>(source: Optional<T>, action: (value: T) => void, elseAction?: () => void): Optional<T>;
export function onHasValue<T>(source: Observable<Optional<T>>, action: (value: T) => void, elseAction?: () => void): Observable<Optional<T>>;
export function onHasValue<T>(action: (value: T) => void, elseAction?: () => void): MonoTypeOperatorFunction<Optional<T>>;
export function onHasNoValue<T>(source: Optional<T>, action: () => void, elseAction?: (value: T) => void): Optional<T>;
export function onHasNoValue<T>(source: Observable<Optional<T>>, action: () => void, elseAction?: (value: T) => void): Observable<Optional<T>>;
export function onHasNoValue<T>(action: () => void, elseAction?: (value: T) => void): MonoTypeOperatorFunction<Optional<T>>;
export function selectValues<T>(source: Iterable<Optional<T>>): T[];
export function selectValues<T>(source: Observable<Optional<T>>): Observable<T>;
export function selectValues<T>(): OperatorFunction<Optional<T>, T>;
export function lookup<T, K>(source: Map<K, T> | Pick<ReadonlyCache<T, K>, 'lookup'>, key: K): Optional<T>;
export function lookup<T extends object, K extends keyof T>(source: T, key: K): Optional<T[K]>;
export function removeIfContained<T, K>(source: Map<K, T>, key: K): boolean;
export function removeIfContained<T extends object>(source: T, key: keyof T): boolean;
export function getValueOrDefault<T, K, F = undefined>(source: Map<K, T> | Pick<ReadonlyCache<T, K>, 'lookup'>, key: K, fallback?: F): T | F;
export function getValueOrDefault<T, F = undefined>(source: Optional<T>, fallback?: F): T | F;
export class OptionElse { constructor(shouldRunAction?: boolean); readonly shouldRunAction: boolean; else(action: () => void): void; Else: OptionElse['else']; }
export function ifHasValue<T>(source: Optional<T> | null | undefined, action: (value: T) => void): OptionElse;
export class ItemWithIndex<T> { constructor(item: T, index: number); readonly item: T; readonly Item: T; readonly index: number; readonly Index: number; equals(other: unknown): boolean; Equals: ItemWithIndex<T>['equals']; toString(): string; ToString(): string; }
export class ItemWithValue<T, V> { constructor(item: T, value: V); readonly item: T; readonly Item: T; readonly value: V; readonly Value: V; equals(other: unknown): boolean; Equals: ItemWithValue<T, V>['equals']; toString(): string; ToString(): string; }
export class ErrorInfo<T, K = unknown> { constructor(exception: unknown, value: T, key: K); readonly exception: unknown; readonly Exception: unknown; readonly value: T; readonly Value: T; readonly key: K; readonly Key: K; equals(other: unknown): boolean; Equals: ErrorInfo<T, K>['equals']; toString(): string; ToString(): string; }
export { ErrorInfo as Error };
export class SortExpression<T, V = unknown> { constructor(expression: KeySelector<T, V>, direction?: SortDirection | 0); readonly expression: KeySelector<T, V>; readonly selector: KeySelector<T, V>; readonly Expression: KeySelector<T, V>; readonly direction: 'ascending' | 'descending'; readonly Direction: 'ascending' | 'descending'; compare(left: T, right: T): number; Compare: SortExpression<T, V>['compare']; }

export const SortDirection: Readonly<{ Ascending: 'ascending'; Descending: 'descending' }>;
export class SortExpressionComparer<T> {
  constructor(expressions?: Iterable<{ selector: (item: T) => unknown; direction: SortDirection }>);
  expressions: Array<{ selector: (item: T) => unknown; direction: SortDirection }>;
  compare(a: T, b: T): number; Compare: SortExpressionComparer<T>['compare'];
  thenBy<V>(selector: KeySelector<T, V>, direction?: SortDirection): SortExpressionComparer<T>; ThenBy: SortExpressionComparer<T>['thenBy'];
  thenByAscending<V>(selector: KeySelector<T, V>): SortExpressionComparer<T>; ThenByAscending: SortExpressionComparer<T>['thenByAscending'];
  thenByDescending<V>(selector: KeySelector<T, V>): SortExpressionComparer<T>; ThenByDescending: SortExpressionComparer<T>['thenByDescending'];
  static ascending<T, V>(selector: KeySelector<T, V>): SortExpressionComparer<T>; static Ascending: typeof SortExpressionComparer.ascending;
  static descending<T, V>(selector: KeySelector<T, V>): SortExpressionComparer<T>; static Descending: typeof SortExpressionComparer.descending;
}
export class PageRequest implements PageRequestLike { constructor(page?: number, size?: number); readonly page: number; readonly size: number; readonly Page: number; readonly Size: number; equals(other: unknown): boolean; static readonly Default: PageRequest; }
export class VirtualRequest implements VirtualRequestLike { constructor(startIndex?: number, size?: number); readonly startIndex: number; readonly size: number; readonly StartIndex: number; readonly Size: number; equals(other: unknown): boolean; static readonly Default: VirtualRequest; }
/** Compatibility options. Framework-specific collection reset flags are advisory. */
export class BindingOptions { constructor(options?: { resetThreshold?: number; useReplaceForUpdates?: boolean }); resetThreshold: number; useReplaceForUpdates?: boolean; static neverFireReset(useReplaceForUpdates?: boolean): BindingOptions; static NeverFireReset: typeof BindingOptions.neverFireReset; }
export class SortAndBindOptions extends BindingOptions {}
export const SortOptions: Readonly<{ None: 0; UseBinarySearch: 1; ComparesImmutableValuesOnly: 2 }>;
export const DynamicDataOptions: Readonly<{ binding: BindingOptions; scheduler: SchedulerLike }>;
export class ObservableCollectionExtended<T> extends SourceList<T> { constructor(items?: T[]); replaceAll(items: Iterable<T>): this; ReplaceAll: ObservableCollectionExtended<T>['replaceAll']; suspendCountNotifications(): Disposable; }
export const minimum: typeof min; export const maximum: typeof max;
export function isEmpty<T, K = unknown>(): OperatorFunction<ChangeSet<T, K>, boolean>; export const isNotEmpty: typeof isEmpty;
export function addOrUpdate<T, K>(source: SourceCache<T, K>, itemOrItems: T | Iterable<T> | Map<K, T>, keyOrComparer?: K | EqualityComparer<T>): SourceCache<T, K>;
export function clear<T, K>(source: SourceCache<T, K>): SourceCache<T, K>; export function clear<T>(source: SourceList<T>): SourceList<T>;
export function refresh<T, K>(source: SourceCache<T, K>, itemOrItems?: T | K | Iterable<T | K>): SourceCache<T, K>; export function refresh<T>(source: SourceList<T>, item?: T): SourceList<T>;
export function remove<T, K>(source: SourceCache<T, K>, itemOrItems: T | K | Iterable<T | K>): SourceCache<T, K>; export function remove<T>(source: SourceList<T>, item: T): boolean;
export function removeKeys<T, K>(source: SourceCache<T, K>, keys: Iterable<K>): SourceCache<T, K>;
export function editDiff<T, K>(source: SourceCache<T, K>, items: Iterable<T>, equality?: (left: T, right: T) => boolean): SourceCache<T, K>; export function editDiff<T>(source: SourceList<T>, items: Iterable<T>, equality?: (left: T, right: T) => boolean): SourceList<T>;
export function observeOn<T>(scheduler: SchedulerLike): MonoTypeOperatorFunction<T>; export function subscribeOn<T>(scheduler: SchedulerLike): MonoTypeOperatorFunction<T>; export function observeOnDispatcher<T>(): MonoTypeOperatorFunction<T>;
export function fluent<T, K>(observable: Observable<ChangeSet<T, K>>): DataObservable<T, K>;
export function fluent<T>(observable: Observable<T>): FluentObservable<T>;
export function installFluentOperators(registry: Record<string, unknown>): void;

export function addOrInsertRange<T>(source: T[], items: Iterable<T>, index?: number): T[]; export function addOrInsertRange<T>(source: SourceList<T>, items: Iterable<T>, index?: number): SourceList<T>;
export function binarySearch<T>(source: ArrayLike<T> | SourceList<T>, value: T, comparer?: Comparer<T>): number;
export function getChangeType(reason: ChangeReasonValue): 'item' | 'range';
export function indexOfOptional<T>(source: Iterable<T>, item: T, comparer?: EqualityComparer<T>): Optional<ItemWithIndex<T>>;
export function replaceOrAdd<T>(source: T[], original: T, replacement: T, comparer?: EqualityComparer<T>): T[];
export function replaceOrAdd<T>(source: SourceList<T>, original: T, replacement: T, comparer?: EqualityComparer<T>): SourceList<T>;
export function yieldWithoutIndex<T>(source: Iterable<ChangeRecord<T>>): Generator<ListChange<T>>;
export interface AggregatorOptions<T, K> { kind?: CollectionKind; keySelector?: KeySelector<T, K>; }
export class ChangeSetAggregator<T, K = unknown> {
  constructor(source: ChangeSource<T, K>, options?: AggregatorOptions<T, K> | CollectionKind);
  readonly data: ObservableCache<T, K> | ObservableList<T>; readonly Data: ObservableCache<T, K> | ObservableList<T>;
  readonly messages: ChangeSet<T, K>[]; readonly Messages: ChangeSet<T, K>[]; readonly error: unknown; readonly Error: unknown; readonly exception: unknown; readonly Exception: unknown;
  readonly isCompleted: boolean; readonly IsCompleted: boolean; readonly isDisposed: boolean; readonly summary: ChangeSummary; readonly Summary: ChangeSummary;
  dispose(): void; Dispose(): void; unsubscribe(): void;
}
export function asAggregator<T, K = unknown>(source: ChangeSource<T, K>, options?: AggregatorOptions<T, K> | CollectionKind): ChangeSetAggregator<T, K>;
export class Watcher<T, K = unknown> { constructor(source: ChangeSource<T, K>, scheduler?: SchedulerLike); readonly cache: ObservableCache<T, K>; readonly scheduler?: SchedulerLike; watch(key: K): Observable<Change<T, K>>; Watch: Watcher<T, K>['watch']; dispose(): void; Dispose(): void; unsubscribe(): void; }
export function asWatcher<T, K = unknown>(source: ChangeSource<T, K>, scheduler?: SchedulerLike): Watcher<T, K>;
export type BackOffStrategy = (error: unknown, zeroBasedFailure: number) => number | Observable<unknown> | null | undefined;
export interface RetryOptions extends TimerOptions { backOffStrategy?: BackOffStrategy; count?: number; maxRetries?: number; initialDelay?: number; delay?: number | BackOffStrategy; factor?: number; maxDelay?: number; resetOnSuccess?: boolean; errorPredicate?: (error: unknown) => boolean; }
export function retryWithBackOff<T>(strategyOrOptions?: BackOffStrategy | RetryOptions): MonoTypeOperatorFunction<T>;
export function scheduleRecurringAction(scheduler: SchedulerLike, interval: number | (() => number), action: () => void): Subscription & Disposable;
export function scheduleRecurringAction(interval: number | (() => number), action: () => void): Subscription & Disposable;

/** PascalCase export aliases retain the exact generic signatures above. */
export {
  adapt as Adapt,
  addKey as AddKey,
  addOrInsertRange as AddOrInsertRange,
  addOrUpdate as AddOrUpdate,
  and as And,
  asAggregator as AsAggregator,
  asArray as AsArray,
  asArray as AsList,
  asObservableCache as AsObservableCache,
  asObservableList as AsObservableList,
  asWatcher as AsWatcher,
  asyncDisposeMany as AsyncDisposeMany,
  autoRefresh as AutoRefresh,
  autoRefreshOnObservable as AutoRefreshOnObservable,
  average as Average,
  average as Avg,
  batch as Batch,
  batchIf as BatchIf,
  binarySearch as BinarySearch,
  bind as Bind,
  bind as BindToObservableCollection,
  bind as BindToObservableList,
  batchIf as BufferIf,
  bufferInitial as BufferInitial,
  cast as Cast,
  castToObject as CastToObject,
  changeKey as ChangeKey,
  clear as Clear,
  clone as Clone,
  collectUpdateStats as CollectUpdateStats,
  combine as Combine,
  convert as Convert,
  convertOptional as ConvertOptional,
  convertOr as ConvertOr,
  count as Count,
  createObservableObject as CreateObservableObject,
  createOptional as CreateOptional,
  deferUntilLoaded as DeferUntilLoaded,
  disposeMany as DisposeMany,
  distinctValues as DistinctValues,
  duplicates as Duplicates,
  editDiff as EditDiff,
  ensureUniqueKeys as EnsureUniqueKeys,
  except as Except,
  excludeUpdateWhen as ExcludeUpdateWhen,
  expireAfter as ExpireAfter,
  filter as Filter,
  filterImmutable as FilterImmutable,
  filterOnObservable as FilterOnObservable,
  filterOnProperty as FilterOnProperty,
  filterWithState as FilterWithState,
  finallySafe as FinallySafe,
  firstOrOptional as FirstOrOptional,
  flatten as Flatten,
  flattenBufferResult as FlattenBufferResult,
  flatten as FlattenChanges,
  forAggregation as ForAggregation,
  forEachChange as ForEachChange,
  forEachItemChange as ForEachItemChange,
  fromOptional as FromOptional,
  fullJoin as FullJoin,
  fullJoinMany as FullJoinMany,
  getChangeType as GetChangeType,
  getValueOrDefault as GetValueOrDefault,
  groupOn as GroupOn,
  groupOnImmutable as GroupOnImmutable,
  groupOnObservable as GroupOnObservable,
  groupOnProperty as GroupOnProperty,
  groupOnPropertyWithImmutableState as GroupOnPropertyWithImmutableState,
  groupOnImmutable as GroupWithImmutableState,
  groupWithSpecifiedGroups as GroupWithSpecifiedGroups,
  ifHasValue as IfHasValue,
  ignoreSameReferenceUpdate as IgnoreSameReferenceUpdate,
  excludeUpdateWhen as IgnoreUpdateWhen,
  includeUpdateWhen as IncludeUpdateWhen,
  indexOfMany as IndexOfMany,
  indexOfOptional as IndexOfOptional,
  innerJoin as InnerJoin,
  innerJoinMany as InnerJoinMany,
  invalidateWhen as InvalidateWhen,
  invokeEvaluate as InvokeEvaluate,
  isEmpty as IsEmpty,
  isNotEmpty as IsNotEmpty,
  itemChanges as ItemChanges,
  leftJoin as LeftJoin,
  leftJoinMany as LeftJoinMany,
  limitSizeTo as LimitSizeTo,
  lookup as Lookup,
  max as Max,
  max as Maximum,
  mergeChangeSets as MergeChangeSets,
  mergeMany as MergeMany,
  mergeManyChangeSets as MergeManyChangeSets,
  mergeManyItems as MergeManyItems,
  min as Min,
  min as Minimum,
  monitorStatus as MonitorStatus,
  notEmpty as NotEmpty,
  notifyPropertyChanged as NotifyPropertyChanged,
  createObservableObject as ObservableObject,
  observeCollectionChanges as ObserveCollectionChanges,
  observeOn as ObserveOn,
  observeOnDispatcher as ObserveOnDispatcher,
  observeProperty as ObserveProperty,
  ofType as OfType,
  onHasNoValue as OnHasNoValue,
  onHasValue as OnHasValue,
  onItemAdded as OnItemAdded,
  onItemRefreshed as OnItemRefreshed,
  onItemRemoved as OnItemRemoved,
  onItemUpdated as OnItemUpdated,
  or as Or,
  orElse as OrElse,
  page as Page,
  populateFrom as PopulateFrom,
  populateInto as PopulateInto,
  queryWhenChanged as QueryWhenChanged,
  refCount as RefCount,
  refresh as Refresh,
  remove as Remove,
  removeIfContained as RemoveIfContained,
  removeIndex as RemoveIndex,
  removeKey as RemoveKey,
  removeKeys as RemoveKeys,
  replaceOrAdd as ReplaceOrAdd,
  retryWithBackOff as RetryWithBackOff,
  reverse as Reverse,
  rightJoin as RightJoin,
  rightJoinMany as RightJoinMany,
  scheduleRecurringAction as ScheduleRecurringAction,
  selectValues as SelectValues,
  skipInitial as SkipInitial,
  sort as Sort,
  sortAndBind as SortAndBind,
  sortAndPage as SortAndPage,
  sortAndVirtualise as SortAndVirtualise,
  sortAndVirtualise as SortAndVirtualize,
  sortBy as SortBy,
  standardDeviation as StandardDeviation,
  startWithEmpty as StartWithEmpty,
  startWithItem as StartWithItem,
  stdDev as StdDev,
  subscribeMany as SubscribeMany,
  subscribeOn as SubscribeOn,
  sum as Sum,
  sumMany as SumMany,
  suppressRefresh as SuppressRefresh,
  switchLatest as Switch,
  toCollection as ToCollection,
  toObservableChangeSet as ToObservableChangeSet,
  toObservableOptional as ToObservableOptional,
  toOptional as ToOptional,
  toSortedCollection as ToSortedCollection,
  top as Top,
  convert as Transform,
  transformAsync as TransformAsync,
  transformImmutable as TransformImmutable,
  transformMany as TransformMany,
  transformManyAsync as TransformManyAsync,
  transformManySafeAsync as TransformManySafeAsync,
  transformOnObservable as TransformOnObservable,
  transformSafe as TransformSafe,
  transformSafeAsync as TransformSafeAsync,
  transformToTree as TransformToTree,
  transformWithInlineUpdate as TransformWithInlineUpdate,
  treatMovesAsRemoveAdd as TreatMovesAsRemoveAdd,
  trueForAll as TrueForAll,
  trueForAny as TrueForAny,
  updateIndex as UpdateIndex,
  valueOr as ValueOr,
  valueOrDefault as ValueOrDefault,
  valueOrThrow as ValueOrThrow,
  virtualise as Virtualise,
  virtualise as Virtualize,
  watch as Watch,
  watchValue as WatchValue,
  whenAnyPropertyChanged as WhenAnyPropertyChanged,
  whenChanged as WhenChanged,
  whenPropertyChanged as WhenPropertyChanged,
  observeProperty as WhenValueChanged,
  whereReasonsAre as WhereReasonsAre,
  whereReasonsAreNot as WhereReasonsAreNot,
  xor as Xor,
  yieldWithoutIndex as YieldWithoutIndex
};
