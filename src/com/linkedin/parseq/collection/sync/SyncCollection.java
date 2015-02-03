package com.linkedin.parseq.collection.sync;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import com.linkedin.parseq.collection.ParSeqCollection;
import com.linkedin.parseq.collection.GroupedAsyncCollection;
import com.linkedin.parseq.collection.async.ParCollection;
import com.linkedin.parseq.collection.async.SeqCollection;
import com.linkedin.parseq.stream.CancellableSubscription;
import com.linkedin.parseq.stream.PushablePublisher;
import com.linkedin.parseq.task.Task;
import com.linkedin.parseq.task.Tasks;
import com.linkedin.parseq.transducer.Foldable;
import com.linkedin.parseq.transducer.Reducer.Step;
import com.linkedin.parseq.transducer.Transducer;
import com.linkedin.parseq.transducer.Transducible;

/**
 * Synchronous collection which does not require ParSeq engine to execute.
 *
 * @author jodzga
 *
 */
public class SyncCollection<T, R> extends Transducible<T, R> implements ParSeqCollection<R> {

  protected final Iterable<T> _input;

  public SyncCollection(Transducer<T, R> transducer, Iterable<T> input) {
    super(transducer);
    _input = input;
  }

  /*
   * Collection transformations:
   */

  protected <B> SyncCollection<T, B> createCollection(Transducer<T, B> transducer) {
    return new SyncCollection<T, B>(transducer, _input);
  }

  @Override
  public <A> SyncCollection<T, A> map(final Function<R, A> f) {
    return map(f, this::createCollection);
  }

  @Override
  public SyncCollection<T, R> forEach(final Consumer<R> consumer) {
    return forEach(consumer, this::createCollection);
  }

  @Override
  public ParSeqCollection<R> withSideEffect(Consumer<R> consumer) {
    return forEach(consumer, this::createCollection);
  }


  @Override
  public SyncCollection<T, R> filter(final Predicate<R> predicate) {
    return filter(predicate, this::createCollection);
  }

  @Override
  public SyncCollection<T, R> take(final int n) {
    return take(n, this::createCollection);
  }

  @Override
  public SyncCollection<T, R> takeWhile(final Predicate<R> predicate) {
    return takeWhile(predicate, this::createCollection);
  }

  @Override
  public SyncCollection<T, R> drop(final int n) {
    return drop(n, this::createCollection);
  }

  @Override
  public SyncCollection<T, R> dropWhile(final Predicate<R> predicate) {
    return dropWhile(predicate, this::createCollection);
  }

  /*
   * Foldings:
   */

  protected <Z> Foldable<Z, T, RichCallable<Z>> foldable() {
    return new SyncFoldable<Z, T>(_input);
  }

  @Override
  public <Z> Task<Z> fold(final Z zero, final BiFunction<Z, R, Z> op) {
    return Tasks.callable("fold", fold(zero, op, foldable()));
  }

  @Override
  public Task<R> first() {
    return Tasks.callable("first", checkEmptySync(first(foldable())));
  }

  @Override
  public Task<R> last() {
    return Tasks.callable("last", checkEmptySync(last(foldable())));
  }

  @Override
  public Task<List<R>> all() {
    return Tasks.callable("all", all(foldable()));
  }

  @Override
  public Task<R> reduce(final BiFunction<R, R, R> op) {
    return Tasks.callable("reduce", checkEmptySync(reduce(op, foldable())));
  }

  @Override
  public Task<R> find(final Predicate<R> predicate) {
    return Tasks.callable("find", checkEmptySync(find(predicate, foldable())));
  }

  @Override
  public Task<Integer> count() {
    return Tasks.callable("count", all(foldable()).map(r -> r.size()));
  }

  /*
   * FlatMaps:
   */

  private Task<?> publisherTask(final PushablePublisher<R> pushable, final CancellableSubscription subscription) {
    final Task<?> fold = Tasks.callable("SyncCollectionPublisher", foldable().fold(Optional.empty(), transduce((z, ackR) -> {
      //TODO verify that cancellation semantics is consistent across all collection types and operations
      if (subscription.isCancelled()) {
        return Step.done(z);
      } else {
        pushable.next(ackR);
        return Step.cont(z);
      }
    })));
    fold.onResolve(p -> {
      //this is executed in correct thread because it is sync collection
      if (p.isFailed()) {
        pushable.error(p.getError());
      } else {
        pushable.complete();
      }
    });
    return fold;
  }

  public <A> ParCollection<A, A> par(final Function<R, Task<A>> f) {
    CancellableSubscription subscription = new CancellableSubscription();
    PushablePublisher<R> pushablePublisher = new PushablePublisher<R>(subscription);
    Task<?> publisherTask = publisherTask(pushablePublisher, subscription);
    return new ParCollection<A, A>(Transducer.identity(), pushablePublisher.collection().map(f), Optional.of(publisherTask));
  }

  public <A> SeqCollection<A, A> seq(final Function<R, Task<A>> f) {
    CancellableSubscription subscription = new CancellableSubscription();
    PushablePublisher<R> pushablePublisher = new PushablePublisher<R>(subscription);
    Task<?> publisherTask = publisherTask(pushablePublisher, subscription);
    return new SeqCollection<A, A>(Transducer.identity(), pushablePublisher.collection().map(f), Optional.of(publisherTask));
  }

  @Override
  public <K> ParSeqCollection<GroupedAsyncCollection<K, R>> groupBy(Function<R, K> classifier) {
    Iterable<GroupedAsyncCollection<K, R>> dataSource = new Iterable<GroupedAsyncCollection<K, R>>() {
      @Override
      public Iterator<GroupedAsyncCollection<K, R>> iterator() {
        final Map<K, List<R>> innerMap = new HashMap<K, List<R>>();
        for (R element: all(foldable()).call()) {
          K group = classifier.apply(element);
          List<R> list = innerMap.get(group);
          if (list == null) {
            list = new ArrayList<R>();
            innerMap.put(group, list);
          }
          list.add(element);
        }
        return new Iterator<GroupedAsyncCollection<K, R>>() {

          private final Iterator<Entry<K, List<R>>> _entrySetIterator =
              innerMap.entrySet().iterator();

          @Override
          public boolean hasNext() {
            return _entrySetIterator.hasNext();
          }

          @Override
          public GroupedAsyncCollection<K, R> next() {
            Entry<K, List<R>> entry = _entrySetIterator.next();
            return new GroupedSyncCollection<K, R, R>(entry.getKey(), Transducer.identity(), entry.getValue());
          }
        };
      }
    };
    return new SyncCollection<GroupedAsyncCollection<K, R>, GroupedAsyncCollection<K, R>>(Transducer.identity(), dataSource);
  }

  @Override
  public <A> ParSeqCollection<A> mapTask(Function<R, Task<A>> f) {
//    return par(f);
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <A> ParSeqCollection<A> flatMap(Function<R, ParSeqCollection<A>> f) {
    // TODO Auto-generated method stub
    return null;
  }

  protected static final <R> RichCallable<R> checkEmptySync(RichCallable<Optional<R>> result) {
    return () -> checkEmpty(result.call());
  }

  @Override
  public ParSeqCollection<R> toSeq() {
    //SyncCollection does not run asynchronous
    return this;
  }

//  @Override
//  public void subscribe(Subscriber<R> subscriber) {
//    //TODO
//  }
}
