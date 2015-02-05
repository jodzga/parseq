package com.linkedin.parseq.collection.async;

import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import com.linkedin.parseq.collection.GroupedAsyncCollection;
import com.linkedin.parseq.collection.ParSeqCollection;
import com.linkedin.parseq.stream.CancellableSubscription;
import com.linkedin.parseq.stream.GroupedStreamCollection;
import com.linkedin.parseq.stream.PushablePublisher;
import com.linkedin.parseq.stream.StreamCollection;
import com.linkedin.parseq.task.Task;
import com.linkedin.parseq.transducer.Foldable;
import com.linkedin.parseq.transducer.Reducer.Step;
import com.linkedin.parseq.transducer.Transducer;
import com.linkedin.parseq.transducer.Transducible;

/**
 * TODO EARLY_FINISH
 *
 * @author jodzga
 *
 * @param <T>
 * @param <R>
 */
public abstract class AsyncCollectionImpl<T, R> extends Transducible<T, R> implements ParSeqCollection<R> {

  protected final StreamCollection<?, Task<T>> _input;
  protected final Optional<Task<?>> _predecessor;

  public AsyncCollectionImpl(Transducer<T, R> transducer, StreamCollection<?, Task<T>> input, Optional<Task<?>> predecessor) {
    super(transducer);
    _input = input;
    _predecessor = predecessor;
  }

  protected abstract <Z> Foldable<Z, T, Task<Z>> foldable();

  abstract <A, B> AsyncCollectionImpl<A, B> createAsyncCollection(final StreamCollection<?, Task<A>> input,
      Transducer<A, B> transducer,
      Optional<Task<?>> predecessor);

  /*
   * Collection transformations:
   */

  protected <B> AsyncCollectionImpl<T, B> create(Transducer<T, B> transducer) {
    return createAsyncCollection(_input, transducer, _predecessor);
  }

  public <A> AsyncCollectionImpl<T, A> map(final Function<R, A> f) {
    return map(f, this::create);
  }

  public AsyncCollectionImpl<T, R> forEach(final Consumer<R> consumer) {
    return forEach(consumer, this::create);
  }

  public AsyncCollectionImpl<T, R> filter(final Predicate<R> predicate) {
    return filter(predicate, this::create);
  }

  public AsyncCollectionImpl<T, R> take(final int n) {
    return take(n, this::create);
  }

  public AsyncCollectionImpl<T, R> takeWhile(final Predicate<R> predicate) {
    return takeWhile(predicate, this::create);
  }

  public AsyncCollectionImpl<T, R> drop(final int n) {
    return drop(n, this::create);
  }

  public AsyncCollectionImpl<T, R> dropWhile(final Predicate<R> predicate) {
    return dropWhile(predicate, this::create);
  }

  /*
   * Foldings:
   */

  public <Z> Task<Z> fold(final Z zero, final BiFunction<Z, R, Z> op) {
    return fold(zero, op, foldable());
  }

  public Task<R> first() {
    return checkEmptyAsync(first(foldable()));
  }

  public Task<R> last() {
    return checkEmptyAsync(last(foldable()));
  }

  public Task<List<R>> all() {
    return all(foldable());
  }

  public Task<R> reduce(final BiFunction<R, R, R> op) {
    return checkEmptyAsync(reduce(op, foldable()));
  }

  public Task<R> find(final Predicate<R> predicate) {
    return checkEmptyAsync(find(predicate, foldable()));
  }

  public Task<Integer> count() {
    return all().map(r -> r.size());
  }

  //Used only for side-effects
  public Task<?> task() {
    return null;
//    return foldable().fold(Optional.empty(), acking(transduce((z, r) -> {
//      return Step.cont(z);
//    })));
  }

  /*
   * FlatMaps:
   */

  private Task<?> publisherTask(final PushablePublisher<R> pushable, final CancellableSubscription subscription) {
//    final Task<?> fold = foldable().fold(Optional.empty(), transduce((z, ackR) -> {
//      //TODO verify that cancellation semantics is consistent across all collection types and operations
//      if (subscription.isCancelled()) {
//        return Step.done(z);
//      } else {
//        pushable.next(ackR);
//        return Step.cont(z);
//      }
//    }));
//    fold.onResolve(p -> {
//      if (p.isFailed()) {
//        pushable.error(p.getError());
//      } else {
//        pushable.complete();
//      }
//    });
//    return fold;
    return null;
  }

  public <A> AsyncCollectionImpl<A, A> mapTask(final Function<R, Task<A>> f) {
//    CancellableSubscription subscription = new CancellableSubscription();
//    PushablePublisher<R> pushablePublisher = new PushablePublisher<R>(subscription);
//    Task<?> publisherTask = publisherTask(pushablePublisher, subscription);
//    return createAsyncCollection(pushablePublisher.collection().map(f), Transducer.identity(), Optional.of(publisherTask));
    return null;
  }

  private <A> AsyncCollectionImpl<A, A> flatMapAsync(final Function<R, AsyncCollectionImpl<A, A>> f) {
//    CancellableSubscription subscription = new CancellableSubscription();
//    PushablePublisher<R> pushablePublisher = new PushablePublisher<R>(subscription);
//    Task<?> publisherTask = publisherTask(pushablePublisher, subscription);
//    Function<R, StreamCollection<?, Task<A>>> mapper = r -> f.apply(r)._input;
//    return createAsyncCollection(pushablePublisher.collection().flatMap(mapper), Transducer.identity(), Optional.of(publisherTask));
    return null;
  }

//  public <A> AsyncCollection<GroupedStreamCollection<A, R, R>, GroupedStreamCollection<A, R, R>> groupBy(final Function<R, A> classifier) {
//    return null;
//  }


  @Override
  public ParSeqCollection<R> withSideEffect(Consumer<R> consumer) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ParSeqCollection<R> toSeq() {
    // TODO Auto-generated method stub
    return null;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private <A> StreamCollection<?, Task<A>> flatMapper(ParSeqCollection<A> collection) {
    if (collection instanceof AsyncCollectionImpl) {
      return ((AsyncCollectionImpl)collection)._input;
    }
    throw new UnsupportedOperationException("Unrecognized ParSeqCollection type:");
  }

  @Override
  public <A> ParSeqCollection<A> flatMap(Function<R, ParSeqCollection<A>> f) {
//    CancellableSubscription subscription = new CancellableSubscription();
//    PushablePublisher<R> pushablePublisher = new PushablePublisher<R>(subscription);
//    Task<?> publisherTask = publisherTask(pushablePublisher, subscription);
//    Function<R, StreamCollection<?, Task<A>>> mapper = r -> flatMapper(f.apply(r));
//    return createAsyncCollection(pushablePublisher.collection().flatMap(mapper), Transducer.identity(), Optional.of(publisherTask));
    return null;
  }

  @Override
  public <K> ParSeqCollection<GroupedAsyncCollection<K, R>> groupBy(Function<R, K> classifier) {
    // TODO Auto-generated method stub
    return null;
  }

  protected static final <R> Task<R> checkEmptyAsync(Task<Optional<R>> result) {
    return result.map("checkEmpty", Transducible::checkEmpty);
  }

}
