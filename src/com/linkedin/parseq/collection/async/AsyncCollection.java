package com.linkedin.parseq.collection.async;

import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import com.linkedin.parseq.collection.ParSeqCollection;
import com.linkedin.parseq.collection.sync.RichCallable;
import com.linkedin.parseq.collection.sync.SyncCollection;
import com.linkedin.parseq.function.Tuple2;
import com.linkedin.parseq.internal.stream.Publisher;
import com.linkedin.parseq.internal.stream.PushablePublisher;
import com.linkedin.parseq.task.Task;
import com.linkedin.parseq.transducer.Foldable;
import com.linkedin.parseq.transducer.Reducer.Step;
import com.linkedin.parseq.transducer.Transducer;

/**
 * TODO EARLY_FINISH
 *
 * @author jodzga
 *
 * @param <T>
 * @param <R>
 */
public abstract class AsyncCollection<T, R> extends ParSeqCollection<T, R> {

  protected final Publisher<Task<T>> _input;
  protected final Optional<Task<?>> _predecessor;

  public AsyncCollection(Transducer<T, R> transducer, Publisher<Task<T>> input, Optional<Task<?>> predecessor) {
    super(transducer);
    _input = input;
    _predecessor = predecessor;
  }

  protected abstract <Z> Foldable<Z, T, Task<Z>> foldable();

  abstract <A, B> AsyncCollection<A, B> createAsyncCollection(final Publisher<Task<A>> input,
      Transducer<A, B> transducer,
      Optional<Task<?>> predecessor);

  /*
   * Collection transformations:
   */

  protected <B> AsyncCollection<T, B> createCollection(Transducer<T, B> transducer) {
    return createAsyncCollection(_input, transducer, _predecessor);
  }

  public <A> AsyncCollection<T, A> map(final Function<R, A> f) {
    return map(f, this::createCollection);
  }

  public AsyncCollection<T, R> forEach(final Consumer<R> consumer) {
    return forEach(consumer, this::createCollection);
  }

  public AsyncCollection<T, R> filter(final Predicate<R> predicate) {
    return filter(predicate, this::createCollection);
  }

  public AsyncCollection<T, R> take(final int n) {
    return take(n, this::createCollection);
  }

  public AsyncCollection<T, R> takeWhile(final Predicate<R> predicate) {
    return takeWhile(predicate, this::createCollection);
  }

  public AsyncCollection<T, R> drop(final int n) {
    return drop(n, this::createCollection);
  }

  public AsyncCollection<T, R> dropWhile(final Predicate<R> predicate) {
    return dropWhile(predicate, this::createCollection);
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

  //TODO how does within propagate? add tests

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

  public Task<?> task() {
    return foldable().fold(Optional.empty(), acking(transduce((z, r) -> {
      return Step.cont(z);
    })));
  }

  /*
   * FlatMaps:
   */

  private Task<?> publisherTask(final PushablePublisher<R> pushable) {
    final Task<?> fold = foldable().fold(Optional.empty(), transduce((z, ackR) -> {
      pushable.next(ackR);
      return Step.cont(z);
    }));
    fold.onResolve(p -> {
      if (p.isFailed()) {
        pushable.error(p.getError());
      } else {
        pushable.complete();
      }
    });
    return fold;
  }

  public <A> AsyncCollection<A, A> mapTask(final Function<R, Task<A>> f) {
    PushablePublisher<R> pushablePublisher = new PushablePublisher<R>();
    Task<?> publisherTask = publisherTask(pushablePublisher);
    return createAsyncCollection(pushablePublisher.map(f), Transducer.identity(), Optional.of(publisherTask));
  }

  public <A> AsyncCollection<A, A> flatMap(final Function<R, AsyncCollection<A, A>> f) {
    PushablePublisher<R> pushablePublisher = new PushablePublisher<R>();
    Task<?> publisherTask = publisherTask(pushablePublisher);
    Function<R, Publisher<Task<A>>> mapper = r -> f.apply(r)._input;
    return createAsyncCollection(pushablePublisher.flatMap(mapper), Transducer.identity(), Optional.of(publisherTask));
  }

  public <A> AsyncCollection<Tuple2<A, AsyncCollection<R, R>>, Tuple2<A, AsyncCollection<R, R>>> groupBy(final Function<R, A> classifier) {

    return null;
  }

}
