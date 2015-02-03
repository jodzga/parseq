package com.linkedin.parseq.collection.unified;

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
import com.linkedin.parseq.stream.Publisher;
import com.linkedin.parseq.stream.PushablePublisher;
import com.linkedin.parseq.stream.StreamCollection;
import com.linkedin.parseq.task.Task;
import com.linkedin.parseq.task.TaskOrValue;
import com.linkedin.parseq.transducer.Foldable;
import com.linkedin.parseq.transducer.Reducer.Step;
import com.linkedin.parseq.transducer.Transducer;
import com.linkedin.parseq.transducer.Transducible;

/**
 * @author jodzga
 *
 * @param <T>
 * @param <R>
 */
public class AsyncCollection<T, R> extends Transducible<T, R> implements ParSeqCollection<R> {

  private final Optional<Task<?>> _predecessor;
  private final StreamCollection<?, TaskOrValue<T>> _source;

  private AsyncCollection(Transducer<T, R> transducer, StreamCollection<?, TaskOrValue<T>> source, Optional<Task<?>> predecessor) {
    super(transducer);
    _source = source;
    _predecessor = predecessor;
  }

  protected <Z> Foldable<Z, T, Task<Z>> foldable() {
    return null;
  }

  private <B> AsyncCollection<T, B> create(Transducer<T, B> transducer) {
    return new AsyncCollection<T, B>(transducer, _source, _predecessor);
  }

  public <A> AsyncCollection<T, A> map(final Function<R, A> f) {
    return map(f, this::create);
  }

  public AsyncCollection<T, R> forEach(final Consumer<R> consumer) {
    return forEach(consumer, this::create);
  }

  public AsyncCollection<T, R> filter(final Predicate<R> predicate) {
    return filter(predicate, this::create);
  }

  public AsyncCollection<T, R> take(final int n) {
    return take(n, this::create);
  }

  public AsyncCollection<T, R> takeWhile(final Predicate<R> predicate) {
    return takeWhile(predicate, this::create);
  }

  public AsyncCollection<T, R> drop(final int n) {
    return drop(n, this::create);
  }

  public AsyncCollection<T, R> dropWhile(final Predicate<R> predicate) {
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
    return foldable().fold(Optional.empty(), acking(transduce((z, r) -> {
      return Step.cont(z);
    })));
  }

  @Override
  public <A> AsyncCollection<A, A> mapTask(final Function<R, Task<A>> f) {
//    StreamCollection<TaskOrValue<T>, TaskOrValue<R>> lifted =
//        new StreamCollection<TaskOrValue<T>, TaskOrValue<R>>(_source, TaskOrValue.lift(_transducer));
//    Function<R, TaskOrValue<A>> liftedF = f.andThen(u -> TaskOrValue.task(u));
//    StreamCollection<TaskOrValue<T>, TaskOrValue<A>> s = lifted.map(t -> t.flatMap(liftedF));
//    return new AsyncCollection<A, A>(Transducer.identity(), s, _predecessor);
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

  @Override
  public <A> ParSeqCollection<A> flatMap(Function<R, ParSeqCollection<A>> f) {
    // TODO Auto-generated method stub
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
