package com.linkedin.parseq.collection.sync;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import com.linkedin.parseq.collection.Collections;
import com.linkedin.parseq.collection.ParSeqCollection;
import com.linkedin.parseq.collection.async.AsyncCollection;
import com.linkedin.parseq.collection.async.ParCollection;
import com.linkedin.parseq.collection.async.SeqCollection;
import com.linkedin.parseq.function.Tuple2;
import com.linkedin.parseq.internal.stream.Publisher;
import com.linkedin.parseq.internal.stream.PushablePublisher;
import com.linkedin.parseq.task.Task;
import com.linkedin.parseq.task.Tasks;
import com.linkedin.parseq.transducer.Foldable;
import com.linkedin.parseq.transducer.Reducer.Step;
import com.linkedin.parseq.transducer.Transducer;

/**
 * Synchronous collection which does not require ParSeq engine to execute.
 *
 * @author jodzga
 *
 */
public class SyncCollection<T, R> extends ParSeqCollection<T, R> {

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

  public <A> SyncCollection<T, A> map(final Function<R, A> f) {
    return map(f, this::createCollection);
  }

  public SyncCollection<T, R> forEach(final Consumer<R> consumer) {
    return forEach(consumer, this::createCollection);
  }

  public SyncCollection<T, R> filter(final Predicate<R> predicate) {
    return filter(predicate, this::createCollection);
  }

  public SyncCollection<T, R> take(final int n) {
    return take(n, this::createCollection);
  }

  public SyncCollection<T, R> takeWhile(final Predicate<R> predicate) {
    return takeWhile(predicate, this::createCollection);
  }

  public SyncCollection<T, R> drop(final int n) {
    return drop(n, this::createCollection);
  }

  public SyncCollection<T, R> dropWhile(final Predicate<R> predicate) {
    return dropWhile(predicate, this::createCollection);
  }

  /*
   * Foldings:
   */

  protected <Z> Foldable<Z, T, RichCallable<Z>> foldable() {
    return new SyncFoldable<Z, T>(_input);
  }

  public <Z> RichCallable<Z> fold(final Z zero, final BiFunction<Z, R, Z> op) {
    return fold(zero, op, foldable());
  }

  public RichCallable<R> first() {
    return checkEmptySync(first(foldable()));
  }

  public RichCallable<R> last() {
    return checkEmptySync(last(foldable()));
  }

  public RichCallable<List<R>> all() {
    return all(foldable());
  }

  public RichCallable<R> reduce(final BiFunction<R, R, R> op) {
    return checkEmptySync(reduce(op, foldable()));
  }

  public RichCallable<R> find(final Predicate<R> predicate) {
    return checkEmptySync(find(predicate, foldable()));
  }

  /*
   * FlatMaps:
   */

  private Task<?> publisherTask(final PushablePublisher<R> pushable) {
    final Task<?> fold = Tasks.callable("SyncCollectionPublisher", foldable().fold(Optional.empty(), transduce((z, ackR) -> {
      pushable.next(ackR);
      return Step.cont(z);
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
    PushablePublisher<R> pushablePublisher = new PushablePublisher<R>();
    Task<?> publisherTask = publisherTask(pushablePublisher);
    return new ParCollection<A, A>(Transducer.identity(), pushablePublisher.map(f), Optional.of(publisherTask));
  }

  public <A> SeqCollection<A, A> seq(final Function<R, Task<A>> f) {
    PushablePublisher<R> pushablePublisher = new PushablePublisher<R>();
    Task<?> publisherTask = publisherTask(pushablePublisher);
    return new SeqCollection<A, A>(Transducer.identity(), pushablePublisher.map(f), Optional.of(publisherTask));
  }

  public <A> SyncCollection<A, A> flatMap(final Function<R, SyncCollection<A, A>> f) {
    //TODO
    return null;
  }

  public <A> SyncCollection<Tuple2<A, SyncCollection<R, R>>, Tuple2<A, SyncCollection<R, R>>> groupBy(final Function<R, A> classifier) {
    //TODO
    return null;
  }
}
