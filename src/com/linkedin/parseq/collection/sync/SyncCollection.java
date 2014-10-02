package com.linkedin.parseq.collection.sync;

import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import com.linkedin.parseq.Task;
import com.linkedin.parseq.collection.ParSeqCollection;
import com.linkedin.parseq.collection.async.AsyncCollection;
import com.linkedin.parseq.collection.async.ParCollection;
import com.linkedin.parseq.collection.async.SeqCollection;
import com.linkedin.parseq.internal.stream.Publisher;
import com.linkedin.parseq.internal.stream.PushablePublisher;
import com.linkedin.parseq.internal.stream.Subscriber;
import com.linkedin.parseq.transducer.Foldable;
import com.linkedin.parseq.transducer.Reducer.Step;
import com.linkedin.parseq.transducer.Transducer;

/**
 * Synchronous collection does not require ParSeq engine to execute.
 *
 * @author jodzga
 *
 */
public class SyncCollection<T, R> extends ParSeqCollection<T, R> {

  protected final Publisher<T> _input;

  public SyncCollection(Transducer<T, R> transducer, Publisher<T> input) {
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

  private <Z> Foldable<Z, T, Z> foldable() {
    return new SyncFoldable<Z, T>(_input);
  }

  public <Z> Z fold(final Z zero, final BiFunction<Z, R, Z> op) {
    return fold(zero, op, foldable());
  }

  public R first() {
    return checkEmpty(first(foldable()));
  }

  public R last() {
    return checkEmpty(last(foldable()));
  }

  public List<R> all() {
    return all(foldable());
  }

  public R reduce(final BiFunction<R, R, R> op) {
    return checkEmpty(reduce(op, foldable()));
  }

  public R find(final Predicate<R> predicate) {
    return checkEmpty(find(predicate, foldable()));
  }

  /*
   * FlatMaps:
   */

  private Publisher<R> publisher() {
    return new Publisher<R>() {
      private int _counter = 0;
      @Override
      public void subscribe(final Subscriber<R> subscriber) {
        try {
          foldable().fold(Optional.empty(), transduce((z, ackR) -> {
            subscriber.onNext(ackR);
            _counter++;
            return Step.cont(z);
          }));
          subscriber.onComplete(_counter);
        } catch (Throwable t) {
          subscriber.onError(t);
        }
      }
    };
  }

  public <A> ParCollection<A, A> par(final Function<R, Task<A>> f) {
    Publisher<Task<A>> tasks = map(f).publisher();
    return new ParCollection<A, A>(x -> x, tasks, Optional.empty());
  }

  public <A> SeqCollection<A, A> seq(final Function<R, Task<A>> f) {
    Publisher<Task<A>> tasks = map(f).publisher();
    return new SeqCollection<A, A>(x -> x, tasks, Optional.empty());
  }

  public <A> SyncCollection<A, A> flatMap(final Function<R, SyncCollection<A, A>> f) {
    return new SyncCollection<A, A>(Transducer.identity(),
        publisher().flatMap(x -> f.apply(x)._input));
  }


}
