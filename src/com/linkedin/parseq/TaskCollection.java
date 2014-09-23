package com.linkedin.parseq;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import com.linkedin.parseq.stream.Publisher;
import com.linkedin.parseq.stream.PushablePublisher;
import com.linkedin.parseq.transducer.Reducer;
import com.linkedin.parseq.transducer.Reducer.Step;
import com.linkedin.parseq.transducer.Transducer;

/**
 * TODO add creating trace for functional operators without the need of creating new tasks
 *
 * TODO 1. verbs: yielding (to collection), emitting (to stream)
 * 2. only final fold should call consume, which gets propagated back to the source publishers
 *
 * @author Jaroslaw Odzga (jodzga@linkedin.com)
 */
public abstract class TaskCollection<T, R> {

  protected final Publisher<Task<T>> _tasks;
  protected final Optional<Task<?>> _predecessor;
  //TODO add name parameter

  /**
   * This function transforms folding function from the one which folds type R to the one
   * which folds type T.
   */
  protected final Transducer<T, R> _transducer;

  protected TaskCollection(final Publisher<Task<T>> tasks,
      Transducer<T, R> transducer,
      Optional<Task<?>> predecessor) {
    _tasks = tasks;
    _transducer = transducer;
    _predecessor = predecessor;
  }

  abstract <A, B> TaskCollection<A, B> createCollection(final Publisher<Task<A>> tasks,
      Transducer<A, B> transducer,
      Optional<Task<?>> predecessor);

  abstract <Z> Task<Z> createFoldTask(String name, Z zero, final Reducer<Z, T> reducer,
      Optional<Task<?>> predecessor);

  private <Z> Task<Z> createAckingFoldTask(String name, Z zero, final Reducer<Z, R> reducer) {
    return createFoldTask(name, zero, acking(transduce(reducer)), _predecessor);
  }

  @SuppressWarnings("unchecked")
  protected <Z> Reducer<Z, T> transduce(final Reducer<Z, R> reducer) {
    return (Reducer<Z, T>)_transducer.apply((Reducer<Object, R>)reducer);
  }

  protected <Z, A> Reducer<Z, A> acking(final Reducer<Z, A> reducer) {
    return (z, ackA) -> { ackA.ack(); return reducer.apply(z, ackA); };
  }

  //TODO here we already publish values
  //this is something client could interact with
  public Task<?> publisherTask(final PushablePublisher<R> pushable) {
    final Reducer<Object, T> reducer = transduce((z, ackR) -> {
      pushable.next(ackR);
      return Step.cont(z);
    });
    //TODO task name
    final Task<?> fold = createFoldTask("publishingReducer(TODO)", Optional.empty(), reducer, _predecessor);
    fold.onResolve(p -> {
      if (p.isFailed()) {
        pushable.error(p.getError());
      } else {
        pushable.complete();
      }
    });
    return fold;
  }

  public <A> TaskCollection<T, A> map(final String desc, final Function<R, A> f) {
    return createCollection(_tasks, _transducer.map(ackR -> ackR.map(f)),
        _predecessor);
  }

  public <A> TaskCollection<A, A> flatMapTask(final String desc, final Function<R, Task<A>> f) {
    PushablePublisher<R> pushablePublisher = new PushablePublisher<R>();
    Task<?> publisherTask = publisherTask(pushablePublisher);
    return createCollection(pushablePublisher.map(f), a -> a, Optional.of(publisherTask));
  }

  public <A> TaskCollection<A, A> flatMap(final String desc, final Function<R, TaskCollection<A, A>> f) {
    PushablePublisher<R> pushablePublisher = new PushablePublisher<R>();
    Task<?> publisherTask = publisherTask(pushablePublisher);
    Function<R, Publisher<Task<A>>> mapper = r -> f.apply(r)._tasks;
    return createCollection(pushablePublisher.flatMap(mapper), a -> a, Optional.of(publisherTask));
  }

  public TaskCollection<T, R> forEach(final String desc, final Consumer<R> consumer) {
    return map(desc, e -> {
      consumer.accept(e);
      return e;
    });
  }

  public <Z> Task<Z> fold(final String name, final Z zero, final BiFunction<Z, R, Z> op) {
    return createAckingFoldTask("fold: " + name, zero, (z, e) -> Step.cont(op.apply(z, e.get())));
  }

  private static class BooleanHolder {
    private boolean _value = false;
    public BooleanHolder(boolean value) {
      _value = value;
    }
  }

  public Task<R> reduce(final String name, final BiFunction<R, R, R> op) {
    final BooleanHolder first = new BooleanHolder(true);
    return createAckingFoldTask("reduce: " + name, null, (z, e) -> {
      if (first._value) {
        first._value = false;
        return Step.cont(e.get());
      } else {
        return Step.cont(op.apply(z, e.get()));
      }
    });
  }

  public Task<R> first() {
    return createAckingFoldTask("first", null, (z, r) -> Step.done(r.get()));
  }

  public Task<R> last() {
    return createAckingFoldTask("last", null, (z, r) -> Step.cont(r.get()));
  }

  public Task<List<R>> all() {
    return createAckingFoldTask("last", new ArrayList<R>(), (z, r) -> {
      z.add(r.get());
      return Step.cont(z);
    });
  }

  public Task<Optional<R>> find(final String name, final Predicate<R> predicate) {
    return createAckingFoldTask("find: " + name, Optional.empty(), (z, e) -> {
      if (predicate.test(e.get())) {
        return Step.done(Optional.of(e.get()));
      } else {
        return Step.cont(z);
      }
    });
  }

  public TaskCollection<T, R> filter(final String name, final Predicate<R> predicate) {
    return createCollection(_tasks, _transducer.filter(predicate), _predecessor);
  }

  public TaskCollection<T, R> take(final String name, final int n) {
    return createCollection(_tasks, _transducer.take(n), _predecessor);
  }

  public TaskCollection<T, R> takeWhile(final String name, final Predicate<R> predicate) {
    return createCollection(_tasks, _transducer.takeWhile(predicate), _predecessor);
  }

  /**
   * buffering (time and count)
   */

  /**
   * API: parFun, seqFun ???
   */

  /**
   * toSeq - example writing to disk, sending via webSocket etc.
   */

  /**
   * other operations proposal:
   * partition
   * split
   * groupBy
   */

}
