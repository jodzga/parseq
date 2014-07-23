package com.linkedin.parseq;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import com.linkedin.parseq.BaseFoldTask.Step;
import com.linkedin.parseq.stream.Publisher;
import com.linkedin.parseq.stream.Subscriber;

/**
 * TODO add creating trace for functional operators without the need of creating new tasks
 *
 * @author Jaroslaw Odzga (jodzga@linkedin.com)
 */
public abstract class TaskCollection<T, R> {

  protected final Publisher<Task<T>> _tasks;
  private final Optional<Task<?>> _predecessor;

  /**
   * This function transforms folding function from the one which folds type R to the one
   * which folds type T.
   */
  protected final Function<BiFunction<Object, R, Step<Object>>, BiFunction<Object, T, Step<Object>>> _foldF;

  protected TaskCollection(final Publisher<Task<T>> tasks,
      Function<BiFunction<Object, R, Step<Object>>, BiFunction<Object, T, Step<Object>>> foldF,
      Optional<Task<?>> predecessor) {
    _tasks = tasks;
    _foldF = foldF;
    _predecessor = predecessor;
  }

  abstract <B, A> TaskCollection<B, A> createCollection(final Publisher<Task<B>> tasks,
      Function<BiFunction<Object, A, Step<Object>>, BiFunction<Object, B, Step<Object>>> foldF,
      Optional<Task<?>> predecessor);

  abstract <Z> Task<Z> createFoldTask(String name, Z zero, final BiFunction<Z, T, Step<Z>> op,
      Optional<Task<?>> predecessor);

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private <Z> Task<Z> createFoldFTask(String name, Z zero, final BiFunction<Z, R, Step<Z>> op) {
    return createFoldTask(name, zero, (BiFunction<Z, T, Step<Z>>)((Function)_foldF).apply(op), _predecessor);
  }

  public <A> TaskCollection<T, A> map(final String desc, final Function<R, A> f) {
    return createCollection(_tasks, fa -> _foldF.apply((z, r) -> fa.apply(z, f.apply(r))), _predecessor);
  }

  private static class TaskPublisher<A> implements Publisher<A> {
    Subscriber<A> _subscriber;
    int count = 0;
    @Override
    public void subscribe(Subscriber<A> subscriber) {
      _subscriber = subscriber;
    }
    public void next(A element) {
      count++;
      _subscriber.onNext(element);
    }
    public void complete() {
      _subscriber.onComplete(count);
    }
    public void error(Throwable cause) {
      _subscriber.onError(cause);
    }
  }

  //TODO this probably does not make sense
  public <A> TaskCollection<A, A> flatMapTask(final String desc, final Function<R, Task<A>> f) {
    final TaskPublisher<Task<A>> publisher = new TaskPublisher<>();
    SeqPublisher<Task<T>> pub = (_tasks instanceof SeqPublisher) ? (SeqPublisher<Task<T>>)_tasks : new SeqPublisher<Task<T>>(_tasks);
    final TaskCollection<T, R> lazyCollection = createCollection(pub, _foldF, _predecessor);
    final Task<?> fold = lazyCollection.map(desc, f).fold(desc, Optional.empty(), (z, e) -> {
      publisher.next(e);
      e.onResolve( p -> {
        pub.publishNext();
      });
      return z;
    });
    fold.onResolve(p -> {
      if (p.isFailed()) {
        publisher.error(p.getError());
      } else {
        publisher.complete();
      }
    });
    return createCollection(publisher, Function.identity(), Optional.of(fold));
  }

  /**
   * TODO
   * maybe:
   * - separate hierarchies for SeqFoldTask and ParFoldTask, because ParFoldTask is much
   *   simpler
   * - for SeqFoldTask:
   *   - change result of filter to something like "break" and in base fold task notify
   *     publisher to release next task
   *   - two modes???
   *     - first: release next task every time
   *     - second: release next task only on "break"
   */

  //TODO semantics of this method is clear
  public <A> TaskCollection<A, A> flatMap(final String desc, final Function<R, TaskCollection<?, A>> f) {
    /**
     * task publisher will have to wait with publishing next task until the the publisher from returned
     * TaskCollection is done.
     * - new publisher wrapper (with buffer) which can be "moved forward" by calling next()
     *   it has to subscribe to original publisher
     * - next() will be called by fold task - fold task will have a
     */
    return null;
  }

  public TaskCollection<T, R> forEach(final String desc, final Consumer<R> consumer) {
    return map(desc, e -> {
      consumer.accept(e);
      return e;
    });
  }

  public <Z> Task<Z> fold(final String name, final Z zero, final BiFunction<Z, R, Z> op) {
    return createFoldFTask("fold: " + name, zero, (z, e) -> Step.cont(op.apply(z, e)));
  }

  private static class BooleanHolder {
    private boolean _value = false;
    public BooleanHolder(boolean value) {
      _value = value;
    }
  }

  public Task<R> reduce(final String name, final BiFunction<R, R, R> op) {
    final BooleanHolder first = new BooleanHolder(true);
    return createFoldFTask("reduce: " + name, null, (z, e) -> {
      if (first._value) {
        first._value = false;
        return Step.cont(e);
      } else {
        return Step.cont(op.apply(z, e));
      }
    });
  }

  public Task<Optional<R>> find(final String name, final Predicate<R> predicate) {
    return createFoldFTask("find: " + name, Optional.empty(), (z, e) -> {
      if (predicate.test(e)) {
        return Step.done(Optional.of(e));
      } else {
        return Step.cont(z);
      }
    });
  }

  public TaskCollection<T, R> filter(final String name, final Predicate<R> predicate) {
    return createCollection(_tasks, fr -> _foldF.apply((z, r) -> {
      if (predicate.test(r)) {
        return fr.apply(z, r);
      } else {
        return Step.ignore();
      }
    }), _predecessor);
  }

  private static class Counter {
    int _counter;
    public Counter(int counter) {
      _counter = counter;
    }
    int inc() {
      _counter++;
      return _counter;
    }
  }

  public TaskCollection<T, R> take(final String name, final int n) {
    final Counter counter = new Counter(0);
    return createCollection(_tasks, fr -> _foldF.apply((z, r) -> {
      Step<Object> step = fr.apply(z, r);
      if (counter.inc() < n) {
        return step;
      } else {
        if (step.getType() == Step.Type.cont) {
          return Step.done(step.getValue());
        } else {
          return step;
        }
      }
    }), _predecessor);
  }

  public TaskCollection<T, R> takeWhile(final String name, final Predicate<R> predicate) {
    return createCollection(_tasks, fr -> _foldF.apply((z, r) -> {
      if (predicate.test(r)) {
        return fr.apply(z, r);
      } else {
        return Step.stop();
      }
    }), _predecessor);
  }

  /**
   * other operations proposal:
   * first
   * all
   * partition
   * split
   * groupBy
   */

}
