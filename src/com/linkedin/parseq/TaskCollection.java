package com.linkedin.parseq;

import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import com.linkedin.parseq.BaseFoldTask.Step;

/**
 * TODO add creating trace for functional operators without the need of creating new tasks
 *
 * @author Jaroslaw Odzga (jodzga@linkedin.com)
 */
public abstract class TaskCollection<T, R> {

  protected final List<Task<T>> _tasks;

  /**
   * This function transforms folding function from the one which folds type R to the one
   * which folds type T.
   */
  protected final Function<BiFunction<Object, R, Step<Object>>, BiFunction<Object, T, Step<Object>>> _foldF;

  protected TaskCollection(final List<Task<T>> tasks, Function<BiFunction<Object, R, Step<Object>>, BiFunction<Object, T, Step<Object>>> foldF)
  {
    _tasks = tasks;
    _foldF = foldF;
  }

  abstract <A> TaskCollection<T, A> createCollection(final List<Task<T>> tasks, Function<BiFunction<Object, A, Step<Object>>, BiFunction<Object, T, Step<Object>>> foldF);
  abstract <Z> Task<Z> createFoldTask(String name, Z zero, final BiFunction<Z, T, Step<Z>> op);

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private <Z> Task<Z> createFoldFTask(String name, Z zero, final BiFunction<Z, R, Step<Z>> op) {
    return createFoldTask(name, zero, (BiFunction<Z, T, Step<Z>>)((Function)_foldF).apply(op));
  }

  public <A> TaskCollection<T, A> map(final String desc, final Function<R, A> f) {
    return createCollection(_tasks, fa -> _foldF.apply((z, r) -> fa.apply(z, f.apply(r))));
  }

  public TaskCollection<T, R> forEach(final String desc, final Consumer<R> consumer) {
    return map(desc, e -> {
      consumer.accept(e);
      return e;
    });
  }

  public <B> Task<B> fold(final String name, final B zero, final BiFunction<B, R, B> op) {
    return createFoldFTask("fold: " + name, zero, (z, e) -> Step.cont(op.apply(z, e)));
  }

  public Task<R> reduce(final String name, final BiFunction<R, R, R> op) {
    boolean first = true;
    return createFoldFTask("reduce: " + name, null, (z, e) -> {
      if (first) {
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
    //TODO
    return null;
  }

  public TaskCollection<T, R> take(final String name, final int n) {
    //TODO
    return null;
  }

  public TaskCollection<T, R> takeWhile(final String name, final Predicate<R> predicate) {
    //TODO
    return null;
  }

}
