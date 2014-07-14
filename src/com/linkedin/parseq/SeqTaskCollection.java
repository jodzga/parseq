package com.linkedin.parseq;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

import com.linkedin.parseq.BaseFoldTask.Step;
import com.linkedin.parseq.stream.Publisher;


/**
 * @author Jaroslaw Odzga (jodzga@linkedin.com)
 */
public class SeqTaskCollection<T, R> extends TaskCollection<T, R> {


  private SeqTaskCollection(final Publisher<Task<T>> tasks,
      Function<BiFunction<Object, R, Step<Object>>, BiFunction<Object, T, Step<Object>>> foldF,
      Optional<Task<?>> predecessor) {
    super(tasks, foldF, predecessor);
  }

  public static <T> SeqTaskCollection<T, T> fromTasks(final Publisher<Task<T>> tasks) {
    return new SeqTaskCollection<T, T>(tasks, Function.identity(), Optional.empty());
  }

  @Override
  <B, A> TaskCollection<B, A> createCollection(final Publisher<Task<B>> tasks,
      Function<BiFunction<Object, A, Step<Object>>, BiFunction<Object, B, Step<Object>>> foldF,
      Optional<Task<?>> predecessor) {
    return new SeqTaskCollection<B, A>(tasks, foldF, predecessor);
  }

  @Override
  <Z> Task<Z> createFoldTask(String name, Z zero, BiFunction<Z, T, Step<Z>> op, Optional<Task<?>> predecessor) {
    return new SeqFoldTask<Z, T>(name, _tasks, zero, op, predecessor);
  }

}
