package com.linkedin.parseq;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

import com.linkedin.parseq.BaseFoldTask.Step;
import com.linkedin.parseq.stream.Publisher;


/**
 * @author Jaroslaw Odzga (jodzga@linkedin.com)
 */
public class ParTaskCollection<T, R> extends TaskCollection<T, R> {

  private ParTaskCollection(final Publisher<Task<T>> tasks,
      Function<BiFunction<Object, R, Step<Object>>, BiFunction<Object, T, Step<Object>>> foldF) {
    super(tasks, foldF);
  }

  public static <T> ParTaskCollection<T, T> fromTasks(final Publisher<Task<T>> tasks) {
    return new ParTaskCollection<T, T>(tasks, Function.identity());
  }

  @Override
  <A> TaskCollection<T, A> createCollection(final Publisher<Task<T>> tasks,
      Function<BiFunction<Object, A, Step<Object>>, BiFunction<Object, T, Step<Object>>> foldF) {
    return new ParTaskCollection<T, A>(tasks, foldF);
  }

  @Override
  <Z> Task<Z> createFoldTask(String name, Z zero, BiFunction<Z, T, Step<Z>> op) {
    return new ParFoldTask<Z, T>(name, _tasks, zero, op);
  }

}
