package com.linkedin.parseq;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

import com.linkedin.parseq.BaseFoldTask.Step;


/**
 * @author Jaroslaw Odzga (jodzga@linkedin.com)
 */
public class SeqTaskCollection<T, R> extends TaskCollection<T, R> {


  private SeqTaskCollection(List<Task<T>> tasks,
      Function<BiFunction<Object, R, Step<Object>>, BiFunction<Object, T, Step<Object>>> foldF) {
    super(tasks, foldF);
  }

  public static <T> SeqTaskCollection<T, T> fromTasks(List<Task<T>> tasks) {
    return new SeqTaskCollection<T, T>(tasks, Function.identity());
  }

  @Override
  <A> TaskCollection<T, A> createCollection(List<Task<T>> tasks,
      Function<BiFunction<Object, A, Step<Object>>, BiFunction<Object, T, Step<Object>>> foldF) {
    return new SeqTaskCollection<T, A>(tasks, foldF);
  }

  @Override
  <Z> Task<Z> createFoldTask(String name, Z zero, BiFunction<Z, T, Step<Z>> op) {
    return new SeqFoldTask<Z, T>(name, _tasks, zero, op);
  }

}
