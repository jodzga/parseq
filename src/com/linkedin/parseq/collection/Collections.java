package com.linkedin.parseq.collection;

import com.linkedin.parseq.collection.async.AsyncCollection;
import com.linkedin.parseq.task.Task;

public class Collections {

  private Collections() {}

  public static <T> ParSeqCollection<T> fromTasks(final Iterable<Task<T>> tasks)
  {
    return AsyncCollection.fromTasks(tasks);
  }

  public static <T> ParSeqCollection<T> fromTask(final Task<T> task)
  {
    return AsyncCollection.fromTasks(java.util.Collections.singleton(task));
  }

  public static <T> ParSeqCollection<T> fromValues(final Iterable<T> input)
  {
    return AsyncCollection.fromValues(input);
  }

  public static <T> ParSeqCollection<T> fromValue(final T input)
  {
    return AsyncCollection.fromValues(java.util.Collections.singleton(input));
  }

}
