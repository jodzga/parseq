package com.linkedin.parseq.collection;

import com.linkedin.parseq.stream.StreamCollection;
import com.linkedin.parseq.task.Task;

public class Collections {

  private Collections() {}

  public static <T> ParSeqCollection<T> fromTasks(final Iterable<Task<T>> tasks)
  {
    return StreamCollection.fromTasks(tasks);
  }

  public static <T> ParSeqCollection<T> fromValues(final Iterable<T> input)
  {
    return StreamCollection.fromValues(input);
  }

}
