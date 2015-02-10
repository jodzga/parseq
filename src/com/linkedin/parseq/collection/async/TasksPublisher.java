package com.linkedin.parseq.collection.async;

import com.linkedin.parseq.task.Task;
import com.linkedin.parseq.task.TaskOrValue;

public class TasksPublisher<T> extends IterablePublisher<Task<T>, T> {

  public TasksPublisher(final Iterable<Task<T>> iterable) {
    super(iterable, TaskOrValue::task);
  }

}
