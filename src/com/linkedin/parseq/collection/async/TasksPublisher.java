package com.linkedin.parseq.collection.async;

import com.linkedin.parseq.task.Task;
import com.linkedin.parseq.task.TaskOrValue;

public class TasksPublisher<T> extends IterablePublisher<Task<T>, T> {

  private final Iterable<Task<T>> _elements;
  
  public TasksPublisher(final Iterable<Task<T>> elements) {
    super(TaskOrValue::task);
    _elements = elements;
  }

  @Override
  Iterable<Task<T>> getElements() {
    return _elements;
  }

}
