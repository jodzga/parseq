package com.linkedin.parseq.collection.async;

import com.linkedin.parseq.task.TaskOrValue;

public class ValuesPublisher<T> extends IterablePublisher<T, T> {

  public ValuesPublisher(final Iterable<T> iterable) {
    super(iterable, TaskOrValue::value);
  }

}
