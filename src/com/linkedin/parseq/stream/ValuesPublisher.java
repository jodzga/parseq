package com.linkedin.parseq.stream;

import com.linkedin.parseq.task.TaskOrValue;

public class ValuesPublisher<T> extends IterablePublisher<T, T> {

  public ValuesPublisher(final Iterable<T> iterable) {
    super(iterable, TaskOrValue::value);
  }

}
