package com.linkedin.parseq.collection.async;

import com.linkedin.parseq.task.TaskOrValue;

public class ValuesPublisher<T> extends IterablePublisher<T, T> {

  private final Iterable<T> _elements;
  
  public ValuesPublisher(final Iterable<T> elements) {
    super(TaskOrValue::value);
    _elements = elements;
  }

  @Override
  Iterable<T> getElements() {
    return _elements;
  }

}
