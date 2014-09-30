package com.linkedin.parseq.collection.async;

import java.util.Optional;

import com.linkedin.parseq.Task;
import com.linkedin.parseq.stream.Publisher;
import com.linkedin.parseq.transducer.Foldable;

public abstract class AsyncFoldable<Z, T> implements Foldable<Z, T, Task<Z>>  {

  protected final Publisher<Task<T>> _input;
  protected final Optional<Task<?>> _predecessor;

  public AsyncFoldable(Publisher<Task<T>> input, Optional<Task<?>> predecessor) {
    _input = input;
    _predecessor = predecessor;
  }

}
