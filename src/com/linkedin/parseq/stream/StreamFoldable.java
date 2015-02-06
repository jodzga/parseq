package com.linkedin.parseq.stream;

import java.util.Optional;

import com.linkedin.parseq.task.Task;
import com.linkedin.parseq.task.TaskOrValue;
import com.linkedin.parseq.transducer.Foldable;
import com.linkedin.parseq.transducer.Reducer;

public class StreamFoldable<Z, T> implements Foldable<Z, T, Task<Z>> {

  private final Publisher<TaskOrValue<T>> _input;
  private final Optional<Task<?>> _predecessor;

  public StreamFoldable(Publisher<TaskOrValue<T>> input, Optional<Task<?>> predecessor) {
    _input = input;
    _predecessor = predecessor;
  }

  @Override
  public Task<Z> fold(final String name, final Z zero, final Reducer<Z, T> reducer) {
    return new StreamFoldTask<Z, T>(name, _input, zero, reducer, _predecessor);
  }

}
