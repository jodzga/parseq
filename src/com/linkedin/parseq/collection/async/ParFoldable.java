package com.linkedin.parseq.collection.async;

import java.util.Optional;

import com.linkedin.parseq.Task;
import com.linkedin.parseq.internal.stream.Publisher;
import com.linkedin.parseq.transducer.Reducer;

public class ParFoldable<Z, T> extends AsyncFoldable<Z, T> {

  public ParFoldable(Publisher<Task<T>> input, Optional<Task<?>> predecessor) {
    super(input, predecessor);
  }

  @Override
  public Task<Z> fold(Z zero, Reducer<Z, T> reducer) {
    return new ParFoldTask<Z, T>("par fold TODO", _input, zero, reducer, _predecessor);
  }

}
