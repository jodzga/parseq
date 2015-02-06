package com.linkedin.parseq.collection.async;

import java.util.Optional;

import com.linkedin.parseq.stream.StreamCollection;
import com.linkedin.parseq.task.Task;
import com.linkedin.parseq.transducer.Reducer;

public class SeqFoldable<Z, T> extends AsyncFoldable<Z, T> {

  public SeqFoldable(StreamCollection<?, Task<T>> input, Optional<Task<?>> predecessor) {
    super(input, predecessor);
  }

  @Override
  public Task<Z> fold(String s, Z zero, Reducer<Z, T> reducer) {
    return new SeqFoldTask<Z, T>("seq fold TODO", _input, zero, reducer, _predecessor);
  }

}
