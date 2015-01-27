package com.linkedin.parseq.collection.async;

import java.util.Optional;

import com.linkedin.parseq.stream.StreamCollection;
import com.linkedin.parseq.task.Task;
import com.linkedin.parseq.transducer.Foldable;
import com.linkedin.parseq.transducer.Transducer;

public class SeqCollection<T, R> extends AsyncCollection<T, R> {

  public SeqCollection(Transducer<T, R> transducer, StreamCollection<?, Task<T>> input, Optional<Task<?>> predecessor) {
    super(transducer, input, predecessor);
  }

  @Override
  protected <Z> Foldable<Z, T, Task<Z>> foldable() {
    return new SeqFoldable<Z, T>(_input, _predecessor);
  }

  @Override
  <A, B> SeqCollection<A, B> createAsyncCollection(StreamCollection<?, Task<A>> input, Transducer<A, B> transducer,
      Optional<Task<?>> predecessor) {
    return new SeqCollection<A, B>(transducer, input, predecessor);
  }

}
