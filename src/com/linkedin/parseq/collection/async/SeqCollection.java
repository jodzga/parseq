package com.linkedin.parseq.collection.async;

import java.util.Optional;

import com.linkedin.parseq.internal.stream.Publisher;
import com.linkedin.parseq.task.Task;
import com.linkedin.parseq.transducer.Foldable;
import com.linkedin.parseq.transducer.Transducer;

public class SeqCollection<T, R> extends AsyncCollection<T, R> {

  public SeqCollection(Transducer<T, R> transducer, Publisher<Task<T>> input, Optional<Task<?>> predecessor) {
    super(transducer, input, predecessor);
  }

  @Override
  protected <Z> Foldable<Z, T, FoldTask<Z>> foldable() {
    return new SeqFoldable<Z, T>(_input, _predecessor);
  }

  @Override
  <A, B> SeqCollection<A, B> createAsyncCollection(Publisher<Task<A>> input, Transducer<A, B> transducer,
      Optional<Task<?>> predecessor) {
    return new SeqCollection<A, B>(transducer, input, predecessor);
  }

}
