package com.linkedin.parseq.collection.async;

import java.util.Optional;

import com.linkedin.parseq.Task;
import com.linkedin.parseq.stream.Publisher;
import com.linkedin.parseq.transducer.Foldable;
import com.linkedin.parseq.transducer.Transducer;

public class ParCollection<T, R> extends AsyncCollection<T, R> {

  public ParCollection(Transducer<T, R> transducer, Publisher<Task<T>> input, Optional<Task<?>> predecessor) {
    super(transducer, input, predecessor);
  }

  @Override
  protected <Z> Foldable<Z, T, Task<Z>> foldable() {
    return new ParFoldable<Z, T>(_input, _predecessor);
  }

  @Override
  <A, B> ParCollection<A, B> createAsyncCollection(Publisher<Task<A>> input, Transducer<A, B> transducer,
      Optional<Task<?>> predecessor) {
    return new ParCollection<A, B>(transducer, input, predecessor);
  }



}
