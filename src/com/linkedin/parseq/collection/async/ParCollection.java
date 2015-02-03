package com.linkedin.parseq.collection.async;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

import com.linkedin.parseq.collection.GroupedAsyncCollection;
import com.linkedin.parseq.collection.ParSeqCollection;
import com.linkedin.parseq.stream.StreamCollection;
import com.linkedin.parseq.task.Task;
import com.linkedin.parseq.transducer.Foldable;
import com.linkedin.parseq.transducer.Transducer;

public class ParCollection<T, R> extends AsyncCollectionImpl<T, R> {

  public ParCollection(Transducer<T, R> transducer, StreamCollection<?, Task<T>> input, Optional<Task<?>> predecessor) {
    super(transducer, input, predecessor);
  }

  @Override
  protected <Z> Foldable<Z, T, Task<Z>> foldable() {
    return new ParFoldable<Z, T>(_input, _predecessor);
  }

  @Override
  <A, B> ParCollection<A, B> createAsyncCollection(StreamCollection<?, Task<A>> input, Transducer<A, B> transducer,
      Optional<Task<?>> predecessor) {
    return new ParCollection<A, B>(transducer, input, predecessor);
  }

}
