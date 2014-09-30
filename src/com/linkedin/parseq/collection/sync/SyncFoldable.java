package com.linkedin.parseq.collection.sync;

import com.linkedin.parseq.stream.Publisher;
import com.linkedin.parseq.transducer.Foldable;
import com.linkedin.parseq.transducer.Reducer;

public class SyncFoldable<Z, T> implements Foldable<Z, T, Z> {

  private final Publisher<T> _input;

  public SyncFoldable(Publisher<T> input) {
    _input = input;
  }

  @Override
  public Z fold(Z zero, Reducer<Z, T> reducer) {
    return new SyncFolder<Z, T>(zero, reducer).fold(_input);
  }

}
