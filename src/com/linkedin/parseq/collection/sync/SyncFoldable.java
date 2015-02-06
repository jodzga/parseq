package com.linkedin.parseq.collection.sync;

import com.linkedin.parseq.transducer.Foldable;
import com.linkedin.parseq.transducer.Reducer;

public class SyncFoldable<Z, T> implements Foldable<Z, T, RichCallable<Z>>  {

  protected final Iterable<T> _input;

  public SyncFoldable(Iterable<T> input) {
    _input = input;
  }

  @Override
  public RichCallable<Z> fold(String s, Z zero, Reducer<Z, T> reducer) {
    return new SyncFoldCallable<Z, T>(_input, zero, reducer);
  }

}
