package com.linkedin.parseq.stream;

import com.linkedin.parseq.promise.Promise;
import com.linkedin.parseq.transducer.Foldable;
import com.linkedin.parseq.transducer.Reducer;

public class PublisherFoldable<Z, T> implements Foldable<Z, T, Promise<Z>>  {

  private final Publisher<T> _source;

  public PublisherFoldable(Publisher<T> source) {
    _source = source;
  }

  @Override
  public Promise<Z> fold(Z zero, Reducer<Z, T> reducer) {
    return new StreamFoldSubscriber<Z, T>(_source, zero, reducer);
  }

}
