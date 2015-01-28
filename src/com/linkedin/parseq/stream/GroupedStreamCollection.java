package com.linkedin.parseq.stream;

import com.linkedin.parseq.transducer.Transducer;

public class GroupedStreamCollection<K, T, R> extends StreamCollection<T, R> {

  private final K _key;

  public GroupedStreamCollection(K key, Publisher<T> source, Transducer<T, R> transducer) {
    super(source, transducer);
    _key = key;
  }

  public K getKey() {
    return _key;
  }

}
