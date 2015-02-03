package com.linkedin.parseq.collection.sync;

import com.linkedin.parseq.collection.GroupedAsyncCollection;
import com.linkedin.parseq.transducer.Transducer;

public class GroupedSyncCollection<K, T, R> extends SyncCollection<T, R> implements GroupedAsyncCollection<K, R> {

  public GroupedSyncCollection(K key, Transducer<T, R> transducer, Iterable<T> input) {
    super(transducer, input);
    _key = key;
  }

  private final K _key;

  public K getKey() {
    return _key;
  }

}
