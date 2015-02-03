package com.linkedin.parseq.collection;

public interface GroupedAsyncCollection<K, T> extends ParSeqCollection<T> {

  public K getKey();

}
