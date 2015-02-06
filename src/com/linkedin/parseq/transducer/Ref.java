package com.linkedin.parseq.transducer;

public interface Ref<T> {

  T refGet();

  void refSet(T t);

}
