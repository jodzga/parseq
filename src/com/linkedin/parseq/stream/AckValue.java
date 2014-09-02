package com.linkedin.parseq.stream;

public interface AckValue<T> {

  T get();

  void ack();

  Runnable getAck();
}
