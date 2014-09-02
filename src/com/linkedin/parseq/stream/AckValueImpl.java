package com.linkedin.parseq.stream;

import java.util.Objects;

public class AckValueImpl<T> implements AckValue<T> {

  public static final Runnable NO_OP = () -> {};

  private final T _value;
  private final Runnable _ack;

  public AckValueImpl(T value) {
    this(value, NO_OP);
  }

  public AckValueImpl(T value, Runnable ack) {
    Objects.requireNonNull(ack);
    _value = value;
    _ack = ack;
  }

  @Override
  public T get() {
    return _value;
  }

  @Override
  public void ack() {
    if (_ack != NO_OP) {
      _ack.run();
    }
  }

  @Override
  public Runnable getAck() {
    return _ack;
  }
}
