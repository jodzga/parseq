package com.linkedin.parseq.stream;

import java.util.function.Function;

import com.linkedin.parseq.transducer.FlowControl;
import com.linkedin.parseq.util.Objects;

public class AckValue<T> {

  private final T _value;
  private final Ack _ack;

  public AckValue(T value, Ack ack) {
    Objects.requireNonNull(ack);
    _value = value;
    _ack = ack;
  }

  public T get() {
    return _value;
  }

  public void ack(final FlowControl flow) {
    if (_ack != Ack.NO_OP) {
      _ack.ack(flow);
    }
  }

  public Ack getAck() {
    return _ack;
  }

  public <R> AckValue<R> map(final Function<T, R> f) {
    return new AckValue<R>(f.apply(_value), _ack);
  }

}
