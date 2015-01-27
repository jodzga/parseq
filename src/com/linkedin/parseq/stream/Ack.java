package com.linkedin.parseq.stream;

import com.linkedin.parseq.transducer.FlowControl;

@FunctionalInterface
public interface Ack {

  public static final Ack NO_OP = flow -> {};

  void ack(FlowControl flow);

  default Ack andThen(final Ack ack) {
    if (ack == NO_OP) {
      return this;
    } else if (this == NO_OP) {
      return ack;
    } else {
      return flow -> {
        ack(flow);
        ack.ack(flow);
      };
    }
  }
}
