package com.linkedin.parseq.stream;

@FunctionalInterface
public interface Ack {

  public static final Ack NO_OP = () -> {};

  void ack();

  default Ack andThen(final Ack ack) {
    if (ack == NO_OP) {
      return this;
    } else if (this == NO_OP) {
      return ack;
    } else {
      return () -> {
        this.ack();
        ack.ack();
      };
    }
  }
}
