package com.linkedin.parseq.stream;

@FunctionalInterface
public interface Subscription {
  void cancel();
}
