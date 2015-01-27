package com.linkedin.parseq.stream;

public interface Publisher<T> {

  void subscribe(AckingSubscriber<T> subscriber);

  default void subscribe(Subscriber<T> subscriber) {
    subscribe(AckingSubscriber.adopt(subscriber));
  }

  default StreamCollection<T, T> collection() {
    return new StreamCollection<T, T>(this, x -> x);
  }
}