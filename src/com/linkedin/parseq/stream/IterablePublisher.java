package com.linkedin.parseq.stream;

import com.linkedin.parseq.internal.ArgumentUtil;



public class IterablePublisher<T> implements Publisher<T> {

  final Iterable<T> _elements;

  public IterablePublisher(Iterable<T> iterable) {
    ArgumentUtil.notNull(iterable, "iterable");
    _elements = iterable;
  }

  @Override
  public void subscribe(final AckingSubscriber<T> subscriber) {
    ArgumentUtil.notNull(subscriber, "subscriber");
    try {
      int i = 0;
      for (T e : _elements) {
        subscriber.onNext(new AckValue<T>(e, Ack.NO_OP));
        i++;
      }
      subscriber.onComplete(i);
    } catch (Throwable t) {
      subscriber.onError(t);
    }
  }
}
