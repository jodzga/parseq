package com.linkedin.parseq.internal.stream;

public class IterablePublisher<T> implements Publisher<T> {

  final Iterable<T> _elements;

  public IterablePublisher(Iterable<T> tasks) {
    this._elements = tasks;
  }

  @Override
  public void subscribe(Subscriber<T> subscriber) {
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