package com.linkedin.parseq.stream;

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
        subscriber.onNext(e);
        i++;
      }
      subscriber.onComplete(i);
    } catch (Throwable t) {
      subscriber.onError(t);
    }
  }

}
