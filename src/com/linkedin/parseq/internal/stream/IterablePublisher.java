package com.linkedin.parseq.internal.stream;


public class IterablePublisher<T> implements Publisher<T> {

  final Iterable<T> _elements;
  Subscriber<T> _subscriber;

  public IterablePublisher(Iterable<T> tasks) {
    this._elements = tasks;
  }

  @Override
  public void subscribe(Subscriber<T> subscriber) {
    _subscriber = subscriber;
  }

  public Runnable iterateActivator() {
    return () -> {
      if (_subscriber != null) {
        try {
          int i = 0;
          for (T e : _elements) {
            _subscriber.onNext(new AckValue<T>(e, Ack.NO_OP));
            i++;
          }
          _subscriber.onComplete(i);
        } catch (Throwable t) {
          _subscriber.onError(t);
        }
      }
    };
  }

}
