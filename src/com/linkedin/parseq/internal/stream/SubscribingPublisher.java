package com.linkedin.parseq.internal.stream;

public class SubscribingPublisher<T> implements Publisher<T>, Subscriber<T> {

  private Subscriber<T> _subscriber;

  @Override
  public void onNext(final AckValue<T> element) {
    if (_subscriber != null) {
      _subscriber.onNext(element);
    }
  }

  @Override
  public void onComplete(final int totalTasks) {
    if (_subscriber != null) {
      _subscriber.onComplete(totalTasks);
    }
  }

  @Override
  public void onError(final Throwable cause) {
    if (_subscriber != null) {
      _subscriber.onError(cause);
    }
  }

  @Override
  public void subscribe(final Subscriber<T> subscriber) {
    _subscriber = subscriber;
  }

}
