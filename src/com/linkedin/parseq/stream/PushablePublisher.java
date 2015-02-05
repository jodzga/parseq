package com.linkedin.parseq.stream;


public class PushablePublisher<T> implements Publisher<T>{
  private Subscriber<? super T> _subscriber;
  private int count = 0;
  private final Subscription _subscription;

  public PushablePublisher(Subscription subscription) {
    _subscription = subscription;
  }

  @Override
  public void subscribe(Subscriber<? super T> subscriber) {
    _subscriber = subscriber;
    subscriber.onSubscribe(_subscription);
  }

  public void complete() {
    _subscriber.onComplete();
  }

  public void error(Throwable cause) {
    _subscriber.onError(cause);
  }

  public void next(T value) {
    count++;
    _subscriber.onNext(value);
  }

}
