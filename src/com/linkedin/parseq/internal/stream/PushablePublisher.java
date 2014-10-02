package com.linkedin.parseq.internal.stream;

public class PushablePublisher<T> implements Publisher<T>{
  private Subscriber<T> _subscriber;
  private int count = 0;

  @Override
  public void subscribe(Subscriber<T> subscriber) {
    _subscriber = subscriber;
  }

  public void complete() {
    _subscriber.onComplete(count);
  }

  public void error(Throwable cause) {
    _subscriber.onError(cause);
  }

  public void next(AckValue<T> value) {
    count++;
    _subscriber.onNext(value);
  }

}
