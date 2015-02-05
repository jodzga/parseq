package com.linkedin.parseq.stream;

import com.linkedin.parseq.internal.ArgumentUtil;
import com.linkedin.parseq.task.TaskOrValue;



public class IterablePublisher<T> implements Publisher<TaskOrValue<T>> {

  private final Iterable<T> _elements;
  private Subscriber<? super TaskOrValue<T>> _subscriber;

  public IterablePublisher(Iterable<T> iterable) {
    ArgumentUtil.notNull(iterable, "iterable");
    _elements = iterable;
  }

  @Override
  public void subscribe(final Subscriber<? super TaskOrValue<T>> subscriber) {
    ArgumentUtil.notNull(subscriber, "subscriber");
    _subscriber = subscriber;
  }

  public void run() {
    if (_subscriber != null) {
      CancellableSubscription subscription = new CancellableSubscription();
      _subscriber.onSubscribe(subscription);
      try {
        for (T e : _elements) {
          if (!subscription.isCancelled()) {
            _subscriber.onNext(TaskOrValue.value(e));
          }
        }
        if (!subscription.isCancelled()) {
          _subscriber.onComplete();
        }
      } catch (Throwable t) {
        if (!subscription.isCancelled()) {
          _subscriber.onError(t);
        }
      }
    } else {
      //TODO does it makes sense to throw here? is there a use case where this is legal
    }
  }
}
