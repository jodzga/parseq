package com.linkedin.parseq.collection.async;

import java.util.function.Function;

import com.linkedin.parseq.internal.ArgumentUtil;
import com.linkedin.parseq.task.TaskOrValue;



public class IterablePublisher<A, T> implements Publisher<TaskOrValue<T>> {

  private final Iterable<A> _elements;
  private Subscriber<? super TaskOrValue<T>> _subscriber;
  private final Function<A, TaskOrValue<T>> _converter;

  public IterablePublisher(Iterable<A> iterable, Function<A, TaskOrValue<T>> converter) {
    ArgumentUtil.notNull(iterable, "iterable");
    ArgumentUtil.notNull(iterable, "converter");
    _elements = iterable;
    _converter = converter;
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
        for (A e : _elements) {
          if (!subscription.isCancelled()) {
            _subscriber.onNext(_converter.apply(e));
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
