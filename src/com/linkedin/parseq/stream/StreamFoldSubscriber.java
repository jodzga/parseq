package com.linkedin.parseq.stream;

import com.linkedin.parseq.promise.DelegatingPromise;
import com.linkedin.parseq.promise.Promises;
import com.linkedin.parseq.promise.SettablePromise;
import com.linkedin.parseq.transducer.Reducer;
import com.linkedin.parseq.transducer.Reducer.Step;

public class StreamFoldSubscriber<B, T> extends DelegatingPromise<B> {

  protected Publisher<T> _source;
  private B _partialResult;
  private final Reducer<B, T> _reducer;

  public StreamFoldSubscriber(Publisher<T> source, B zero, Reducer<B, T> reducer) {
    super(Promises.<B>settable());
    _source = source;
    _reducer = reducer;
    _partialResult = zero;

    source.subscribe(new AckingSubscriber<T>() {

      @Override
      public void onSubscribe(Subscription subscription) {
      }

      @Override
      public void onNext(AckValue<T> element) {
        Step<B> step = _reducer.apply(_partialResult, element);
        switch (step.getType()) {
          case cont:
            _partialResult = step.getValue();
            break;
          case done:
            getSettableDelegate().done(step.getValue());
            _partialResult = null;
            break;
        }
      }

      @Override
      public void onComplete(int totalTasks) {
        if (!getDelegate().isDone()) {
          getSettableDelegate().done(_partialResult);
          _partialResult = null;
        }
      }

      @Override
      public void onError(Throwable cause) {
        if (!getDelegate().isDone()) {
          getSettableDelegate().fail(cause);
          _partialResult = null;
        }
      }
    });
  }

  @SuppressWarnings("unchecked")
  private SettablePromise<B> getSettableDelegate() {
    return (SettablePromise<B>)super.getDelegate();
  }

}
