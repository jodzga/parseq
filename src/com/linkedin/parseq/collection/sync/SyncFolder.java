package com.linkedin.parseq.collection.sync;

import com.linkedin.parseq.internal.stream.AckValue;
import com.linkedin.parseq.internal.stream.Publisher;
import com.linkedin.parseq.internal.stream.Subscriber;
import com.linkedin.parseq.transducer.Reducer;
import com.linkedin.parseq.transducer.Reducer.Step;

public class SyncFolder<Z, T> {
  private final Reducer<Z, T> _reducer;
  private Z _current;
  private boolean _done = false;

  /**
   * Publisher is single threaded and synchronous.
   * @param zero
   * @param reducer
   */
  public SyncFolder(Z zero, Reducer<Z, T> reducer) {
    _current = zero;
    _reducer = reducer;
  }

  public Z fold(final Publisher<T> input) {
    input.subscribe(new Subscriber<T>() {

      @Override
      public void onNext(AckValue<T> element) {
        if (!_done) {
          Step<Z> step =  _reducer.apply(_current, element);
          _current = step.getValue();
          switch (step.getType()) {
            case cont:
              break;
            case done:
              _done = true;
              break;
          }
        } else {
          element.ack();
        }
      }

      @Override
      public void onComplete(int totalTasks) {
        _done = true;
      }

      @Override
      public void onError(Throwable cause) {
        throw new RuntimeException(cause);
      }
    });
    return _current;
  }

}
