package com.linkedin.parseq.collection.sync;

import com.linkedin.parseq.internal.stream.Ack;
import com.linkedin.parseq.internal.stream.AckValue;
import com.linkedin.parseq.transducer.Reducer;
import com.linkedin.parseq.transducer.Reducer.Step;

/**
 * @author Jaroslaw Odzga (jodzga@linkedin.com)
 */
public class SyncFoldCallable<B, T> implements RichCallable<B> {

  protected Iterable<T> _values;
  private B _partialResult;
  private final Reducer<B, T> _reducer;


  public SyncFoldCallable(final Iterable<T> values, final B zero,
      final Reducer<B, T> reducer) {
    _partialResult = zero;
    _reducer = reducer;
    _values = values;
  }

  @Override
  public B call() throws Exception {
    for (T value: _values) {
      Step<B> step = _reducer.apply(_partialResult, new AckValue<T>(value, Ack.NO_OP));
      switch (step.getType()) {
        case cont:
          _partialResult = step.getValue();
          break;
        case done:
          return step.getValue();
      }
    }
    return _partialResult;
  }

}
