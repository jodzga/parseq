package com.linkedin.parseq.transducer;

import java.util.function.BiFunction;
import java.util.function.Function;

import com.linkedin.parseq.transducer.Reducer.Step;

public abstract class ReducibleStream<T, R> {

  private final Transducer<T, R> _transducer;

  public ReducibleStream(Transducer<T, R> transfolder) {
    _transducer = transfolder;
  }

  abstract <Z, P extends Result<Z>> P doFold(final Z zero, Reducer<Z, T> op);

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private <Z, P extends Result<Z>> P checkedDoFold(Z zero, final Reducer<Z, R> op) {
    return doFold(zero, (Reducer<Z, T>)((Function)_transducer).apply(op));
  }

  public <Z, P extends Result<Z>> P fold(final Z zero, final BiFunction<Z, R, Z> op) {
    return checkedDoFold(zero, (z, e) -> Step.cont(op.apply(z, e)));
  }

}
