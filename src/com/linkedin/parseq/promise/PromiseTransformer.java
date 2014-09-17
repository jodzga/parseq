package com.linkedin.parseq.promise;

import java.util.function.Function;

public class PromiseTransformer<S, T> implements PromisePropagator<S, T> {

  private final Function<S, T> _transform;

  public PromiseTransformer(Function<S, T> transform) {
    _transform = transform;
  }

  /**
   * TODO test that this optimization works when mapped multiple times
   */

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public <R> PromisePropagator<S, R> compose(final PromisePropagator<T, R> propagator) {
    if (propagator instanceof PromiseTransformer) {
      return doCompose((PromiseTransformer)propagator);
    } else {
      return PromisePropagator.super.compose(propagator);
    }
  }

  private <R> PromiseTransformer<S, R> doCompose(final PromiseTransformer<T, R> propagator) {
    return new PromiseTransformer<S, R>(_transform.andThen(propagator._transform));
  }

  @Override
  public void accept(Promise<S> src, Settable<T> dst) {
    if (src.isFailed()) {
      dst.fail(src.getError());
    } else {
      try {
        dst.done(_transform.apply(src.get()));
      } catch (Throwable t) {
        dst.fail(t);
      }
    }
  }

  @Override
  public <R> PromiseTransformer<S, R> map(final Function<T, R> f) {
    return new PromiseTransformer<S, R>(_transform.andThen(f));
  }
}
