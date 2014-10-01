package com.linkedin.parseq.function;

import java.util.NoSuchElementException;

public class Failure<T> implements Try<T> {

  private final Throwable _error;

  private Failure(Throwable error) {
    _error = error;
  }

  @Override
  public T get() {
    throw (NoSuchElementException)new NoSuchElementException().initCause(_error);
  }

  @Override
  public boolean isFailed() {
    return true;
  }

  @Override
  public Throwable getError() {
    return _error;
  }

  @Override
  public ResultType resultType() {
    return ResultType.failure;
  }

  public static <R> Try<R> of(Throwable t) {
    return new Failure<R>(t);
  }

}
