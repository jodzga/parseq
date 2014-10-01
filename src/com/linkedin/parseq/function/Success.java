package com.linkedin.parseq.function;

public class Success<T> implements Try<T>{

  private final T _value;

  private Success(T value) {
    this._value = value;
  }

  @Override
  public T get() {
    return _value;
  }

  @Override
  public boolean isFailed() {
    return false;
  }

  @Override
  public Throwable getError() {
    return null;
  }

  @Override
  public ResultType resultType() {
    return ResultType.success;
  }

  public static <R> Try<R> of(R value) {
    return new Success<R>(value);
  }

}
