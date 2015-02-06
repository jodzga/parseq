package com.linkedin.parseq.task;

import java.util.function.Consumer;
import java.util.function.Function;

@FunctionalInterface
public interface RichCallable<T> {

  T call();

  default <R> RichCallable<R> map(final Function<T, R> f) {
    return () -> f.apply(call());
  }

  default RichCallable<T> andThen(final Consumer<T> consumer) {
    return () -> {
      final T value = call();
      consumer.accept(value);
      return value;
    };
  }

  default <R> RichCallable<R> flatMap(final Function<T, RichCallable<R>> f) {
    return () -> f.apply(call()).call();
  }


  //TODO rest of combinators

}
