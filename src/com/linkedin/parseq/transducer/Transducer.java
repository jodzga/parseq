package com.linkedin.parseq.transducer;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import com.linkedin.parseq.task.TaskOrValue;
import com.linkedin.parseq.transducer.Reducer.Step;
import com.linkedin.parseq.util.Integers;

@FunctionalInterface
public interface Transducer<T, R> extends Function<Reducer<Object, R>, Reducer<Object, T>> {

  default <A> Transducer<T, A> map(final Function<TaskOrValue<R>, TaskOrValue<A>> f) {
    return fa -> apply((z, r) -> fa.apply(z, f.apply(r)));
  }

  default <A> Transducer<T, A> compose(final Transducer<R, A> tf) {
    return fa -> apply(tf.apply(fa));
  }

  default Transducer<T, R> forEach(final Consumer<TaskOrValue<R>> consumer) {
    return map(e -> {
      consumer.accept(e);
      return e;
    });
  }

  default Transducer<T, R> filter(final Predicate<R> predicate) {
    return fr -> apply((z, r) -> r.flatMap(rValue -> {
        if (predicate.test(rValue)) {
          return fr.apply(z, TaskOrValue.value(rValue));
        } else {
          return TaskOrValue.value(Step.cont(z));
        }
      }));
  }

  static final class Counter {
    int _counter;
    public Counter(int counter) {
      _counter = counter;
    }
    int inc() {
      _counter++;
      return _counter;
    }
  }

  default Transducer<T, R> take(final int n) {
    Integers.requireNonNegative(n);
    if (n > 0) {
      final Counter counter = new Counter(0);
      return fr -> apply((z, r) -> r.flatMap(rValue -> {
        if (counter.inc() < n) {
          return fr.apply(z, TaskOrValue.value(rValue));
        } else {
          return fr.apply(z, TaskOrValue.value(rValue)).flatMap(s -> TaskOrValue.value(Step.done(s.getValue())));
        }
      }));
    } else {
      return fr -> apply((z, r) -> TaskOrValue.value(Step.done(z)));
    }
  }

  default Transducer<T, R> drop(final int n) {
    Integers.requireNonNegative(n);
    if (n >= 0) {
      final Counter counter = new Counter(0);
      return fr -> apply((z, r) -> r.flatMap( rValue -> {
        if (counter.inc() < n) {
          return TaskOrValue.value(Step.cont(z));
        } else {
          return fr.apply(z, TaskOrValue.value(rValue));
        }
      }));
    } else {
      return this;
    }
  }

  default Transducer<T, R> takeWhile(final Predicate<R> predicate) {
    return fr -> this.apply((z, r) -> r.flatMap(rValue -> {
      if (predicate.test(rValue)) {
        return fr.apply(z, TaskOrValue.value(rValue));
      } else {
        return TaskOrValue.value(Step.done(z));
      }
    }));
  }

  static final class Trap {
    boolean _closed = false;
    void trigger() {
      _closed = true;
    }
    boolean closed() {
      return _closed;
    }
  }

  default Transducer<T, R> dropWhile(final Predicate<R> predicate) {
    final Trap trap = new Trap();
    return fr -> apply((z, r) -> r.flatMap(rValue -> {
      if (!trap.closed()) {
        if (predicate.test(rValue)) {
          return TaskOrValue.value(Step.cont(z));
        } else {
          trap.trigger();
        }
      }
      return fr.apply(z, TaskOrValue.value(rValue));
    }));
  }

  /**
   * other operations proposal:
   *
   * partition
   * split
   * groupBy
   *
   * grouped(n)
   */

  @SuppressWarnings("rawtypes")
  static final Transducer IDENTITY = x -> x;

  @SuppressWarnings("unchecked")
  static <A> Transducer<A, A> identity() {
    return IDENTITY;
  }
}
