package com.linkedin.parseq.transducer;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import com.linkedin.parseq.stream.AckValue;
import com.linkedin.parseq.transducer.Reducer.Step;

@FunctionalInterface
public interface Transducer<T, R> extends Function<Reducer<Object, R>, Reducer<Object, T>> {

  default <A> Transducer<T, A> map(final Function<AckValue<R>, AckValue<A>> f) {
    return fa -> apply((z, r) -> fa.apply(z, f.apply(r)));
  }

  default <A> Transducer<T, A> compose(final Transducer<R, A> tf) {
    return fa -> apply(tf.apply(fa));
  }

  default Transducer<T, R> forEach(final Consumer<AckValue<R>> consumer) {
    return map(e -> {
      consumer.accept(e);
      return e;
    });
  }

  default Transducer<T, R> filter(final Predicate<R> predicate) {
    return fr -> apply((z, r) -> {
      if (predicate.test(r.get())) {
        return fr.apply(z, r);
      } else {
        r.ack();
        return Step.cont(z);
      }
    });
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
    final Counter counter = new Counter(0);
    return fr -> this.apply((z, r) -> {
      if (n == 0) {
        r.ack();
        return Step.done(z);
      } else if (counter.inc() < n) {
        return fr.apply(z, r);
      } else {
        return Step.done(fr.apply(z, r).getValue());
      }
    });
  }

  default Transducer<T, R> takeWhile(final Predicate<R> predicate) {
    return fr -> this.apply((z, r) -> {
      if (predicate.test(r.get())) {
        return fr.apply(z, r);
      } else {
        r.ack();
        return Step.done(z);
      }
    });
  }

  /**
   * other operations proposal:
   * all
   * partition
   * split
   * groupBy
   */

}
