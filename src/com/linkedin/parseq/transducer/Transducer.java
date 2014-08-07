package com.linkedin.parseq.transducer;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import com.linkedin.parseq.transducer.Reducer.Step;

@FunctionalInterface
public interface Transducer<T, R> extends Function<Reducer<Object, R>, Reducer<Object, T>> {

  default <A> Transducer<T, A> map(final Function<R, A> f) {
    return fa -> this.apply((z, r) -> fa.apply(z, f.apply(r)));
  }

  default <A> Transducer<T, A> flatMap(final Transducer<R, A> tf) {
    return fa -> this.apply(tf.apply(fa));
  }

  default Transducer<T, R> forEach(final Consumer<R> consumer) {
    return map(e -> {
      consumer.accept(e);
      return e;
    });
  }

  default Transducer<T, R> filter(final Predicate<R> predicate) {
    return fr -> this.apply((z, r) -> {
      if (predicate.test(r)) {
        return fr.apply(z, r);
      } else {
        return Step.ignore();
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
      Step<Object> step = fr.apply(z, r);
      if (counter.inc() < n) {
        return step;
      } else {
        if (step.getType() == Step.Type.cont) {
          return Step.done(step.getValue());
        } else {
          return step;
        }
      }
    });
  }

  default Transducer<T, R> takeWhile(final Predicate<R> predicate) {
    return fr -> this.apply((z, r) -> {
      if (predicate.test(r)) {
        return fr.apply(z, r);
      } else {
        return Step.stop();
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
