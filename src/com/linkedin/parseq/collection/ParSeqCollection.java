package com.linkedin.parseq.collection;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import com.linkedin.parseq.transducer.Foldable;
import com.linkedin.parseq.transducer.Reducer;
import com.linkedin.parseq.transducer.Reducer.Step;
import com.linkedin.parseq.transducer.Transducer;

/**
 *
 * @author jodzga
 *
 * @param <T>
 * @param <R>
 */
public abstract class ParSeqCollection<T, R> {
  /**
   * This function transforms folding function from the one which folds type R to the one
   * which folds type T.
   */
  protected final Transducer<T, R> _transducer;

  protected ParSeqCollection(Transducer<T, R> transducer) {
    _transducer = transducer;
  }

  @SuppressWarnings("unchecked")
  protected <Z> Reducer<Z, T> transduce(final Reducer<Z, R> reducer) {
    return (Reducer<Z, T>)_transducer.apply((Reducer<Object, R>)reducer);
  }

  protected <Z, A> Reducer<Z, A> acking(final Reducer<Z, A> reducer) {
    return (z, ackA) -> { ackA.ack(); return reducer.apply(z, ackA); };
  }

  protected static final <R> R checkEmpty(Optional<R> result) {
    if (result.isPresent()) {
      return result.get();
    } else {
      throw new NoSuchElementException();
    }
  }


  /*
   * Collection transformations:
   */

  protected <A, V extends ParSeqCollection<T, A>> V map(final Function<R, A> f,
      Function<Transducer<T, A>, V> collectionBuilder) {
    return collectionBuilder.apply(_transducer.map(ackR -> ackR.map(f)));
  }

  protected <V extends ParSeqCollection<T, R>> V forEach(final Consumer<R> consumer,
      Function<Transducer<T, R>, V> collectionBuilder) {
    return map(e -> {
      consumer.accept(e);
      return e;
    }, collectionBuilder);
  }

  protected <V extends ParSeqCollection<T, R>> V filter(final Predicate<R> predicate,
      Function<Transducer<T, R>, V> collectionBuilder) {
    return collectionBuilder.apply(_transducer.filter(predicate));
  }

  protected <V extends ParSeqCollection<T, R>> V take(final int n,
      Function<Transducer<T, R>, V> collectionBuilder) {
    return collectionBuilder.apply(_transducer.take(n));
  }

  protected <V extends ParSeqCollection<T, R>> V takeWhile(final Predicate<R> predicate,
      Function<Transducer<T, R>, V> collectionBuilder) {
    return collectionBuilder.apply(_transducer.takeWhile(predicate));
  }

  protected <V extends ParSeqCollection<T, R>> V drop(final int n,
      Function<Transducer<T, R>, V> collectionBuilder) {
    return collectionBuilder.apply(_transducer.drop(n));
  }

  protected <V extends ParSeqCollection<T, R>> V dropWhile(final Predicate<R> predicate,
      Function<Transducer<T, R>, V> collectionBuilder) {
    return collectionBuilder.apply(_transducer.dropWhile(predicate));
  }

  /*
   * Foldings:
   */

  protected <Z, V> V fold(final Z zero, final BiFunction<Z, R, Z> op, final Foldable<Z, T, V> foldable) {
    return foldable.fold(zero, acking(transduce((z, e) -> Step.cont(op.apply(z, e.get())))));
  }

  protected <V> V first(final Foldable<Optional<R>, T, V> foldable) {
    return foldable.fold(Optional.empty(), acking(transduce((z, r) -> Step.done(Optional.of(r.get())))));
  }

  protected <V> V last(final Foldable<Optional<R>, T, V> foldable) {
    return foldable.fold(Optional.empty(), acking(transduce((z, r) -> Step.cont(Optional.of(r.get())))));
  }

  protected <V> V all(final Foldable<List<R>, T, V> foldable) {
    return foldable.fold(new ArrayList<R>(), acking(transduce((z, r) -> {
      z.add(r.get());
      return Step.cont(z);
    })));
  }

  private static class BooleanHolder {
    private boolean _value = false;
    public BooleanHolder(boolean value) {
      _value = value;
    }
  }

  protected <V> V reduce(final BiFunction<R, R, R> op, final Foldable<Optional<R>, T, V> foldable) {
    final BooleanHolder first = new BooleanHolder(true);
    return foldable.fold(Optional.empty(), acking(transduce((z, e) -> {
      if (first._value) {
        first._value = false;
        return Step.cont(Optional.of(e.get()));
      } else {
        return Step.cont(Optional.of(op.apply(z.get(), e.get())));
      }
    })));
  }

  protected <V> V find(final Predicate<R> predicate, final Foldable<Optional<R>, T, V> foldable) {
    return foldable.fold(Optional.empty(), acking(transduce((z, e) -> {
      if (predicate.test(e.get())) {
        return Step.done(Optional.of(e.get()));
      } else {
        return Step.cont(z);
      }
    })));
  }

  /**
   * distinct
   * sort
   * group by
   * partition
   * split
   * buffering (time and count)
   *
   * functions which take simplified subscriber e.g.
   * all(Subscriber<T>)
   */
}
