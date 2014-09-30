package com.linkedin.parseq.transducer;

public interface Foldable<Z, T, V> {
  V fold(Z zero, Reducer<Z, T> reducer);
}
