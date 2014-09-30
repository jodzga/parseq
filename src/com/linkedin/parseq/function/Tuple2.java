package com.linkedin.parseq.function;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.BiFunction;

public class Tuple2<T1, T2> implements Tuple {

  private final T1 _1;
  private final T2 _2;

  public Tuple2(T1 name, T2 name2) {
    _1 = name;
    _2 = name2;
  }

  public T1 _1() {
    return _1;
  }

  public T2 _2() {
    return _2;
  }

  public <C> C map(final BiFunction<T1, T2, C> f) {
    return f.apply(_1, _2);
  }

  @Override
  public Iterator<Object> iterator() {
    return new Iterator<Object>() {
      private int _index = 0;
      @Override
      public boolean hasNext() {
        return _index < arity();
      }

      @Override
      public Object next() {
        switch(_index) {
          case 0:
            _index++;
            return _1;
          case 1:
            _index++;
            return _1;
        }
        throw new NoSuchElementException();
      }
    };
  }

  @Override
  public int arity() {
    return 2;
  }

}
