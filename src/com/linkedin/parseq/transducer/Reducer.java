package com.linkedin.parseq.transducer;

import java.util.function.BiFunction;

import com.linkedin.parseq.stream.AckValue;

@FunctionalInterface
public interface Reducer<Z, T> extends BiFunction<Z, AckValue<T>, Reducer.Step<Z>>{

  static final class Step<S> {

    public enum Type {
      cont,  //continue folding
      done  //finish folding with this value
    };

    private final S _value;
    private final Type _type;

    private Step(Type type, S value) {
      _type = type;
      _value = value;
    }

    public static <S> Step<S> cont(S value) {
      return new Step<S>(Type.cont, value);
    }

    public static <S> Step<S> done(S value) {
      return new Step<S>(Type.done, value);
    }

    public S getValue() {
      return _value;
    }

    public Type getType() {
      return _type;
    }
  }

}
