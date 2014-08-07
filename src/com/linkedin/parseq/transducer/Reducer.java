package com.linkedin.parseq.transducer;

import java.util.function.BiFunction;

@FunctionalInterface
public interface Reducer<Z, T> extends BiFunction<Z, T, Reducer.Step<Z>>{

  static final class Step<S> {

    public enum Type {
      cont,  //continue folding
      done,  //finish folding with this value
      fail,  //folding failed
      stop,  //finish folding with last remembered value
      ignore //ignore this step, move forward
    };

    private final S _value;
    private final Type _type;
    private final Throwable _error;

    private Step(Type type, S value, Throwable error) {
      _type = type;
      _value = value;
      _error = error;
    }

    public static <S> Step<S> cont(S value) {
      return new Step<S>(Type.cont, value, null);
    }

    public static <S> Step<S> done(S value) {
      return new Step<S>(Type.done, value, null);
    }

    public static <S> Step<S> fail(Throwable t) {
      return new Step<S>(Type.fail, null, t);
    }

    public static <S> Step<S> stop() {
      return new Step<S>(Type.stop, null, null);
    }

    public static <S> Step<S> ignore() {
      return new Step<S>(Type.ignore, null, null);
    }

    public S getValue() {
      return _value;
    }

    public Type getType() {
      return _type;
    }

    public Throwable getError() {
      return _error;
    }

  }

}
