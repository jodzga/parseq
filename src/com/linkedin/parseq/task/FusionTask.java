package com.linkedin.parseq.task;

import java.util.function.Consumer;
import java.util.function.Function;

import com.linkedin.parseq.internal.SystemHiddenTask;
import com.linkedin.parseq.promise.Promise;
import com.linkedin.parseq.promise.PromisePropagator;
import com.linkedin.parseq.promise.PromiseResolvedException;
import com.linkedin.parseq.promise.Promises;
import com.linkedin.parseq.promise.Settable;
import com.linkedin.parseq.promise.SettablePromise;

/**
 * TODO define how cancellation is supposed to work
 *
 * @author jodzga
 *
 * @param <S>
 * @param <T>
 */
//TODO zmienic w jaki sposob task jest hidded - nie przez inheritance
public class FusionTask<S, T>  extends SystemHiddenTask<T> {

  private static final String FUSION_TRACE_SYMBOL = " - ";

  private PromisePropagator<S, T> _propagator;
  private final Task<S> _task;

  public FusionTask(final String name, final Task<S> task, final PromisePropagator<S, T> propagator) {
    super(name);
    _propagator = propagator;
    _task = task;
  }

  @SuppressWarnings("unchecked")
  public static <S, T> FusionTask<?, T> fuse(final String name, final Task<S> task, final PromisePropagator<S, T> propagator) {
    if (task instanceof FusionTask) {
      return ((FusionTask<?, S>)task).apply(name, propagator);
    } else {
      return new FusionTask<S, T>(name, task, propagator);
    }
  }

  @Override
  public <R> FusionTask<?, R> apply(String desc, PromisePropagator<T,R> propagator) {
    return new FusionTask<S, R>(desc, _task, _propagator.compose(propagator));
  };

  @Override
  public <R> Task<R> map(final String desc, final Function<T,R> f) {
    return new FusionTask<S, R>(getName() + FUSION_TRACE_SYMBOL + desc, _task, _propagator.map(f));
  }

  @Override
  public Task<T> andThen(final String desc, final Consumer<T> consumer) {
    return new FusionTask<S, T>(getName() + FUSION_TRACE_SYMBOL + desc, _task,
        _propagator.andThen(consumer));
  }

  @Override
  public Task<T> recover(final String desc, final Function<Throwable, T> f) {
    return new FusionTask<S, T>(getName() +FUSION_TRACE_SYMBOL + desc, _task, (src, dst) -> {
      _propagator.accept(src, new Settable<T>() {
        @Override
        public void done(T value) throws PromiseResolvedException {
          dst.done(value);
        }
        @Override
        public void fail(Throwable error) throws PromiseResolvedException {
          try {
            dst.done(f.apply(error));
          } catch (Throwable t) {
            dst.fail(t);
          }
        }
      });
    });
  }

  //TODO implement other functions

  protected SettablePromise<T> propagate(Promise<S> promise, SettablePromise<T> result) {
    try {
      _propagator.accept(_task, result);
      return result;
    } catch (Throwable t) {
      result.fail(t);
      return result;
    }
  }

  @Override
  protected Promise<? extends T> run(Context context) throws Throwable {
    final SettablePromise<T> result = Promises.settable();
    context.after(_task).run(new SystemHiddenTask<T>(FusionTask.this.getName()) {
      @Override
      protected Promise<? extends T> run(Context context) throws Throwable {
        return propagate(_task, result);
      }
    });
    context.run(_task);
    return result;
  }
}
