package com.linkedin.parseq;

import java.util.function.Consumer;
import java.util.function.Function;

import com.linkedin.parseq.internal.SystemHiddenTask;
import com.linkedin.parseq.promise.Promise;
import com.linkedin.parseq.promise.PromisePropagator;
import com.linkedin.parseq.promise.PromiseResolvedException;
import com.linkedin.parseq.promise.Promises;
import com.linkedin.parseq.promise.Settable;
import com.linkedin.parseq.promise.SettablePromise;

public class FunctionalTask<S, T>  extends SystemHiddenTask<T> {

  private PromisePropagator<S, T> _propagator;
  private final Task<S> _task;

  public FunctionalTask(final String name, Task<S> task, PromisePropagator<S, T> propagator) {
    super(name);
    _propagator = propagator;
    _task = task;
  }

  @Override
  public <R> Task<R> apply(String desc, PromisePropagator<T,R> propagator) {
    return new FunctionalTask<S, R>(desc, _task, _propagator.compose(propagator));
  };

  @Override
  public <R> Task<R> map(final String desc, final Function<T,R> f) {
    return new FunctionalTask<S, R>(desc + "(" + getName() + ")", _task, _propagator.map(f));
  }

  @Override
  public Task<T> andThen(final String desc, final Consumer<T> consumer) {
    return new FunctionalTask<S, T>("andThen(" + getName() + ", "+ desc + ")", _task,
        _propagator.andThen(consumer));
  }

  @Override
  public Task<T> recover(final String desc, final Function<Throwable, T> f) {
    return new FunctionalTask<S, T>("recover(" + getName() +", " + desc + ")", _task, (src, dst) -> {
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

  @Override
  protected Promise<? extends T> run(Context context) throws Throwable {
    final SettablePromise<T> result = Promises.settable();
    context.after(_task).run(new SystemHiddenTask<T>(FunctionalTask.this.getName()) {
      @Override
      protected Promise<? extends T> run(Context context) throws Throwable {
        try {
          _propagator.accept(_task, result);
          return result;
        } catch (Throwable t) {
          result.fail(t);
          return result;
        }
      }
    });
    context.run(_task);
    return result;
  }
}
