/*
 * Copyright 2012 LinkedIn, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.linkedin.parseq.task;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

import com.linkedin.parseq.engine.Engine;
import com.linkedin.parseq.function.Failure;
import com.linkedin.parseq.function.Success;
import com.linkedin.parseq.function.Try;
import com.linkedin.parseq.internal.SystemHiddenTask;
import com.linkedin.parseq.internal.TaskLogger;
import com.linkedin.parseq.promise.Promise;
import com.linkedin.parseq.promise.PromisePropagator;
import com.linkedin.parseq.promise.PromiseTransformer;
import com.linkedin.parseq.promise.Promises;
import com.linkedin.parseq.promise.SettablePromise;
import com.linkedin.parseq.promise.TransformingPromiseListener;
import com.linkedin.parseq.trace.Related;
import com.linkedin.parseq.trace.ShallowTrace;
import com.linkedin.parseq.trace.Trace;

/**
 * TODO modify functional operators to inline functions instead of creating
 * separate tasks for each of them (performance reasons).
 * This requires improved tracing so that functions can be part of task trace.
 *
 * TODO safety of operations which return task - promise listeners etc.
 *
 * A task represents a deferred execution that also contains its resulting
 * value. In addition, tasks include some tracing information that can be
 * used with various trace printers.
 * <p/>
 * Tasks should generally be run using either an {@link Engine} or a
 * {@link Context}. They should not be run directly.
 *
 * @author Chris Pettitt (cpettitt@linkedin.com)
 * @author Jaroslaw Odzga (jodzga@linkedin.com)
 */
public interface Task<T> extends Promise<T>, Cancellable
{
  /**
   * Returns the name of this task.
   *
   * @return the name of this task
   */
  public String getName();

  /**
   * Returns the priority for this task.
   *
   * @return the priority for this task.
   */
  int getPriority();

  /**
   * Overrides the priority for this task. Higher priority tasks will be
   * executed before lower priority tasks in the same context. In most cases,
   * the default priority is sufficient.
   * <p/>
   * The default priority is 0. Use {@code priority < 0} to make a task
   * lower priority and {@code priority > 0} to make a task higher
   * priority.
   * <p/>
   * If the task has already started execution the priority cannot be
   * changed.
   *
   * @param priority the new priority for the task.
   * @return {@code true} if the priority was set; otherwise {@code false}.
   * @throws IllegalArgumentException if the priority is out of range
   * @see Priority
   */
  boolean setPriority(int priority);

  /**
   * Attempts to run the task with the given context. This method is
   * reserved for use by {@link Engine} and {@link Context}.
   *
   * @param context the context to use while running this step
   * @param taskLogger the logger used for task events
   * @param parent the parent of this task
   * @param predecessors that lead to the execution of this task
   */
  void contextRun(Context context, TaskLogger taskLogger,
                  Task<?> parent, Collection<Task<?>> predecessors);

  void wrapContextRun(ContextRunWrapper<T> wrapper);

  /**
   * Returns the ShallowTrace for this task. The ShallowTrace will be
   * a point-in-time snapshot and may change over time until the task is
   * completed.
   *
   * @return the ShallowTrace related to this task
   */
  ShallowTrace getShallowTrace();

  /**
   * Returns the Trace for this task. The Trace will be a point-in-time snapshot
   * and may change over time until the task is completed.
   *
   * @return the Trace related to this task
   */
  Trace getTrace();

  /**
   * Returns the set of relationships of this task. The parent relationships are not included.
   *
   * @see com.linkedin.parseq.trace.Relationship the available relationships
   * @return the set of relationships of this task.
   */
  Set<Related<Task<?>>> getRelationships();

  default <R> Task<R> apply(final String desc, final PromisePropagator<T, R> propagator) {
    return FusionTask.fuse(desc, this, propagator);
  }

  /**
   * Creates a new Task by applying a function to the successful result of this Task.
   * If this Task is completed with an exception then the new Task will also contain that exception.
   *
   * @param desc description of a mapping function, it will show up in a trace
   * @param f function to be applied to successful result of this Task.
   * @return a new Task which will apply given function on result of successful completion of this task
   */
  default <R> Task<R> map(final String desc, final Function<T, R> f) {
    return apply(desc, new PromiseTransformer<T, R>(f));
  }

  //TODO move description to be a last parameter and add enum or fla to signify if it is supposed to be hidden

  default <R> Task<R> map(final Function<T, R> f) {
    return map("map", f);
  }

  /**
   * Creates a new Task by applying a function to the successful result of this Task and
   * returns the result of a function as the new Task.
   * If this Task is completed with an exception then the new Task will also contain that exception.
   *
   * @param desc description of a mapping function, it will show up in a trace
   * @param f function to be applied to successful result of this Task.
   * @return a new Task which will apply given function on result of successful completion of this task
   */
  default <R> Task<R> flatMap(final String desc, final Function<T, Task<R>> f) {
    return mapOrFlatMap(desc, t -> TaskOrValue.task(f.apply(t)));
  }

  default <R> Task<R> flatMap(final Function<T, Task<R>> f) {
    return flatMap("flatMap", f);
  }

  default <R> Task<R> mapOrFlatMap(final String name, final Function<T, TaskOrValue<R>> f) {
    final Task<T> that = this;
    return new SystemHiddenTask<R>(name) {
      @Override
      protected Promise<R> run(Context context) throws Throwable {
        final SettablePromise<R> result = Promises.settable();
        context.after(that).run(new SystemHiddenTask<R>(name) {
          @Override
          protected Promise<R> run(Context context) throws Throwable {
            try {
              TaskOrValue<R> taskOrValueR = f.apply(that.get());
              if (taskOrValueR.isTask()) {
                Task<R> taskR = taskOrValueR.getTask();
                Promises.propagateResult(taskR, result);
                context.run(taskR);
                return taskR;
              } else {
                Promise<R> valueR = Promises.value(taskOrValueR.getValue());
                Promises.propagateResult(valueR, result);
                return valueR;
              }
            } catch (Throwable t) {
              result.fail(t);
              return Promises.error(t);
            }
          }
        });
        context.run(that);
        return result;
      }
    };
  }

  default <R> Task<R> mapOrFlatMap(final Function<T, TaskOrValue<R>> f) {
    return mapOrFlatMap("mapOrFlatMap", f);
  }

  /**
   * Applies the function to the result of this Task, and returns
   * a new Task with the result of this Task to allow fluent chaining.
   *
   * @param desc description of a side-effecting function, it will show up in a trace
   * @param consumer side-effecting function
   * @return a new Task with the result of this Task
   */
  default Task<T> andThen(final String desc, final Consumer<T> consumer) {
    return apply(desc,
        new PromiseTransformer<T,T>(t -> {
          consumer.accept(t);
          return t;
        }));
  }

  default Task<T> andThen(final Consumer<T> consumer) {
    return andThen("andThen", consumer);
  }

  default <R> Task<R> andThen(final String desc, final Task<R> task) {
    final Task<T> that = this;
    return new SystemHiddenTask<R>(desc) {
      @Override
      protected Promise<R> run(Context context) throws Throwable {
        final SettablePromise<R> result = Promises.settable();
        context.after(that).run(task);
        Promises.propagateResult(task, result);
        context.run(that);
        return result;
      }
    };
  }

  default <R> Task<R> andThen(final Task<R> task) {
    return andThen("andThen", task);
  }

  /**
   * Creates a new Task that will handle any Throwable that this Task might throw
   * or Task cancellation.
   * If this task completes successfully, then recovery function is not invoked.
   *
   * @param desc description of a recovery function, it will show up in a trace
   * @param f recovery function which can complete Task with a value depending on
   *        Throwable thrown by this Task
   * @return a new Task which can recover from Throwable thrown by this Task
   */
  default Task<T> recover(final String desc, final Function<Throwable, T> f) {
    return apply(desc,  (src, dst) -> {
      if (src.isFailed()) {
        try {
          dst.done(f.apply(src.getError()));
        } catch (Throwable t) {
          dst.fail(t);
        }
      } else {
        dst.done(src.get());
      }
    });
  }

  default Task<T> recover(final Function<Throwable, T> f) {
    return recover("recover", f);
  }

  default Task<Try<T>> withTry() {
    return map("withTry", t -> Success.of(t))
             .recover("withTry", t -> Failure.of(t));
  }

  /**
   * Creates a new Task that will handle any Throwable that this Task might throw
   * or Task cancellation. If this task completes successfully,
   * then recovery function is not invoked. Task returned by recovery function
   * will become a new result of this Task. This means that if recovery function fails,
   * then result of this task will fail with a Throwable from recovery function.
   *
   * @param desc description of a recovery function, it will show up in a trace
   * @param f recovery function which can return Task which will become a new result of
   * this Task
   * @return a new Task which can recover from Throwable thrown by this Task or cancellation
   */
  default Task<T> recoverWith(final String desc, final Function<Throwable, Task<T>> f) {
    final Task<T> that = this;
    return new SystemHiddenTask<T>(desc) {
      @Override
      protected Promise<T> run(Context context) throws Throwable {
        final SettablePromise<T> result = Promises.settable();
        context.after(that).run(new SystemHiddenTask<T>(desc) {
          @Override
          protected Promise<T> run(Context context) throws Throwable {
            if (that.isFailed()) {
              try {
                Task<T> recovery = f.apply(that.getError());
                Promises.propagateResult(recovery, result);
                context.run(recovery);
              } catch (Throwable t) {
                result.fail(t);
              }
            } else {
              result.done(that.get());
            }
            return result;
          }
        });
        context.run(that);
        return result;
      }
    };
  }

  default Task<T> recoverWith(final Function<Throwable, Task<T>> f) {
    return recoverWith("recoverWith", f);
  }  
  /**
   * Creates a new Task that will handle any Throwable that this Task might throw
   * or Task cancellation. If this task completes successfully,
   * then fall-back function is not invoked. If Task returned by fall-back function
   * completes successfully with a value, then that value becomes a result of this
   * Task. If Task returned by fall-back function fails with a Throwable or is cancelled,
   * then this Task will fail with the original Throwable, not the one coming from
   * the fall-back function's Task.
   *
   * @param desc description of a recovery function, it will show up in a trace
   * @param f recovery function which can return Task which will become a new result of
   * this Task
   * @return a new Task which can recover from Throwable thrown by this Task or cancellation
   */
  default Task<T> fallBackTo(final String desc, final Function<Throwable, Task<T>> f) {
    //TODO

    return new SystemHiddenTask<T>(desc) {
      @Override
      protected Promise<T> run(final Context context) throws Throwable {
        final SettablePromise<T> result = Promises.settable();
        context.run(apply(desc, (src, dst) -> {
          if (src.isFailed()) {
            try {
              Task<T> recovery = f.apply(src.getError());  //TODO get rid of TransformingPromiseListener
              recovery.addListener(new TransformingPromiseListener<T, T>(result, (s, d) -> {
                if (s.isFailed()) {
                  d.fail(src.getError());  //this is the main difference from recoverWith: return original error
                } else {
                  d.done(s.get());
                }
              }));
              context.run(recovery);
            } catch (Throwable t) {
              dst.fail(t);
            }
          } else {
            dst.done(src.get());
          }
        }));
        return result;
      }
    };
  }

  default Task<T> fallBackTo(final Function<Throwable, Task<T>> f) {
    return fallBackTo("fallBackTo", f);
  }
  
  static class TimeoutContextRunWrapper<T> implements ContextRunWrapper<T> {

    protected final SettablePromise<T> _result = Promises.settable();
    protected final AtomicBoolean _committed = new AtomicBoolean();
    private final long _time;
    private final TimeUnit _unit;
    private final Exception _exception;

    public TimeoutContextRunWrapper(long time, TimeUnit unit, final Exception exception) {
      _time = time;
      _unit = unit;
      _exception = exception;
    }

    @Override
    public void before(Context context) {
      final Task<?> timeoutTask = Tasks.action("timeoutTimer", () -> {
        if (_committed.compareAndSet(false, true)) {
          _result.fail(_exception);
        }
      });
      //timeout tasks should run as early as possible
      timeoutTask.setPriority(Priority.MAX_PRIORITY);
      context.createTimer(_time, _unit, timeoutTask);
    }

    @Override
    public Promise<T> after(Context context, Promise<T> promise) {
      promise.addListener(p -> {
        if (_committed.compareAndSet(false, true)) {
          Promises.propagateResult(promise, _result);
        }
      });
      return _result;
    }
  }

  /**
   * TODO document
   *
   * @param time the time to wait before timing out
   * @param unit the units for the time
   * @param <T> the value type for the task
   * @return the new Task with a timeout
   */
  default Task<T> withTimeout(final long time, final TimeUnit unit)
  {
    wrapContextRun(new TimeoutContextRunWrapper<T>(time, unit, Exceptions.TIMEOUT_EXCEPTION));
    return this;
  }

  default Task<T> within(final long time, final TimeUnit unit) {
    wrapContextRun(new TimeoutContextRunWrapper<T>(time, unit, Exceptions.noSuchElement()));
    return this;
  }

  public interface ContextRunWrapper<T> {

    void before(Context context);

    Promise<T> after(Context context, Promise<T> promise);

    default ContextRunWrapper<T> compose(final ContextRunWrapper<T> wrapper) {
      ContextRunWrapper<T> that = this;
      return new ContextRunWrapper<T>() {

        @Override
        public void before(Context context) {
          wrapper.before(context);
          that.before(context);
        }

        @Override
        public Promise<T> after(Context context, Promise<T> promise) {
          return wrapper.after(context, that.after(context, promise));
        }
      };
    }
  }


}
