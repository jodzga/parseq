package com.linkedin.parseq.stream;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import com.linkedin.parseq.promise.Promise;
import com.linkedin.parseq.promise.Promises;
import com.linkedin.parseq.promise.SettablePromise;
import com.linkedin.parseq.task.BaseTask;
import com.linkedin.parseq.task.Context;
import com.linkedin.parseq.task.FusionTask;
import com.linkedin.parseq.task.Priority;
import com.linkedin.parseq.task.Task;
import com.linkedin.parseq.task.TaskOrValue;
import com.linkedin.parseq.task.Tasks;
import com.linkedin.parseq.transducer.Reducer;
import com.linkedin.parseq.transducer.Reducer.Step;
import com.linkedin.parseq.transducer.Ref;

/**
 * @author Jaroslaw Odzga (jodzga@linkedin.com)
 */
public class StreamFoldTask<Z, T> extends BaseTask<Z> implements Ref<Z> {

  private Publisher<TaskOrValue<T>> _tasks;
  private boolean _streamingComplete = false;
  private int _pending = 0;
  private Z _partialResult;
  private Subscription _subscription;
  private final Reducer<Z, T> _reducer;
  private final Optional<Task<?>> _predecessor;
  private final String _name;


  public StreamFoldTask(final String name, final Publisher<TaskOrValue<T>> tasks, final Z zero,
      final Reducer<Z, T> reducer, Optional<Task<?>> predecessor) {
    super(name);
    _partialResult = zero;
    _reducer = reducer;
    _tasks = tasks;
    _predecessor = predecessor;
    _name = name;
  }

  //TODO: when result is resolved, then tasks should be early finished, not started?

  @Override
  protected Promise<? extends Z> run(final Context context) throws Exception
  {
    final SettablePromise<Z> result = Promises.settable();

    _tasks.subscribe(new Subscriber<TaskOrValue<T>>() {

      private void onNextStep(Step<Z> step) {
        switch (step.getType()) {
          case cont:
            _partialResult = step.getValue();
            if (_streamingComplete && _pending == 0) {
              result.done(_partialResult);
              _partialResult = null;
            }
            break;
          case done:
            _partialResult = null;
            _subscription.cancel();
            result.done(step.getValue());
            break;
        }
      }

      private void onNextValue(TaskOrValue<T> tValue) {
        try {
          TaskOrValue<Step<Z>> step = _reducer.apply(StreamFoldTask.this, tValue);
          if (step.isTask()) {
            _pending++;
            scheduleTask(fusedPropgatingTask("reduce", step.getTask(),
                s -> {
                  _pending--;
                  onNextStep(s);
                }), context, StreamFoldTask.this);
          } else {
            onNextStep(step.getValue());
          }
        } catch (Throwable e) {
          _streamingComplete = true;
          _partialResult = null;
          result.fail(e);
        }
      }

      private void onNextTask(Task<T> task) {
        _pending++;
        scheduleTask(fusedPropgatingTask("step", task,
            t -> {
              _pending--;
              onNextValue(TaskOrValue.value(t));
            }), context, StreamFoldTask.this);
      }

      private <A> FusionTask<?, A> fusedPropgatingTask(final String description, final Task<A> task, final Consumer<A> consumer) {
        return FusionTask.fuse(description + "(" + _name + ")", task,
            (p, t) -> {
              try
              {
                //propagate result
                if (p.isFailed()) {
                  t.fail(p.getError());
                } else {
                  t.done(p.get());
                }
              } finally {
                if (!result.isDone()) {
                  if (p.isFailed()) {
                    _subscription.cancel();
                    _partialResult = null;
                    result.fail(p.getError());
                  } else {
                    consumer.accept(p.get());
                  }
                } else {
                  /**
                   * result is resolved, it means that stream has completed or
                   * it has been cancelled
                   */
                }
              }
            });
      }

      /**
       * It is expected that onNext method is called
       * from within Task's run method.
       */
      @Override
      public void onNext(final TaskOrValue<T> taskOrValue) {
        if (taskOrValue.isTask()) {
          onNextTask(taskOrValue.getTask());
        } else {
          onNextValue(taskOrValue);
        }
      }

      @Override
      public void onComplete() {
        _streamingComplete = true;
        if (_pending == 0) {
          if (!result.isDone()) {
            result.done(_partialResult);
            _partialResult = null;
          }
        }
      }

      @Override
      public void onError(Throwable cause) {
        _streamingComplete = true;
        if (!result.isDone()) {
          result.fail(cause);
          _partialResult = null;
        }
      }

      @Override
      public void onSubscribe(Subscription subscription) {
        _subscription = subscription;
      }
    });

    if (_predecessor.isPresent()) {
      context.run(_predecessor.get());
    }

    _tasks = null;
    return result;
  }

  class WithinContextRunWrapper implements ContextRunWrapper<Z> {

    protected final SettablePromise<Z> _result = Promises.settable();
    protected final AtomicBoolean _committed = new AtomicBoolean();
    private final long _time;
    private final TimeUnit _unit;

    public WithinContextRunWrapper(long time, TimeUnit unit) {
      _time = time;
      _unit = unit;
    }

    @Override
    public void before(Context context) {
      final Task<?> withinTask = Tasks.action("withinTimer", () -> {
        if (_committed.compareAndSet(false, true)) {
          //
          _streamingComplete = true;
          _result.done(_partialResult);
        }
      });
      //within tasks should run as early as possible
      withinTask.setPriority(Priority.MAX_PRIORITY);
      context.createTimer(_time, _unit, withinTask);
    }

    @Override
    public Promise<Z> after(Context context, Promise<Z> promise) {
      promise.addListener(p -> {
        if (_committed.compareAndSet(false, true)) {
          Promises.propagateResult(promise, _result);
        }
      });
      return _result;
    }
  }

  @Override
  public Task<Z> within(long time, TimeUnit unit) {
    wrapContextRun(new WithinContextRunWrapper(time, unit));
    return this;
  }

  void scheduleTask(Task<?> task, Context context, Task<?> rootTask) {
    context.runSubTask(task, rootTask);
  }

  @Override
  public Z refGet() {
    return _partialResult;
  }

  @Override
  public void refSet(Z t) {
    _partialResult = t;
  }

}
