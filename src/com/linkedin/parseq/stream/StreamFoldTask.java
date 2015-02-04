package com.linkedin.parseq.stream;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.linkedin.parseq.promise.Promise;
import com.linkedin.parseq.promise.Promises;
import com.linkedin.parseq.promise.SettablePromise;
import com.linkedin.parseq.task.BaseTask;
import com.linkedin.parseq.task.Context;
import com.linkedin.parseq.task.FunctionalTask;
import com.linkedin.parseq.task.Priority;
import com.linkedin.parseq.task.Task;
import com.linkedin.parseq.task.TaskOrValue;
import com.linkedin.parseq.task.Tasks;
import com.linkedin.parseq.transducer.FlowControl;
import com.linkedin.parseq.transducer.Reducer;
import com.linkedin.parseq.transducer.Reducer.Step;

/**
 * @author Jaroslaw Odzga (jodzga@linkedin.com)
 */
public class StreamFoldTask<Z, T> extends BaseTask<Z> {

  private Publisher<TaskOrValue<T>> _tasks;
  private boolean _streamingComplete = false;
  private int _totalElements;
  private int _elementsCompleted = 0;
  private Z _partialResult;
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
    final Task<Z> that = this; //TODO ???

    _tasks.subscribe(new AckingSubscriber<TaskOrValue<T>>() {

      private void onNextValue(AckValue<T> t) {
        try {
          //ack() is called by reducer
          Step<Z> step = _reducer.apply(_partialResult, t);
          switch (step.getType()) {
            case cont:
              _partialResult = step.getValue();
              if (_streamingComplete && _elementsCompleted == _totalElements) {
                result.done(_partialResult);
                _partialResult = null;
              }
              break;
            case done:
              result.done(step.getValue());
              _partialResult = null;
              _streamingComplete = true;
              break;
          }
        } catch (Throwable e) {
          _streamingComplete = true;
          _partialResult = null;
          result.fail(e);
        }
      }

      private void onNextTask(Task<T> task, Ack ack) {
          scheduleTask(new FunctionalTask<T, T>("step(" + _name + ")", task,
              (p, t) -> {
                try
                {
                  _elementsCompleted++;
                  if (!result.isDone()) {
                    if (p.isFailed()) {
                      _streamingComplete = true;
                      _partialResult = null;
                      result.fail(p.getError());
                      ack.ack(FlowControl.done);
                    } else {
                      onNextValue(new AckValue<T>(p.get(), ack));
                    }
                  } else {
                    //result is resolved, just ack() the task
                    ack.ack(FlowControl.done);
                  }
                } finally {
                  //propagate result
                  if (p.isFailed()) {
                    t.fail(p.getError());
                  } else {
                    t.done(p.get());
                  }
                }
              }), context, that);
      }

      /**
       * It is expected that onNext method is called
       * from within Task's run method.
       */
      @Override
      public void onNext(final AckValue<TaskOrValue<T>> taskOrValueWrapped) {
        if (!_streamingComplete) {  //TODO questionable: when is _streamingComplete set?
          final TaskOrValue<T> taskOrValue = taskOrValueWrapped.get();
          if (taskOrValue.isTask()) {
            onNextTask(taskOrValue.getTask(), taskOrValueWrapped.getAck());
          } else {
            _elementsCompleted++;
            onNextValue(new AckValue<T>(taskOrValueWrapped.get().getValue(), taskOrValueWrapped.getAck()));
          }
        } else {
          taskOrValueWrapped.ack(FlowControl.done);
        }
      }

      @Override
      public void onComplete(int totalTasks) {
        _streamingComplete = true;
        _totalElements = totalTasks;
        if (_elementsCompleted == _totalElements) {
          result.done(_partialResult);
          _partialResult = null;
        }
      }

      @Override
      public void onError(Throwable cause) {
        _streamingComplete = true;
        if (!result.isDone()) {
          result.fail(cause);
        }
      }

      @Override
      public void onSubscribe(Subscription subscription) {
        // TODO handle subscription cancellation

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

  void scheduleTask(Task<T> task, Context context, Task<Z> rootTask) {
    context.runSubTask(task, (Task<Object>) rootTask);
  }

}
