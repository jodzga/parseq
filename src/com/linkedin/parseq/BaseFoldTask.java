package com.linkedin.parseq;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.linkedin.parseq.promise.Promise;
import com.linkedin.parseq.promise.Promises;
import com.linkedin.parseq.promise.SettablePromise;
import com.linkedin.parseq.stream.AckValue;
import com.linkedin.parseq.stream.AckValueImpl;
import com.linkedin.parseq.stream.Publisher;
import com.linkedin.parseq.stream.Subscriber;
import com.linkedin.parseq.transducer.Reducer;
import com.linkedin.parseq.transducer.Reducer.Step;

/**
 * @author Jaroslaw Odzga (jodzga@linkedin.com)
 */
public abstract class BaseFoldTask<B, T> extends BaseTask<B> implements FoldTask<B> {

  abstract void scheduleTask(Task<T> task, Context context, Task<B> rootTask);

  protected Publisher<Task<T>> _tasks;
  private boolean _streamingComplete = false;
  private int _totalTasks;
  private int _tasksCompleted = 0;
  private B _partialResult;
  private final Reducer<B, T> _reducer;
  private final Optional<Task<?>> _predecessor;
  private final String _name;


  public BaseFoldTask(final String name, final Publisher<Task<T>> tasks, final B zero,
      final Reducer<B, T> reducer, Optional<Task<?>> predecessor) {
    super(name);
    _partialResult = zero;
    _reducer = reducer;
    _tasks = tasks;
    _predecessor = predecessor;
    _name = name;
  }

  //TODO: when result is resolved, then tasks should be early finished, not started?

  @Override
  protected Promise<? extends B> run(final Context context) throws Exception
  {
    final SettablePromise<B> result = Promises.settable();
    final Task<B> that = this;

    _tasks.subscribe(new Subscriber<Task<T>>() {
      /**
       * It is expected that onNext method is called
       * from within Task's run method.
       */
      @Override
      public void onNext(final AckValue<Task<T>> task) {
        if (!_streamingComplete) {
          scheduleTask(new FunctionalTask<T, T>("step(" + _name + ")", task.get(),
              (p, t) -> {
                try
                {
                  _tasksCompleted++;
                  if (!result.isDone()) {
                    if (p.isFailed()) {
                      _streamingComplete = true;
                      _partialResult = null;
                      result.fail(p.getError());
                      task.ack();
                    } else {
                      try {
                        //ack() is called by reducer
                        Step<B> step = _reducer.apply(_partialResult, new AckValueImpl<T>(p.get(), task.getAck()));
                        switch (step.getType()) {
                          case cont:
                            _partialResult = step.getValue();
                            if (_streamingComplete && _tasksCompleted == _totalTasks) {
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
                  } else {
                    //result is resolved, just ack() the task
                    task.ack();
                  }
                } finally {
                  //propagate result
                  if (p.isFailed()) {
                    t.fail(p.getError());
                  } else {
                    t.done(p.get());
                  }
                }
              } ), context, that);
        } else {
          task.ack();
        }
      }

      @Override
      public void onComplete(int totalTasks) {
        _streamingComplete = true;
        _totalTasks = totalTasks;
        if (_tasksCompleted == _totalTasks) {
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
    });

    if (_predecessor.isPresent()) {
      context.run(_predecessor.get());
    }

    _tasks = null;
    return result;
  }

  class WithinContextRunWrapper implements ContextRunWrapper<B> {

    protected final SettablePromise<B> _result = Promises.settable();
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
          _streamingComplete = true;
          _result.done(_partialResult);
        }
      });
      //within tasks should run as early as possible
      withinTask.setPriority(Priority.MAX_PRIORITY);
      context.createTimer(_time, _unit, withinTask);
    }

    @Override
    public Promise<B> after(Context context, Promise<B> promise) {
      promise.addListener(p -> {
        if (_committed.compareAndSet(false, true)) {
          Promises.propagateResult(promise, _result);
        }
      });
      return _result;
    }
  }

  @Override
  public FoldTask<B> within(long time, TimeUnit unit) {
    wrapContextRun(new WithinContextRunWrapper(time, unit));
    return this;
  }

}
