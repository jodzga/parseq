package com.linkedin.parseq.collection.async;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.linkedin.parseq.promise.Promise;
import com.linkedin.parseq.promise.Promises;
import com.linkedin.parseq.promise.SettablePromise;
import com.linkedin.parseq.stream.StreamCollection;
import com.linkedin.parseq.stream.Subscription;
import com.linkedin.parseq.task.BaseTask;
import com.linkedin.parseq.task.Context;
import com.linkedin.parseq.task.FusionTask;
import com.linkedin.parseq.task.Priority;
import com.linkedin.parseq.task.Task;
import com.linkedin.parseq.task.Tasks;
import com.linkedin.parseq.transducer.FlowControl;
import com.linkedin.parseq.transducer.Reducer;
import com.linkedin.parseq.transducer.Reducer.Step;

/**
 * @author Jaroslaw Odzga (jodzga@linkedin.com)
 */
public abstract class BaseFoldTask<B, T> extends BaseTask<B> {

  abstract void scheduleTask(Task<T> task, Context context, Task<B> rootTask);

  protected StreamCollection<?, Task<T>> _tasks;
  private boolean _streamingComplete = false;
  private int _totalTasks;
  private int _tasksCompleted = 0;
  private B _partialResult;
  private final Reducer<B, T> _reducer;
  private final Optional<Task<?>> _predecessor;
  private final String _name;


  public BaseFoldTask(final String name, final StreamCollection<?, Task<T>> tasks, final B zero,
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
//    final Task<B> that = this; //TODO ???
//
//    _tasks.subscribe(new AckingSubscriber<Task<T>>() {
//      /**
//       * It is expected that onNext method is called
//       * from within Task's run method.
//       */
//      @Override
//      public void onNext(final AckValue<Task<T>> task) {
//        if (!_streamingComplete) {  //TODO questionable: when is _streamingComplete set?
//          scheduleTask(new FunctionalTask<T, T>("step(" + _name + ")", task.get(),
//              (p, t) -> {
//                try
//                {
//                  _tasksCompleted++;
//                  if (!result.isDone()) {
//                    if (p.isFailed()) {
//                      _streamingComplete = true;
//                      _partialResult = null;
//                      result.fail(p.getError());
//                      task.ack(FlowControl.done);
//                    } else {
//                      try {
//                        //ack() is called by reducer
//                        Step<B> step = _reducer.apply(_partialResult, new AckValue<T>(p.get(), task.getAck()));
//                        switch (step.getType()) {
//                          case cont:
//                            _partialResult = step.getValue();
//                            if (_streamingComplete && _tasksCompleted == _totalTasks) {
//                              result.done(_partialResult);
//                              _partialResult = null;
//                            }
//                            break;
//                          case done:
//                            result.done(step.getValue());
//                            _partialResult = null;
//                            _streamingComplete = true;
//                            break;
//                        }
//                      } catch (Throwable e) {
//                        _streamingComplete = true;
//                        _partialResult = null;
//                        result.fail(e);
//                      }
//                    }
//                  } else {
//                    //result is resolved, just ack() the task
//                    task.ack(FlowControl.done);
//                  }
//                } finally {
//                  //propagate result
//                  if (p.isFailed()) {
//                    t.fail(p.getError());
//                  } else {
//                    t.done(p.get());
//                  }
//                }
//              } ), context, that);
//        } else {
//          task.ack(FlowControl.done);
//        }
//      }
//
//      @Override
//      public void onComplete(int totalTasks) {
//        _streamingComplete = true;
//        _totalTasks = totalTasks;
//        if (_tasksCompleted == _totalTasks) {
//          result.done(_partialResult);
//          _partialResult = null;
//        }
//      }
//
//      @Override
//      public void onError(Throwable cause) {
//        _streamingComplete = true;
//        if (!result.isDone()) {
//          result.fail(cause);
//        }
//      }
//
//      @Override
//      public void onSubscribe(Subscription subscription) {
//        // TODO handle subscription cancellation
//
//      }
//    });
//
//    if (_predecessor.isPresent()) {
//      context.run(_predecessor.get());
//    }
//
//    _tasks = null;
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
  public Task<B> within(long time, TimeUnit unit) {
    wrapContextRun(new WithinContextRunWrapper(time, unit));
    return this;
  }

}
