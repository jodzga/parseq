package com.linkedin.parseq;

import java.util.Optional;

import com.linkedin.parseq.internal.SystemHiddenTask;
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
public abstract class BaseFoldTask<B, T> extends SystemHiddenTask<B> {

  abstract void scheduleTask(Task<T> task, Context context, Task<B> rootTask);

  protected Publisher<Task<T>> _tasks;
  private boolean _streamingComplete = false;
  private int _totalTasks;
  private int _tasksCompleted = 0;
  private B _partialResult;
  private final Reducer<B, T> _reducer;
  private final Optional<Task<?>> _predecessor;


  public BaseFoldTask(final String name, final Publisher<Task<T>> tasks, final B zero,
      final Reducer<B, T> reducer, Optional<Task<?>> predecessor) {
    super(name);
    _partialResult = zero;
    _reducer = reducer;
    _tasks = tasks;
    _predecessor = predecessor;
  }

  //TODO: when result is resolved, then tasks should be early finished, not started

  @Override
  protected Promise<? extends B> run(final Context context) throws Exception
  {
    final SettablePromise<B> result = Promises.settable();
    final Task<B> that = this;

    _tasks.subscribe(new Subscriber<Task<T>>() {
      @Override
      public void onNext(final AckValue<Task<T>> task) {
        if (!_streamingComplete) { //don't schedule tasks if streaming has finished e.g. by onError()
          task.get().onResolve(p -> {
            _tasksCompleted++;
            if (!result.isDone()) {
              if (p.isFailed()) {
                _streamingComplete = true;
                _partialResult = null;
                result.fail(p.getError());
                task.ack();
              } else {
                try {  //TODO who calls ack()?
                  Step<B> step = _reducer.apply(_partialResult,
                      new AckValueImpl<T>(p.get(), task.getAck()));
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
                } catch (Throwable t) {
                  _streamingComplete = true;
                  _partialResult = null;
                  result.fail(t);
                }
              }
            } else {
              task.ack();
            }
          });
          scheduleTask(task.get(), context, that);
        } else {
          task.ack();
        }
      }

      @Override
      public void onComplete(int totalTasks) {
        _streamingComplete = true;
        _totalTasks = totalTasks;
        //TODO check if this can be resolved
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
}
