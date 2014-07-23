package com.linkedin.parseq;

import java.util.Optional;
import java.util.function.BiFunction;

import com.linkedin.parseq.internal.SystemHiddenTask;
import com.linkedin.parseq.promise.Promise;
import com.linkedin.parseq.promise.Promises;
import com.linkedin.parseq.promise.SettablePromise;
import com.linkedin.parseq.stream.Publisher;
import com.linkedin.parseq.stream.Subscriber;

/**
 * @author Jaroslaw Odzga (jodzga@linkedin.com)
 */
public abstract class BaseFoldTask<B, T> extends SystemHiddenTask<B> {

  abstract void scheduleNextTask(Task<T> task, Context context, Task<B> rootTask);
  abstract void publishNext();

  protected Publisher<Task<T>> _tasks;
  private boolean _streamingComplete = false;
  private int _totalTasks;
  private int _tasksCompleted = 0;
  private B _partialResult;
  private final BiFunction<B, T, Step<B>> _op;
  private final Optional<Task<?>> _predecessor;


  public BaseFoldTask(final String name, final Publisher<Task<T>> tasks, final B zero, final BiFunction<B, T, Step<B>> op,
      Optional<Task<?>> predecessor)
  {
    super(name);
    _partialResult = zero;
    _op = op;
    _tasks = tasks;
    _predecessor = predecessor;
  }

  @Override
  protected Promise<? extends B> run(final Context context) throws Exception
  {
    final SettablePromise<B> result = Promises.settable();
    final Task<B> that = this;

    _tasks.subscribe(new Subscriber<Task<T>>() {
      @Override
      public void onNext(Task<T> task) {
        if (!_streamingComplete) { //don't schedule tasks if streaming has finished e.g. by onError()
          task.onResolve(p -> {
            _tasksCompleted++;
            if (!result.isDone()) {
              if (p.isFailed()) {
                _streamingComplete = true;
                _partialResult = null;
                result.fail(p.getError());
              } else {
                try {
                  Step<B> step = _op.apply(_partialResult, p.get());
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
                    case stop:
                      result.done(_partialResult);
                      _partialResult = null;
                      _streamingComplete = true;
                      break;
                    case fail:
                      _streamingComplete = true;
                      _partialResult = null;
                      result.fail(step.getError());
                      break;
                    case ignore:
                      publishNext();
                      break;
                  }
                } catch (Throwable t) {
                  _streamingComplete = true;
                  _partialResult = null;
                  result.fail(t);
                }
              }
            }
          });
          scheduleNextTask(task, context, that);
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

    /**
     * This might be a bit counter intuitive, but we first need to subscribe to
     * stream of tasks and then run task, which is publisher of for that source.
     */
    if (_predecessor.isPresent()) {
      context.run(_predecessor.get());
    }

    _tasks = null;
    return result;
  }

  static class Step<S> {

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
