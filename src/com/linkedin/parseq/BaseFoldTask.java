package com.linkedin.parseq;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;

import com.linkedin.parseq.promise.Promise;
import com.linkedin.parseq.promise.Promises;
import com.linkedin.parseq.promise.SettablePromise;

/**
 * @author Jaroslaw Odzga (jodzga@linkedin.com)
 */
public abstract class BaseFoldTask<B, T> extends BaseTask<B> {

  abstract void scheduleTasks(List<Task<T>> tasks, Context context);

  private volatile List<Task<T>> _tasks;
  private B _partialResult;
  private final BiFunction<B, T, Step<B>> _op;
  private int _counter = 0;

  public BaseFoldTask(final String name, final Iterable<? extends Task<T>> tasks, final B zero, final BiFunction<B, T, Step<B>> op)
  {
    super(name);
    List<Task<T>> taskList = new ArrayList<Task<T>>();
    for(Task<T> task : tasks)
    {
      taskList.add(task);
    }

    if (taskList.size() == 0)
    {
      throw new IllegalArgumentException("No tasks to fold!");
    }

    _tasks = Collections.unmodifiableList(taskList);
    _partialResult = zero;
    _op = op;
  }

  @Override
  protected Promise<? extends B> run(final Context context) throws Exception
  {
    final SettablePromise<B> result = Promises.settable();

    _counter = _tasks.size();

    for(Task<T> task : _tasks)
    {
      task.onResolve(p -> {
        _counter--;
        if (!result.isDone()) {
          if (p.isFailed()) {
            result.fail(p.getError());
          } else {
            try {
              Step<B> step = _op.apply(_partialResult, p.get());
              switch (step.getType()) {
                case cont:
                  if (_counter > 0) {
                    _partialResult = step.getValue();
                  } else {
                    _partialResult = null;
                    result.done(step.getValue());
                  }
                  break;
                case done:
                  _partialResult = null;
                  result.done(step.getValue());
                  break;
                case stop:
                  result.done(_partialResult);
                  _partialResult = null;
                  break;
                case fail:
                  _partialResult = null;
                  result.fail(step.getError());
                  break;
              }
            } catch (Throwable t) {
              _partialResult = null;
              result.fail(t);
            }
          }
        }
      });
    }

    scheduleTasks(_tasks, context);

    _tasks = null;
    return result;
  }

  static class Step<S> {

    public enum Type { cont, done, fail, stop };

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
