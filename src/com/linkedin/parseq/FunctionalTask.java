package com.linkedin.parseq;

import java.util.Collection;
import java.util.Set;
import java.util.function.Function;

import com.linkedin.parseq.internal.TaskLogger;
import com.linkedin.parseq.promise.DelegatingPromise;
import com.linkedin.parseq.promise.PromisePropagator;
import com.linkedin.parseq.promise.Promises;
import com.linkedin.parseq.promise.SettablePromise;
import com.linkedin.parseq.promise.TransformingPromiseListener;
import com.linkedin.parseq.trace.Related;
import com.linkedin.parseq.trace.ShallowTrace;
import com.linkedin.parseq.trace.Trace;

public class FunctionalTask<S, T>  extends DelegatingPromise<T> implements Task<T> {

  private final Task<S> _task;
  private PromisePropagator<S, T> _propagator;
  private final String _name;
  private final SettablePromise<T> _promise;

  public FunctionalTask(final String name, final Task<S> task, PromisePropagator<S, T> propagator) {
    this(name, task, propagator, Promises.<T>settable());
  }

  public FunctionalTask(final String name, final Task<S> task, PromisePropagator<S, T> propagator, SettablePromise<T> promise) {
    super(promise);
    _promise = promise;
    _task = task;
    _propagator = propagator;
    _name = name;
  }

  //TODO implement all other default methods from Task

  @Override
  public <R> Task<R> map(final String desc, final Function<T,R> f) {
    return new FunctionalTask<S, R>(desc + "(" + getName() + ")", _task, _propagator.map(f));
  }

  @Override
  public boolean cancel(Exception reason) {
    return _task.cancel(reason);
  }

  @Override
  public String getName() {
    return _name;
  }

  @Override
  public int getPriority() {
    return _task.getPriority();
  }

  @Override
  public boolean setPriority(int priority) {
    return _task.setPriority(priority);
  }

  @Override
  public void contextRun(Context context, TaskLogger taskLogger, Task<?> parent, Collection<Task<?>> predecessors) {
    if (!_promise.isDone()) {
      _task.addListener(new TransformingPromiseListener<S, T>(_promise, _propagator));
      _task.contextRun(context, taskLogger, parent, predecessors);
    }
  }

  @Override
  public ShallowTrace getShallowTrace() {
    return _task.getShallowTrace();
  }

  @Override
  public Trace getTrace() {
    //TODO
    return _task.getTrace();
  }

  @Override
  public Set<Related<Task<?>>> getRelationships() {
    //TODO
    return _task.getRelationships();
  }
}
