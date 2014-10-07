package com.linkedin.parseq.task;

import java.util.NoSuchElementException;

import com.linkedin.parseq.util.Objects;

public class TaskOrValue<T> {

  private final Task<T> _task;
  private final T _value;

  private TaskOrValue(Task<T> task, T value) {
    _task = task;
    _value = value;
  }

  public boolean isTask() {
    return _task != null;
  }

  public T getValue() {
    if (isTask()) {
      throw new NoSuchElementException();
    } else {
      return _value;
    }
  }

  public Task<T> getTask() {
    if (isTask()) {
      return _task;
    } else {
      throw new NoSuchElementException();
    }
  }

  public static <T> TaskOrValue<T> task(Task<T> task) {
    Objects.requireNonNull(task);
    return new TaskOrValue<T>(task, null);
  }

  public static <T> TaskOrValue<T> value(T value) {
    return new TaskOrValue<T>(null, value);
  }
}
