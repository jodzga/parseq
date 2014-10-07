package com.linkedin.parseq.collection.async;

import java.util.concurrent.TimeUnit;

import com.linkedin.parseq.task.Task;

public interface FoldTask<T> extends Task<T> {
  FoldTask<T> within(final long time, final TimeUnit unit);
}
