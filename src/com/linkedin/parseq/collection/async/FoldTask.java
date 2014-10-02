package com.linkedin.parseq.collection.async;

import java.util.concurrent.TimeUnit;

public interface FoldTask<T> {
  FoldTask<T> within(final long time, final TimeUnit unit);
}
