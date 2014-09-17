package com.linkedin.parseq;

import java.util.concurrent.TimeUnit;

public interface FoldTask<T> {
  FoldTask<T> within(final long time, final TimeUnit unit);
}
