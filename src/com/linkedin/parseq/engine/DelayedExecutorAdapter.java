package com.linkedin.parseq.engine;

import com.linkedin.parseq.engine.DelayedExecutor;
import com.linkedin.parseq.internal.CancellableScheduledFuture;
import com.linkedin.parseq.task.Cancellable;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Adapts a {@link ScheduledExecutorService} to the simpler
 * {@link DelayedExecutor} interface.
 *
 * @author Chris Pettitt
 */
public class DelayedExecutorAdapter implements DelayedExecutor
{
  private final ScheduledExecutorService _scheduler;

  public DelayedExecutorAdapter(final ScheduledExecutorService scheduler)
  {
    _scheduler = scheduler;
  }

  @Override
  public Cancellable schedule(final long delay, final TimeUnit unit,
                              final Runnable command)
  {
    return new CancellableScheduledFuture(_scheduler.schedule(command, delay, unit));
  }
}
