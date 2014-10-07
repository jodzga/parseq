package com.linkedin.parseq.task;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.testng.annotations.Test;

import com.linkedin.parseq.collection.Collections;
import com.linkedin.parseq.engine.BaseEngineTest;
import com.linkedin.parseq.engine.Engine;
import com.linkedin.parseq.engine.EngineBuilder;

/**
 * @author wfender
 * @version $Revision:$
 */
public class TestAsyncCallableTask extends BaseEngineTest
{
  @Test
  public void testConcurrentTasks() throws InterruptedException
  {
    // This test ensures that a single plan can run more than one blocking task
    // at a time if the AsyncCallableTask feature is used.

    final int size = 2; // Degree of parallelism
    final CountDownLatch latch = new CountDownLatch(size);

    final List<Task<Void>> tasks = new ArrayList<Task<Void>>(size);
    for (int counter = 0; counter < size; counter++)
    {
      tasks.add(counter, new AsyncCallableTask<Void>(new Callable<Void>() {
        @Override
        public Void call() throws Exception
        {
          latch.countDown();
          if (!latch.await(5, TimeUnit.SECONDS))
          {
            throw new TimeoutException("Latch should have reached 0 before timeout");
          }
          return null;
        }
      }));
    }

    final Task<?> par = Collections.par(tasks).task();
    getEngine().run(par);

    assertTrue(par.await(5, TimeUnit.SECONDS));

    for (int counter = 0; counter < size; counter++)
    {
      assertTrue(tasks.get(counter).isDone());
      assertFalse(tasks.get(counter).isFailed());
    }
  }

  @Test
  public void testThrowingCallable() throws InterruptedException
  {
    // Ensures that if a callable wrapped in an AsyncCallableTask throws that
    // the wrapping task correctly reports the error state.
    final Error error = new Error();
    final Task<Void> task = new AsyncCallableTask<Void>(new Callable<Void>()
    {
      @Override
      public Void call() throws Exception
      {
        throw error;
      }
    });

    getEngine().run(task);

    assertTrue(task.await(5, TimeUnit.SECONDS));

    assertTrue(task.isDone());
    assertTrue(task.isFailed());
    assertEquals(error, task.getError());
  }

  @Test
  public void testTaskWithoutExecutor() throws InterruptedException
  {
    final int numCores = Runtime.getRuntime().availableProcessors();
    final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(numCores + 1);
    final Engine engine = new EngineBuilder()
        .setTaskExecutor(scheduler)
        .setTimerScheduler(scheduler)
        .build();

    try
    {
      final Task<Integer> task = new AsyncCallableTask<Integer>(new Callable<Integer>()
      {
        @Override
        public Integer call() throws Exception
        {
          return 1;
        }
      });
      engine.run(task);

      assertTrue(task.await(5, TimeUnit.SECONDS));

      assertTrue(task.isFailed());
      assertTrue(task.getError() instanceof IllegalStateException);
    }
    finally
    {
      engine.shutdown();
      engine.awaitTermination(1, TimeUnit.SECONDS);
      scheduler.shutdownNow();
    }
  }
}