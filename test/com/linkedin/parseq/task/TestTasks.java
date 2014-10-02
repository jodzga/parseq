/*
 * Copyright 2012 LinkedIn, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.linkedin.parseq.task;

import static com.linkedin.parseq.TestUtil.value;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.testng.annotations.Test;

import com.linkedin.parseq.TestUtil;
import com.linkedin.parseq.engine.BaseEngineTest;
import com.linkedin.parseq.promise.Promise;
import com.linkedin.parseq.promise.PromiseListener;
import com.linkedin.parseq.promise.Promises;
import com.linkedin.parseq.promise.SettablePromise;
import com.linkedin.parseq.task.BaseTask;
import com.linkedin.parseq.task.Context;
import com.linkedin.parseq.task.Priority;
import com.linkedin.parseq.task.Task;
import com.linkedin.parseq.task.Tasks;
import com.linkedin.parseq.task.ThrowableCallable;

/**
 * @author Chris Pettitt (cpettitt@linkedin.com)
 * @author Chi Chan (ckchan@linkedin.com)
 */
public class TestTasks extends BaseEngineTest
{

  @Test
  public void testTaskThatThrows() throws InterruptedException
  {
    final Exception error = new Exception();

    final Task<?> task = new BaseTask<Object>()
    {
      @Override
      protected Promise<Object> run(final Context context) throws Exception
      {
        throw error;
      }
    };

    getEngine().run(task);

    assertTrue(task.await(5, TimeUnit.SECONDS));

    assertTrue(task.isFailed());
    assertEquals(error, task.getError());
  }

  @Test
  public void testAwait() throws InterruptedException
  {
    final String value = "value";
    final Task<String> task = value("value", value);
    final AtomicReference<Boolean> resultRef = new AtomicReference<Boolean>(false);

    task.addListener(new PromiseListener<String>()
    {
      @Override
      public void onResolved(Promise<String> stringPromise)
      {
        try
        {
          Thread.sleep(100);
        } catch (InterruptedException e)
        {
          //ignore
        } finally
        {
          resultRef.set(true);
        }
      }
    });

    getEngine().run(task);
    assertTrue(task.await(5, TimeUnit.SECONDS));
    assertEquals(Boolean.TRUE, resultRef.get());
  }

  @Test
  public void testTaskWithTimeout() throws InterruptedException
  {
    // This task will not complete on its own, which allows us to test the timeout
    final Task<String> timeoutTask = new BaseTask<String>("task")
    {
      @Override
      protected Promise<? extends String> run(
          final Context context) throws Exception
      {
        return Promises.settable();
      }
    }.withTimeout(200, TimeUnit.MILLISECONDS);

    getEngine().run(timeoutTask);

    assertTrue(timeoutTask.await(5, TimeUnit.SECONDS));

    assertTrue(timeoutTask.isFailed());
    assertTrue(timeoutTask.getError() instanceof TimeoutException);

    assertTrue(timeoutTask.await(5, TimeUnit.SECONDS));
  }

  @Test
  public void testWithoutTimeout() throws InterruptedException
  {
    final String value = "value";
    final Task<String> timeoutTask = Tasks.callable("task", new Callable<String>()
    {
      @Override
      public String call() throws Exception
      {
        return value;
      }
    }).withTimeout(200, TimeUnit.MILLISECONDS);

    getEngine().run(timeoutTask);

    assertTrue(timeoutTask.await(5, TimeUnit.SECONDS));

    assertEquals(value, timeoutTask.get());
  }

  @Test
  public void testTimeoutTaskWithError() throws InterruptedException
  {
    final Exception error = new Exception();
    final Task<String> timeoutTask = Tasks.callable("task", new Callable<String>()
    {
      @Override
      public String call() throws Exception
      {
        throw error;
      }
    }).withTimeout(200, TimeUnit.MILLISECONDS);

    getEngine().run(timeoutTask);

    // We should complete with an error almost immediately
    assertTrue(timeoutTask.await(100, TimeUnit.MILLISECONDS));

    assertTrue(timeoutTask.isFailed());
    assertEquals(error, timeoutTask.getError());
  }


  private Task<?> testManyTimeoutTaskWithoutTimeoutOnAQueeuRound(ScheduledExecutorService scheduler) throws InterruptedException, IOException
  {
    final String value = "value";
    List<Task<String>> tasks = new ArrayList<Task<String>>();
    for (int i = 0; i < 50; i++) {

      // Task which simulates doing something for 0.5ms and setting response
      // asynchronously after 5ms.
      Task<String> t = new BaseTask<String>("test") {
        @Override
        protected Promise<? extends String> run(Context context) throws Throwable {
          final SettablePromise<String> result = Promises.settable();
          Thread.sleep(0, 500000);
          scheduler.schedule(new Runnable() {
            @Override
            public void run() {
              result.done(value);
            }
          }, 5, TimeUnit.MILLISECONDS);
          return result;
        }
      }.withTimeout(50, TimeUnit.MILLISECONDS);
      tasks.add(t);
    }

    // final task runs all the tasks in parallel
    final Task<?> timeoutTask = Tasks.par(tasks);

    getEngine().run(timeoutTask);

    assertTrue(timeoutTask.await(5, TimeUnit.SECONDS));

    return timeoutTask;

  }

  /**
   * Test scenario in which there are many TimeoutWithErrorTasks scheduled for execution
   * e.g. by using Tasks.par().
   */
  @Test
  public void testManyTimeoutTaskWithoutTimeoutOnAQueeu() throws InterruptedException, IOException
  {
    final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    /**
     * warm up lambdas
     * this test is time sensitive, if it is executed in isolation or as a first test in a suite
     * then jvm is cold and it may take even 50ms to warm it up (including resolving all lambdas)
     */
    testManyTimeoutTaskWithoutTimeoutOnAQueeuRound(scheduler);

    Task<?> timeoutTask = testManyTimeoutTaskWithoutTimeoutOnAQueeuRound(scheduler);

    scheduler.shutdown();

    //tasks should not time out
    assertEquals(false, timeoutTask.isFailed());
  }

  @Test
  public void testSetPriorityBelowMinValue()
  {
    try
    {
      TestUtil.noop().setPriority(Priority.MIN_PRIORITY - 1);
      fail("Should have thrown IllegalArgumentException");
    }
    catch (IllegalArgumentException e)
    {
      // Expected case
    }
  }

  @Test
  public void testSetPriorityAboveMaxValue()
  {
    try
    {
      TestUtil.noop().setPriority(Priority.MAX_PRIORITY + 1);
      fail("Should have thrown IllegalArgumentException");
    }
    catch (IllegalArgumentException e)
    {
      // Expected case
    }
  }

  @Test
  public void testThrowableCallableNoError() throws InterruptedException
  {
    final Integer magic = 0x5f3759df;
    final ThrowableCallable<Integer> callable = new ThrowableCallable<Integer>()
    {
      @Override
      public Integer call() throws Throwable
      {
        return magic;
      }
    };
    final Task<Integer> task = Tasks.callable("magic", callable);

    getEngine().run(task);
    task.await(100, TimeUnit.MILLISECONDS);

    assertTrue(task.isDone());
    assertFalse(task.isFailed());
    assertEquals(magic, task.get());
    assertEquals("magic", task.getName());
  }

  @Test
  public void testThrowableCallableWithError() throws InterruptedException
  {
    final Throwable throwable = new Throwable();
    final ThrowableCallable<Integer> callable = new ThrowableCallable<Integer>()
    {
      @Override
      public Integer call() throws Throwable
      {
        throw throwable;
      }
    };
    final Task<Integer> task = Tasks.callable("error", callable);

    getEngine().run(task);
    task.await(100, TimeUnit.MILLISECONDS);

    assertTrue(task.isDone());
    assertTrue(task.isFailed());
    assertEquals("Throwable should not be wrapped", throwable, task.getError());
    assertEquals("error", task.getName());
  }
}

