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

import java.util.concurrent.Callable;

/**
 * This class provides a set of factory methods for create common
 * {@link Task}s.
 *
 * @author Chris Pettitt (cpettitt@linkedin.com)
 * @author Chi Chan (ckchan@linkedin.com)
 */
public class Tasks
{
  private Tasks() {}

  /**
   * TODO
   * Czy te wszystkie sa jeszcze relevant???
   * Wyglada na to, ze nie, bo mozna po prostu uzyc kombinatorow na tasku
   */


  /**
   * Creates a new {@link Task} that have a value of type Void. Because the
   * returned task returns no value, it is typically used to produce side-effects.
   *
   * @param name a name that describes the action
   * @param runnable the action that will be executed when the task is run
   * @return the new task
   */
  public static Task<Void> action(final String name, final Runnable runnable)
  {
    return new ActionTask(name, runnable);
  }



  /**
   * Creates a new {@link Task} that's value will be set to the value returned
   * from the supplied callable. This task is useful when doing basic
   * computation that does not require asynchrony. It is not appropriate for
   * long running or blocking tasks.
   *
   * @param name a name that describes the action
   * @param callable the callable to execute when this task is run
   * @param <T> the type of the return value for this task
   * @return the new task
   */
  public static <T> Task<T> callable(final String name, final Callable<? extends T> callable) {
    return new CallableTask<T>(name, callable);
  }

  public static <T1, T2> Par2Task<T1, T2> par(final Task<T1> task1,
                                              final Task<T2> task2) {
    return new Par2Task<T1, T2>("par(" + task1.getName() + ", " +
                                         task2.getName() + ")", task1,
                                                                task2);
  }

  public static <T1, T2, T3> Par3Task<T1, T2, T3> par(final Task<T1> task1,
                                                      final Task<T2> task2,
                                                      final Task<T3> task3) {
    return new Par3Task<T1, T2, T3>("par(" + task1.getName() + ", " +
                                             task2.getName() + ", " +
                                             task3.getName() + ")", task1,
                                                                    task2,
                                                                    task3);
  }

}