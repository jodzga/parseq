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

package com.linkedin.parseq;

import java.util.Optional;
import java.util.function.BiFunction;

import com.linkedin.parseq.stream.Publisher;

/**
 * @author Jaroslaw Odzga (jodzga@linkedin.com)
 */
/* package private */ class SeqFoldTask<B, T> extends BaseFoldTask<B, T>
{
  private Task<T> _prevTask;

  public SeqFoldTask(final String name, final Publisher<Task<T>> tasks, final B zero, final BiFunction<B, T, Step<B>> op, Optional<Task<?>> predecessor)
  {
    super(name, tasks, zero, op, predecessor);
  }

  @SuppressWarnings("unchecked")
  @Override
  void scheduleNextTask(Task<T> task, Context context, Task<B> rootTask) {
    if (_prevTask != null) {
      //sequence tasks
      context.afterTask((Task<Object>)rootTask, _prevTask).run(task);
    } else {
      //run only the first task
      context.runSubTask(task, (Task<Object>)rootTask);
    }
    _prevTask = task;
  }

  @SuppressWarnings("rawtypes")
  @Override
  void publishNext() {
    if (_tasks instanceof SeqPublisher) {
      ((SeqPublisher)_tasks).publishNext();
    }
  }

}
