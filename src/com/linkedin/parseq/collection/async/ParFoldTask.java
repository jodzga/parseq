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

package com.linkedin.parseq.collection.async;

import java.util.Optional;

import com.linkedin.parseq.stream.StreamCollection;
import com.linkedin.parseq.task.Context;
import com.linkedin.parseq.task.Task;
import com.linkedin.parseq.transducer.Reducer;

/**
 * @author Jaroslaw Odzga (jodzga@linkedin.com)
 */
public class ParFoldTask<B, T> extends BaseFoldTask<B, T>
{
  public ParFoldTask(String name, StreamCollection<?, Task<T>> tasks, B zero, Reducer<B, T> reducer,
      Optional<Task<?>> predecessor) {
    super(name, tasks, zero, reducer, predecessor);
  }

  @SuppressWarnings("unchecked")
  @Override
  void scheduleTask(Task<T> task, Context context, Task<B> rootTask) {
    context.runSubTask(task, (Task<Object>) rootTask);
  }

}
