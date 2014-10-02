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

import com.linkedin.parseq.promise.Promise;
import com.linkedin.parseq.promise.Promises;

/**
 * A {@link Task} that runs a {@link Runnable} and returns no value. Use
 * {@link Tasks#action(String, Runnable)} to create instances of this class.
 *
 * @author Chris Pettitt (cpettitt@linkedin.com)
 */
/* package private */ class ActionTask extends BaseTask<Void>
{
  private final Runnable _runnable;

  public ActionTask(final String name, final Runnable runnable)
  {
    super(name);
    _runnable = runnable;
  }

  @Override
  protected Promise<Void> run(final Context context) throws Exception
  {
    _runnable.run();
    return Promises.value(null);
  }
}
