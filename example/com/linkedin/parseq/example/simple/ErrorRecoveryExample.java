/* $Id$ */
package com.linkedin.parseq.example.simple;

import com.linkedin.parseq.engine.Engine;
import com.linkedin.parseq.example.common.AbstractExample;
import com.linkedin.parseq.example.common.ExampleUtil;
import com.linkedin.parseq.example.common.MockService;
import com.linkedin.parseq.task.Task;

import java.util.concurrent.Callable;

import static com.linkedin.parseq.example.common.ExampleUtil.fetch404Url;
import static com.linkedin.parseq.task.Tasks.callable;
import static com.linkedin.parseq.task.Tasks.seq;

/**
 * @author Chris Pettitt (cpettitt@linkedin.com)
 */
public class ErrorRecoveryExample extends AbstractExample
{
  public static void main(String[] args) throws Exception
  {
    new ErrorRecoveryExample().runExample();
  }

  @Override
  protected void doRunExample(final Engine engine) throws Exception
  {
    final MockService<String> httpClient = getService();

    final Task<String> fetch = fetch404Url(httpClient, "http://www.google.com");
    final Task<Integer> length = callable("length", new Callable<Integer>()
    {
      @Override
      public Integer call()
      {
        return fetch.getOrDefault("").length();
      }
    });

    final Task<Integer> fetchAndLength = seq(fetch, length);

    engine.run(fetchAndLength);

    fetchAndLength.await();

    System.out.println("Response length: " + fetchAndLength.get());

    ExampleUtil.printTracingResults(fetchAndLength);
  }
}
