/* $Id$ */
package com.linkedin.parseq.example.collections;

import java.util.Arrays;
import java.util.List;

import com.linkedin.parseq.collection.Collections;
import com.linkedin.parseq.engine.Engine;
import com.linkedin.parseq.example.common.AbstractExample;
import com.linkedin.parseq.example.common.ExampleUtil;
import com.linkedin.parseq.task.Task;

/**
 * @author Jaroslaw Odzga (jodzga@linkedin.com)
 */
public class SyncCollectionExample extends AbstractExample
{
  public static void main(String[] args) throws Exception
  {
    new SyncCollectionExample().runExample();
  }

  @Override
  protected void doRunExample(final Engine engine) throws Exception
  {
    List<String> urls = Arrays.asList("http://www.linkedin.com", "http://www.google.com", "http://www.twitter.com");


    Task<String> task = Collections.fromIterable(urls)
      .reduce((a, b) -> a + ", " + b)
      .andThen(System.out::println);

    engine.run(task);

    task.await();

    ExampleUtil.printTracingResults(task);

  }
}
