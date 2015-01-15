/* $Id$ */
package com.linkedin.parseq.example.collections;

import static com.linkedin.parseq.example.common.ExampleUtil.fetchUrl;

import java.util.Arrays;
import java.util.List;

import com.linkedin.parseq.collection.Collections;
import com.linkedin.parseq.engine.Engine;
import com.linkedin.parseq.example.common.AbstractExample;
import com.linkedin.parseq.example.common.ExampleUtil;
import com.linkedin.parseq.example.common.MockService;
import com.linkedin.parseq.task.Task;

/**
 * @author Jaroslaw Odzga (jodzga@linkedin.com)
 */
public class GroupByExample extends AbstractExample
{
  public static void main(String[] args) throws Exception
  {
    new GroupByExample().runExample();
  }

  @Override
  protected void doRunExample(final Engine engine) throws Exception
  {
    final MockService<String> httpClient = getService();
    List<String> urls = Arrays.asList("http://www.linkedin.com", "http://www.google.com", "http://www.twitter.com",
        "http://www.linkedin.com", "http://www.google.com", "http://www.linkedin.com");

    Task<String> result =
        Collections.fromIterable(urls)
          .par(url -> fetchUrl(httpClient, url))
          .groupBy(i -> i)
          .mapTask(group ->
                    (Task<String>)group._2()
                      .count()
                      .map(cnt -> "group: " + group._1() + ", count: " + cnt))
          .reduce((a, b) -> a + "\n" + b );

    engine.run(result);

    result.await();

    System.out.println(result.get());

    ExampleUtil.printTracingResults(result);
  }
}
