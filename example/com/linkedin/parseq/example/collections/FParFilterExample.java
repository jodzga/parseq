/* $Id$ */
package com.linkedin.parseq.example.collections;

import static com.linkedin.parseq.example.common.ExampleUtil.fetchUrl;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.linkedin.parseq.collection.Collections;
import com.linkedin.parseq.engine.Engine;
import com.linkedin.parseq.example.common.AbstractExample;
import com.linkedin.parseq.example.common.ExampleUtil;
import com.linkedin.parseq.example.common.MockService;
import com.linkedin.parseq.task.Task;

/**
 * @author Jaroslaw Odzga (jodzga@linkedin.com)
 */
public class FParFilterExample extends AbstractExample
{
  public static void main(String[] args) throws Exception
  {
    new FParFilterExample().runExample();
  }

  @Override
  protected void doRunExample(final Engine engine) throws Exception
  {
    final MockService<String> httpClient = getService();
    List<String> urls = Arrays.asList("http://www.linkedin.com", "http://www.google.com", "http://www.twitter.com");

    Task<String> find =
        Collections.fromIterable(urls)
          .mapTask(url -> fetchUrl(httpClient, url)
                        .withTimeout(200, TimeUnit.MILLISECONDS)
                        .recover("default", t -> ""))
            .filter(s -> s.contains("google"))
            .find(s -> s.contains("google"));

    engine.run(find);

    find.await();

    System.out.println("found: " + !find.isFailed());

    ExampleUtil.printTracingResults(find);
  }
}
