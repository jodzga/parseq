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
import com.linkedin.parseq.stream.StreamCollection;
import com.linkedin.parseq.task.Task;

/**
 * @author Jaroslaw Odzga (jodzga@linkedin.com)
 */
public class SyncCollectionFlatMapExample extends AbstractExample
{
  public static void main(String[] args) throws Exception
  {
    new SyncCollectionFlatMapExample().runExample();
  }

  @Override
  protected void doRunExample(final Engine engine) throws Exception
  {
    final MockService<String> httpClient = getService();

    List<String> urls = Arrays.asList("http://www.linkedin.com", "http://www.google.com", "http://www.twitter.com");
    List<String> paths = Arrays.asList("/p1", "/p2");


    Task<String> task = Collections.fromIterable(urls)
      .flatMap(base -> (StreamCollection<?, String>)Collections.fromIterable(paths)
          .map(path -> base + path)
          .mapTask(url -> fetchUrl(httpClient, url)))
      .take(3)
      .reduce((a, b) -> a + "\n" + b)
      .andThen(System.out::println);

    engine.run(task);

    task.await();

    ExampleUtil.printTracingResults(task);

  }
}