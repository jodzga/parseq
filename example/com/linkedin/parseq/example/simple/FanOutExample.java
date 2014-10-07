/* $Id$ */
package com.linkedin.parseq.example.simple;

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
 * @author Chris Pettitt (cpettitt@linkedin.com)
 * @author Jaroslaw Odzga (jodzga@linkedin.com)
 */
public class FanOutExample extends AbstractExample
{
  public static void main(String[] args) throws Exception
  {
    new FanOutExample().runExample();
  }

  @Override
  protected void doRunExample(final Engine engine) throws Exception
  {
    List<String> urls = Arrays.asList("http://www.bing.com",
                                      "http://www.yahoo.com",
                                      "http://www.google.com");

    final MockService<String> httpClient = getService();

    Task<?> parFetch = Collections.formIterable(urls)
        .par(url -> fetchUrl(httpClient, url))
        .forEach(System.out::println)
        .task();

    engine.run(parFetch);

    parFetch.await();

    ExampleUtil.printTracingResults(parFetch);
  }
}
