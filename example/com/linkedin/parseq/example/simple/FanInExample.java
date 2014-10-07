/* $Id$ */
package com.linkedin.parseq.example.simple;

import com.linkedin.parseq.engine.Engine;
import com.linkedin.parseq.example.common.AbstractExample;
import com.linkedin.parseq.example.common.MockService;
import com.linkedin.parseq.task.Task;

import static com.linkedin.parseq.example.common.ExampleUtil.fetchUrl;
import static com.linkedin.parseq.example.common.ExampleUtil.printTracingResults;
import static com.linkedin.parseq.task.Tasks.*;

/**
 * @author Chris Pettitt (cpettitt@linkedin.com)
 * @author Jaroslaw Odzga (jodzga@linkedin.com)
 */
public class FanInExample extends AbstractExample
{
  public static void main(String[] args) throws Exception
  {
    new FanInExample().runExample();
  }

  @Override
  protected void doRunExample(final Engine engine) throws Exception
  {
    final MockService<String> httpClient = getService();

    final Task<String> fetchBing = fetchUrl(httpClient, "http://www.bing.com");
    final Task<String> fetchYahoo = fetchUrl(httpClient, "http://www.yahoo.com");
    final Task<String> fetchGoogle = fetchUrl(httpClient, "http://www.google.com");

    final Task<?> fanIn = par(fetchBing, fetchGoogle, fetchYahoo)
                            .andThen(tuple -> {
                              System.out.println("Bing   => " + tuple._1());
                              System.out.println("Yahoo  => " + tuple._2());
                              System.out.println("Google => " + tuple._3());
                            });
    engine.run(fanIn);

    fanIn.await();

    printTracingResults(fanIn);
  }
}
