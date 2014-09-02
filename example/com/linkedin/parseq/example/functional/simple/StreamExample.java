/* $Id$ */
package com.linkedin.parseq.example.functional.simple;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.linkedin.parseq.Engine;
import com.linkedin.parseq.Task;
import com.linkedin.parseq.Tasks;
import com.linkedin.parseq.example.common.AbstractExample;
import com.linkedin.parseq.example.common.ExampleUtil;
import com.linkedin.parseq.example.common.MockService;

import static com.linkedin.parseq.example.common.ExampleUtil.fetchUrl;

/**
 * @author Jaroslaw Odzga (jodzga@linkedin.com)
 */
public class StreamExample extends AbstractExample
{
  public static void main(String[] args) throws Exception
  {
    new StreamExample().runExample();
  }

  @Override
  protected void doRunExample(final Engine engine) throws Exception
  {
    final MockService<String> httpClient = getService();
    List<String> urls = Arrays.asList("http://www.linkedin.com", "http://www.google.com", "http://www.twitter.com");

    /**
     * Calculate average response length of a list of urls.
     * All fetches are done in parallel with specified timeout
     * and fall-back default value.
     */
    Task<Double> fetchSizes =
      urls.stream()
        .map(url ->
              fetchUrl(httpClient, url)
                 .within(100, TimeUnit.MILLISECONDS)
                 .recover("default", t -> "")
                 .<Integer>map("length", s -> s.length()))
        .collect(Tasks.toPar())
        .map("average", list -> list.stream().mapToInt(x -> x).average().getAsDouble());

    engine.run(fetchSizes);

    fetchSizes.await();

    System.out.println("Response length: " + fetchSizes.get());

    ExampleUtil.printTracingResults(fetchSizes);
  }
}
