/* $Id$ */
package com.linkedin.parseq.example.functional.simple;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.linkedin.parseq.engine.Engine;
import com.linkedin.parseq.example.common.AbstractExample;
import com.linkedin.parseq.example.common.ExampleUtil;
import com.linkedin.parseq.example.common.MockService;
import com.linkedin.parseq.task.Collections;
import com.linkedin.parseq.task.Task;

import static com.linkedin.parseq.example.common.ExampleUtil.fetchUrl;

/**
 * @author Jaroslaw Odzga (jodzga@linkedin.com)
 */
public class SeqFindExample extends AbstractExample
{
  public static void main(String[] args) throws Exception
  {
    new SeqFindExample().runExample();
  }

  @Override
  protected void doRunExample(final Engine engine) throws Exception
  {
    final MockService<String> httpClient = getService();
    List<String> urls = Arrays.asList("http://www.linkedin.com", "http://www.google.com", "http://www.twitter.com");

    List<Task<String>> fetchSizes = fetchList(httpClient, urls);

    Task<String> find =
        Collections.seq(fetchSizes)
          .filter(s -> s.contains("google"))
          .mapTask(z -> {
            return  Collections.seq(fetchList(httpClient, urls))
                .find(s -> s.contains("linkedin"));
          }).first();

    engine.run(find);

    find.await();

    System.out.println("found: " + !find.isFailed());

    ExampleUtil.printTracingResults(find);
  }

  private List<Task<String>> fetchList(final MockService<String> httpClient, List<String> urls) {
    List<Task<String>> fetchSizes =
      urls.stream()
        .map(url ->
              fetchUrl(httpClient, url)
                 .withTimeout(200, TimeUnit.MILLISECONDS)
                 .recover("default", t -> ""))
        .collect(Collectors.toList());
    return fetchSizes;
  }

}
