/* $Id$ */
package com.linkedin.parseq.example.functional.simple;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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

    List<Task<String>> fetchSizes = fetchList(httpClient, urls);

    Task<Optional<String>> find =
        Tasks.parColl(fetchSizes)
          .filter("google only", s -> s.contains("google"))
          .flatMap("flatMap", z -> Tasks.parColl(fetchList(httpClient, urls))
                .filter("linkedin", s -> s.contains("linkedin")))
          .find("find linkedin", s -> s.contains("linkedin"));

    engine.run(find);

    find.await();

    System.out.println("found: " + find.get());

    ExampleUtil.printTracingResults(find);
  }

  private List<Task<String>> fetchList(final MockService<String> httpClient, List<String> urls) {
    List<Task<String>> fetchSizes =
      urls.stream()
        .map(url ->
              fetchUrl(httpClient, url)
                 .within(200, TimeUnit.MILLISECONDS)
                 .recover("default", t -> ""))
        .collect(Collectors.toList());
    return fetchSizes;
  }

}
