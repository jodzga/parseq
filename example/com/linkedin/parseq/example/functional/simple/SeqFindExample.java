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

    List<Task<String>> fetchSizes =
      urls.stream()
        .map(url ->
              fetchUrl(httpClient, url)
                 .within("100ms", 100, TimeUnit.MILLISECONDS)
                 .recover("default", t -> ""))
        .collect(Collectors.toList());

    Task<Optional<Integer>> positiveLength = Tasks.seqColl(fetchSizes).map("length", s -> s.length()).find("positive length", x -> x > 0);

    engine.run(positiveLength);

    positiveLength.await();

    System.out.println("Positive length: " + positiveLength.get());

    ExampleUtil.printTracingResults(positiveLength);
  }
}
