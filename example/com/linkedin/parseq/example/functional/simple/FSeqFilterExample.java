/* $Id$ */
package com.linkedin.parseq.example.functional.simple;

import static com.linkedin.parseq.example.common.ExampleUtil.fetchUrl;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.linkedin.parseq.Engine;
import com.linkedin.parseq.Task;
import com.linkedin.parseq.Tasks;
import com.linkedin.parseq.example.common.AbstractExample;
import com.linkedin.parseq.example.common.ExampleUtil;
import com.linkedin.parseq.example.common.MockService;

/**
 * @author Jaroslaw Odzga (jodzga@linkedin.com)
 */
public class FSeqFilterExample extends AbstractExample
{
  public static void main(String[] args) throws Exception
  {
    new FSeqFilterExample().runExample();
  }

  @Override
  protected void doRunExample(final Engine engine) throws Exception
  {
    final MockService<String> httpClient = getService();
    List<String> urls = Arrays.asList("http://www.linkedin.com", "http://www.google.com", "http://www.twitter.com");

    Task<Optional<String>> find =
        Tasks.syncCall(urls)
          .flatMapTaskSeq(url -> fetchUrl(httpClient, url)
                                  .withTimeout(100, TimeUnit.MILLISECONDS)
                                  .recover("default", t -> ""))
            .filter(s -> s.contains("google"))
            .find(s -> s.contains("google"));

    engine.run(find);

    find.await();

    System.out.println("found: " + find.get());

    ExampleUtil.printTracingResults(find);
  }

}
