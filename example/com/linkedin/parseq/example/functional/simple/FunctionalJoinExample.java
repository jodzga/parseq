/* $Id$ */
package com.linkedin.parseq.example.functional.simple;

import static com.linkedin.parseq.example.common.ExampleUtil.*;

import java.util.concurrent.TimeUnit;

import com.linkedin.parseq.Engine;
import com.linkedin.parseq.Task;
import com.linkedin.parseq.example.common.AbstractExample;
import com.linkedin.parseq.example.common.ExampleUtil;
import com.linkedin.parseq.example.common.MockService;

/**
 * @author Jaroslaw Odzga (jodzga@linkedin.com)
 */
public class FunctionalJoinExample extends AbstractExample
{
  public static void main(String[] args) throws Exception
  {
    new FunctionalJoinExample().runExample();
  }

  @Override
  protected void doRunExample(final Engine engine) throws Exception
  {
    final MockService<String> httpClient = getService();

    final Task<String> fetchString =
        fetchUrl(httpClient, "http://www.first.com", 100)
          .within(500, TimeUnit.MILLISECONDS)
          .recover("default", t -> "");

    final Task<Integer> fetchInt =
        fetchUrl(httpClient, "http://www.second.com", 100)
          .within(50, TimeUnit.MILLISECONDS)
          .recover("default", t -> "")
          .map("length", s -> s.length());

    final Task<String> combined = fetchString.join(fetchInt, (s, x) -> "combined: " + s + ", " + x);

    //add side effect e.g. println
    final Task<?> action = combined.andThen("print", System.out::println);

    engine.run(action);

    action.await();

    ExampleUtil.printTracingResults(action);
  }
}
