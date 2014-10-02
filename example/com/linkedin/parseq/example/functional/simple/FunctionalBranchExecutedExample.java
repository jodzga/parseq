/* $Id$ */
package com.linkedin.parseq.example.functional.simple;

import static com.linkedin.parseq.example.common.ExampleUtil.callService;
import static com.linkedin.parseq.example.common.ExampleUtil.printTracingResults;

import java.util.concurrent.Callable;

import com.linkedin.parseq.engine.Engine;
import com.linkedin.parseq.example.common.AbstractExample;
import com.linkedin.parseq.example.common.MockService;
import com.linkedin.parseq.example.common.SimpleMockRequest;
import com.linkedin.parseq.task.Task;
import com.linkedin.parseq.task.Tasks;

/**
 * @author Jaroslaw Odzga (jodzga@linkedin.com)
 */
public class FunctionalBranchExecutedExample extends AbstractExample
{
  public static void main(String[] args) throws Exception
  {
    new FunctionalBranchExecutedExample().runExample();
  }

  @Override
  protected void doRunExample(final Engine engine) throws Exception
  {
    final MockService<Integer> serviceX = getService();
    final Task<Integer> fetchX = fetchX(serviceX, 24);
    
    final Task<Integer> bigX = fetchX.flatMap("make x >= 42", x -> {
      if (x < 42) {
        final int toAdd = 42 - x;
        return add(x, toAdd);
      } else {
        return fetchX;
      }
    });

    engine.run(bigX);

    bigX.await();

    System.out.println("Resulting value: " + bigX.get());

    printTracingResults(bigX);
  }

  private static Task<Integer> add(final int x, final int toAdd)
  {
    return Tasks.callable("add " + toAdd, (Callable<Integer>)() -> x + toAdd);
  }

  private Task<Integer> fetchX(final MockService<Integer> serviceX,
                                      final int x)
  {
    return callService("fetch x (x := " + x + ")", serviceX, new SimpleMockRequest<Integer>(10, x));
  }
}
