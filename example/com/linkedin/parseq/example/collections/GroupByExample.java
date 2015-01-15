/* $Id$ */
package com.linkedin.parseq.example.collections;

import java.util.Arrays;
import java.util.List;

import com.linkedin.parseq.collection.Collections;
import com.linkedin.parseq.engine.Engine;
import com.linkedin.parseq.example.common.AbstractExample;
import com.linkedin.parseq.example.common.ExampleUtil;
import com.linkedin.parseq.task.Task;

/**
 * @author Jaroslaw Odzga (jodzga@linkedin.com)
 */
public class GroupByExample extends AbstractExample
{
  public static void main(String[] args) throws Exception
  {
    new GroupByExample().runExample();
  }

  @Override
  protected void doRunExample(final Engine engine) throws Exception
  {
//    List<Integer> ints = Arrays.asList(1, 2, 3, 4, 5, 6, 2, 3, 5, 3);
//
//    Task<?> task = Collections.fromIterable(ints)
//        .groupBy(i -> i)
//        .forEach(group -> {
//            System.out.println("group: " + group._1() + ": "
//              + group._2().map(i -> i.toString() + ", ").reduce((a, b) -> a + ", " + b));
//        })
//        .all();
//
//    engine.run(task);
//
//    task.await();
//
//    ExampleUtil.printTracingResults(task);

  }
}
