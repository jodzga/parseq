/* $Id$ */
package com.linkedin.parseq.example.collections;

import java.util.Arrays;
import java.util.List;

import javafx.scene.Group;

import com.linkedin.parseq.collection.ParSeqCollections;
import com.linkedin.parseq.engine.Engine;
import com.linkedin.parseq.example.common.AbstractExample;
import com.linkedin.parseq.task.Task;

/**
 * @author Jaroslaw Odzga (jodzga@linkedin.com)
 */
public class SyncGroupByExample extends AbstractExample
{
  public static void main(String[] args) throws Exception
  {
    new SyncGroupByExample().runExample();
  }

  @Override
  protected void doRunExample(final Engine engine) throws Exception
  {
    List<Integer> ints = Arrays.asList(1, 2, 3, 4, 5, 6, 2, 3, 5, 3);

    Task<String> task = null;
//        Collections.fromIterable(ints)
//        .groupBy(i -> i)
//        .mapTask(group ->
//            (Task<String>)group.count().map(count ->
//              "group: " + group.getKey() + ", count: " + count))
//        .reduce((a, b) -> a + "\n" + b );

    System.out.println(task);
  }
}
