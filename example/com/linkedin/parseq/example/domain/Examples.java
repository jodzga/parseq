/* $Id$ */
package com.linkedin.parseq.example.domain;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.linkedin.parseq.collection.ParSeqCollection;
import com.linkedin.parseq.collection.ParSeqCollections;
import com.linkedin.parseq.engine.Engine;
import com.linkedin.parseq.example.common.AbstractDomainExample;
import com.linkedin.parseq.example.common.ExampleUtil;
import com.linkedin.parseq.task.Task;
import com.linkedin.parseq.task.Tasks;

/**
 * @author Jaroslaw Odzga (jodzga@linkedin.com)
 */
public class Examples extends AbstractDomainExample
{
  public static void main(String[] args) throws Exception
  {
    new Examples().runExample();
  }

  //---------------------------------------------------------------

  //create summary for a person: "<first name> <last name>"
  Task<String> createSummary(int id) {
    return null;
  }

  //---------------------------------------------------------------

  //handles failures delivering degraded experience
  Task<String> createResilientSummary(int id) {
    return null;
  }

  //---------------------------------------------------------------

  //handles failures delivering degraded experience in timely fashion
  Task<String> createResponsiveSummary(int id) {
    return null;
  }

  //---------------------------------------------------------------

  /** Tasks composition */

  //create extended summary for a person: "<first name> <last name> working at <company name>"
  Task<String> createExtendedSummary(int id) {
    return null;
  }

  //---------------------------------------------------------------

  //create mailbox summary for a person: "<first name> <last name> has <X> messages"
  Task<String> createMailboxSummary(int id) {
    return null;
  }

  //---------------------------------------------------------------

  /** Task collections */

  //---------------------------------------------------------------

  //create summary of connections
  //<first name> <last name> working at <company name>
  //<first name> <last name> working at <company name>
  //(...)
  Task<String> createSummariesOfConnections(Integer id) {
    return null;
  }

  //---------------------------------------------------------------

  //Find a message which contains given word
  Task<String> findMessageWithWord(String word) {
    return null;
  }

  //---------------------------------------------------------------

  @Override
  protected void doRunExample(final Engine engine) throws Exception {
    Task<String> task = null;

    runTaskAndPrintResults(engine, task);
  }

  private void runTaskAndPrintResults(final Engine engine, Task<?> task) throws InterruptedException {
    Task<?> printRsults = task.andThen("println", System.out::println);
    engine.run(printRsults);
    printRsults.await();
    ExampleUtil.printTracingResults(printRsults);
  }

}
