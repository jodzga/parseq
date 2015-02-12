/* $Id$ */
package com.linkedin.parseq.example.domain;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.linkedin.parseq.collection.Collections;
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

  //mapping results
  //create summary for a person: "<first name> <last name>"
  Task<String> createSummary(int id) {
    return fetchPerson(id)
        .map(person -> person.getFirstName() + " " + person.getLastName());
  }

  //preferred way
  String shortSummary(Person person) {
    return person.getFirstName() + " " + person.getLastName();
  }

  //handles failures delivering degraded experience
  Task<String> createResilientSummary(int id) {
    return fetchPerson(id)
        .map(this::shortSummary)
        .recover(e -> "Member " + id);
  }

  //handles failures delivering degraded experience in timely fashion
  Task<String> createReactiveSummary(int id) {
    return fetchPerson(id)
        .withTimeout(100, TimeUnit.MILLISECONDS)
        .map(this::shortSummary)
        .recover(e -> "Member " + id);
  }

  /** Tasks composition */

  //create extended summary for a person: "<first name> <last name> working at <company name>"
  //chaining sequence of tasks
  Task<String> createExtendedSummary(int id) {
    return fetchPerson(id).flatMap(this::createExtendedSummary);
  }

  Task<String> createExtendedSummary(final Person person) {
    return fetchCompany(person.getCompanyId())
      .map(company -> shortSummary(person) + " working at " + company.getName());
  }

  //create mailbox summary for a person: "<first name> <last name> has <X> messages"
  Task<String> createMailboxSummary(int id) {
    return Tasks.par(createSummary(id), fetchMailbox(id))
        .map((summary, mailbox) -> summary + " has " + mailbox.size() + " messages");
  }

  /** Task collections */

  Task<String> createSummaries(List<Integer> ids) {
    return Collections.fromValues(ids)
        .mapTask(this::createReactiveSummary)
        .reduce((a, b) -> a + "\n" + b);
  }

  Task<String> createSummariesForConnections(Integer id) {
    return Collections.fromValue(id)
      .mapTask(this::fetchPerson)
      .flatMap(person ->
        Collections.fromValues(person.getConnections())
          .mapTask(this::fetchPerson))
      .mapTask(connection -> createExtendedSummary(connection))
      .reduce((a, b) -> a + "\n" + b)
      .within(300, TimeUnit.MILLISECONDS);
  }

  @Override
  protected void doRunExample(final Engine engine) throws Exception
  {
    //Task<String> task = createReactiveSummary(1);
    //Task<List<String>> task = createSummariesHavingTasks(fetchPersons(DB.personIds));

    Task<String> task = createSummariesForConnections(1);

    runTaskAndPrintResults(engine, task);
  }

  private void runTaskAndPrintResults(final Engine engine, Task<?> task) throws InterruptedException {
    Task<?> printRsults = task.andThen("println", System.out::println);
    engine.run(printRsults);
    printRsults.await();
    ExampleUtil.printTracingResults(printRsults);
  }

}
