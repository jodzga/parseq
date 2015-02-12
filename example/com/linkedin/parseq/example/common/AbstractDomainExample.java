/*
 * Copyright 2012 LinkedIn, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.linkedin.parseq.example.common;

import java.util.List;

import com.linkedin.parseq.example.domain.Company;
import com.linkedin.parseq.example.domain.DB;
import com.linkedin.parseq.example.domain.Message;
import com.linkedin.parseq.example.domain.Person;
import com.linkedin.parseq.task.Task;


public abstract class AbstractDomainExample extends AbstractExample {

  protected MockService<Person> personService;
  protected MockService<Company> companyService;
  protected MockService<Message> messageService;
  protected MockService<List<Integer>> mailboxService;

  public Task<Person> fetchPerson(int id) {
    if (personService == null) {
      personService = getService();
    }
    return ExampleUtil.fetch("Person", personService, id, DB.personDB);
  }

  public Task<Company> fetchCompany(int id) {
    if (companyService == null) {
      companyService = getService();
    }
    return ExampleUtil.fetch("Comapny", companyService, id, DB.companyDB);
  }

  public Task<Message> fetchMessage(int id) {
    if (messageService == null) {
      messageService = getService();
    }
    return ExampleUtil.fetch("Message", messageService, id, DB.messageDB);
  }

  public Task<List<Integer>> fetchMailbox(int id) {
    if (mailboxService == null) {
      mailboxService = getService();
    }
    return ExampleUtil.fetch("Mailbox", mailboxService, id, DB.mailboxDB);
  }
}
