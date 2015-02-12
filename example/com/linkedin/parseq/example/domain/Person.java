package com.linkedin.parseq.example.domain;

import java.util.List;

public class Person {

  final String _firstName;
  final String _lastName;
  final int _companyId;
  final List<Integer> _connections;

  public Person(String firstName, String lastName, int companyId, List<Integer> connections) {
    _firstName = firstName;
    _lastName = lastName;
    _companyId = companyId;
    _connections = connections;
  }

  public String getFirstName() {
    return _firstName;
  }

  public String getLastName() {
    return _lastName;
  }

  public int getCompanyId() {
    return _companyId;
  }

  public List<Integer> getConnections() {
    return _connections;
  }
}
