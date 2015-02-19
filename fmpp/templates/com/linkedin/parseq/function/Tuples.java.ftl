<#include "../../../../macros/macros.ftl">
<@pp.dropOutputFile />
<@pp.changeOutputFile name="Tuples.java" />
package com.linkedin.parseq.function;

public class Tuples {
  private Tuples() {}

<@lines 2..max; i>  public static <<@csv 1..i; j>T${j}</@csv>> Tuple${i}<<@csv 1..i; j>T${j}</@csv>> tuple(<@csv 1..i; j>final T${j} t${j}</@csv>) {
    return new Tuple${i}<<@csv 1..i; j>T${j}</@csv>>(<@csv 1..i; j>t${j}</@csv>);
  }
</@lines>

}
