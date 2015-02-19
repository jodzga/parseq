<#include "../../../../macros/macros.ftl">
<@pp.dropOutputFile />
<#list 3..max as i>
<@pp.changeOutputFile name="Consumer" + i + ".java" />
package com.linkedin.parseq.function;


@FunctionalInterface
public interface Consumer${i}<<@csv 1..i; j>T${j}</@csv>> {

    void accept(<@csv 1..i; j>T${j} t${j}</@csv>);

}
</#list>
