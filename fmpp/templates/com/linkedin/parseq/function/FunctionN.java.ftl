<#include "../../../../macros/macros.ftl">
<@pp.dropOutputFile />
<#list 3..max as i>
<@pp.changeOutputFile name="Function" + i + ".java" />
package com.linkedin.parseq.function;

import java.util.Objects;
import java.util.function.Function;

@FunctionalInterface
public interface Function${i}<<@csv 1..i; j>T${j}</@csv>, R> {

    R apply(<@csv 1..i; j>T${j} t${j}</@csv>);

    default <V> Function${i}<<@csv 1..i; j>T${j}</@csv>, V> map(Function<? super R, ? extends V> f) {
        Objects.requireNonNull(f);
        return (<@csv 1..i; j>T${j} t${j}</@csv>) ->
          f.apply(apply(<@csv 1..i; j>t${j}</@csv>));
    }

}
</#list>
