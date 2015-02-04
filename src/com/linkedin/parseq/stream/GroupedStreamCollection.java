package com.linkedin.parseq.stream;

import java.util.Optional;

import com.linkedin.parseq.task.Task;
import com.linkedin.parseq.task.TaskOrValue;
import com.linkedin.parseq.transducer.Transducer;

public class GroupedStreamCollection<K, T, R> extends StreamCollection<T, R> {

  private final K _key;

  public GroupedStreamCollection(K key, Publisher<TaskOrValue<T>> source, Transducer<T, R> transducer, Optional<Task<?>> predecessor) {
    super(source, transducer, predecessor);
    _key = key;
  }

  public K getKey() {
    return _key;
  }

}
