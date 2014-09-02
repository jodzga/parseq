package com.linkedin.parseq;

import java.util.Optional;

import com.linkedin.parseq.stream.Publisher;
import com.linkedin.parseq.stream.SeqPublisher;
import com.linkedin.parseq.transducer.Reducer;
import com.linkedin.parseq.transducer.Transducer;

/**
 * @author Jaroslaw Odzga (jodzga@linkedin.com)
 */
public class SeqTaskCollection<T, R> extends TaskCollection<T, R> {

  private SeqTaskCollection(Publisher<Task<T>> tasks, Transducer<T, R> transducer, Optional<Task<?>> predecessor) {
    super(new SeqPublisher<Task<T>>(tasks), transducer, predecessor);
  }

  public static <T> SeqTaskCollection<T, T> fromTasks(final Publisher<Task<T>> tasks) {
    return new SeqTaskCollection<T, T>(tasks, x -> x, Optional.empty());
  }

  @Override
  <A, B> TaskCollection<A, B> createCollection(Publisher<Task<A>> tasks, Transducer<A, B> transducer,
      Optional<Task<?>> predecessor) {
    return new SeqTaskCollection<A, B>(tasks, transducer, predecessor);
  }

  @Override
  <Z> Task<Z> createFoldTask(String name, Z zero, Reducer<Z, T> reducer, Optional<Task<?>> predecessor) {
    return new SeqFoldTask<Z, T>(name, _tasks, zero, reducer, predecessor);
  }

}
