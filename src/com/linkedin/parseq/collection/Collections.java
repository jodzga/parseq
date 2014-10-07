package com.linkedin.parseq.collection;

import java.util.Optional;

import com.linkedin.parseq.collection.async.ParCollection;
import com.linkedin.parseq.collection.async.SeqCollection;
import com.linkedin.parseq.collection.sync.SyncCollection;
import com.linkedin.parseq.internal.stream.IterablePublisher;
import com.linkedin.parseq.task.Task;

public class Collections {

  private Collections() {}

  public static <T> SeqCollection<T, T> seq(final Iterable<Task<T>> tasks)
  {
    return new SeqCollection<T, T>(x -> x, new IterablePublisher<>(tasks), Optional.empty());
  }

  /**
   * TODO allow the following:
   *
   * List<AsyncCallableTask<Void>> list = new ...
   * Collections.par(list);
   */


  public static <T> ParCollection<T, T> par(final Iterable<Task<T>> tasks)
  {
    return new ParCollection<T, T>(x -> x, new IterablePublisher<>(tasks), Optional.empty());
  }

  public static <T> SyncCollection<T, T> fromIterable(final Iterable<T> input)
  {
    return new SyncCollection<T, T>(x -> x, new IterablePublisher<>(input));
  }


}
