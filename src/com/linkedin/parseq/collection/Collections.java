package com.linkedin.parseq.collection;

import java.util.Optional;

import com.linkedin.parseq.collection.async.ParCollection;
import com.linkedin.parseq.collection.async.SeqCollection;
import com.linkedin.parseq.collection.sync.SyncCollection;
import com.linkedin.parseq.stream.IterablePublisher;
import com.linkedin.parseq.task.Task;
import com.linkedin.parseq.transducer.Transducer;

public class Collections {

  private Collections() {}

  public static <T> SeqCollection<T, T> seq(final Iterable<Task<T>> tasks)
  {
    return new SeqCollection<T, T>(Transducer.identity(), new IterablePublisher<>(tasks).collection(), Optional.empty());
  }

  public static <T> ParCollection<T, T> par(final Iterable<Task<T>> tasks)
  {
    return new ParCollection<T, T>(Transducer.identity(), new IterablePublisher<>(tasks).collection(), Optional.empty());
  }

  public static <T> SyncCollection<T, T> fromIterable(final Iterable<T> input)
  {
    return new SyncCollection<T, T>(Transducer.identity(), input);
  }

}
