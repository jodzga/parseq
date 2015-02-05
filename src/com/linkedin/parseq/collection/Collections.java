package com.linkedin.parseq.collection;

import java.util.Optional;

import com.linkedin.parseq.collection.async.ParCollection;
import com.linkedin.parseq.collection.async.SeqCollection;
import com.linkedin.parseq.collection.sync.SyncCollection;
import com.linkedin.parseq.stream.IterablePublisher;
import com.linkedin.parseq.stream.StreamCollection;
import com.linkedin.parseq.task.Task;
import com.linkedin.parseq.transducer.Transducer;

public class Collections {

  private Collections() {}

  public static <T> ParCollection<T, T> par(final Iterable<Task<T>> tasks)
  {
    return null;
//    return new ParCollection<T, T>(Transducer.identity(), new IterablePublisher<>(tasks).collection(), Optional.empty());
  }

  public static <T> StreamCollection<T, T> fromIterable(final Iterable<T> input)
  {
    return StreamCollection.fromIterable(input);
  }

}
