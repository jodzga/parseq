package com.linkedin.parseq.stream;

import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import com.linkedin.parseq.task.Task;
import com.linkedin.parseq.task.TaskOrValue;
import com.linkedin.parseq.task.Tasks;
import com.linkedin.parseq.transducer.Foldable;
import com.linkedin.parseq.transducer.Transducer;
import com.linkedin.parseq.transducer.Transducible;
import com.linkedin.parseq.transducer.Reducer.Step;

//TODO implement Publisher interface for streaming
public class StreamCollection<T, R> extends Transducible<T, R> {

  private final Publisher<TaskOrValue<T>> _source;
  private final Optional<Task<?>> _predecessor;

  protected StreamCollection(Publisher<TaskOrValue<T>> source, Transducer<T, R> transducer, Optional<Task<?>> predecessor) {
    super(transducer);
    _source = source;
    _predecessor = predecessor;
  }

  protected <Z> Foldable<Z, T, Task<Z>> foldable() {
    return new StreamFoldable<Z, T>(_source, _predecessor);
  }

  private <A, B> StreamCollection<A, B> createStreamCollection(Publisher<TaskOrValue<A>> source, Transducer<A, B> transducer) {
    return new StreamCollection<A, B>(source, transducer, _predecessor);
  }

  private <B> StreamCollection<T, B> create(Transducer<T, B> transducer) {
    return createStreamCollection(_source, transducer);
  }

  public <A> StreamCollection<T, A> map(final Function<R, A> f) {
    return map(f, this::create);
  }

  public StreamCollection<T, R> forEach(final Consumer<R> consumer) {
    return forEach(consumer, this::create);
  }

  public StreamCollection<T, R> filter(final Predicate<R> predicate) {
    return filter(predicate, this::create);
  }

  public StreamCollection<T, R> take(final int n) {
    return take(n, this::create);
  }

  public StreamCollection<T, R> takeWhile(final Predicate<R> predicate) {
    return takeWhile(predicate, this::create);
  }

  public StreamCollection<T, R> drop(final int n) {
    return drop(n, this::create);
  }

  public StreamCollection<T, R> dropWhile(final Predicate<R> predicate) {
    return dropWhile(predicate, this::create);
  }

  /*
   * Foldings:
   */

  public <Z> Task<Z> fold(final Z zero, final BiFunction<Z, R, Z> op) {
    return fold(zero, op, foldable());
  }

  public Task<R> first() {
    return checkEmptyAsync(first(foldable()));
  }

  public Task<R> last() {
    return checkEmptyAsync(last(foldable()));
  }

  public Task<List<R>> all() {
    return all(foldable());
  }

  public Task<R> reduce(final BiFunction<R, R, R> op) {
    return checkEmptyAsync(reduce(op, foldable()));
  }

  public Task<R> find(final Predicate<R> predicate) {
    return checkEmptyAsync(find(predicate, foldable()));
  }

  private final TaskOrValue<Step<Object>> CONTINUE = TaskOrValue.value(Step.cont(Optional.empty()));
  private final Runnable NO_ACTION = () -> {};

  private Task<Void> terminalTask() {
    return Tasks.action("done", NO_ACTION);
  }

  //Used only for side-effects
  public Task<Void> task() {
    return foldable().fold(Optional.empty(), transduce((z, r) -> {
      return CONTINUE;
    })).andThen(terminalTask());
  }

  public <A> StreamCollection<T, A> mapTask(final Function<R, Task<A>> f) {
    return create(_transducer.map(r -> r.mapTask(f)));
  }

  public PublisherTask<R> stream() {
    //TODO
    return null;

//    Task<?> subscribe = Tasks.action("subscribe", () -> subscriber.onSubscribe(subscription));
//    Task<?> foldTask = fold(Optional.empty(), (z, e) -> {
//      subscriber.onNext(e);
//      return z;
//    });
//    foldTask.addListener(p -> {
//      if (p.isFailed()) {
//        subscriber.onError(p.getError());
//      } else {
//        subscriber.onComplete(0); //TODO remove counter
//      }
//    });
//    return subscribe.andThen(foldTask.andThen(terminalTask()));
  }

  private PublisherTask<TaskOrValue<R>> streamImpl() {
    //TODO
    return null;

//    Task<?> subscribe = Tasks.action("subscribe", () -> subscriber.onSubscribe(subscription));
//    Task<?> foldTask = fold(Optional.empty(), (z, e) -> {
//      subscriber.onNext(e);
//      return z;
//    });
//    foldTask.addListener(p -> {
//      if (p.isFailed()) {
//        subscriber.onError(p.getError());
//      } else {
//        subscriber.onComplete(0); //TODO remove counter
//      }
//    });
//    return subscribe.andThen(foldTask.andThen(terminalTask()));
  }


  public <A> StreamCollection<?, A> flatMap(final Function<R, StreamCollection<?, A>> f) {
    PublisherTask<PublisherTask<TaskOrValue<A>>> publishersTask =
        mapTask(f.andThen(collection -> collection.streamImpl())).stream();
    return new StreamCollection<A, A>(Publisher.flatten(publishersTask), Transducer.identity(),
        Optional.of(publishersTask));
  }

//private <A> Task<?> publisherTask(final PushablePublisher<A> pushable,
//final Function<R, A> f,
//final CancellableSubscription subscription) {
//final Task<?> fold = foldable().fold(Optional.empty(), transduce((z, ackR) -> {
////TODO verify that cancellation semantics is consistent across all collection types and operations
//if (subscription.isCancelled()) {
//  return Step.done(z);
//} else {
//  pushable.next(ackR.map(r -> f.apply(r)));
//  return Step.cont(z);
//}
//}));
//fold.onResolve(p -> {
//if (p.isFailed()) {
//  pushable.error(p.getError());
//} else {
//  pushable.complete();
//}
//});
//return fold;
//}

  public <A> StreamCollection<GroupedStreamCollection<A, R, R>, GroupedStreamCollection<A, R, R>> groupBy(final Function<R, A> classifier) {
    return null;
//    final Publisher<R> that = this;
//    return new Publisher<GroupedStreamCollection<A, R, R>>() {
//      private int groupCount = 0;
//
//      @Override
//      public void subscribe(final AckingSubscriber<GroupedStreamCollection<A, R, R>> subscriber) {
//
//        final Map<A, PushablePublisher<R>> publishers = new HashMap<A, PushablePublisher<R>>();
//        final Set<A> calcelledGroups = new HashSet<A>();
//
//        that.subscribe(new AckingSubscriber<R>() {
//
//          @Override
//          public void onNext(final AckValue<R> element) {
//            /**
//             * TODO
//             * Update documentation about ack: it is not a mechanism for backpressure:
//             * - is backpressure relevant problem for a processing finite streams?
//             * - ack is used to provide Seq semantics
//             *
//             * add try/catch to all those methods
//             */
//            final A group = classifier.apply(element.get());
//            if (calcelledGroups.contains(group)) {
//              element.ack(FlowControl.cont);
//            } else {
//              PushablePublisher<R> pub = publishers.get(group);
//              if (pub == null) {
//                final CancellableSubscription subscription = new CancellableSubscription();
//                pub = new PushablePublisher<R>(() -> {
//                  subscription.cancel();
//                  calcelledGroups.add(group);
//                });
//                publishers.put(group, pub);
//                subscriber.onNext(new AckValue<>(new GroupedStreamCollection<A, R, R>(group, pub, Transducer.identity()), Ack.NO_OP));
//                groupCount++;
//              }
//              //at this point subscription might have been already cancelled
//              if (!calcelledGroups.contains(group)) {
//                pub.next(element);
//              }
//            }
//          }
//
//          @Override
//          public void onComplete(final int totalTasks) {
//            subscriber.onComplete(groupCount);
//            for (PushablePublisher<R> pub: publishers.values()) {
//              pub.complete();
//            }
//          }
//
//          @Override
//          public void onError(Throwable cause) {
//            subscriber.onError(cause);
//            for (PushablePublisher<R> pub: publishers.values()) {
//              pub.error(cause);
//            }
//          }
//
//          @Override
//          public void onSubscribe(Subscription subscription) {
//            //we would be able to cancel stream if all groups cancelled their streams
//            //unfortunately we can't cancel stream because we don't know
//            //what elements are coming in the stream so we don't know list of all groups
//          }
//
//        });
//      }
//    }.collection();
  }

  protected static final <R> Task<R> checkEmptyAsync(Task<Optional<R>> result) {
    return result.map("checkEmpty", Transducible::checkEmpty);
  }

  public static <A> StreamCollection<A, A> fromIterable(final Iterable<A> iterable) {
    IterablePublisher<A> publisher = new IterablePublisher<A>(iterable);
    return new StreamCollection<A, A>(publisher, Transducer.identity(),
        Optional.of(Tasks.action("iterator", publisher::run)));
  }

}