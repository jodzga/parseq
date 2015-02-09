package com.linkedin.parseq.stream;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import com.linkedin.parseq.collection.GroupedAsyncCollection;
import com.linkedin.parseq.collection.ParSeqCollection;
import com.linkedin.parseq.task.Task;
import com.linkedin.parseq.task.TaskOrValue;
import com.linkedin.parseq.task.Tasks;
import com.linkedin.parseq.transducer.Foldable;
import com.linkedin.parseq.transducer.Reducer.Step;
import com.linkedin.parseq.transducer.Transducer;
import com.linkedin.parseq.transducer.Transducible;

//TODO implement Publisher interface for streaming
public class StreamCollection<T, R> extends Transducible<T, R> implements ParSeqCollection<R> {

  private final Publisher<TaskOrValue<T>> _source;
  private final Optional<Task<?>> _predecessor;

  protected StreamCollection(Publisher<TaskOrValue<T>> source, Transducer<T, R> transducer, Optional<Task<?>> predecessor) {
    super(transducer);
    _source = source;
    _predecessor = predecessor;
  }

  private <Z> Foldable<Z, T, Task<Z>> foldable() {
    return new StreamFoldable<Z, T>(_source, _predecessor);
  }

  private <A, B> StreamCollection<A, B> createStreamCollection(Publisher<TaskOrValue<A>> source, Transducer<A, B> transducer) {
    return new StreamCollection<A, B>(source, transducer, _predecessor);
  }

  private <B> StreamCollection<T, B> create(Transducer<T, B> transducer) {
    return createStreamCollection(_source, transducer);
  }

  @Override
  public <A> ParSeqCollection<A> map(final Function<R, A> f) {
    return map(f, this::create);
  }

  @Override
  public ParSeqCollection<R> forEach(final Consumer<R> consumer) {
    return forEach(consumer, this::create);
  }

  @Override
  public ParSeqCollection<R> filter(final Predicate<R> predicate) {
    return filter(predicate, this::create);
  }

  @Override
  public ParSeqCollection<R> take(final int n) {
    return take(n, this::create);
  }

  @Override
  public ParSeqCollection<R> takeWhile(final Predicate<R> predicate) {
    return takeWhile(predicate, this::create);
  }

  @Override
  public ParSeqCollection<R> drop(final int n) {
    return drop(n, this::create);
  }

  @Override
  public ParSeqCollection<R> dropWhile(final Predicate<R> predicate) {
    return dropWhile(predicate, this::create);
  }

  /*
   * Foldings:
   */

  @Override
  public <Z> Task<Z> fold(final Z zero, final BiFunction<Z, R, Z> op) {
    return fold(zero, op, foldable());
  }

  @Override
  public Task<R> first() {
    return checkEmptyAsync(first(foldable()));
  }

  @Override
  public Task<R> last() {
    return checkEmptyAsync(last(foldable()));
  }

  @Override
  public Task<List<R>> toList() {
    return toList(foldable());
  }

  @Override
  public Task<R> reduce(final BiFunction<R, R, R> op) {
    return checkEmptyAsync(reduce(op, foldable()));
  }

  @Override
  public Task<R> find(final Predicate<R> predicate) {
    return checkEmptyAsync(find(predicate, foldable()));
  }

  private final TaskOrValue<Step<Object>> CONTINUE = TaskOrValue.value(Step.cont(Optional.empty()));

  //Used only for side-effects
  @Override
  public Task<?> task() {
    return foldable().fold("task", Optional.empty(), transduce((z, r) -> r.map(rValue -> {
      return Step.cont(z);
    })));
  }

  @Override
  public <A> ParSeqCollection<A> mapTask(final Function<R, Task<A>> f) {
    return create(_transducer.map(r -> r.mapTask(f)));
  }

  @Override
  public <A> ParSeqCollection<A> flatMap(Function<R, ParSeqCollection<A>> f) {

    CancellableSubscription subscription = new CancellableSubscription();
    final PushablePublisher<Publisher<TaskOrValue<A>>> publisher = new PushablePublisher<Publisher<TaskOrValue<A>>>(subscription);

    @SuppressWarnings("unchecked")
    Task<?> publisherTask = mapTask(r -> {
      final PushablePublisher<TaskOrValue<A>> pushablePublisher = new PushablePublisher<TaskOrValue<A>>(subscription);
      publisher.next(pushablePublisher);
      return ((StreamCollection<?, A>)f.apply(r)).publisherTask(pushablePublisher, subscription);
    }).task();

    //TODO order of onResolve?
    publisherTask.onResolve(p -> {
      if (p.isFailed()) {
        publisher.error(p.getError());
      } else {
        publisher.complete();
      }
    });

    return new StreamCollection<A, A>(Publisher.flatten(publisher), Transducer.identity(), Optional.of(publisherTask));
  }

  protected Task<?> publisherTask(final PushablePublisher<TaskOrValue<R>> pushable,
      final CancellableSubscription subscription) {
    final Task<?> fold = foldable().fold("toStream", Optional.empty(), transduce((z, r) -> {
        if (subscription.isCancelled()) {
          return TaskOrValue.value(Step.done(z));
        } else {
          pushable.next(r);
          return CONTINUE;
        }
      }));
    fold.onResolve(p -> {
      if (p.isFailed()) {
        pushable.error(p.getError());
      } else {
        pushable.complete();
      }
    });
    return fold;
  }

  @Override
  public <K> ParSeqCollection<GroupedAsyncCollection<K, R>> groupBy(Function<R, K> classifier) {
//  final Publisher<R> that = this;
//  return new Publisher<GroupedStreamCollection<A, R, R>>() {
//    private int groupCount = 0;
//
//    @Override
//    public void subscribe(final AckingSubscriber<GroupedStreamCollection<A, R, R>> subscriber) {
//
//      final Map<A, PushablePublisher<R>> publishers = new HashMap<A, PushablePublisher<R>>();
//      final Set<A> calcelledGroups = new HashSet<A>();
//
//      that.subscribe(new AckingSubscriber<R>() {
//
//        @Override
//        public void onNext(final AckValue<R> element) {
//          /**
//           * TODO
//           * Update documentation about ack: it is not a mechanism for backpressure:
//           * - is backpressure relevant problem for a processing finite streams?
//           * - ack is used to provide Seq semantics
//           *
//           * add try/catch to all those methods
//           */
//          final A group = classifier.apply(element.get());
//          if (calcelledGroups.contains(group)) {
//            element.ack(FlowControl.cont);
//          } else {
//            PushablePublisher<R> pub = publishers.get(group);
//            if (pub == null) {
//              final CancellableSubscription subscription = new CancellableSubscription();
//              pub = new PushablePublisher<R>(() -> {
//                subscription.cancel();
//                calcelledGroups.add(group);
//              });
//              publishers.put(group, pub);
//              subscriber.onNext(new AckValue<>(new GroupedStreamCollection<A, R, R>(group, pub, Transducer.identity()), Ack.NO_OP));
//              groupCount++;
//            }
//            //at this point subscription might have been already cancelled
//            if (!calcelledGroups.contains(group)) {
//              pub.next(element);
//            }
//          }
//        }
//
//        @Override
//        public void onComplete(final int totalTasks) {
//          subscriber.onComplete(groupCount);
//          for (PushablePublisher<R> pub: publishers.values()) {
//            pub.complete();
//          }
//        }
//
//        @Override
//        public void onError(Throwable cause) {
//          subscriber.onError(cause);
//          for (PushablePublisher<R> pub: publishers.values()) {
//            pub.error(cause);
//          }
//        }
//
//        @Override
//        public void onSubscribe(Subscription subscription) {
//          //we would be able to cancel stream if all groups cancelled their streams
//          //unfortunately we can't cancel stream because we don't know
//          //what elements are coming in the stream so we don't know list of all groups
//        }
//
//      });
//    }
//  }.collection();
    return null;
  }

  protected static final <R> Task<R> checkEmptyAsync(Task<Optional<R>> result) {
    return result.map("checkEmpty", Transducible::checkEmpty);
  }

  public static <A> ParSeqCollection<A> fromValues(final Iterable<A> iterable) {
    IterablePublisher<A, A> publisher = new ValuesPublisher<A>(iterable);
    return new StreamCollection<>(publisher, Transducer.identity(),
        Optional.of(Tasks.action("values", publisher::run)));
  }

  public static <A> ParSeqCollection<A> fromTasks(final Iterable<Task<A>> iterable) {
    IterablePublisher<Task<A>, A> publisher = new TasksPublisher<A>(iterable);
    return new StreamCollection<>(publisher, Transducer.identity(),
        Optional.of(Tasks.action("tasks", publisher::run)));
  }

  @Override
  public ParSeqCollection<R> withSideEffect(Consumer<R> consumer) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Task<Integer> count() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ParSeqCollection<R> within(long time, TimeUnit unit) {
    CancellableSubscription subscription = new CancellableSubscription();
    final PushablePublisher<TaskOrValue<R>> publisher = new PushablePublisher<TaskOrValue<R>>(subscription);
    Task<?> task = publisherTask(publisher, subscription).within(time, unit);
    return new StreamCollection<>(publisher, Transducer.identity(), Optional.of(task));
  }

}