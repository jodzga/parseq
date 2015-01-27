package com.linkedin.parseq.stream;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import com.linkedin.parseq.function.Tuple2;
import com.linkedin.parseq.promise.Promise;
import com.linkedin.parseq.promise.Promises;
import com.linkedin.parseq.promise.SettablePromise;
import com.linkedin.parseq.task.Exceptions;
import com.linkedin.parseq.transducer.FlowControl;
import com.linkedin.parseq.transducer.Foldable;
import com.linkedin.parseq.transducer.Reducer;
import com.linkedin.parseq.transducer.Transducer;
import com.linkedin.parseq.transducer.Transducible;
import com.linkedin.parseq.transducer.Reducer.Step;



public class StreamCollection<T, R> extends Transducible<T, R> implements Publisher<R>{

  private final Publisher<T> _source;

  public StreamCollection(Publisher<T> source, Transducer<T, R> transducer) {
    super(transducer);
    _source = source;
  }

  protected <Z> Foldable<Z, T, Promise<Z>> foldable() {
    return new PublisherFoldable<Z, T>(_source);
  }

  protected <A, B> StreamCollection<A, B> createStreamCollection(Publisher<A> source, Transducer<A, B> transducer) {
    return new StreamCollection<A, B>(source, transducer);
  }

  protected <B> StreamCollection<T, B> create(Transducer<T, B> transducer) {
    return createStreamCollection(_source, transducer);
  }

  @Override
  public void subscribe(final AckingSubscriber<R> subscriber) {
    //TODO handle cancellation
    Promise<Integer> foldPromise = notAckingFold(0, (z, rAck) -> {
      subscriber.onNext(rAck);
      return z + 1;
    }, foldable());
    foldPromise.onResolve(p -> {
      if (p.isDone()) {
        if (p.isFailed()) {
          subscriber.onError(p.getError());
        } else {
          subscriber.onComplete(p.get());
        }
      } else {
        subscriber.onError(Exceptions.noSuchElement());
      }
    });
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

  public <Z> Promise<Z> fold(final Z zero, final BiFunction<Z, R, Z> op) {
    return fold(zero, op, foldable());
  }

  public Promise<R> first() {
    return checkEmptyStream(first(foldable()));
  }

  public Promise<R> last() {
    return checkEmptyStream(last(foldable()));
  }

  public Promise<List<R>> all() {
    return all(foldable());
  }

  public Promise<R> reduce(final BiFunction<R, R, R> op) {
    return checkEmptyStream(reduce(op, foldable()));
  }

  public Promise<R> find(final Predicate<R> predicate) {
    return checkEmptyStream(find(predicate, foldable()));
  }

  public Promise<Integer> count() {
    final SettablePromise<Integer> r = Promises.settable();
    all().onResolve(p -> {
      if (p.isDone()) {
        if (p.isFailed()) {
          r.fail(p.getError());
        } else {
          r.done(p.get().size());
        }
      } else {
        r.fail(Exceptions.noSuchElement());
      }
    });
    return r;
  }


  public <R> StreamCollection<?, Tuple2<R, StreamCollection<?, T>>> groupBy(final Function<T, R> classifier) {
//    final Publisher<T> that = this;
//    return new Publisher<Tuple2<R, Publisher<T>>>() {
//      private int groupCount = 0;
//
//      @Override
//      public void subscribe(final AckingSubscriber<Tuple2<R, Publisher<T>>> subscriber) {
//
//        final Map<R, PushablePublisher<T>> publishers = new HashMap<R, PushablePublisher<T>>();
//
//        that.subscribe(new AckingSubscriber<T>() {
//
//          @Override
//          public void onNext(final AckValue<T> element) {
//            //TODO add try/catch to all those methods
//            final R group = classifier.apply(element.get());
//            PushablePublisher<T> pub = publishers.get(group);
//            if (pub == null) {
//              pub = new PushablePublisher<T>();
//              publishers.put(group, pub);
//              subscriber.onNext(new AckValue<>(tuple(group, pub), Ack.NO_OP));
//              groupCount++;
//            }
//            pub.next(element);
//          }
//
//          @Override
//          public void onComplete(final int totalTasks) {
//            subscriber.onComplete(groupCount);
//            for (PushablePublisher<T> pub: publishers.values()) {
//              pub.complete();
//            }
//          }
//
//          @Override
//          public void onError(Throwable cause) {
//            subscriber.onError(cause);
//            for (PushablePublisher<T> pub: publishers.values()) {
//              pub.error(cause);
//            }
//          }
//
//        });
//      }
//
//    };
    return null;
  }

  public <A> StreamCollection<?, A> flatMap(final Function<R, StreamCollection<?, A>> f) {
    final Publisher<R> that = this;
    return new Publisher<A>() {
      private AckingSubscriber<A> _subscriberOfFlatMappedA = null;
      private final Queue<StreamCollection<?, A>> _publishers = new ArrayDeque<StreamCollection<?, A>>();
      private final Map<StreamCollection<?, A>, Subscription> _subscriptions = new HashMap<StreamCollection<?, A>, Subscription>();
      boolean _sourceDone = false;
      int _count = 0;

      @Override
      public void subscribe(final AckingSubscriber<A> subscriberOfFlatMappedA) {
        _subscriberOfFlatMappedA = subscriberOfFlatMappedA;
        that.subscribe(new AckingSubscriber<R>() {

          /**
           * Subscribe to received publishers
           */
          private void subscribe(final StreamCollection<?, A> publisher, final Ack ack) {
            publisher.subscribe(new AckingSubscriber<A>() {

              @Override
              public void onComplete(int totalTasks) {
                try {
                  _count += totalTasks;
                  if (_sourceDone) {
                    _publishers.clear();
                    _subscriptions.clear();
                    subscriberOfFlatMappedA.onComplete(_count);
                  }
                } finally {
                  ack.ack(FlowControl.cont);
                }
              }

              @Override
              public void onError(Throwable cause) {
                try {
                  subscriberOfFlatMappedA.onError(cause);
                  cancelOtherPublishers();
                } finally {
                  ack.ack(FlowControl.done);
                }
              }

              private void cancelOtherPublishers() {
                for (Map.Entry<StreamCollection<?, A>, Subscription> entry: _subscriptions.entrySet()) {
                  if (!entry.getKey().equals(publisher)) {
                    entry.getValue().cancel();
                  }
                }
              }

              @Override
              public void onNext(AckValue<A> element) {
                subscriberOfFlatMappedA.onNext(new AckValue<A>(element.get(), element.getAck().andThen(flow -> {
                  if (flow == FlowControl.done) {
                    cancelOtherPublishers();
                  }
                })));
              }

              @Override
              public void onSubscribe(Subscription subscription) {
                _subscriptions.put(publisher, subscription);
              }
            });
          }

          @Override
          public void onComplete(int totalTasks) {
            // source publisher has finished
            _sourceDone = true;
            if (_publishers.size() == 0) {
              _publishers.clear();
              _subscriptions.clear();
              subscriberOfFlatMappedA.onComplete(_count);
            }
          }

          private void cancelAllSubscriptions() {
            for (Subscription s: _subscriptions.values()) {
              s.cancel();
            }
          }

          @Override
          public void onError(Throwable cause) {
            // source publisher has failed
            _sourceDone = true;
            cancelAllSubscriptions();
            _publishers.clear();
            _subscriptions.clear();
            subscriberOfFlatMappedA.onError(cause);
          }

          @Override
          public void onNext(AckValue<R> element) {
            StreamCollection<?, A> publisher = f.apply(element.get());
            _publishers.add(publisher);
            subscribe(publisher, element.getAck());
          }

          @Override
          public void onSubscribe(final Subscription subscription) {
            _subscriberOfFlatMappedA.onSubscribe(() -> {
              subscription.cancel();
              cancelAllSubscriptions();
              _publishers.clear();
              _subscriptions.clear();
            });

          }
        });
      }

    }.collection();
  }

  protected static <R> Promise<R> checkEmptyStream(Promise<Optional<R>> result) {
    final SettablePromise<R> r = Promises.settable();
    result.onResolve(p -> {
      if (p.isDone()) {
        if (p.isFailed()) {
          r.fail(Exceptions.noSuchElement(p.getError()));
        } else {
          Optional<R> res = p.get();
          if (res.isPresent()) {
            r.done(res.get());
          } else {
            r.fail(Exceptions.noSuchElement());
          }
        }
      } else {
        r.fail(Exceptions.noSuchElement());
      }
    });
    return r;
  }
}