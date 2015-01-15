package com.linkedin.parseq.internal.stream;

import static com.linkedin.parseq.function.Tuples.tuple;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.function.Function;
import java.util.function.Predicate;

import com.linkedin.parseq.function.Tuple2;



/**
 * A Publisher is a source of elements of a given type. One or more {@link Subscriber Subscriber} may be connected
 * to this Publisher in order to receive the published elements, contingent on availability of these
 * elements as well as the presence of demand signaled by the Subscriber via {@link Subscription#requestMore(int) requestMore}.
 */
public interface Publisher<T> {

  /**
   * Subscribe the given {@link Subscriber Subscriber} to this Publisher. A Subscriber can at most be subscribed once
   * to a given Publisher, and to at most one Publisher in total.
   * @param subscriber The subscriber to register with this publisher.
   */
  void subscribe(Subscriber<T> subscriber);


  default Publisher<T> withSubscriber(final Subscriber<T> embeddedSubscriber) {
    final Publisher<T> that = this;
    return new Publisher<T>() {
      @Override
      public void subscribe(final Subscriber<T> subscriber) {
        that.subscribe(embeddedSubscriber.with(subscriber));
      }
    };
  }

  default Tuple2<Publisher<T>, Publisher<T>> fork() {
    final SubscribingPublisher<T> p1 = new SubscribingPublisher<T>();
    return tuple(p1, this.withSubscriber(p1));
  }

  default <R> Publisher<R> map(final Function<T, R> f) {
    final Publisher<T> that = this;
    return new Publisher<R>() {
      @Override
      public void subscribe(final Subscriber<R> subscriber) {
        that.subscribe(new Subscriber<T>() {

          @Override
          public void onNext(final AckValue<T> element) {
            subscriber.onNext(element.map(f));
          }

          @Override
          public void onComplete(final int totalTasks) {
            subscriber.onComplete(totalTasks);
          }

          @Override
          public void onError(Throwable cause) {
            subscriber.onError(cause);
          }
        });
      }
    };
  }

  default Publisher<T> filter(final Predicate<T> predicate) {
    final Publisher<T> that = this;
    return new Publisher<T>() {
      private int _skipped = 0;
      @Override
      public void subscribe(final Subscriber<T> subscriber) {
        that.subscribe(new Subscriber<T>() {

          @Override
          public void onNext(final AckValue<T> element) {
            if (predicate.test(element.get())) {
              subscriber.onNext(element);
            } else {
              _skipped++;
              element.ack();
            }
          }

          @Override
          public void onComplete(final int totalTasks) {
            subscriber.onComplete(totalTasks - _skipped);
          }

          @Override
          public void onError(Throwable cause) {
            subscriber.onError(cause);
          }
        });
      }
    };
  }

  default <R> Publisher<Tuple2<R, Publisher<T>>> groupBy(final Function<T, R> classifier) {
    final Publisher<T> that = this;
    return new Publisher<Tuple2<R, Publisher<T>>>() {
      private int groupCount = 0;

      @Override
      public void subscribe(final Subscriber<Tuple2<R, Publisher<T>>> subscriber) {

        final Map<R, PushablePublisher<T>> publishers = new HashMap<R, PushablePublisher<T>>();

        that.subscribe(new Subscriber<T>() {

          @Override
          public void onNext(final AckValue<T> element) {
            //TODO add try/catch to all those methods
            final R group = classifier.apply(element.get());
            PushablePublisher<T> pub = publishers.get(group);
            if (pub == null) {
              pub = new PushablePublisher<T>();
              publishers.put(group, pub);
              subscriber.onNext(new AckValue<>(tuple(group, pub), Ack.NO_OP));
              groupCount++;
            }
            pub.next(element);
          }

          @Override
          public void onComplete(final int totalTasks) {
            subscriber.onComplete(groupCount);
            for (PushablePublisher<T> pub: publishers.values()) {
              pub.complete();
            }
          }

          @Override
          public void onError(Throwable cause) {
            subscriber.onError(cause);
            for (PushablePublisher<T> pub: publishers.values()) {
              pub.error(cause);
            }
          }

        });
      }

    };
  }

  default <R> Publisher<R> flatMap(final Function<T, Publisher<R>> f) {
    final Publisher<T> that = this;
    return new Publisher<R>() {
      private Queue<Publisher<R>> _publishers = new ArrayDeque<Publisher<R>>();
      boolean _sourceDone = false;
      int _count = 0;

      @Override
      public void subscribe(final Subscriber<R> subscriber) {
        that.subscribe(new Subscriber<T>() {

          /**
           * Subscribe to one of received publishers.
           *
           * TODO probably don't need to create new subscriber every time
           */
          private void subscribe(Publisher<R> publisher, Ack ack) {
            publisher.subscribe(new Subscriber<R>() {

              @Override
              public void onComplete(int totalTasks) {
                _count += totalTasks;
                if (_sourceDone) {
                  _publishers = null;
                  subscriber.onComplete(_count);
                  ack.ack();
                }
              }

              @Override
              public void onError(Throwable cause) {
                subscriber.onError(cause);
                ack.ack();
              }

              @Override
              public void onNext(AckValue<R> element) {
                subscriber.onNext(element);
              }
            });
          }

          @Override
          public void onComplete(int totalTasks) {
            // source publisher has finished
            _sourceDone = true;
            if (_publishers.size() == 0) {
              _publishers = null;
              subscriber.onComplete(_count);
            }
          }

          @Override
          public void onError(Throwable cause) {
            // source publisher has failed
            _sourceDone = true;
            _publishers = null;
            subscriber.onError(cause);
          }

          @Override
          public void onNext(AckValue<T> element) {
            Publisher<R> publisher = f.apply(element.get());
            _publishers.add(publisher);
            subscribe(publisher, element.getAck());
          }
        });
      }
    };
  }

}