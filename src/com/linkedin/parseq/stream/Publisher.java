package com.linkedin.parseq.stream;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.function.Function;



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
          private void subscribe(Publisher<R> publisher, Runnable ack) {
            publisher.subscribe(new Subscriber<R>() {

              @Override
              public void onComplete(int totalTasks) {
                _count += totalTasks;
                if (_sourceDone) {
                  _publishers = null;
                  subscriber.onComplete(_count);
                  ack.run();
                }
              }

              @Override
              public void onError(Throwable cause) {
                ack.run();
                subscriber.onError(cause);
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