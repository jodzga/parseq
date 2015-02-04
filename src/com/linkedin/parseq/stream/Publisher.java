package com.linkedin.parseq.stream;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.function.Function;

import com.linkedin.parseq.transducer.FlowControl;


public interface Publisher<T> {

  void subscribe(AckingSubscriber<T> subscriber);

  default void subscribe(Subscriber<T> subscriber) {
    subscribe(AckingSubscriber.adopt(subscriber));
  }

  default <A> Publisher<A> flatMap(Function<T, Publisher<A>> f) {
      final Publisher<T> that = this;
      return new Publisher<A>() {
        private AckingSubscriber<A> _subscriberOfFlatMappedA = null;
        private final Queue<Publisher<A>> _publishers = new ArrayDeque<Publisher<A>>();
        private final Map<Publisher<A>, Subscription> _subscriptions = new HashMap<Publisher<A>, Subscription>();
        boolean _sourceDone = false;
        int _count = 0;

        @Override
        public void subscribe(final AckingSubscriber<A> subscriberOfFlatMappedA) {
          _subscriberOfFlatMappedA = subscriberOfFlatMappedA;
          that.subscribe(new AckingSubscriber<T>() {

            /**
             * Subscribe to received publishers
             */
            private void subscribe(final Publisher<A> publisher, final Ack ack) {
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
                  for (Map.Entry<Publisher<A>, Subscription> entry: _subscriptions.entrySet()) {
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
            public void onNext(AckValue<T> element) {
              Publisher<A> publisher = f.apply(element.get());
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

      };
  }
}