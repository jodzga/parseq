package com.linkedin.parseq.stream;

import com.linkedin.parseq.transducer.FlowControl;




public interface AckingSubscriber<T> {

  /**
   * Called before first element is sent to the Subscriber, allows stopping subscription before
   * even first element gets sent.
   * @param subscription
   */
  public void onSubscribe(Subscription subscription);

  /**
   * The {@link StreamCollection Publisher} calls this method to pass one element to this Subscriber. The element
   * must not be <code>null</code>.
   * @param element The element that is passed from publisher to subscriber.
   */
  public void onNext(AckValue<T> element);

  /**
   * The {@link StreamCollection Publisher} calls this method in order to signal that it terminated normally.
   * No more elements will be forthcoming and none of the Subscriber’s methods will be called hereafter.
   */
  public void onComplete(int totalTasks);

  /**
   * The {@link StreamCollection Publisher} calls this method to signal that the stream of elements has failed
   * and is being aborted. The Subscriber should abort its processing as soon as possible.
   * No more elements will be forthcoming and none of the Subscriber’s methods will be called hereafter.
   * @param cause An exception which describes the reason for tearing down this stream.
   */
  public void onError(Throwable cause);

  public static <T> AckingSubscriber<T> adopt(final Subscriber<T> subscriber) {
    return new AckingSubscriber<T>() {

      @Override
      public void onNext(AckValue<T> element) {
        try {
          subscriber.onNext(element.get());
        } finally {
          element.ack(FlowControl.cont);
        }
      }

      @Override
      public void onComplete(final int totalTasks) {
        subscriber.onComplete(totalTasks);
      }

      @Override
      public void onError(final Throwable cause) {
        subscriber.onError(cause);
      }

      @Override
      public void onSubscribe(Subscription subscription) {
      }
    };
  }
}
