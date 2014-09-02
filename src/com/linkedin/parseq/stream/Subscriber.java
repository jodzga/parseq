package com.linkedin.parseq.stream;



public interface Subscriber<T> {
  /**
   * The {@link Publisher Publisher} calls this method to pass one element to this Subscriber. The element
   * must not be <code>null</code>. The Publisher must not call this method more often than
   * the Subscriber has signaled demand for via the corresponding {@link Subscription Subscription}.
   * @param element The element that is passed from publisher to subscriber.
   */
  public void onNext(AckValue<T> element);

  /**
   * The {@link Publisher Publisher} calls this method in order to signal that it terminated normally.
   * No more elements will be forthcoming and none of the Subscriber’s methods will be called hereafter.
   */
  public void onComplete(int totalTasks);

  /**
   * The {@link Publisher Publisher} calls this method to signal that the stream of elements has failed
   * and is being aborted. The Subscriber should abort its processing as soon as possible.
   * No more elements will be forthcoming and none of the Subscriber’s methods will be called hereafter.
   * <p>
   * This method is not intended to pass validation errors or similar from Publisher to Subscriber
   * in order to initiate an orderly shutdown of the exchange; it is intended only for fatal
   * failure conditions which make it impossible to continue processing further elements.
   * @param cause An exception which describes the reason for tearing down this stream.
   */
  public void onError(Throwable cause);
}
