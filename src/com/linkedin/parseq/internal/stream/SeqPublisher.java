package com.linkedin.parseq.internal.stream;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * A Publisher, which does not publish element until previous element has
 * been consumed.
 */
public class SeqPublisher<T> implements Publisher<T>, Subscriber<T> {

  private final Publisher<T> _source;
  private Subscriber<T> _subscriber;
  private final Deque<AckValue<T>> _pending = new ArrayDeque<>();
  private boolean _completed = false;
  private int _published = 0;
  private int _total = 0;
  private boolean _publishPending = true;  //publish first element

  public SeqPublisher(Publisher<T> source) {
    _source = source;
  }

  @Override
  public void subscribe(Subscriber<T> subscriber) {
    _subscriber = subscriber;
    _source.subscribe(this);
  }

  @Override
  public void onNext(AckValue<T> element) {
    _pending.add(element);
    if (_publishPending) {
      doPublishNext();
    }
  }

  @Override
  public void onComplete(int total) {
    _completed = true;
    _total = total;
    if (_published == total) {
      _subscriber.onComplete(total);
    }
  }

  @Override
  public void onError(Throwable cause) {
    _subscriber.onError(cause);
  }

  private void onAck() {
    if (_pending.isEmpty()) {
      _publishPending = true;
    } else {
      doPublishNext();
    }
  }

  private Ack thisOnAck() {
    return this::onAck;
  }

  private void doPublishNext() {
    final AckValue<T> v = _pending.poll();
    _subscriber.onNext(new AckValue<T>(v.get(), thisOnAck().andThen(v.getAck())));
    _publishPending = false;
    _published++;
    if (_completed && _published == _total) {
      _subscriber.onComplete(_total);
    }
  }

}
