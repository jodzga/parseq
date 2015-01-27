package com.linkedin.parseq.task;

import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeoutException;

public class Exceptions {

  public static final Exception EARLY_FINISH_EXCEPTION = sanitize(new EarlyFinishException("Task cancelled because parent was already finished"));
  public static final Exception TIMEOUT_EXCEPTION = sanitize(new TimeoutException());
  public static final Exception NO_SUCH_ELEMENT_EXCEPTION = sanitize(new NoSuchElementException());

  private Exceptions() {}

  private static Exception sanitize(Exception e) {
    // Clear out everything but the last frame
    final StackTraceElement[] stackTrace = e.getStackTrace();
    if (stackTrace.length > 0) {
      e.setStackTrace(Arrays.copyOf(e.getStackTrace(), 1));
    }
    return e;
  }

  public static Exception noSuchElement() {
    return NO_SUCH_ELEMENT_EXCEPTION;
  }

  private static Exception addCause(Exception e, Throwable cause) {
    return (Exception)e.initCause(cause);
  }

  public static Exception noSuchElement(final Throwable cause) {
    return addCause(new NoSuchElementException(), cause);
  }
}
