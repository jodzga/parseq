package com.linkedin.parseq.util;

public final class Integers {

  private Integers() {}

  public static void requireNonNegative(final int n) {
    if (n < 0) {
      throw new IllegalArgumentException("Argument must be non negative integer numebr, but is: " + n);
    }
  }
}
