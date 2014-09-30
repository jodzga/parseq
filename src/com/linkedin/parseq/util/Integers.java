package com.linkedin.parseq.util;

public final class Integers {
  public static void requireNonNegative(final int n) {
    if (n < 0) {
      throw new IllegalArgumentException();
    }
  }
}
