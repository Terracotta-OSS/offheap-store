/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.util;

/**
 *
 * @author Chris Dennis
 */
public final class DebuggingUtils {

  private static final String[] BASE_10_SUFFIXES = new String[] { "", "k", "M", "G", "T", "P", "E" };
  //Also use the non i syntax to refer to base 2 as generally accepted. See http://en.wikipedia.org/wiki/Gigabyte
  private static final String[] BASE_2_SUFFIXES = new String[] { "", "K", "M", "G", "T", "P", "E" };

  private static final long[] BASE_10_DIVISORS = new long[BASE_10_SUFFIXES.length];
  static {
    for (int i = 0; i < BASE_10_DIVISORS.length; i++) {
      long n = 1;
      for (int j = 0; j < i; j++) {
        n *= 1000;
      }
      BASE_10_DIVISORS[i] = n;
    }
  }

  public static String toBase2SuffixedString(long n) {
    if (n > 0 && Long.bitCount(n) == 1) {
      int i = Long.numberOfTrailingZeros(Math.abs(n)) / 10;
      return (n >> (i * 10)) + BASE_2_SUFFIXES[i];
    } else {
      int i = (63 - Long.numberOfLeadingZeros(n)) / 10;

      long factor = 1L << (i * 10);
      long leading = n / factor;
      long decimalFactor = factor / 10;
      if (decimalFactor == 0) {
        return leading + BASE_2_SUFFIXES[i];
      } else {
        long decimal = (n - (leading * factor)) / (factor / 10);
        return leading + "." + decimal + BASE_2_SUFFIXES[i];
      }
    }
  }

  public static String toBase10SuffixedString(long n) {
    for (int i = 0; i < BASE_10_SUFFIXES.length; i++) {
      long d = (n / 1000) / BASE_10_DIVISORS[i];

      if (d == 0) {
        return (n / BASE_10_DIVISORS[i]) + BASE_10_SUFFIXES[i];
      }
    }

    throw new AssertionError();
  }

  private DebuggingUtils() {
    //
  }
}
