package com.terracottatech.offheapstore.util;

public enum MemoryUnit {

  BITS(-3), NIBBLES(-1), BYTES(0), KILOBYTES(10), MEGABYTES(20), GIGABYTES(30), TERABYTES(40);

  /** the index of this unit */
  private final int index;

  /** Internal constructor */
  MemoryUnit(int index) {
    this.index = index;
  }

  public long convert(long duration, MemoryUnit unit) {
    return doConvert(unit.index - index, duration);
  }

  public long toBits(long amount) {
    return doConvert(index - BITS.index, amount);
  }

  public int toBits(int amount) {
    return doConvert(index - BITS.index, amount);
  }
  
  public long toBytes(long amount) {
    return doConvert(index - BYTES.index, amount);
  }

  public int toBytes(int amount) {
    return doConvert(index - BYTES.index, amount);
  }
  
  private static long doConvert(int delta, long amount) {
    if (amount >= 0L) {
      if (delta == 0) {
        return amount;
      } else if (delta < 0) {
        return amount >>> -delta;
      } else if (delta >= Long.numberOfLeadingZeros(amount)) {
        return Long.MAX_VALUE;
      } else {
        return amount << delta;
      }
    } else {
      throw new IllegalArgumentException();
    }
  }

  private static int doConvert(int delta, int amount) {
    if (amount >= 0) {
      if (delta == 0) {
        return amount;
      } else if (delta < 0) {
        return amount >>> -delta;
      } else if (delta >= Integer.numberOfLeadingZeros(amount)) {
        return Integer.MAX_VALUE;
      } else {
        return amount << delta;
      }
    } else {
      throw new IllegalArgumentException();
    }
  }
}
