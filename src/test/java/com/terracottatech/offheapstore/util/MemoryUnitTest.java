package com.terracottatech.offheapstore.util;

import junit.framework.Assert;

import org.junit.Test;

public class MemoryUnitTest {

  @Test
  public void testBasicConversions() {
    Assert.assertEquals(1L, MemoryUnit.BITS.toBits(1L));
    Assert.assertEquals(4L, MemoryUnit.NIBBLES.toBits(1L));
    Assert.assertEquals(8L, MemoryUnit.BYTES.toBits(1L));
    Assert.assertEquals(8L * 1024L, MemoryUnit.KILOBYTES.toBits(1L));
    Assert.assertEquals(8L * 1024L * 1024L, MemoryUnit.MEGABYTES.toBits(1L));
    Assert.assertEquals(8L * 1024L * 1024L * 1024L, MemoryUnit.GIGABYTES.toBits(1L));
    Assert.assertEquals(8L * 1024L * 1024L * 1024L * 1024L, MemoryUnit.TERABYTES.toBits(1L));
    
    Assert.assertEquals(0L, MemoryUnit.BITS.toBytes(1L));
    Assert.assertEquals(0L, MemoryUnit.NIBBLES.toBytes(1L));
    Assert.assertEquals(1L, MemoryUnit.BYTES.toBytes(1L));
    Assert.assertEquals(1024L, MemoryUnit.KILOBYTES.toBytes(1L));
    Assert.assertEquals(1024L * 1024L, MemoryUnit.MEGABYTES.toBytes(1L));
    Assert.assertEquals(1024L * 1024L * 1024L, MemoryUnit.GIGABYTES.toBytes(1L));
    Assert.assertEquals(1024L * 1024L * 1024L * 1024L, MemoryUnit.TERABYTES.toBytes(1L));

    Assert.assertEquals(1L, MemoryUnit.BITS.convert(1L, MemoryUnit.BITS));
    Assert.assertEquals(0L, MemoryUnit.NIBBLES.convert(1L, MemoryUnit.BITS));
    Assert.assertEquals(0L, MemoryUnit.BYTES.convert(1L, MemoryUnit.BITS));
    Assert.assertEquals(0L, MemoryUnit.KILOBYTES.convert(1L, MemoryUnit.BITS));
    Assert.assertEquals(0L, MemoryUnit.MEGABYTES.convert(1L, MemoryUnit.BITS));
    Assert.assertEquals(0L, MemoryUnit.GIGABYTES.convert(1L, MemoryUnit.BITS));
    Assert.assertEquals(0L, MemoryUnit.TERABYTES.convert(1L, MemoryUnit.BITS));

    Assert.assertEquals(4L, MemoryUnit.BITS.convert(1L, MemoryUnit.NIBBLES));
    Assert.assertEquals(1L, MemoryUnit.NIBBLES.convert(1L, MemoryUnit.NIBBLES));
    Assert.assertEquals(0L, MemoryUnit.BYTES.convert(1L, MemoryUnit.NIBBLES));
    Assert.assertEquals(0L, MemoryUnit.KILOBYTES.convert(1L, MemoryUnit.NIBBLES));
    Assert.assertEquals(0L, MemoryUnit.MEGABYTES.convert(1L, MemoryUnit.NIBBLES));
    Assert.assertEquals(0L, MemoryUnit.GIGABYTES.convert(1L, MemoryUnit.NIBBLES));
    Assert.assertEquals(0L, MemoryUnit.TERABYTES.convert(1L, MemoryUnit.NIBBLES));

    Assert.assertEquals(8L, MemoryUnit.BITS.convert(1L, MemoryUnit.BYTES));
    Assert.assertEquals(2L, MemoryUnit.NIBBLES.convert(1L, MemoryUnit.BYTES));
    Assert.assertEquals(1L, MemoryUnit.BYTES.convert(1L, MemoryUnit.BYTES));
    Assert.assertEquals(0L, MemoryUnit.KILOBYTES.convert(1L, MemoryUnit.BYTES));
    Assert.assertEquals(0L, MemoryUnit.MEGABYTES.convert(1L, MemoryUnit.BYTES));
    Assert.assertEquals(0L, MemoryUnit.GIGABYTES.convert(1L, MemoryUnit.BYTES));
    Assert.assertEquals(0L, MemoryUnit.TERABYTES.convert(1L, MemoryUnit.BYTES));

    Assert.assertEquals(MemoryUnit.BITS.convert(1L, MemoryUnit.KILOBYTES), 8L * 1024L);
    Assert.assertEquals(MemoryUnit.NIBBLES.convert(1L, MemoryUnit.KILOBYTES), 2L * 1024L);
    Assert.assertEquals(MemoryUnit.BYTES.convert(1L, MemoryUnit.KILOBYTES), 1024L);
    Assert.assertEquals(MemoryUnit.KILOBYTES.convert(1L, MemoryUnit.KILOBYTES), 1L);
    Assert.assertEquals(MemoryUnit.MEGABYTES.convert(1L, MemoryUnit.KILOBYTES), 0L);
    Assert.assertEquals(MemoryUnit.GIGABYTES.convert(1L, MemoryUnit.KILOBYTES), 0L);
    Assert.assertEquals(MemoryUnit.TERABYTES.convert(1L, MemoryUnit.KILOBYTES), 0L);

    Assert.assertEquals(MemoryUnit.BITS.convert(1L, MemoryUnit.MEGABYTES), 8L * 1024L * 1024L);
    Assert.assertEquals(MemoryUnit.NIBBLES.convert(1L, MemoryUnit.MEGABYTES), 2L * 1024L * 1024L);
    Assert.assertEquals(MemoryUnit.BYTES.convert(1L, MemoryUnit.MEGABYTES), 1024L * 1024L);
    Assert.assertEquals(MemoryUnit.KILOBYTES.convert(1L, MemoryUnit.MEGABYTES), 1024L);
    Assert.assertEquals(MemoryUnit.MEGABYTES.convert(1L, MemoryUnit.MEGABYTES), 1L);
    Assert.assertEquals(MemoryUnit.GIGABYTES.convert(1L, MemoryUnit.MEGABYTES), 0L);
    Assert.assertEquals(MemoryUnit.TERABYTES.convert(1L, MemoryUnit.MEGABYTES), 0L);

    Assert.assertEquals(MemoryUnit.BITS.convert(1L, MemoryUnit.GIGABYTES), 8L * 1024L * 1024L * 1024L);
    Assert.assertEquals(MemoryUnit.NIBBLES.convert(1L, MemoryUnit.GIGABYTES), 2L * 1024L * 1024L * 1024L);
    Assert.assertEquals(MemoryUnit.BYTES.convert(1L, MemoryUnit.GIGABYTES), 1024L * 1024L * 1024L);
    Assert.assertEquals(MemoryUnit.KILOBYTES.convert(1L, MemoryUnit.GIGABYTES), 1024L * 1024L);
    Assert.assertEquals(MemoryUnit.MEGABYTES.convert(1L, MemoryUnit.GIGABYTES), 1024L);
    Assert.assertEquals(MemoryUnit.GIGABYTES.convert(1L, MemoryUnit.GIGABYTES), 1L);
    Assert.assertEquals(MemoryUnit.TERABYTES.convert(1L, MemoryUnit.GIGABYTES), 0L);

    Assert.assertEquals(MemoryUnit.BITS.convert(1L, MemoryUnit.TERABYTES), 8L * 1024L * 1024L * 1024L * 1024L);
    Assert.assertEquals(MemoryUnit.NIBBLES.convert(1L, MemoryUnit.TERABYTES), 2L * 1024L * 1024L * 1024L * 1024L);
    Assert.assertEquals(MemoryUnit.BYTES.convert(1L, MemoryUnit.TERABYTES), 1024L * 1024L * 1024L * 1024L);
    Assert.assertEquals(MemoryUnit.KILOBYTES.convert(1L, MemoryUnit.TERABYTES), 1024L * 1024L * 1024L);
    Assert.assertEquals(MemoryUnit.MEGABYTES.convert(1L, MemoryUnit.TERABYTES), 1024L * 1024L);
    Assert.assertEquals(MemoryUnit.GIGABYTES.convert(1L, MemoryUnit.TERABYTES), 1024L);
    Assert.assertEquals(MemoryUnit.TERABYTES.convert(1L, MemoryUnit.TERABYTES), 1L);
  }
    
  @Test
  public void testThresholdConditions() {
    Assert.assertEquals(0L, MemoryUnit.BITS.toBytes(7L));
    Assert.assertEquals(1L, MemoryUnit.BITS.toBytes(9L));
    Assert.assertEquals(0L, MemoryUnit.NIBBLES.toBytes(1L));
    Assert.assertEquals(1L, MemoryUnit.NIBBLES.toBytes(3L));

    Assert.assertEquals(0L, MemoryUnit.NIBBLES.convert(3L, MemoryUnit.BITS));
    Assert.assertEquals(1L, MemoryUnit.NIBBLES.convert(5L, MemoryUnit.BITS));
    Assert.assertEquals(0L, MemoryUnit.BYTES.convert(7L, MemoryUnit.BITS));
    Assert.assertEquals(1L, MemoryUnit.BYTES.convert(9L, MemoryUnit.BITS));
    Assert.assertEquals(0L, MemoryUnit.KILOBYTES.convert((8L * 1024L) - 1, MemoryUnit.BITS));
    Assert.assertEquals(1L, MemoryUnit.KILOBYTES.convert((8L * 1024L) + 1, MemoryUnit.BITS));
    Assert.assertEquals(0L, MemoryUnit.MEGABYTES.convert((8L * 1024L * 1024L) - 1, MemoryUnit.BITS));
    Assert.assertEquals(1L, MemoryUnit.MEGABYTES.convert((8L * 1024L * 1024L) + 1, MemoryUnit.BITS));
    Assert.assertEquals(0L, MemoryUnit.GIGABYTES.convert((8L * 1024L * 1024L * 1024L) - 1, MemoryUnit.BITS));
    Assert.assertEquals(1L, MemoryUnit.GIGABYTES.convert((8L * 1024L * 1024L * 1024L) + 1, MemoryUnit.BITS));
    Assert.assertEquals(0L, MemoryUnit.TERABYTES.convert((8L * 1024L * 1024L * 1024L * 1024L) - 1, MemoryUnit.BITS));
    Assert.assertEquals(1L, MemoryUnit.TERABYTES.convert((8L * 1024L * 1024L * 1024L * 1024L) + 1, MemoryUnit.BITS));

    Assert.assertEquals(0L, MemoryUnit.BYTES.convert(1L, MemoryUnit.NIBBLES));
    Assert.assertEquals(1L, MemoryUnit.BYTES.convert(3L, MemoryUnit.NIBBLES));
    Assert.assertEquals(0L, MemoryUnit.KILOBYTES.convert((2 * 1024L) - 1, MemoryUnit.NIBBLES));
    Assert.assertEquals(1L, MemoryUnit.KILOBYTES.convert((2 * 1024L) + 1, MemoryUnit.NIBBLES));
    Assert.assertEquals(0L, MemoryUnit.MEGABYTES.convert((2 * 1024L * 1024L) - 1, MemoryUnit.NIBBLES));
    Assert.assertEquals(1L, MemoryUnit.MEGABYTES.convert((2 * 1024L * 1024L) + 1, MemoryUnit.NIBBLES));
    Assert.assertEquals(0L, MemoryUnit.GIGABYTES.convert((2 * 1024L * 1024L * 1024L) - 1, MemoryUnit.NIBBLES));
    Assert.assertEquals(1L, MemoryUnit.GIGABYTES.convert((2 * 1024L * 1024L * 1024L) + 1, MemoryUnit.NIBBLES));
    Assert.assertEquals(0L, MemoryUnit.TERABYTES.convert((2 * 1024L * 1024L * 1024L * 1024L) - 1, MemoryUnit.NIBBLES));
    Assert.assertEquals(1L, MemoryUnit.TERABYTES.convert((2 * 1024L * 1024L * 1024L * 1024L) + 1, MemoryUnit.NIBBLES));

    Assert.assertEquals(0L, MemoryUnit.KILOBYTES.convert(1024L - 1, MemoryUnit.BYTES));
    Assert.assertEquals(1L, MemoryUnit.KILOBYTES.convert(1024L + 1, MemoryUnit.BYTES));
    Assert.assertEquals(0L, MemoryUnit.MEGABYTES.convert((1024L * 1024L) - 1, MemoryUnit.BYTES));
    Assert.assertEquals(1L, MemoryUnit.MEGABYTES.convert((1024L * 1024L) + 1, MemoryUnit.BYTES));
    Assert.assertEquals(0L, MemoryUnit.GIGABYTES.convert((1024L * 1024L * 1024L) - 1, MemoryUnit.BYTES));
    Assert.assertEquals(1L, MemoryUnit.GIGABYTES.convert((1024L * 1024L * 1024L) + 1, MemoryUnit.BYTES));
    Assert.assertEquals(0L, MemoryUnit.TERABYTES.convert((1024L * 1024L * 1024L * 1024L) - 1, MemoryUnit.BYTES));
    Assert.assertEquals(1L, MemoryUnit.TERABYTES.convert((1024L * 1024L * 1024L * 1024L) + 1, MemoryUnit.BYTES));

    Assert.assertEquals(0L, MemoryUnit.MEGABYTES.convert(1024L - 1, MemoryUnit.KILOBYTES));
    Assert.assertEquals(1L, MemoryUnit.MEGABYTES.convert(1024L + 1, MemoryUnit.KILOBYTES));
    Assert.assertEquals(0L, MemoryUnit.GIGABYTES.convert((1024L * 1024L) - 1, MemoryUnit.KILOBYTES));
    Assert.assertEquals(1L, MemoryUnit.GIGABYTES.convert((1024L * 1024L) + 1, MemoryUnit.KILOBYTES));
    Assert.assertEquals(0L, MemoryUnit.TERABYTES.convert((1024L * 1024L * 1024L) - 1, MemoryUnit.KILOBYTES));
    Assert.assertEquals(1L, MemoryUnit.TERABYTES.convert((1024L * 1024L * 1024L) + 1, MemoryUnit.KILOBYTES));

    Assert.assertEquals(0L, MemoryUnit.GIGABYTES.convert(1024L - 1, MemoryUnit.MEGABYTES));
    Assert.assertEquals(1L, MemoryUnit.GIGABYTES.convert(1024L + 1, MemoryUnit.MEGABYTES));
    Assert.assertEquals(0L, MemoryUnit.TERABYTES.convert((1024L * 1024L) - 1, MemoryUnit.MEGABYTES));
    Assert.assertEquals(1L, MemoryUnit.TERABYTES.convert((1024L * 1024L) + 1, MemoryUnit.MEGABYTES));

    Assert.assertEquals(0L, MemoryUnit.TERABYTES.convert(1024L - 1, MemoryUnit.GIGABYTES));
    Assert.assertEquals(1L, MemoryUnit.TERABYTES.convert(1024L + 1, MemoryUnit.GIGABYTES));
  }
}
