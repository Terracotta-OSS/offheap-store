/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.disk.paging;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author cdennis
 */
public class PowerOfTwoFileAllocatorTest {

  @Test
  public void testUniformSizedAllocations() {
    PowerOfTwoFileAllocator test = new PowerOfTwoFileAllocator();

    for (long i = 0; i < 100; i++) {
      Assert.assertEquals(i, test.allocate(1).longValue());
    }

    Assert.assertEquals(100, test.occupied());
  }

  @Test
  public void testUniformSizedFrees() {
    PowerOfTwoFileAllocator test = new PowerOfTwoFileAllocator();

    List<Long> allocated = new ArrayList<Long>();

    for (int i = 0; i < 100; i++) {
      allocated.add(test.allocate(1));
    }

    Random rndm = new Random();
    for (int i = 0; i < 100; i++) {
      test.free(allocated.remove(rndm.nextInt(allocated.size())), 1);
    }
    Assert.assertEquals(0, test.occupied());
  }

  @Test
  public void testUniformRepeatedAllocFree() {
    PowerOfTwoFileAllocator test = new PowerOfTwoFileAllocator();
    
    for (long i = 1; i < Integer.highestOneBit(Integer.MAX_VALUE); i <<= 1) {
      List<Long> pointers = new ArrayList<Long>();
      for (int k = 0; k < 16; k++) {
        long p = test.allocate(i);
        pointers.add(p);
      }
      for (Long p : pointers) {
        test.free(p, i);
      }
      Assert.assertEquals("Testing regions of size " + i, 0, test.occupied());
    }
  }

  @Test
  public void testRandomAllocFree() {
    PowerOfTwoFileAllocator test = new PowerOfTwoFileAllocator();

    List<AllocatedRegion> allocated = new ArrayList<AllocatedRegion>();
    Random rndm = new Random();

    for (int i = 0; i < 100; i++) {
      if (rndm.nextBoolean()) {
        int size = rndm.nextInt(10);
        long p = test.allocate(1 << size);
        if (p >= 0) {
          allocated.add(new AllocatedRegion(p, 1 << size));
        }
      } else {
        if (!allocated.isEmpty()) {
          AllocatedRegion r = allocated.remove(rndm.nextInt(allocated.size()));
          test.free(r.address, r.size);
        }
      }
    }
  }

  @Test
  public void testLargeAllocations() {
    PowerOfTwoFileAllocator test = new PowerOfTwoFileAllocator();

    for (long i = 1; i < Long.MAX_VALUE >>> 2; i <<= 1) {
      Assert.assertTrue(test.allocate(i) >= 0);
    }
  }

  static class AllocatedRegion {

    private final long address;
    private final int size;

    public AllocatedRegion(long address, int size) {
      this.address = address;
      this.size = size;
    }
  }
}
