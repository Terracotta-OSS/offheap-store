/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.disk.storage;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import junit.framework.Assert;

import org.junit.Ignore;
import org.junit.Test;

/**
 * 
 * @author cdennis
 */
public class AATreeFileAllocatorTest {

  private static final int MAX_SIZE = 1024;
  private static final int CYCLES = 100;

  private static final int ENTRIES = 10000;

  @Test
  public void testAllocatorCorrectness() {
    AATreeFileAllocator allocator = new AATreeFileAllocator(Integer.MAX_VALUE);
    Map<Integer, AllocatedRegion> allocated = new HashMap<Integer, AllocatedRegion>();

    long occupied = 0;
    Random rndm = new Random();
    for (int i = 0; i < ENTRIES; i++) {
      allocator.allocate(Integer.SIZE + 1);
      occupied += Integer.SIZE << 1;
      long size = 1 + rndm.nextInt(MAX_SIZE - 1);// + Integer.SIZE + 1;
      long address = allocator.allocate(size);
      occupied += Long.bitCount(size) == 1 ? size : Long.highestOneBit(size) << 1;
      Assert.assertEquals(occupied, allocator.occupied());
      allocated.put(i, new AllocatedRegion(address, size));
    }

    for (int i = 0; i < ENTRIES; i++) {
      int key = rndm.nextInt(ENTRIES);
      AllocatedRegion existing = allocated.get(key);
      long size = 1 + rndm.nextInt(MAX_SIZE - 1);// + Integer.SIZE + 1;
      long address = allocator.allocate(size);
      allocated.put(key, new AllocatedRegion(address, size));
      allocator.free(existing.address(), existing.size());
    }
  }

  @Test
  @Ignore("performance test")
  public void testFragmentation() {
    AATreeFileAllocator allocator = new AATreeFileAllocator(Integer.MAX_VALUE);
    Map<Integer, AllocatedRegion> allocated = new HashMap<Integer, AllocatedRegion>();

    long total = 0;

    Random rndm = new Random();
    for (int i = 0; i < ENTRIES; i++) {
      allocator.allocate(Integer.SIZE + 1);
      long size = 1 + rndm.nextInt(MAX_SIZE - 1);// + Integer.SIZE + 1;
      long address = allocator.allocate(size);
      allocated.put(i, new AllocatedRegion(address, size));
    }

    for (int c = 0; c < CYCLES; c++) {
      for (int i = 0; i < ENTRIES; i++) {
        int key = rndm.nextInt(ENTRIES);
        AllocatedRegion existing = allocated.get(key);
        long size = 1 + rndm.nextInt(MAX_SIZE - 1);// + Integer.SIZE + 1;
        long address = allocator.allocate(size);
        allocated.put(key, new AllocatedRegion(address, size));
        allocator.free(existing.address(), existing.size());
      }
      // long count = allocator.getRegionCount();
      // total += count;
      System.err.println(c);
    }
    System.err.println("Running Average " + (((double) total) / CYCLES));
  }

  static class AllocatedRegion {

    final long address;
    final long size;

    AllocatedRegion(long address, long size) {
      this.address = address;
      this.size = size;
    }

    long address() {
      return address;
    }

    long size() {
      return size;
    }
  }
}
