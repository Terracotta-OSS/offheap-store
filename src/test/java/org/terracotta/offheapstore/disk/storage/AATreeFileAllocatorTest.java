/*
 * Copyright 2015-2023 Terracotta, Inc., a Software AG company.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.terracotta.offheapstore.disk.storage;

import org.terracotta.offheapstore.disk.storage.AATreeFileAllocator;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import static org.hamcrest.core.Is.is;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertThat;

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
    Map<Integer, AllocatedRegion> allocated = new HashMap<>();

    long occupied = 0;
    Random rndm = new Random();
    for (int i = 0; i < ENTRIES; i++) {
      allocator.allocate(Integer.SIZE + 1);
      occupied += Integer.SIZE << 1;
      long size = 1 + rndm.nextInt(MAX_SIZE - 1);// + Integer.SIZE + 1;
      long address = allocator.allocate(size);
      occupied += Long.bitCount(size) == 1 ? size : Long.highestOneBit(size) << 1;
      assertThat(allocator.occupied(), is(occupied));
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
    Map<Integer, AllocatedRegion> allocated = new HashMap<>();

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
