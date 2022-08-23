/*
 * Copyright 2015 Terracotta, Inc., a Software AG company.
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
package org.terracotta.offheapstore.storage.allocator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.Test;

import org.terracotta.offheapstore.OffHeapHashMap;
import org.terracotta.offheapstore.buffersource.HeapBufferSource;
import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import org.terracotta.offheapstore.paging.OffHeapStorageArea;
import org.terracotta.offheapstore.paging.UnlimitedPageSource;
import org.terracotta.offheapstore.storage.SerializableStorageEngine;
import org.terracotta.offheapstore.util.PointerSizeParameterizedTest;

import org.hamcrest.core.Is;
import org.junit.Assert;

public class BestFitAllocatorIT extends PointerSizeParameterizedTest {

  @Test
  public void testUniformSizedAllocations() {
    OffHeapStorageArea test = new OffHeapStorageArea(getPointerSize(), null, new UnlimitedPageSource(new HeapBufferSource()), 2048, false, false);

    int width = getPointerSize().byteSize();

    for (int i = 0; i < 100; i++) {
      Assert.assertEquals(2 * width + (i * 4 * width), test.allocate(1));
    }

    Assert.assertEquals(100 * 4 * width, test.getOccupiedMemory());
  }

  @Test
  public void testUniformSizedFrees() {
    OffHeapStorageArea test = new OffHeapStorageArea(getPointerSize(), null, new UnlimitedPageSource(new HeapBufferSource()), 2048, false, false);

    List<Long> allocated = new ArrayList<>();

    for (int i = 0; i < 100; i++) {
      allocated.add(test.allocate(1));
    }

    Random rndm = new Random();
    for (int i = 0; i < 100; i++) {
      test.free(allocated.remove(rndm.nextInt(allocated.size())));
    }
    Assert.assertEquals(0, test.getOccupiedMemory());
  }

  @Test
  public void testUniformRepeatedAllocFree() {
    OffHeapStorageArea test = new OffHeapStorageArea(getPointerSize(), null, new UnlimitedPageSource(new HeapBufferSource()), 2048, false, false);

    for (int i = 1; i < 100; i++) {
      int count = (int) Math.floor(100d / i);
      for (int j = 1; j <= count; j++) {
        List<Long> pointers = new ArrayList<>();
        for (int k = 0; k < j; k++) {
          long p = test.allocate(i);
          pointers.add(p);
        }
        for (Long p : pointers) {
          test.free(p);
        }
        Assert.assertEquals("Testing " + j + " Regions of size " + i, 0, test.getOccupiedMemory());
      }
    }
  }

  @Test
  public void testRandomAllocFree() {
    for (int n = 0; n < 1000; n++) {
      OffHeapStorageArea test = new OffHeapStorageArea(getPointerSize(), null, new UnlimitedPageSource(new HeapBufferSource()), 100 * 1024, false, false);

      List<Long> allocated = new ArrayList<>();
      Random rndm = new Random();

      for (int i = 0; i < 100; i++) {
        if (rndm.nextBoolean()) {
          int size = rndm.nextInt(1024);
          long p = test.allocate(size);
          test.validateStorageArea();
          if (p >= 0) {
            allocated.add(p);
          }
        } else {
          if (!allocated.isEmpty()) {
            long p = allocated.remove(rndm.nextInt(allocated.size()));
            test.free(p);
            test.validateStorageArea();
          }
        }
      }
    }
  }

  @Test
  public void testRandomAllocReallocFree() {
    for (int n = 0; n < 1000; n++) {
      OffHeapStorageArea test = new OffHeapStorageArea(getPointerSize(), null, new UnlimitedPageSource(new HeapBufferSource()), 100 * 1024, false, false);

      List<Long> allocated = new ArrayList<>();
      long seed = System.nanoTime();
      Random rndm = new Random(seed);

      for (int i = 0; i < 100; i++) {
        switch (rndm.nextInt(2)) {
          case 0: {
            int size = rndm.nextInt(1024);
            long p = test.allocate(size);
            test.validateStorageArea();
            if (p >= 0) {
              allocated.add(p);
            }
          } break;
          case 1: {
            if (!allocated.isEmpty()) {
              long p = allocated.remove(rndm.nextInt(allocated.size()));
              test.free(p);
              test.validateStorageArea();
            }
          } break;
        }
      }

      for (Long p : allocated) {
        test.free(p);
      }

      Assert.assertThat(test.getOccupiedMemory(), Is.is(0L));
    }
  }

  @Test
  public void repeatedPutTest() {
    Map<String, byte[]> map = new OffHeapHashMap<>(new UnlimitedPageSource(new OffHeapBufferSource()), new SerializableStorageEngine(getPointerSize(), new UnlimitedPageSource(new OffHeapBufferSource()), 1024));

    Random rndm = new Random();
    for (int i = 0; i < 100; i++) {
      byte[] value = new byte[rndm.nextInt(1024)];
      rndm.nextBytes(value);
      putAll(map, value);
    }
  }

  private static void putAll(Map<String, byte[]> map, byte[] value) {
    for (int j = 0; j < 100; j++) {
      map.put(Integer.toString(j), value);
    }
  }

  /*
   * We were forgetting to clear the 'present' tree and small bin maps when
   * clearing.  This mean we were trying to use a '-1' invalid chunk pointer
   * because the small bin root value was invalid even though the map said the
   * bin was non-empty.
   */
  @Test
  public void testSmallMapClearing() {
    OffHeapStorageArea storage = new OffHeapStorageArea(getPointerSize(), null, new UnlimitedPageSource(new OffHeapBufferSource()), 16 * 1024, false, false);

    /*
     * Allocate a small chunk
     */
    long p = storage.allocate(13);
    /*
     * Make sure it doesn't border the top chunk.
     */
    storage.allocate(13);
    /*
     * Free the small chunk - this ensures that the small bin for this size is
     * non-empty.
     */
    storage.free(p);
    /*
     * Clear the data area and allocator.
     */
    storage.clear();
    /*
     * Allocate - this will attempt to allocate from an empty small bin if we
     * didn't correctly clear the small map.
     */
    storage.allocate(13);
  }
}
