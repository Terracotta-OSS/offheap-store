/*
 * Copyright 2015-2023 Terracotta, Inc., a Software AG company.
 * Copyright IBM Corp. 2024, 2025
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
package org.terracotta.offheapstore.pinning;

import org.terracotta.offheapstore.pinning.PinnableCache;
import static org.terracotta.offheapstore.util.MemoryUnit.KILOBYTES;
import static org.terracotta.offheapstore.util.MemoryUnit.MEGABYTES;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Assert;
import org.junit.Test;

import org.terracotta.offheapstore.buffersource.HeapBufferSource;
import org.terracotta.offheapstore.exceptions.OversizeMappingException;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;
import org.terracotta.offheapstore.util.PointerSizeParameterizedTest;

public abstract class AbstractPinningIT extends PointerSizeParameterizedTest {

  protected abstract PinnableCache<Integer, Integer> createPinnedIntegerCache(PageSource source);

  protected abstract PinnableCache<Integer, byte[]> createPinnedByteArrayCache(PageSource source);

  protected abstract PinnableCache<Integer, byte[]> createSharingPinnedByteArrayCache(PageSource source);

  @Test
  public void testPinnedMappingStays() {
    PageSource source = new UpfrontAllocatingPageSource(new HeapBufferSource(), MEGABYTES.toBytes(2), MEGABYTES.toBytes(2));
    PinnableCache<Integer, Integer> cache = createPinnedIntegerCache(source);

    cache.setPinning(-1, true);
    Assert.assertFalse(cache.isPinned(-1));
    cache.putPinned(-1, 42);

    for (int i = 0; cache.size() == i + 1; i++) {
      cache.put(i, i);
    }

    Assert.assertTrue(cache.containsKey(-1));

    int capacity = cache.size();
    for (int i = 0; i < capacity * 10; i++) {
      cache.put(capacity + i, i);
    }

    Assert.assertEquals(Integer.valueOf(42), cache.get(-1));
    Assert.assertTrue(cache.isPinned(-1));
  }

  @Test
  public void testPinnedMappingCausesPutFailure() {
    final int cacheSize = KILOBYTES.toBytes(64);

    PageSource source = new UpfrontAllocatingPageSource(new HeapBufferSource(), cacheSize, cacheSize);
    PinnableCache<Integer, byte[]> cache = createPinnedByteArrayCache(source);

    int maximalSize = 0;
    for (int i = Integer.SIZE - (Integer.numberOfLeadingZeros(cacheSize) + 1); i >= 0; i--) {
      int bit = 1 << i;
      try {
        cache.put(0, new byte[maximalSize | bit]);
        maximalSize |= bit;
      } catch (OversizeMappingException e) {
        //ignore
      }
      cache.clear();
    }

    Assert.assertNull(cache.putPinned(-1, new byte[maximalSize]));

    try {
      cache.put(0, new byte[1]);
      Assert.fail("Expected OversizeMappingException");
    } catch (OversizeMappingException e) {
      Assert.assertEquals(1, cache.size());
    }

    try {
      cache.setPinning(0, true);
      cache.put(0, new byte[1]);
      Assert.fail("Expected OversizeMappingException");
    } catch (OversizeMappingException e) {
      Assert.assertEquals(1, cache.size());
    }

    Assert.assertNotNull(cache.remove(-1));
    Assert.assertNull(cache.get(-1));

    Assert.assertTrue(cache.isEmpty());

    Assert.assertNull(cache.put(0, new byte[1]));
    cache.setPinning(1, true);
    Assert.assertNull(cache.put(1, new byte[1]));

    Assert.assertNotNull(cache.get(0));
    Assert.assertNotNull(cache.get(1));

    Assert.assertEquals(2, cache.size());
  }

  @Test
  public void testPinningWithSharedPageSource() {
    PageSource source = new UpfrontAllocatingPageSource(new HeapBufferSource(), KILOBYTES.toBytes(256), KILOBYTES.toBytes(256));
    PinnableCache<Integer, byte[]> cacheA = createSharingPinnedByteArrayCache(source);
    PinnableCache<Integer, byte[]> cacheB = createSharingPinnedByteArrayCache(source);

    int maxKey = 0;
    for (; cacheA.size() == maxKey; maxKey++) {
      cacheA.put(maxKey, new byte[128]);
    }

    Assert.assertNull(cacheA.putPinned(-1, new byte[128]));

    for (int i = 0; i < maxKey * 2; i++) {
      cacheB.put(i, new byte[128]);
    }

    Assert.assertTrue(cacheA.containsKey(-1));

    Assert.assertNull(cacheB.putPinned(-1, new byte[128]));
    Assert.assertTrue(cacheA.containsKey(-1));

    for (int i = maxKey + 1; i < maxKey * 3; i++) {
      cacheA.put(i, new byte[128]);
    }

    Assert.assertTrue(cacheB.containsKey(-1));
  }

  @Test
  public void testPutIfAbsent() {
    PageSource source = new UpfrontAllocatingPageSource(new HeapBufferSource(), KILOBYTES.toBytes(64), KILOBYTES.toBytes(64));
    PinnableCache<Integer, Integer> cache = createPinnedIntegerCache(source);

    Assert.assertNull(cache.putIfAbsent(-1, 42));
    cache.setPinning(-1, true);

    for (int i = 0; cache.size() == i + 1; i++) {
      cache.put(i, i);
    }

    Assert.assertTrue(cache.containsKey(-1));

    int capacity = cache.size();
    for (int i = 0; i < capacity * 10; i++) {
      cache.put(capacity + i, i);
    }

    Assert.assertEquals(Integer.valueOf(42), cache.get(-1));
    cache.setPinning(-1, false);
    Assert.assertEquals(Integer.valueOf(42), cache.put(-1, 43));
    Assert.assertEquals(Integer.valueOf(43), cache.putIfAbsent(-1, 42));
    Assert.assertEquals(Integer.valueOf(43), cache.get(-1));

    for (int c = 0; c < 100 && cache.containsKey(-1); c++) {
      for (int i = 0; i < capacity; i++) {
        cache.put((c * capacity) + i, i);
      }
    }

    Assert.assertFalse(cache.containsKey(-1));
  }

  @Test
  public void testReplaceThreeArg() {
    PageSource source = new UpfrontAllocatingPageSource(new HeapBufferSource(), KILOBYTES.toBytes(64), KILOBYTES.toBytes(64));
    PinnableCache<Integer, Integer> cache = createPinnedIntegerCache(source);

    for (int i = 0; cache.size() == i; i++) {
      cache.put(i, i);
    }

    int capacity = cache.size();

    cache.setPinning(-1, true);
    Assert.assertFalse(cache.replace(-1, 42, 43));
    cache.setPinning(-1, false);
    Assert.assertNull(cache.get(-1));

    Assert.assertNull(cache.put(-1, 42));

    for (int c = 0; c < 100 && cache.containsKey(-1); c++) {
      for (int i = 0; i < capacity; i++) {
        cache.put((c * capacity) + i, i);
      }
    }

    Assert.assertFalse(cache.containsKey(-1));

    Assert.assertNull(cache.put(-1, 42));
    cache.setPinning(-1, true);
    Assert.assertFalse(cache.replace(-1, 41, 43));
    cache.setPinning(-1, false);
    for (int c = 0; c < 100 && cache.containsKey(-1); c++) {
      for (int i = 0; i < capacity; i++) {
        cache.put((c * capacity) + i, i);
      }
    }

    Assert.assertFalse(cache.containsKey(-1));

    Assert.assertNull(cache.put(-1, 42));
    cache.setPinning(-1, true);
    Assert.assertTrue(cache.replace(-1, 42, 43));
    Assert.assertFalse(cache.isPinned(-1));

  }

  @Test
  public void testReplaceTwoArg() {
    PageSource source = new UpfrontAllocatingPageSource(new HeapBufferSource(), KILOBYTES.toBytes(64), KILOBYTES.toBytes(64));
    PinnableCache<Integer, Integer> cache = createPinnedIntegerCache(source);

    for (int i = 0; cache.size() == i; i++) {
      cache.put(i, i);
    }

    int capacity = cache.size();

    cache.setPinning(-1, true);
    Assert.assertNull(cache.replace(-1, 43));
    cache.setPinning(-1, false);
    Assert.assertNull(cache.get(-1));

    Assert.assertNull(cache.put(-1, 42));

    for (int c = 0; c < 100 && cache.containsKey(-1); c++) {
      for (int i = 0; i < capacity; i++) {
        cache.put((c * capacity) + i, i);
      }
    }

    Assert.assertFalse(cache.containsKey(-1));

    Assert.assertNull(cache.put(-1, 42));
    cache.setPinning(-1, true);
    Assert.assertEquals(Integer.valueOf(42), cache.replace(-1, 43));
    Assert.assertFalse(cache.isPinned(-1));

  }

  @Test
  public void testPinUnpinWithPresentMapping() {
    PageSource source = new UpfrontAllocatingPageSource(new HeapBufferSource(), KILOBYTES.toBytes(64), KILOBYTES.toBytes(64));
    PinnableCache<Integer, byte[]> cache = createPinnedByteArrayCache(source);

    byte[] data = new byte[1024];
    for (int i = 0 ; i < data.length; i++) {
      data[i] = (byte) i;
    }
    Assert.assertFalse(cache.isPinned(0));
    cache.put(0, data);
    Assert.assertFalse(cache.isPinned(0));
    cache.setPinning(0, true);
    Assert.assertTrue(cache.isPinned(0));
    cache.setPinning(0, false);
    Assert.assertFalse(cache.isPinned(0));
    byte[] value = cache.get(0);
    Assert.assertNotNull(value);
    Assert.assertArrayEquals(data, value);
  }

  @Test
  public void testRemoveWithPinnedMapping() {
    PageSource source = new UpfrontAllocatingPageSource(new HeapBufferSource(), KILOBYTES.toBytes(64), KILOBYTES.toBytes(64));
    PinnableCache<Integer, byte[]> cache = createPinnedByteArrayCache(source);

    Assert.assertFalse(cache.isPinned(0));
    cache.put(0, new byte[1024]);
    Assert.assertFalse(cache.isPinned(0));
    cache.setPinning(0, true);
    Assert.assertTrue(cache.isPinned(0));
    cache.remove(0);
    Assert.assertFalse(cache.isPinned(0));
    cache.setPinning(0, false);
    Assert.assertFalse(cache.isPinned(0));
    byte[] value = cache.get(0);
    Assert.assertNull(value);
  }

  @Test
  public void testHighPinningFraction() {
    PageSource source = new UpfrontAllocatingPageSource(new HeapBufferSource(), KILOBYTES.toBytes(64), KILOBYTES.toBytes(64));
    PinnableCache<Integer, byte[]> cache = createPinnedByteArrayCache(source);

    int lastPut = 0;
    for (; cache.size() == lastPut; lastPut++) {
      if ((lastPut & 1) == 0) {
        cache.putPinned(lastPut, new byte[] {(byte) lastPut});
      } else {
        cache.put(lastPut, new byte[] {(byte) lastPut});
      }
    }

    for (int i = lastPut + 1; i < 10 * lastPut; i++) {
      cache.put(i, new byte[] {(byte) i});
    }

    for (int i = 0; i < lastPut; i+= 2) {
      Assert.assertTrue(cache.containsKey(i));
    }
  }

  @Test
  public void testAllMappingsPinned() {
    PageSource source = new UpfrontAllocatingPageSource(new HeapBufferSource(), KILOBYTES.toBytes(64), KILOBYTES.toBytes(64));
    PinnableCache<Integer, byte[]> cache = createPinnedByteArrayCache(source);

    for (int i = 0; true; i++) {
      try {
        cache.putPinned(i, new byte[] {(byte) i});
      } catch (OversizeMappingException e) {
        break;
      }
    }

    int pinnedCount = cache.size();
    for (int i = 0; i < pinnedCount; i++) {
      Assert.assertTrue(cache.containsKey(i));
    }

    for (int i = -1; i >= -pinnedCount; i--) {
      try {
        cache.put(i, new byte[] {(byte) i});
      } catch (OversizeMappingException e) {
      }
    }

    for (int i = 0; i < pinnedCount; i++) {
      Assert.assertTrue(cache.containsKey(i));
    }
  }

  @Test
  public void testMultiThreaded() throws ExecutionException, InterruptedException {
    PageSource source = new UpfrontAllocatingPageSource(new HeapBufferSource(), KILOBYTES.toBytes(64), KILOBYTES.toBytes(64));
    PinnableCache<Integer, byte[]> cache = createPinnedByteArrayCache(source);

    int lastPut = 0;
    for (; cache.size() == lastPut; lastPut++) {
      if ((lastPut & 1) == 0) {
        cache.putPinned(lastPut, new byte[] {(byte) lastPut});
      } else {
        cache.put(lastPut, new byte[] {(byte) lastPut});
      }
    }

    Collection<Callable<Void>> readers = new ArrayList<>();
    readers.add(new CacheAccessor(cache, lastPut + 1, lastPut * 10));
    readers.add(new CacheAccessor(cache, (lastPut * 10) + 1, lastPut * 20));

    ExecutorService executor = Executors.newFixedThreadPool(readers.size());
    try {
      for (Future<Void> result : executor.invokeAll(readers)) {
        result.get();
      }
    } finally {
      executor.shutdown();
    }

    for (int i = 0; i < lastPut; i+= 2) {
      Assert.assertTrue(cache.containsKey(i));
    }
  }

  static class CacheAccessor implements Callable<Void> {

    private final Map<Integer, byte[]> cache;
    private final int start;
    private final int end;

    CacheAccessor(Map<Integer, byte[]> cache, int start, int end) {
      this.cache = cache;
      this.start = start;
      this.end = end;
    }

    @Override
    public Void call() {
      for (int i = start; i < end; i++) {
        cache.put(i, new byte[] {(byte) i});
      }
      return null;
    }

  }
}
