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
package org.terracotta.offheapstore.concurrent;

import org.terracotta.offheapstore.concurrent.ConcurrentOffHeapClockCache;
import static org.terracotta.offheapstore.util.RetryAssert.assertBy;
import static org.hamcrest.core.Is.is;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import org.terracotta.offheapstore.MapInternals;
import org.terracotta.offheapstore.buffersource.HeapBufferSource;
import org.terracotta.offheapstore.exceptions.OversizeMappingException;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;
import org.terracotta.offheapstore.storage.IntegerStorageEngine;
import org.terracotta.offheapstore.storage.OffHeapBufferHalfStorageEngine;
import org.terracotta.offheapstore.storage.SplitStorageEngine;
import org.terracotta.offheapstore.storage.portability.ByteArrayPortability;
import org.terracotta.offheapstore.util.DebuggingUtils;

/**
 *
 * @author cdennis
 */
public class CrossSegmentEvictionIT {

  @Test
  public void testSingleLargeCacheEntry() {
    PageSource source = new UpfrontAllocatingPageSource(new HeapBufferSource(), 16 * 1024 * 1024, 16 * 1024 * 1024);
    ConcurrentOffHeapClockCache<Integer, byte[]> test = new ConcurrentOffHeapClockCache<>(source, SplitStorageEngine.createFactory(IntegerStorageEngine
      .createFactory(), OffHeapBufferHalfStorageEngine.createFactory(source, 4096, ByteArrayPortability.INSTANCE)));

    //put a very large mapping that fills the cache on its own
    Assert.assertNull(test.put(-1, new byte[15 * 1024 * 1024]));
    Assert.assertEquals(1, test.size());

    System.err.println("Segment Distribution");
    for (MapInternals s : test.getSegmentInternals()) {
      System.err.println("Segment : " + DebuggingUtils.toBase2SuffixedString(s.getOccupiedMemory()) + "B/" + DebuggingUtils.toBase2SuffixedString(s.getAllocatedMemory()) + "B]");
    }
    System.err.println();

    //evict that initial large mapping by putting lots of small mappings
    for (int i = 0; ;) {
      int size = test.size();
      for (int n = 0 ; n < 1024; n++, i++) {
        test.put(i, new byte[16 * 1024]);
      }

      if (size >= test.size()) {
        break;
      }
    }
    Assert.assertNull(test.get(-1));

    System.err.println("Segment Distribution");
    for (MapInternals s : test.getSegmentInternals()) {
      System.err.println("Segment : " + DebuggingUtils.toBase2SuffixedString(s.getOccupiedMemory()) + "B/" + DebuggingUtils.toBase2SuffixedString(s.getAllocatedMemory()) + "B]");
    }
    System.err.println();

    //put a very large mapping that fills the cache on its own
    Assert.assertNull(test.put(-1, new byte[15 * 1024 * 1024]));
    Assert.assertNotNull(test.get(-1));

    System.err.println("Segment Distribution");
    for (MapInternals s : test.getSegmentInternals()) {
      System.err.println("Segment : " + DebuggingUtils.toBase2SuffixedString(s.getOccupiedMemory()) + "B/" + DebuggingUtils.toBase2SuffixedString(s.getAllocatedMemory()) + "B]");
    }
    System.err.println();
  }

  @Test
  public void testReallyOversize() {
    PageSource source = new UpfrontAllocatingPageSource(new HeapBufferSource(), 16 * 1024 * 1024, 16 * 1024 * 1024);
    ConcurrentOffHeapClockCache<Integer, byte[]> test = new ConcurrentOffHeapClockCache<>(source, SplitStorageEngine.createFactory(IntegerStorageEngine
      .createFactory(), OffHeapBufferHalfStorageEngine.createFactory(source, 4096, ByteArrayPortability.INSTANCE)));

    try {
      test.put(-1, new byte[16 * 1024 * 1024]);
      Assert.fail();
    } catch (OversizeMappingException e) {
      //ignore - expected
    }
  }


  @Test
  public void testMultiThreadedOversize() throws InterruptedException {
    PageSource source = new UpfrontAllocatingPageSource(new HeapBufferSource(), 4 * 1024 * 1024, 4 * 1024 * 1024);
    final ConcurrentOffHeapClockCache<Integer, byte[]> test = new ConcurrentOffHeapClockCache<>(source, SplitStorageEngine
      .createFactory(IntegerStorageEngine.createFactory(), OffHeapBufferHalfStorageEngine.createFactory(source, 4096, ByteArrayPortability.INSTANCE)));

    Runnable oversize = () -> {
      for (int i = 0; i < 100; i++) {
        test.put(i, new byte[1 * 1024 * 1024]);
      }
    };

    final Thread t1 = new Thread(oversize, "cross-segment-eviction-deadlock-1");
    final Thread t2 = new Thread(oversize, "cross-segment-eviction-deadlock-2");

    t1.setDaemon(true);
    t2.setDaemon(true);
    t1.start();
    t2.start();

    try {
      assertBy(30, TimeUnit.SECONDS, () -> t1.isAlive() || t2.isAlive(), is(false));
    } catch (AssertionError e) {
      ThreadInfo[] ti = ManagementFactory.getThreadMXBean().getThreadInfo(new long[] {t1.getId(), t2.getId()}, Integer.MAX_VALUE);
      StringBuilder sb = new StringBuilder();
      for (ThreadInfo i : ti) {
        sb.append(i).append("\n");
      }
      throw new AssertionError(sb.toString());
    }
  }
}
