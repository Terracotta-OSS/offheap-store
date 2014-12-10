/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.concurrent;

import static com.terracottatech.offheapstore.util.RetryAssert.assertBy;
import static org.hamcrest.core.Is.is;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.terracottatech.offheapstore.MapInternals;
import com.terracottatech.offheapstore.buffersource.HeapBufferSource;
import com.terracottatech.offheapstore.exceptions.OversizeMappingException;
import com.terracottatech.offheapstore.paging.PageSource;
import com.terracottatech.offheapstore.paging.UpfrontAllocatingPageSource;
import com.terracottatech.offheapstore.storage.IntegerStorageEngine;
import com.terracottatech.offheapstore.storage.OffHeapBufferHalfStorageEngine;
import com.terracottatech.offheapstore.storage.SplitStorageEngine;
import com.terracottatech.offheapstore.storage.portability.ByteArrayPortability;
import com.terracottatech.offheapstore.util.DebuggingUtils;

/**
 *
 * @author cdennis
 */
public class CrossSegmentEvictionIT {

  @Test
  public void testSingleLargeCacheEntry() {
    PageSource source = new UpfrontAllocatingPageSource(new HeapBufferSource(), 16 * 1024 * 1024, 16 * 1024 * 1024);
    ConcurrentOffHeapClockCache<Integer, byte[]> test = new ConcurrentOffHeapClockCache<Integer, byte[]>(source, SplitStorageEngine.createFactory(IntegerStorageEngine.createFactory(), OffHeapBufferHalfStorageEngine.createFactory(source, 4096, ByteArrayPortability.INSTANCE)));

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
    ConcurrentOffHeapClockCache<Integer, byte[]> test = new ConcurrentOffHeapClockCache<Integer, byte[]>(source, SplitStorageEngine.createFactory(IntegerStorageEngine.createFactory(), OffHeapBufferHalfStorageEngine.createFactory(source, 4096, ByteArrayPortability.INSTANCE)));

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
    final ConcurrentOffHeapClockCache<Integer, byte[]> test = new ConcurrentOffHeapClockCache<Integer, byte[]>(source, SplitStorageEngine.createFactory(IntegerStorageEngine.createFactory(), OffHeapBufferHalfStorageEngine.createFactory(source, 4096, ByteArrayPortability.INSTANCE)));

    Runnable oversize = new Runnable() {
      @Override
      public void run() {
        for (int i = 0; i < 100; i++) {
          test.put(i, new byte[1 * 1024 * 1024]);
        }
      }
    };

    final Thread t1 = new Thread(oversize, "cross-segment-eviction-deadlock-1");
    final Thread t2 = new Thread(oversize, "cross-segment-eviction-deadlock-2");

    t1.setDaemon(true);
    t2.setDaemon(true);
    t1.start();
    t2.start();

    try {
      assertBy(30, TimeUnit.SECONDS, new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return t1.isAlive() || t2.isAlive();
        }
      }, is(false));
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
