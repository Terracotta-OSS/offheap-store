/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.paging;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.terracottatech.offheapstore.OffHeapHashMap;
import com.terracottatech.offheapstore.WriteLockedOffHeapClockCache;
import com.terracottatech.offheapstore.buffersource.HeapBufferSource;
import com.terracottatech.offheapstore.concurrent.ConcurrentOffHeapClockCache;
import com.terracottatech.offheapstore.concurrent.ConcurrentOffHeapHashMap;
import com.terracottatech.offheapstore.exceptions.OversizeMappingException;
import com.terracottatech.offheapstore.storage.IntegerStorageEngine;
import com.terracottatech.offheapstore.storage.LongStorageEngine;
import com.terracottatech.offheapstore.storage.OffHeapBufferHalfStorageEngine;
import com.terracottatech.offheapstore.storage.SplitStorageEngine;
import com.terracottatech.offheapstore.storage.portability.ByteArrayPortability;

/**
 *
 * @author Chris Dennis
 */
public class VictimThiefStealingIT {

  @Test
  public void testDataSpaceIsStolen() {
    PageSource source = new UpfrontAllocatingPageSource(new HeapBufferSource(), 1024 * 1024, 1024 * 1024);

    Map<Integer, byte[]> victim = new WriteLockedOffHeapClockCache<Integer, byte[]>(new UnlimitedPageSource(new HeapBufferSource()), new SplitStorageEngine<Integer, byte[]>(new IntegerStorageEngine(), new OffHeapBufferHalfStorageEngine<byte[]>(source, 16 * 1024, ByteArrayPortability.INSTANCE, false, true)));
    
    for (int key = 0; ; key++) {
      int size = victim.size();
      victim.put(key, new byte[128]);
      if (size >= victim.size()) {
        break;
      }
    }

    Assert.assertFalse(victim.isEmpty());
    int victimSize = victim.size();

    System.err.println("Fitted " + victim.size() + " mappings in victim");

    Map<Integer, byte[]> thief = new OffHeapHashMap<Integer, byte[]>(new UnlimitedPageSource(new HeapBufferSource()), new SplitStorageEngine<Integer, byte[]>(new IntegerStorageEngine(), new OffHeapBufferHalfStorageEngine<byte[]>(source, 16 * 1024, ByteArrayPortability.INSTANCE, true, false)));

    try {
      for (int key = 0; ; key++) {
        thief.put(key, new byte[128]);
      }
    } catch (OversizeMappingException e) {
      System.err.println("Fitted " + thief.size() + " mappings in thief");
      Assert.assertTrue(victim.isEmpty());
      Assert.assertFalse(thief.isEmpty());
      Assert.assertEquals(victimSize, thief.size());
    }
  }

  @Test
  public void testBigDataPagesAreStolen() {
    PageSource source = new UpfrontAllocatingPageSource(new HeapBufferSource(), 1024 * 1024, 1024 * 1024);

    Map<Integer, byte[]> victim = new WriteLockedOffHeapClockCache<Integer, byte[]>(new UnlimitedPageSource(new HeapBufferSource()), new SplitStorageEngine<Integer, byte[]>(new IntegerStorageEngine(), new OffHeapBufferHalfStorageEngine<byte[]>(source, 8 * 1024, ByteArrayPortability.INSTANCE, false, true)));

    for (int key = 0; ; key++) {
      int size = victim.size();
      victim.put(key, new byte[128]);
      if (size >= victim.size()) {
        break;
      }
    }

    Assert.assertFalse(victim.isEmpty());
    int victimSize = victim.size();

    System.err.println("Fitted " + victim.size() + " mappings in victim");

    Map<Integer, byte[]> thief = new OffHeapHashMap<Integer, byte[]>(new UnlimitedPageSource(new HeapBufferSource()), new SplitStorageEngine<Integer, byte[]>(new IntegerStorageEngine(), new OffHeapBufferHalfStorageEngine<byte[]>(source, 16 * 1024, ByteArrayPortability.INSTANCE, true, false)));

    try {
      for (int key = 0; ; key++) {
        thief.put(key, new byte[128]);
      }
    } catch (OversizeMappingException e) {
      System.err.println("Fitted " + thief.size() + " mappings in thief");
      Assert.assertTrue(victim.isEmpty());
      Assert.assertFalse(thief.isEmpty());
      Assert.assertEquals(victimSize, thief.size());
    }
  }

  @Test
  public void testDataPagesAreStolenForTables() {
    PageSource source = new UpfrontAllocatingPageSource(new HeapBufferSource(), 1024 * 1024, 1024 * 1024);

    Map<Integer, byte[]> victim = new WriteLockedOffHeapClockCache<Integer, byte[]>(new UnlimitedPageSource(new HeapBufferSource()), new SplitStorageEngine<Integer, byte[]>(new IntegerStorageEngine(), new OffHeapBufferHalfStorageEngine<byte[]>(source, 8 * 1024, ByteArrayPortability.INSTANCE, false, true)));

    for (int key = 0; ; key++) {
      int size = victim.size();
      victim.put(key, new byte[128]);
      if (size >= victim.size()) {
        break;
      }
    }

    Assert.assertFalse(victim.isEmpty());
    int victimSize = victim.size();

    System.err.println("Fitted " + victim.size() + " mappings in victim");

    Map<Integer, byte[]> thief = new OffHeapHashMap<Integer, byte[]>(source, true, new SplitStorageEngine<Integer, byte[]>(new IntegerStorageEngine(), new OffHeapBufferHalfStorageEngine<byte[]>(new UnlimitedPageSource(new HeapBufferSource()), 16 * 1024, ByteArrayPortability.INSTANCE, true, false)));

    try {
      for (int key = 0; ; key++) {
        thief.put(key, new byte[128]);
      }
    } catch (OversizeMappingException e) {
      System.err.println("Fitted " + thief.size() + " mappings in thief");
      Assert.assertTrue(victim.isEmpty());
      Assert.assertFalse(thief.isEmpty());
      Assert.assertTrue(thief.size() > victimSize);
    }
  }

  @Test
  public void testEverythingIsStolen() {
    PageSource source = new UpfrontAllocatingPageSource(new HeapBufferSource(), 1024 * 1024, 1024 * 1024);

    WriteLockedOffHeapClockCache<Integer, byte[]> victim = new WriteLockedOffHeapClockCache<Integer, byte[]>(source, new SplitStorageEngine<Integer, byte[]>(new IntegerStorageEngine(), new OffHeapBufferHalfStorageEngine<byte[]>(source, 16 * 1024, ByteArrayPortability.INSTANCE, false, true)));

    for (int key = 0; ; key++) {
      int size = victim.size();
      victim.put(key, new byte[128]);
      if (size >= victim.size()) {
        break;
      }
    }

    Assert.assertFalse(victim.isEmpty());

    System.err.println("Fitted " + victim.size() + " mappings in victim");

    Map<Integer, byte[]> thief = new OffHeapHashMap<Integer, byte[]>(source, true, new SplitStorageEngine<Integer, byte[]>(new IntegerStorageEngine(), new OffHeapBufferHalfStorageEngine<byte[]>(source, 16 * 1024, ByteArrayPortability.INSTANCE, true, false)));

    try {
      for (int key = 0; ; key++) {
        thief.put(key, new byte[128]);
      }
    } catch (OversizeMappingException e) {
      System.err.println("Fitted " + thief.size() + " mappings in thief");
      Assert.assertFalse(thief.isEmpty());
      Assert.assertTrue(thief.size() > victim.size());
    }
  }

  @Test
  public void testEverythingIsStolenUsingConcurrentMaps() {
    PageSource source = new UpfrontAllocatingPageSource(new HeapBufferSource(), 16 * 1024 * 1024, 16 * 1024 * 1024);

    Map<Integer, byte[]> victim = new ConcurrentOffHeapClockCache<Integer, byte[]>(source, SplitStorageEngine.createFactory(IntegerStorageEngine.createFactory(), OffHeapBufferHalfStorageEngine.createFactory(source, 16 * 1024, ByteArrayPortability.INSTANCE, false, true)));

    for (int key = 0; ; key++) {
      int size = victim.size();
      victim.put(key, new byte[128]);
      if (size >= victim.size()) {
        break;
      }
    }

    Assert.assertFalse(victim.isEmpty());

    System.err.println("Fitted " + victim.size() + " mappings in victim");

    Map<Integer, byte[]> thief = new ConcurrentOffHeapHashMap<Integer, byte[]>(source, true, SplitStorageEngine.createFactory(IntegerStorageEngine.createFactory(), OffHeapBufferHalfStorageEngine.createFactory(source, 16 * 1024, ByteArrayPortability.INSTANCE, true, false)));

    try {
      for (int key = 0; ; key++) {
        thief.put(key, new byte[128]);
      }
    } catch (OversizeMappingException e) {
      System.err.println("Fitted " + thief.size() + " mappings in thief");
      Assert.assertFalse(thief.isEmpty());
      Assert.assertTrue(thief.size() > victim.size());
    }
  }

  @Test
  public void testAccessingVictimWhileStealing() throws InterruptedException, Throwable {
    PageSource source = new UpfrontAllocatingPageSource(new HeapBufferSource(), 16 * 1024 * 1024, 16 * 1024 * 1024);

    final Map<Integer, byte[]> victim = new ConcurrentOffHeapClockCache<Integer, byte[]>(source, SplitStorageEngine.createFactory(IntegerStorageEngine.createFactory(), OffHeapBufferHalfStorageEngine.createFactory(source, 16 * 1024, ByteArrayPortability.INSTANCE, false, true)));

    for (int key = 0; ; key++) {
      int size = victim.size();
      victim.put(key, new byte[128]);
      if (size >= victim.size()) {
        break;
      }
    }

    Assert.assertFalse(victim.isEmpty());

    System.err.println("Fitted " + victim.size() + " mappings in victim");

    Runnable accessor = new Runnable() {

      @Override
      public void run() {
        Random rndm = new Random();
        try {
          int last = 0;
          while (!Thread.interrupted()) {
            if (rndm.nextBoolean()) {
              last = rndm.nextInt();
              victim.put(last, new byte[128]);
            } else {
              victim.get(last);
            }
          }
        } catch (OversizeMappingException e) {
          System.err.println("Cache fully shrunk (caught : " + e + ")");
        }
      }
    };

    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(accessor);
    try {
      Map<Integer, byte[]> thief = new ConcurrentOffHeapHashMap<Integer, byte[]>(source, true, SplitStorageEngine.createFactory(IntegerStorageEngine.createFactory(), OffHeapBufferHalfStorageEngine.createFactory(source, 16 * 1024, ByteArrayPortability.INSTANCE, true, false)));

      try {
        for (int key = 0; ; key++) {
          thief.put(key, new byte[128]);
        }
      } catch (OversizeMappingException e) {
        System.err.println("Fitted " + thief.size() + " mappings in thief");
        Assert.assertFalse(thief.isEmpty());
        Assert.assertTrue(thief.size() > victim.size());
      }
    } finally {
      System.err.println("Left " + victim.size() + " mappings in victim");
      executor.shutdownNow();
    }
  }

  @Test
  public void testTerracottaL2LikeSetup() {
    PageSource source = new UpfrontAllocatingPageSource(new HeapBufferSource(), 16 * 1024 * 1024, 16 * 1024 * 1024);

    Map<Long, byte[]> victim = new ConcurrentOffHeapClockCache<Long, byte[]>(source, LongStorageEngine.createFactory(OffHeapBufferHalfStorageEngine.createFactory(source, 16 * 1024, ByteArrayPortability.INSTANCE, false, true)));

    for (long key = 0; ; key++) {
      int size = victim.size();
      victim.put(key, new byte[128]);
      if (size >= victim.size()) {
        break;
      }
    }

    Assert.assertFalse(victim.isEmpty());

    System.err.println("Fitted " + victim.size() + " mappings in victim");

    Map<Integer, byte[]> thief = new ConcurrentOffHeapHashMap<Integer, byte[]>(source, true, SplitStorageEngine.createFactory(IntegerStorageEngine.createFactory(), OffHeapBufferHalfStorageEngine.createFactory(source, 16 * 1024, ByteArrayPortability.INSTANCE, true, false)));

    try {
      for (int key = 0; ; key++) {
        thief.put(key, new byte[128]);
      }
    } catch (OversizeMappingException e) {
      System.err.println("Fitted " + thief.size() + " mappings in thief");
      Assert.assertFalse(thief.isEmpty());
      Assert.assertTrue(thief.size() > victim.size());
    }
  }

  @Test
  public void testStealWithMultiPageValue() {
    PageSource source = new UpfrontAllocatingPageSource(new HeapBufferSource(), 1024 * 1024, 1024 * 1024);

    Map<Integer, byte[]> victim = new WriteLockedOffHeapClockCache<Integer, byte[]>(new UnlimitedPageSource(new HeapBufferSource()), new SplitStorageEngine<Integer, byte[]>(new IntegerStorageEngine(), new OffHeapBufferHalfStorageEngine<byte[]>(source, 1024, ByteArrayPortability.INSTANCE, false, true)));

    for (int key = 0; ; key++) {
      int size = victim.size();
      victim.put(key, new byte[4 * 1024]);
      if (size >= victim.size()) {
        break;
      }
    }

    Assert.assertFalse(victim.isEmpty());

    System.err.println("Fitted " + victim.size() + " mappings in victim");

    Map<Integer, byte[]> thief = new OffHeapHashMap<Integer, byte[]>(new UnlimitedPageSource(new HeapBufferSource()), new SplitStorageEngine<Integer, byte[]>(new IntegerStorageEngine(), new OffHeapBufferHalfStorageEngine<byte[]>(source, 16 * 1024, ByteArrayPortability.INSTANCE, true, false)));

    try {
      for (int key = 0; ; key++) {
        thief.put(key, new byte[128]);
      }
    } catch (OversizeMappingException e) {
      System.err.println("Fitted " + thief.size() + " mappings in thief");
      Assert.assertTrue(victim.isEmpty());
      Assert.assertFalse(thief.isEmpty());
    }
  }
  
  @Test
  @Ignore
  public void testStealFromPageGrowthArea() {
    PageSource source = new UpfrontAllocatingPageSource(new HeapBufferSource(), 1024 * 1024, 1024 * 1024);

    Map<Integer, byte[]> victim = new WriteLockedOffHeapClockCache<Integer, byte[]>(new UnlimitedPageSource(new HeapBufferSource()), new SplitStorageEngine<Integer, byte[]>(new IntegerStorageEngine(), new OffHeapBufferHalfStorageEngine<byte[]>(source, 1, Integer.highestOneBit(Integer.MAX_VALUE), ByteArrayPortability.INSTANCE, false, true)));
    
    for (int key = 0; ; key++) {
      int size = victim.size();
      victim.put(key, new byte[128]);
      if (size >= victim.size()) {
        break;
      }
    }

    Assert.assertFalse(victim.isEmpty());
    int victimSize = victim.size();

    System.err.println("Fitted " + victim.size() + " mappings in victim");

    Map<Integer, byte[]> thief = new OffHeapHashMap<Integer, byte[]>(new UnlimitedPageSource(new HeapBufferSource()), new SplitStorageEngine<Integer, byte[]>(new IntegerStorageEngine(), new OffHeapBufferHalfStorageEngine<byte[]>(source, 16 * 1024, ByteArrayPortability.INSTANCE, true, false)));

    try {
      for (int key = 0; ; key++) {
        thief.put(key, new byte[128]);
      }
    } catch (OversizeMappingException e) {
      System.err.println("Fitted " + thief.size() + " mappings in thief");
      Assert.assertTrue(victim.isEmpty());
      Assert.assertFalse(thief.isEmpty());
      Assert.assertEquals(victimSize, thief.size());
    }
  }
}
