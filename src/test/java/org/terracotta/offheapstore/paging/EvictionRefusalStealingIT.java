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
package org.terracotta.offheapstore.paging;

import org.terracotta.offheapstore.paging.UnlimitedPageSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;
import org.terracotta.offheapstore.paging.PageSource;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Assert;
import org.junit.Test;

import org.terracotta.offheapstore.OffHeapHashMap;
import org.terracotta.offheapstore.ReadWriteLockedOffHeapClockCache;
import org.terracotta.offheapstore.WriteLockedOffHeapClockCache;
import org.terracotta.offheapstore.buffersource.HeapBufferSource;
import org.terracotta.offheapstore.concurrent.AbstractConcurrentOffHeapCache;
import org.terracotta.offheapstore.concurrent.ConcurrentOffHeapHashMap;
import org.terracotta.offheapstore.exceptions.OversizeMappingException;
import org.terracotta.offheapstore.storage.IntegerStorageEngine;
import org.terracotta.offheapstore.storage.OffHeapBufferHalfStorageEngine;
import org.terracotta.offheapstore.storage.SplitStorageEngine;
import org.terracotta.offheapstore.storage.StorageEngine;
import org.terracotta.offheapstore.storage.portability.ByteArrayPortability;
import org.terracotta.offheapstore.util.Factory;

public class EvictionRefusalStealingIT {

  @Test
  public void testDataSpaceIsStolen() {
    long seed = System.nanoTime();
    System.out.println("EvictionRefusalStealingTest.testDataSpaceIsStolen seed = " + seed);
    Random rndm = new Random(seed);
    PageSource source = new UpfrontAllocatingPageSource(new HeapBufferSource(), 1024 * 1024, 1024 * 1024);

    Map<Integer, byte[]> victim = new EvictionRefusingWriteLockedOffHeapClockCache<Integer, byte[]>(rndm, new UnlimitedPageSource(new HeapBufferSource()), new SplitStorageEngine<Integer, byte[]>(new IntegerStorageEngine(), new OffHeapBufferHalfStorageEngine<byte[]>(source, 16 * 1024, ByteArrayPortability.INSTANCE, false, true)));

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
      System.err.println("Left " + victim.size() + " mappings in victim");
      Assert.assertTrue(victim.size() < victimSize);
      Assert.assertFalse(thief.isEmpty());
    }
  }

  @Test
  public void testBigDataPagesAreStolen() {
    long seed = System.nanoTime();
    System.out.println("EvictionRefusalStealingTest.testBigDataPagesAreStolen seed = " + seed);
    Random rndm = new Random(seed);
    PageSource source = new UpfrontAllocatingPageSource(new HeapBufferSource(), 1024 * 1024, 1024 * 1024);

    Map<Integer, byte[]> victim = new EvictionRefusingWriteLockedOffHeapClockCache<Integer, byte[]>(rndm, new UnlimitedPageSource(new HeapBufferSource()), new SplitStorageEngine<Integer, byte[]>(new IntegerStorageEngine(), new OffHeapBufferHalfStorageEngine<byte[]>(source, 8 * 1024, ByteArrayPortability.INSTANCE, false, true)));

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
      System.err.println("Left " + victim.size() + " mappings in victim");
      Assert.assertTrue(victim.size() < victimSize);
      Assert.assertFalse(thief.isEmpty());
    }
  }

  @Test
  public void testDataPagesAreStolenForTables() {
    long seed = System.nanoTime();
    System.out.println("EvictionRefusalStealingTest.testDataPagesAreStolenForTables seed = " + seed);
    Random rndm = new Random(seed);
    PageSource source = new UpfrontAllocatingPageSource(new HeapBufferSource(), 1024 * 1024, 1024 * 1024);

    Map<Integer, byte[]> victim = new EvictionRefusingWriteLockedOffHeapClockCache<Integer, byte[]>(rndm, new UnlimitedPageSource(new HeapBufferSource()), new SplitStorageEngine<Integer, byte[]>(new IntegerStorageEngine(), new OffHeapBufferHalfStorageEngine<byte[]>(source, 8 * 1024, ByteArrayPortability.INSTANCE, false, true)));

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
      System.err.println("Left " + victim.size() + " mappings in victim");
      Assert.assertTrue(victim.size() < victimSize);
      Assert.assertFalse(thief.isEmpty());
    }
  }

  @Test
  public void testEverythingIsStolen() {
    long seed = System.nanoTime();
    System.out.println("EvictionRefusalStealingTest.testEverythingIsStolen seed = " + seed);
    Random rndm = new Random(seed);
    PageSource source = new UpfrontAllocatingPageSource(new HeapBufferSource(), 1024 * 1024, 1024 * 1024);

    WriteLockedOffHeapClockCache<Integer, byte[]> victim = new EvictionRefusingWriteLockedOffHeapClockCache<Integer, byte[]>(rndm, source, new SplitStorageEngine<Integer, byte[]>(new IntegerStorageEngine(), new OffHeapBufferHalfStorageEngine<byte[]>(source, 16 * 1024, ByteArrayPortability.INSTANCE, false, true)));

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

    Map<Integer, byte[]> thief = new OffHeapHashMap<Integer, byte[]>(source, true, new SplitStorageEngine<Integer, byte[]>(new IntegerStorageEngine(), new OffHeapBufferHalfStorageEngine<byte[]>(source, 16 * 1024, ByteArrayPortability.INSTANCE, true, false)));

    try {
      for (int key = 0; ; key++) {
        thief.put(key, new byte[128]);
      }
    } catch (OversizeMappingException e) {
      System.err.println("Fitted " + thief.size() + " mappings in thief");
      System.err.println("Left " + victim.size() + " mappings in victim");
      Assert.assertTrue(victim.size() < victimSize);
      Assert.assertFalse(thief.isEmpty());
    }
  }

  @Test
  public void testEverythingIsStolenUsingConcurrentMaps() {
    long seed = System.nanoTime();
    System.out.println("EvictionRefusalStealingTest.testEverythingIsStolenUsingConcurrentMaps seed = " + seed);
    Random rndm = new Random(seed);
    PageSource source = new UpfrontAllocatingPageSource(new HeapBufferSource(), 16 * 1024 * 1024, 16 * 1024 * 1024);

    Map<Integer, byte[]> victim = new EvictionRefusingConcurrentOffHeapClockCache<Integer, byte[]>(rndm, source, SplitStorageEngine.createFactory(IntegerStorageEngine.createFactory(), OffHeapBufferHalfStorageEngine.createFactory(source, 16 * 1024, ByteArrayPortability.INSTANCE, false, true)));

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

    Map<Integer, byte[]> thief = new ConcurrentOffHeapHashMap<Integer, byte[]>(source, true, SplitStorageEngine.createFactory(IntegerStorageEngine.createFactory(), OffHeapBufferHalfStorageEngine.createFactory(source, 16 * 1024, ByteArrayPortability.INSTANCE, true, false)));

    try {
      for (int key = 0; ; key++) {
        thief.put(key, new byte[128]);
      }
    } catch (OversizeMappingException e) {
      System.err.println("Fitted " + thief.size() + " mappings in thief");
      Assert.assertFalse(thief.isEmpty());
      Assert.assertTrue(victim.size() < victimSize);
    }
  }

  @Test
  public void testAccessingVictimWhileStealing() throws Throwable {
    long seed = System.nanoTime();
    System.out.println("EvictionRefusalStealingTest.testAccessingVictimWhileStealing seed = " + seed);
    Random rndm = new Random(seed);
    PageSource source = new UpfrontAllocatingPageSource(new HeapBufferSource(), 16 * 1024 * 1024, 16 * 1024 * 1024);

    final Map<Integer, byte[]> victim = new EvictionRefusingConcurrentOffHeapClockCache<Integer, byte[]>(rndm, source, SplitStorageEngine.createFactory(IntegerStorageEngine.createFactory(), OffHeapBufferHalfStorageEngine.createFactory(source, 16 * 1024, ByteArrayPortability.INSTANCE, false, true)));

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
        Assert.assertTrue(victim.size() < victimSize);
      }
    } finally {
      System.err.println("Left " + victim.size() + " mappings in victim");
      executor.shutdownNow();
    }
  }

  @Test
  public void testStealWithMultiPageValue() {
    long seed = System.nanoTime();
    System.out.println("EvictionRefusalStealingTest.testStealWithMultiPageValue seed = " + seed);
    Random rndm = new Random(seed);
    PageSource source = new UpfrontAllocatingPageSource(new HeapBufferSource(), 1024 * 1024, 1024 * 1024);

    Map<Integer, byte[]> victim = new EvictionRefusingWriteLockedOffHeapClockCache<Integer, byte[]>(rndm, new UnlimitedPageSource(new HeapBufferSource()), new SplitStorageEngine<Integer, byte[]>(new IntegerStorageEngine(), new OffHeapBufferHalfStorageEngine<byte[]>(source, 1024, ByteArrayPortability.INSTANCE, false, true)));

    for (int key = 0; ; key++) {
      int size = victim.size();
      victim.put(key, new byte[4 * 1024]);
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
      System.err.println("Left " + victim.size() + " mappings in victim");
      Assert.assertTrue(victim.size() < victimSize);
      Assert.assertFalse(thief.isEmpty());
    }
  }

  static class EvictionRefusingWriteLockedOffHeapClockCache<K, V> extends WriteLockedOffHeapClockCache<K, V> {

    private final Random rndm;

    public EvictionRefusingWriteLockedOffHeapClockCache(Random rndm, PageSource source, StorageEngine<K, V> storageEngine) {
      super(source, storageEngine);
      this.rndm = rndm;
    }

    @Override
    public boolean evict(int index, boolean shrink) {
      return (rndm.nextFloat() > 0.01f) && super.evict(index, shrink);
    }
  }

  static class EvictionRefusingReadWriteLockedOffHeapClockCache<K, V> extends ReadWriteLockedOffHeapClockCache<K, V> {

    private final Random rndm;

    public EvictionRefusingReadWriteLockedOffHeapClockCache(Random rndm, PageSource tableSource, StorageEngine<? super K, ? super V> storageEngine) {
      super(tableSource, storageEngine);
      this.rndm = rndm;
    }

    public EvictionRefusingReadWriteLockedOffHeapClockCache(Random rndm, PageSource tableSource, StorageEngine<? super K, ? super V> storageEngine, int tableSize) {
      super(tableSource, storageEngine, tableSize);
      this.rndm = rndm;
    }

    @Override
    public boolean evict(int index, boolean shrink) {
      return (rndm.nextFloat() > 0.01f) && super.evict(index, shrink);
    }
  }

  static class EvictionRefusingReadWriteLockedOffHeapClockCacheFactory<K, V> implements Factory<EvictionRefusingReadWriteLockedOffHeapClockCache<K, V>> {

    private static final int DEFAULT_TABLE_SIZE = 128;

    private final Factory<? extends StorageEngine<? super K, ? super V>> storageEngineFactory;
    private final PageSource tableSource;
    private final int tableSize;
    private final Random rndm;

    public EvictionRefusingReadWriteLockedOffHeapClockCacheFactory(Random rndm, PageSource tableSource,
                                                                   Factory<? extends StorageEngine<? super K, ? super V>> storageEngineFactory) {
      this(rndm, tableSource, storageEngineFactory, DEFAULT_TABLE_SIZE);
    }

    public EvictionRefusingReadWriteLockedOffHeapClockCacheFactory(Random rndm, PageSource tableSource, Factory<? extends StorageEngine<? super K, ? super V>> storageEngineFactory, int tableSize) {
      this.storageEngineFactory = storageEngineFactory;
      this.tableSource = tableSource;
      this.tableSize = tableSize;
      this.rndm = rndm;
    }

    @Override
    public EvictionRefusingReadWriteLockedOffHeapClockCache<K, V> newInstance() {
      StorageEngine<? super K, ? super V> storageEngine = storageEngineFactory.newInstance();
      try {
        return new EvictionRefusingReadWriteLockedOffHeapClockCache<K, V>(rndm, tableSource, storageEngine, tableSize);
      } catch (RuntimeException e) {
        storageEngine.destroy();
        throw e;
      }
    }
  }

  static class EvictionRefusingConcurrentOffHeapClockCache<K, V> extends AbstractConcurrentOffHeapCache<K, V> {

    public EvictionRefusingConcurrentOffHeapClockCache(Random rndm, PageSource tableSource, Factory<? extends StorageEngine<? super K, ? super V>> storageEngineFactory) {
      super(new EvictionRefusingReadWriteLockedOffHeapClockCacheFactory<K, V>(rndm, tableSource, storageEngineFactory));
    }

  }
}
