package com.terracottatech.offheapstore.paging;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Assert;
import org.junit.Test;

import com.terracottatech.offheapstore.OffHeapHashMap;
import com.terracottatech.offheapstore.ReadWriteLockedOffHeapClockCache;
import com.terracottatech.offheapstore.WriteLockedOffHeapClockCache;
import com.terracottatech.offheapstore.buffersource.HeapBufferSource;
import com.terracottatech.offheapstore.concurrent.AbstractConcurrentOffHeapCache;
import com.terracottatech.offheapstore.concurrent.ConcurrentOffHeapHashMap;
import com.terracottatech.offheapstore.exceptions.OversizeMappingException;
import com.terracottatech.offheapstore.storage.IntegerStorageEngine;
import com.terracottatech.offheapstore.storage.OffHeapBufferHalfStorageEngine;
import com.terracottatech.offheapstore.storage.SplitStorageEngine;
import com.terracottatech.offheapstore.storage.StorageEngine;
import com.terracottatech.offheapstore.storage.portability.ByteArrayPortability;
import com.terracottatech.offheapstore.util.Factory;

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
  public void testAccessingVictimWhileStealing() throws InterruptedException, Throwable {
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
