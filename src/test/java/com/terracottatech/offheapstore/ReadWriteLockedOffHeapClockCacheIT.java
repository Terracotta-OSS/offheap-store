package com.terracottatech.offheapstore;

import java.util.concurrent.ConcurrentMap;

import org.junit.Test;

import com.terracottatech.offheapstore.buffersource.HeapBufferSource;
import com.terracottatech.offheapstore.buffersource.OffHeapBufferSource;
import com.terracottatech.offheapstore.paging.PageSource;
import com.terracottatech.offheapstore.paging.PhantomReferenceLimitedPageSource;
import com.terracottatech.offheapstore.paging.UnlimitedPageSource;
import com.terracottatech.offheapstore.paging.UpfrontAllocatingPageSource;
import com.terracottatech.offheapstore.storage.IntegerStorageEngine;
import com.terracottatech.offheapstore.storage.OffHeapBufferHalfStorageEngine;
import com.terracottatech.offheapstore.storage.SplitStorageEngine;
import com.terracottatech.offheapstore.storage.portability.ByteArrayPortability;
import com.terracottatech.offheapstore.util.Generator;
import com.terracottatech.offheapstore.util.Generator.SpecialInteger;
import com.terracottatech.offheapstore.util.MemoryUnit;

public class ReadWriteLockedOffHeapClockCacheIT extends AbstractConcurrentOffHeapMapIT {

  @Test
  public void testCacheEviction() {
    CapacityLimitedIntegerStorageEngineFactory factory = new CapacityLimitedIntegerStorageEngineFactory();
    CacheTestRoutines.testCacheEviction(new ReadWriteLockedOffHeapClockCache<Integer, Integer>(new UnlimitedPageSource(new OffHeapBufferSource()), factory.newInstance(), 1), factory);
  }

  @Test
  public void testCacheEvictionDueToTableResizeFailure() {
    for (int i = 1; i < 100; i++) {
      CacheTestRoutines.testCacheEvictionMinimal(new ReadWriteLockedOffHeapClockCache<Integer, Integer>(new PhantomReferenceLimitedPageSource(16 * i), new SplitStorageEngine<Integer, Integer>(new IntegerStorageEngine(), new IntegerStorageEngine()), 1));
    }
  }

  @Test
  public void testCacheFillBehavior() {
    CacheTestRoutines.testFillBehavior(createOffHeapBufferMap(new UpfrontAllocatingPageSource(new HeapBufferSource(), MemoryUnit.KILOBYTES.toBytes(8), MemoryUnit.KILOBYTES.toBytes(8))));
  }
  
  @Test
  public void testCacheEvictionThreshold() {
    CacheTestRoutines.testCacheEvictionThreshold(createOffHeapBufferMap(new PhantomReferenceLimitedPageSource(4 * 1024)));
  }

  @Override
  protected ConcurrentMap<SpecialInteger, SpecialInteger> createGoodMap() {
    return new ReadWriteLockedOffHeapClockCache<SpecialInteger, SpecialInteger>(new UnlimitedPageSource(new OffHeapBufferSource()), Generator.GOOD_ENGINE, 1);
  }

  @Override
  protected ConcurrentMap<SpecialInteger, SpecialInteger> createBadMap() {
    return new ReadWriteLockedOffHeapClockCache<SpecialInteger, SpecialInteger>(new UnlimitedPageSource(new OffHeapBufferSource()), Generator.BAD_ENGINE, 1);
  }

  @Override
  protected ConcurrentMap<Integer, byte[]> createOffHeapBufferMap(PageSource source) {
    return new ReadWriteLockedOffHeapClockCache<Integer, byte[]>(source, new SplitStorageEngine<Integer, byte[]>(new IntegerStorageEngine(), new OffHeapBufferHalfStorageEngine<byte[]>(source, 1024, ByteArrayPortability.INSTANCE)));
  }
}
