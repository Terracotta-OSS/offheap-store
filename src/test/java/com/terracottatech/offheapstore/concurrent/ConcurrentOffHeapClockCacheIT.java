package com.terracottatech.offheapstore.concurrent;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.junit.Test;

import com.terracottatech.offheapstore.AbstractConcurrentOffHeapMapIT;
import com.terracottatech.offheapstore.CacheTestRoutines;
import com.terracottatech.offheapstore.CapacityLimitedIntegerStorageEngineFactory;
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

public class ConcurrentOffHeapClockCacheIT extends AbstractConcurrentOffHeapMapIT {

  @Test
  public void testCacheEviction() {
    CapacityLimitedIntegerStorageEngineFactory factory = new CapacityLimitedIntegerStorageEngineFactory();
    CacheTestRoutines.testCacheEviction(new ConcurrentOffHeapClockCache<Integer, Integer>(new UnlimitedPageSource(new OffHeapBufferSource()), factory, 1, 16), factory);
  }

  @Test
  public void testCacheEvictionThreshold() {
    CacheTestRoutines.testCacheEvictionThreshold(createOffHeapBufferMap(new PhantomReferenceLimitedPageSource(64 * 1024)));
  }

  @Test
  public void testCacheFillBehavior() {
    CacheTestRoutines.testFillBehavior(createOffHeapBufferMap(new UpfrontAllocatingPageSource(new HeapBufferSource(), MemoryUnit.KILOBYTES.toBytes(32), MemoryUnit.KILOBYTES.toBytes(32))));
  }
  
  @Override
  protected ConcurrentMap<SpecialInteger, SpecialInteger> createGoodMap() {
    return new ConcurrentOffHeapClockCache<SpecialInteger, SpecialInteger>(new UnlimitedPageSource(new OffHeapBufferSource()), Generator.GOOD_FACTORY, 1, 16);
  }

  @Override
  protected ConcurrentMap<SpecialInteger, SpecialInteger> createBadMap() {
    return new ConcurrentOffHeapClockCache<SpecialInteger, SpecialInteger>(new UnlimitedPageSource(new OffHeapBufferSource()), Generator.BAD_FACTORY, 1, 16);
  }

  @Override
  protected Map<Integer, byte[]> createOffHeapBufferMap(PageSource source) {
    return new ConcurrentOffHeapClockCache<Integer, byte[]>(source, SplitStorageEngine.createFactory(IntegerStorageEngine.createFactory(), OffHeapBufferHalfStorageEngine.createFactory(source, 1024, ByteArrayPortability.INSTANCE)));
  }
}
