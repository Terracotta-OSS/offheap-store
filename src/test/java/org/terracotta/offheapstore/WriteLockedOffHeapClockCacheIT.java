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
package org.terracotta.offheapstore;

import org.terracotta.offheapstore.WriteLockedOffHeapClockCache;
import java.util.concurrent.ConcurrentMap;

import org.junit.Test;

import org.terracotta.offheapstore.buffersource.HeapBufferSource;
import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.paging.PhantomReferenceLimitedPageSource;
import org.terracotta.offheapstore.paging.UnlimitedPageSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;
import org.terracotta.offheapstore.storage.IntegerStorageEngine;
import org.terracotta.offheapstore.storage.OffHeapBufferHalfStorageEngine;
import org.terracotta.offheapstore.storage.SplitStorageEngine;
import org.terracotta.offheapstore.storage.portability.ByteArrayPortability;
import org.terracotta.offheapstore.util.Generator;
import static org.terracotta.offheapstore.util.Generator.BAD_GENERATOR;
import static org.terracotta.offheapstore.util.Generator.GOOD_GENERATOR;
import org.terracotta.offheapstore.util.Generator.SpecialInteger;
import org.terracotta.offheapstore.util.MemoryUnit;
import org.terracotta.offheapstore.util.ParallelParameterized;
import java.util.Arrays;
import java.util.Collection;
import static org.hamcrest.core.Is.is;
import static org.junit.Assume.assumeThat;
import org.junit.runner.RunWith;

@RunWith(ParallelParameterized.class)
public class WriteLockedOffHeapClockCacheIT extends AbstractConcurrentOffHeapMapIT {

  @ParallelParameterized.Parameters(name = "generator={0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[] {GOOD_GENERATOR}, new Object[] {BAD_GENERATOR});
  }

  public WriteLockedOffHeapClockCacheIT(Generator generator) {
    super(generator);
  }

  @Test
  public void testCacheEviction() {
    assumeThat(generator, is(GOOD_GENERATOR));
    CapacityLimitedIntegerStorageEngineFactory factory = new CapacityLimitedIntegerStorageEngineFactory();
    CacheTestRoutines.testCacheEviction(new WriteLockedOffHeapClockCache<Integer, Integer>(new UnlimitedPageSource(new OffHeapBufferSource()), factory.newInstance(), 1), factory);
  }
  
  @Test
  public void testCacheEvictionDueToTableResizeFailure() {
    assumeThat(generator, is(GOOD_GENERATOR));
    for (int i = 1; i < 100; i++) {
      CacheTestRoutines.testCacheEvictionMinimal(new WriteLockedOffHeapClockCache<Integer, Integer>(new PhantomReferenceLimitedPageSource(16 * i), new SplitStorageEngine<Integer, Integer>(new IntegerStorageEngine(), new IntegerStorageEngine()), 1));
    }
  }

  @Test
  public void testCacheEvictionThreshold() {
    CacheTestRoutines.testCacheEvictionThreshold(createOffHeapBufferMap(new PhantomReferenceLimitedPageSource(4 * 1024)));
  }

  @Test
  public void testCacheFillBehavior() {
    CacheTestRoutines.testFillBehavior(createOffHeapBufferMap(new UpfrontAllocatingPageSource(new HeapBufferSource(), MemoryUnit.KILOBYTES.toBytes(8), MemoryUnit.KILOBYTES.toBytes(8))));
  }
  
  @Override
  protected ConcurrentMap<SpecialInteger, SpecialInteger> createMap(Generator generator) {
    return new WriteLockedOffHeapClockCache<SpecialInteger, SpecialInteger>(new UnlimitedPageSource(new OffHeapBufferSource()), generator.engine(), 1);
  }

  @Override
  protected ConcurrentMap<Integer, byte[]> createOffHeapBufferMap(PageSource source) {
    assumeThat(generator, is(GOOD_GENERATOR));
    return new WriteLockedOffHeapClockCache<Integer, byte[]>(source, new SplitStorageEngine<Integer, byte[]>(new IntegerStorageEngine(), new OffHeapBufferHalfStorageEngine<byte[]>(source, 1024, ByteArrayPortability.INSTANCE)));
  }
}
