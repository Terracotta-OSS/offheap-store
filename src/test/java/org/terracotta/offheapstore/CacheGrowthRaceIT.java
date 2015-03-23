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

import org.terracotta.offheapstore.buffersource.HeapBufferSource;
import org.terracotta.offheapstore.concurrent.ConcurrentOffHeapClockCache;
import org.terracotta.offheapstore.exceptions.OversizeMappingException;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;
import org.terracotta.offheapstore.storage.StorageEngine;
import org.terracotta.offheapstore.storage.portability.ByteArrayPortability;
import org.terracotta.offheapstore.storage.portability.SerializablePortability;
import org.terracotta.offheapstore.storage.portability.StringPortability;
import org.terracotta.offheapstore.util.Factory;
import org.terracotta.offheapstore.util.PointerSizeEngineTypeParameterizedTest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Test;

import static org.terracotta.offheapstore.util.MemoryUnit.KILOBYTES;
import static org.terracotta.offheapstore.util.MemoryUnit.MEGABYTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.fail;

public class CacheGrowthRaceIT extends PointerSizeEngineTypeParameterizedTest {

  @Test
  public void testCrossSegmentGrowthCompetition() throws Exception {
    final int TASK_COUNT = 8;
    
    PageSource source = new UpfrontAllocatingPageSource(new HeapBufferSource(), MEGABYTES.toBytes(1), MEGABYTES.toBytes(1));
    Factory<? extends StorageEngine<Integer, byte[]>> factory = createFactory(source, 1024, new SerializablePortability(), ByteArrayPortability.INSTANCE, false, false);
    final Map<Integer, byte[]> cache = new ConcurrentOffHeapClockCache<Integer, byte[]>(source, factory);
    final byte[] large = new byte[KILOBYTES.toBytes(768)];

    ExecutorService executor = Executors.newCachedThreadPool();
    
    Collection<Callable<Void>> tasks = new ArrayList<Callable<Void>>();
    for (int i = 0; i < TASK_COUNT; i++) {
      final int index = i;
      tasks.add(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          cache.put(index, large);
          return null;
        }});
    }
    
    for (Future<?> result : executor.invokeAll(tasks, 10, SECONDS)) {
      result.get();
    }
  }
  
  @Test
  public void testExpectedFailureCrossSegmentGrowthCompetition() throws Exception {
    final int TASK_COUNT = 8;
    
    PageSource source = new UpfrontAllocatingPageSource(new HeapBufferSource(), MEGABYTES.toBytes(1), MEGABYTES.toBytes(1));
    Factory<? extends StorageEngine<String, byte[]>> factory = createFactory(source, 1024, StringPortability.INSTANCE, ByteArrayPortability.INSTANCE, false, false);
    final Map<String, byte[]> cache = new ConcurrentOffHeapClockCache<String, byte[]>(source, factory);
    final byte[] large = new byte[MEGABYTES.toBytes(2)];

    ExecutorService executor = Executors.newCachedThreadPool();
    
    Collection<Callable<Void>> tasks = new ArrayList<Callable<Void>>();
    for (int i = 0; i < TASK_COUNT; i++) {
      final int index = i;
      tasks.add(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          try {
            cache.put(Integer.toString(index), large);
            fail();
          } catch (OversizeMappingException e) {
            //expected
          }
          return null;
        }});
    }
    
    for (Future<?> result : executor.invokeAll(tasks, 10, SECONDS)) {
      result.get();
    }
  }
}
