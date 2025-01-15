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
package org.terracotta.offheapstore;

import org.terracotta.offheapstore.concurrent.ConcurrentOffHeapClockCache;
import org.terracotta.offheapstore.exceptions.OversizeMappingException;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.storage.StorageEngine;
import org.terracotta.offheapstore.storage.portability.ByteArrayPortability;
import org.terracotta.offheapstore.storage.portability.SerializablePortability;
import org.terracotta.offheapstore.storage.portability.StringPortability;
import org.terracotta.offheapstore.util.Factory;
import org.terracotta.offheapstore.util.OffHeapAndDiskStorageEngineDependentTest;

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
import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import org.terracotta.offheapstore.paging.UnlimitedPageSource;

public class CacheGrowthRaceIT extends OffHeapAndDiskStorageEngineDependentTest {

  public CacheGrowthRaceIT(TestMode mode) {
    super(mode);
  }

  @Test
  public void testCrossSegmentGrowthCompetition() throws Exception {
    final int TASK_COUNT = 8;

    PageSource source = createPageSource(1, MEGABYTES);
    Factory<? extends StorageEngine<Integer, byte[]>> factory = createFactory(source, new SerializablePortability(), ByteArrayPortability.INSTANCE);
    final Map<Integer, byte[]> cache = new ConcurrentOffHeapClockCache<>(new UnlimitedPageSource(new OffHeapBufferSource()), factory);
    final byte[] large = new byte[KILOBYTES.toBytes(768)];

    ExecutorService executor = Executors.newCachedThreadPool();

    Collection<Callable<Void>> tasks = new ArrayList<>();
    for (int i = 0; i < TASK_COUNT; i++) {
      final int index = i;
      tasks.add(() -> {
        cache.put(index, large);
        return null;
      });
    }

    for (Future<?> result : executor.invokeAll(tasks, 100000, SECONDS)) {
      result.get();
    }
  }

  @Test
  public void testExpectedFailureCrossSegmentGrowthCompetition() throws Exception {
    final int TASK_COUNT = 8;

    PageSource source = createPageSource(1, MEGABYTES);
    Factory<? extends StorageEngine<String, byte[]>> factory = createFactory(source, StringPortability.INSTANCE, ByteArrayPortability.INSTANCE);
    final Map<String, byte[]> cache = new ConcurrentOffHeapClockCache<>(source, factory);
    final byte[] large = new byte[MEGABYTES.toBytes(2)];

    ExecutorService executor = Executors.newCachedThreadPool();

    Collection<Callable<Void>> tasks = new ArrayList<>();
    for (int i = 0; i < TASK_COUNT; i++) {
      final int index = i;
      tasks.add(() -> {
        try {
          cache.put(Integer.toString(index), large);
          fail();
        } catch (OversizeMappingException e) {
          //expected
        }
        return null;
      });
    }

    for (Future<?> result : executor.invokeAll(tasks, 100000, SECONDS)) {
      result.get();
    }
  }
}
