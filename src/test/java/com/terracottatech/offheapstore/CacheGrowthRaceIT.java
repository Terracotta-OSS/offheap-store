package com.terracottatech.offheapstore;

import com.terracottatech.offheapstore.buffersource.HeapBufferSource;
import com.terracottatech.offheapstore.concurrent.ConcurrentOffHeapClockCache;
import com.terracottatech.offheapstore.exceptions.OversizeMappingException;
import com.terracottatech.offheapstore.paging.PageSource;
import com.terracottatech.offheapstore.paging.UpfrontAllocatingPageSource;
import com.terracottatech.offheapstore.storage.StorageEngine;
import com.terracottatech.offheapstore.storage.portability.ByteArrayPortability;
import com.terracottatech.offheapstore.storage.portability.SerializablePortability;
import com.terracottatech.offheapstore.storage.portability.StringPortability;
import com.terracottatech.offheapstore.util.Factory;
import com.terracottatech.offheapstore.util.PointerSizeEngineTypeParameterizedTest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Test;

import static com.terracottatech.offheapstore.util.MemoryUnit.KILOBYTES;
import static com.terracottatech.offheapstore.util.MemoryUnit.MEGABYTES;
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
