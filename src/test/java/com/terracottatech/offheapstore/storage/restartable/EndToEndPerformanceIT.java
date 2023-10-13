/*
 * Copyright 2014-2023 Terracotta, Inc., a Software AG company.
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
package com.terracottatech.offheapstore.storage.restartable;

import static org.terracotta.offheapstore.util.MemoryUnit.MEGABYTES;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.terracottatech.frs.RestartStore;
import com.terracottatech.frs.RestartStoreFactory;
import com.terracottatech.frs.object.RegisterableObjectManager;
import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import org.terracotta.offheapstore.concurrent.ConcurrentOffHeapHashMap;
import org.terracotta.offheapstore.exceptions.OversizeMappingException;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;
import org.terracotta.offheapstore.storage.OffHeapBufferStorageEngine;
import org.terracotta.offheapstore.storage.PointerSize;
import org.terracotta.offheapstore.storage.portability.ByteArrayPortability;
import org.terracotta.offheapstore.storage.portability.StringPortability;
import org.terracotta.offheapstore.util.DebuggingUtils;
import org.terracotta.offheapstore.util.Factory;

public class EndToEndPerformanceIT {

  private static final long OFFHEAP_SIZE = MEGABYTES.toBytes(32L);
  private static final int OFFHEAP_CHUNK_SIZE = MEGABYTES.toBytes(128);
  private static final int OFFHEAP_PAGE_SIZE = MEGABYTES.toBytes(1);
  private static final long FRS_SEGMENT_SIZE = MEGABYTES.toBytes(1L);
  private static final int PAYLOAD_LENGTH = 1024;
  private static final int WRITE_THREAD_COUNT = 8;
  
  public static void main(String[] args) throws Exception {
    new EndToEndPerformanceIT().testWriteRecover();
  }
  
  @Test
  public void testWriteRecover() throws Exception {
    PageSource source = new UpfrontAllocatingPageSource(new OffHeapBufferSource(), OFFHEAP_SIZE, OFFHEAP_CHUNK_SIZE);
    File directory = RestartabilityTestUtilities.createTempDirectory(getClass().getSimpleName() + ".testWriteRecover_");
    ByteBuffer id = ByteBuffer.wrap("testAsyncPutPath".getBytes("US-ASCII"));
    
    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectMgr = RestartabilityTestUtilities.createObjectManager();
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> persistence =
              RestartStoreFactory.createStore(objectMgr, directory, FRS_SEGMENT_SIZE);
      persistence.startup().get();
      try {
          Factory<RestartableStorageEngine<OffHeapBufferStorageEngine<String, LinkedNode<byte[]>>, ByteBuffer, String, byte[]>> storageEngineFactory = RestartableStorageEngine.createFactory(id, persistence, OffHeapBufferStorageEngine.<String, LinkedNode<byte[]>>createFactory(PointerSize.INT, source, OFFHEAP_PAGE_SIZE, StringPortability.INSTANCE, new LinkedNodePortability<byte[]>(ByteArrayPortability.INSTANCE), false, false), false);
          final ConcurrentOffHeapHashMap<String, byte[]> map = new ConcurrentOffHeapHashMap<String, byte[]>(source, storageEngineFactory);
          try {
            objectMgr.registerObject(new OffHeapObjectManagerStripe<ByteBuffer>(id, map));
    
            final byte[] payload = new byte[PAYLOAD_LENGTH];
            final long start = System.nanoTime();
            Thread[] threads = new Thread[WRITE_THREAD_COUNT];
            for (int i = 0; i < threads.length; i++) {
              final String threadId = i + "-";
              threads[i] = new Thread() {
    
                @Override
                public void run() {
                  for (long i = 0; ; i++) {
                    try {
                      map.put(threadId + String.valueOf(i), payload);
                      if ((i+1) % 1024 == 0) {
                        long duration = System.nanoTime() - start;
                        if (duration > 0) {
                          System.out.println(Thread.currentThread() + " Write Rate : " + DebuggingUtils.toBase2SuffixedString(((TimeUnit.SECONDS.toNanos(1) * (i+1) * PAYLOAD_LENGTH) / duration)) + "b/sec");
                        }
                      }
                    } catch (OversizeMappingException e) {
                      System.out.println(Thread.currentThread() + " stored " + i + " mappings");
                      break;
                    }
                  }
                }
              };
            }
            
            for (Thread t : threads) {
              t.start();
            }
            
            for (Thread t : threads) {
              t.join();
            }
    
            //flush all data
            persistence.shutdown();
            
            long duration = System.nanoTime() - start;
            System.out.println("Total of " + map.size() + " mappings stored at " + DebuggingUtils.toBase2SuffixedString(((TimeUnit.SECONDS.toNanos(1) * map.size() * payload.length) / duration)) + "b/sec");
          } finally {
            map.destroy();
          }
      } finally {
        persistence.shutdown();
      }
    }
    
    //Runtime.getRuntime().exec("purge").waitFor();
    
    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectMgr = RestartabilityTestUtilities.createObjectManager();
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> persistence =
              RestartStoreFactory.createStore(objectMgr, directory, FRS_SEGMENT_SIZE);
      try {
          Factory<RestartableStorageEngine<OffHeapBufferStorageEngine<String, LinkedNode<byte[]>>, ByteBuffer, String, byte[]>> storageEngineFactory = RestartableStorageEngine.createFactory(id, persistence, OffHeapBufferStorageEngine.<String, LinkedNode<byte[]>>createFactory(PointerSize.INT, source, OFFHEAP_PAGE_SIZE, StringPortability.INSTANCE, new LinkedNodePortability<byte[]>(ByteArrayPortability.INSTANCE), false, false), true);
          ConcurrentOffHeapHashMap<String, byte[]> map = new ConcurrentOffHeapHashMap<String, byte[]>(source, storageEngineFactory);
    
          objectMgr.registerObject(new OffHeapObjectManagerStripe<ByteBuffer>(id, map));

  
          long start = System.nanoTime();
          persistence.startup().get();
          long duration = System.nanoTime() - start;
          System.out.println("Recovery took " + TimeUnit.NANOSECONDS.toSeconds(duration) + " seconds and recovered " + map.size() + " mappings at " + DebuggingUtils.toBase2SuffixedString(((TimeUnit.SECONDS.toNanos(1) * map.size() * PAYLOAD_LENGTH) / duration)) + "b/sec");
          
          Set<String> keys = new HashSet<String>();
          for (String key : map.keySet()) {
            if (!keys.add(key)) {
              throw new AssertionError("Duplicate Key " + key + " hashcode:" + key.hashCode());
            }
          }
      } finally {
        persistence.shutdown();
      }
    }
  }
}
