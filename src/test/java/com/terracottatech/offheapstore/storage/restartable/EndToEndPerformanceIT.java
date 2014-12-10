package com.terracottatech.offheapstore.storage.restartable;

import static com.terracottatech.offheapstore.util.MemoryUnit.MEGABYTES;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.terracottatech.frs.RestartStore;
import com.terracottatech.frs.RestartStoreFactory;
import com.terracottatech.frs.object.RegisterableObjectManager;
import com.terracottatech.offheapstore.buffersource.OffHeapBufferSource;
import com.terracottatech.offheapstore.concurrent.ConcurrentOffHeapHashMap;
import com.terracottatech.offheapstore.exceptions.OversizeMappingException;
import com.terracottatech.offheapstore.paging.PageSource;
import com.terracottatech.offheapstore.paging.UpfrontAllocatingPageSource;
import com.terracottatech.offheapstore.storage.OffHeapBufferStorageEngine;
import com.terracottatech.offheapstore.storage.PointerSize;
import com.terracottatech.offheapstore.storage.portability.ByteArrayPortability;
import com.terracottatech.offheapstore.storage.portability.StringPortability;
import com.terracottatech.offheapstore.util.DebuggingUtils;
import com.terracottatech.offheapstore.util.Factory;

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
