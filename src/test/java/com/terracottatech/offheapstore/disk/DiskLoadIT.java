/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.disk;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Test;

import com.terracottatech.offheapstore.concurrent.ConcurrentOffHeapHashMap;
import com.terracottatech.offheapstore.disk.paging.MappedPageSource;
import com.terracottatech.offheapstore.disk.storage.FileBackedStorageEngine;
import com.terracottatech.offheapstore.disk.storage.portability.PersistentByteArrayPortability;
import com.terracottatech.offheapstore.disk.storage.portability.PersistentSerializablePortability;
import com.terracottatech.offheapstore.storage.StorageEngine;
import com.terracottatech.offheapstore.util.Factory;

/**
 *
 * @author cdennis
 */
@Ignore("performance test")
public class DiskLoadIT {

  private static final int BATCH = 50000;
  private static final int THREADS = 1;
  private static final int PAYLOAD = 750;
  private static final int SIZE = 1000000;
  private static final int SEGMENTS = 1;
  
  @Test
  public void testConcurrentMapLoading() throws IOException, InterruptedException, ExecutionException {
    File dataFile = new File("loadtest.data");
    dataFile.deleteOnExit();
    final MappedPageSource source = new MappedPageSource(dataFile);
    final ConcurrentOffHeapHashMap<Integer, byte[]> map = new ConcurrentOffHeapHashMap<Integer, byte[]>(source, new Factory<StorageEngine<Integer, byte[]>>() {
      @Override
      public StorageEngine<Integer, byte[]> newInstance() {
        ThreadPoolExecutor e = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(1000), new RejectedExecutionHandler() {

          @Override
          public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            boolean interrupted = false;
            try {
              while (true) {
                try {
                  executor.getQueue().put(r);
                  return;
                } catch (InterruptedException e) {
                  interrupted = true;
                }
              }
            } finally {
              if (interrupted) {
                Thread.currentThread().interrupt();
              }
            }
          }
        });
        return new FileBackedStorageEngine<Integer, byte[]>(source, new PersistentSerializablePortability(), PersistentByteArrayPortability.INSTANCE, 1, e);
      }
    }, 1, SEGMENTS);
    try {
      final Thread[] threads = new Thread[THREADS];

      for (int i = 0; i < threads.length; i++) {
        final int current = i;
        threads[current] = new Thread() {
          @Override
          public void run() {
            int start = (SIZE / threads.length) * current;
            int end = (SIZE / threads.length) * (current + 1);

            for (int i = start; i < end; ) {
              long startTime = System.nanoTime();
              for (int c = 0; c < BATCH; c++, i++) {
                map.put(i, new byte[PAYLOAD]);
              }
              long endTime = System.nanoTime();
              System.err.println(map.size() +"," + (endTime - startTime));
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
    } finally {
      source.close();
    }
  }
}
