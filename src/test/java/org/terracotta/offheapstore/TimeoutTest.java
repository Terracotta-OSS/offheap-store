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

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @author Henri Tremblay
 */
public class TimeoutTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(TimeoutTest.class);

  @Test
  public void test() throws ExecutionException {
    ExecutorService service = Executors.newSingleThreadExecutor();
    Future<?> future = service.submit(() -> {
      try {
        Thread.sleep(2000);
      } catch (InterruptedException e) {

      }
    });
    while (true) {
      try {
        future.get();
        break;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  public static void main(String[] args) {
    long toAllocate = 2L * 1024 * 1024 * 1024;
    long allocated = 1024 * 1024 * 1024;
    int maxChunk = 1024 * 1024 * 1024;
    System.out.println(toAllocate);
    System.out.println((int) toAllocate);
    System.out.println((int)toAllocate / maxChunk + 1);
    System.out.println((int)(toAllocate / maxChunk + 1));
    System.out.println((100 * allocated) / toAllocate);
    if(args.length != 1) {
      System.err.println("Usage: TimeoutTest size_to_allocate_in_gb");
      System.exit(1);
      return;
    }

    long size = Long.parseLong(args[0]);
    long start = System.currentTimeMillis();

    OffHeapBufferSource source = new OffHeapBufferSource();
    UpfrontAllocatingPageSource pageSource = new UpfrontAllocatingPageSource(source, size * 1024 * 1024 * 1024, 1024 * 1024 * 1024);

    long end = System.currentTimeMillis();

    System.out.println(pageSource.getAllocatedSize());

    System.out.println("Elapsed: " + (end - start));
  }
}
