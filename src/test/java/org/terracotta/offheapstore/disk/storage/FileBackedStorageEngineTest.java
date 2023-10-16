/*
 * Copyright 2015-2023 Terracotta, Inc., a Software AG company.
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
package org.terracotta.offheapstore.disk.storage;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.terracotta.offheapstore.concurrent.ConcurrentOffHeapHashMap;
import org.terracotta.offheapstore.disk.AbstractDiskTest;
import org.terracotta.offheapstore.disk.paging.MappedPageSource;
import org.terracotta.offheapstore.disk.storage.portability.PersistentByteArrayPortability;
import org.terracotta.offheapstore.disk.storage.portability.PersistentSerializablePortability;
import org.terracotta.offheapstore.util.DebuggingUtils;
import org.terracotta.offheapstore.util.MemoryUnit;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 *
 * @author Chris Dennis
 */
public class FileBackedStorageEngineTest extends AbstractDiskTest {

  private static final NumberFormat FLOAT_FORMAT = NumberFormat.getInstance();

  @Test
  public void testEmptyPayload() throws IOException {
    MappedPageSource source = new MappedPageSource(dataFile);
    FileBackedStorageEngine<byte[], byte[]> engine = new FileBackedStorageEngine<>(source, Long.MAX_VALUE, MemoryUnit.BYTES, PersistentByteArrayPortability.INSTANCE, PersistentByteArrayPortability.INSTANCE);
    try {
      long p = engine.writeMapping(new byte[0], new byte[0], 0, 0);
      Assert.assertTrue(p >= 0);

      engine.flush();

      byte[] k = engine.readKey(p, 0);
      Assert.assertNotNull(k);
      Assert.assertEquals(0, k.length);

      byte[] v = engine.readValue(p);
      Assert.assertNotNull(v);
      Assert.assertEquals(0, v.length);
    } finally {
      engine.close();
      source.close();
    }
  }

  @Test
  public void testSmallPayloads() throws IOException {
    MappedPageSource source = new MappedPageSource(dataFile);
    FileBackedStorageEngine<byte[], byte[]> engine = new FileBackedStorageEngine<>(source, Long.MAX_VALUE, MemoryUnit.BYTES, PersistentByteArrayPortability.INSTANCE, PersistentByteArrayPortability.INSTANCE);
    try {
      Random rndm = new Random();

      for (int i = 0; i < 64; i++) {
        byte[] b = new byte[i];

        rndm.nextBytes(b);

        long p = engine.writeMapping(b, b, 0, 0);
        Assert.assertTrue(p >= 0);

        engine.flush();

        byte[] k = engine.readKey(p, 0);
        Assert.assertNotNull(k);
        Assert.assertArrayEquals(b, k);

        byte[] v = engine.readValue(p);
        Assert.assertNotNull(v);
        Assert.assertArrayEquals(b, v);

        engine.freeMapping(p, 0, true);
      }
    } finally {
      engine.close();
      source.close();
    }
  }

  @Test
  public void testInterruptingReadThreads() throws IOException {
    MappedPageSource source = new MappedPageSource(dataFile);
    FileBackedStorageEngine<byte[], byte[]> engine = new FileBackedStorageEngine<>(source, Long.MAX_VALUE, MemoryUnit.BYTES, PersistentByteArrayPortability.INSTANCE, PersistentByteArrayPortability.INSTANCE);
    try {
      long p = engine.writeMapping(new byte[0], new byte[32], 0, 0);
      Assert.assertTrue(p >= 0);

      engine.flush();

      Thread.currentThread().interrupt();
      byte[] v = engine.readValue(p);
      Assert.assertTrue(Thread.interrupted());
      Assert.assertNotNull(v);
      Assert.assertEquals(32, v.length);
    } finally {
      engine.close();
      source.close();
    }
  }

  @Test
  public void testConcurrentReads() throws IOException {
    MappedPageSource source = new MappedPageSource(dataFile);
    FileBackedStorageEngine<byte[], byte[]> engine = new FileBackedStorageEngine<>(source, Long.MAX_VALUE, MemoryUnit.BYTES, PersistentByteArrayPortability.INSTANCE, PersistentByteArrayPortability.INSTANCE);
    try {
      int size = 32;
      long p = engine.writeMapping(new byte[0], new byte[size], 0, 0);
      Assert.assertTrue(p >= 0);
      engine.flush();

      IntStream.range(0, 8)
        .parallel()
        .mapToObj(n -> engine.readValue(p))
        .peek(Assert::assertNotNull)
        .forEach(v -> Assert.assertEquals(size, v.length));
    } finally {
      engine.close();
      source.close();
    }
  }

  @Test(expected = IOException.class)
  public void testConcurrentClose() throws Throwable {
    FileChannel spyChannel = spy(FileChannel.class);
    MappedPageSource source = new MappedPageSource(dataFile) {
      @Override
      public FileChannel getReadableChannel() {
        return spyChannel;
      }
    };
    FileBackedStorageEngine<byte[], byte[]> engine = new FileBackedStorageEngine<>(source, Long.MAX_VALUE, MemoryUnit.BYTES, PersistentByteArrayPortability.INSTANCE, PersistentByteArrayPortability.INSTANCE);
    when(spyChannel.read(notNull(), anyLong()))
      .thenAnswer(o -> {
        //deterministically simulate another thread closing the engine while
        //this thread is reading.
        engine.close();
        throw new ClosedChannelException();
      });
    try {
      byte[] buffer = new byte[10];
      long p = engine.writeMapping(new byte[0], buffer, 0, 0);
      Assert.assertTrue(p >= 0);
      engine.flush();
      engine.readValue(0);
    } catch (Throwable e) {
      Throwable cause = e;
      while (cause.getCause() != null) {
        cause = cause.getCause();
      }
      throw cause;
    } finally {
      source.close();
    }
  }

  @Test
  public void testConcurrentInterrupt() throws Throwable {
    AtomicReference<FileChannel> realChannelRef = new AtomicReference<>();
    FileChannel spyChannel = spy(FileChannel.class);
    MappedPageSource source = new MappedPageSource(dataFile) {
      @Override
      public FileChannel getReadableChannel() {
        FileChannel channel = super.getReadableChannel();
        realChannelRef.set(channel);
        return spyChannel;
      }
    };
    Random random = new Random();
    FileBackedStorageEngine<byte[], byte[]> engine = new FileBackedStorageEngine<>(source, Long.MAX_VALUE, MemoryUnit.BYTES, PersistentByteArrayPortability.INSTANCE, PersistentByteArrayPortability.INSTANCE);
    when(spyChannel.read(notNull(), anyLong()))
      .thenAnswer(invocation -> {
        //simulate periodic interruptions while reading.
        if (random.nextDouble() < 0.5) {
          Thread.currentThread().interrupt();
        }
        ByteBuffer buffer = invocation.getArgument(0);
        Long position = invocation.getArgument(1);
        return realChannelRef.get().read(buffer, position);
      });
    try {
      byte[] buffer = new byte[10];
      long p = engine.writeMapping(new byte[0], buffer, 0, 0);
      Assert.assertTrue(p >= 0);
      engine.flush();
      byte[] bytes = engine.readValue(0);
      Assert.assertArrayEquals(buffer, bytes);
    } finally {
      source.close();
      Thread.interrupted();   // Clear interruption stats
    }
  }

  @Test
  @Ignore("performance test")
  public void testHugeMap() throws IOException, InterruptedException, ExecutionException {
    System.err.println("Using file: " + dataFile.getAbsolutePath());
    MappedPageSource source = new MappedPageSource(dataFile);
    final ConcurrentOffHeapHashMap<Integer, byte[]> map = new ConcurrentOffHeapHashMap<>(source, FileBackedStorageEngine
      .createFactory(source, Long.MAX_VALUE, MemoryUnit.BYTES, new PersistentSerializablePortability(), PersistentByteArrayPortability.INSTANCE), 4 * 1024 * 1024, 1);
    try {
      ExecutorService executor = Executors.newFixedThreadPool(1);

      int i = 0;
      long size = 0;
      while (true) {
        Collection<Callable<Void>> tasks = new ArrayList<>(1024);
        for (int c = 0; c < 1024; c++, i++) {
          final int key = i;
          tasks.add(() -> {
            map.put(key, new byte[1024]);
            return null;
          });
        }
        long start = System.nanoTime();
        for (Future<Void> f : executor.invokeAll(tasks)) {
          f.get();
        }
        long end = System.nanoTime();

        long sizeNow = map.getOccupiedMemory();
        long delta = sizeNow - size;
        size = sizeNow;
        System.out.println("Put " + DebuggingUtils.toBase2SuffixedString(i) + " mappings @ " + FLOAT_FORMAT.format((1000 * 1000 * 1000 * ((double) delta)) / (1024 * 1024 * ((double) (end - start)))) + "MiB/sec [Map Size: " + DebuggingUtils.toBase2SuffixedString(size) + "B Allocated Size: " + DebuggingUtils.toBase2SuffixedString(map.getAllocatedMemory()) + "B]");
      }
    } finally {
      source.close();
    }
  }
}
