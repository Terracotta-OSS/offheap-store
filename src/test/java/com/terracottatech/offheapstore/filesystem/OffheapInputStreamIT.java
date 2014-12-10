package com.terracottatech.offheapstore.filesystem;

import static org.hamcrest.core.IsEqual.equalTo;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.terracottatech.offheapstore.buffersource.HeapBufferSource;
import com.terracottatech.offheapstore.buffersource.OffHeapBufferSource;
import com.terracottatech.offheapstore.filesystem.impl.OffheapFileSystem;
import com.terracottatech.offheapstore.paging.UnlimitedPageSource;
import com.terracottatech.offheapstore.paging.UpfrontAllocatingPageSource;
import com.terracottatech.offheapstore.util.MemoryUnit;

public class OffheapInputStreamIT {
  
  private static final byte[]         TEST_DATA;
  static {
    try {
      TEST_DATA = "testdata".getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new AssertionError(e);
    }
  }
  
  @Test
  public void testGeneralRead() throws IOException {
    FileSystem fs = new OffheapFileSystem(new UnlimitedPageSource(new HeapBufferSource()));
    try {
      File file = fs.getOrCreateDirectory("testGeneralRead").getOrCreateFile("testGeneralRead");
      SeekableOutputStream out = file.getOutputStream();
      SeekableInputStream in = file.getInputStream();
      out.write(TEST_DATA);
      out.flush();
      Assert.assertEquals(TEST_DATA.length, file.length());
      in.seek(0);
      byte[] readData = new byte[TEST_DATA.length];
      try {
        in.read(readData, 0, readData.length + 1);
      } catch (IndexOutOfBoundsException e) {
        //expected
      }
      in.read(readData);
      Assert.assertThat(readData, equalTo(TEST_DATA));
      Assert.assertThat(in.read(readData), equalTo(-1));
    } finally {
      fs.delete();
    }
  }

  @Test
  public void testMultipleInputStreams() throws Throwable {
    FileSystem fs = new OffheapFileSystem(new UnlimitedPageSource(new HeapBufferSource()));
    try {
      final File file = fs.getOrCreateDirectory("testMultipleInputStreams").getOrCreateFile("testMultipleInputStreams");
      SeekableOutputStream out = file.getOutputStream();
      
      out.reset();
      for (int i = 0; i < 10; i++) {
        out.write(TEST_DATA);
      }
      out.flush();
      
      ExecutorService executor = Executors.newCachedThreadPool();
      List<Future<Void>> results = executor.invokeAll(Collections.nCopies(1024, new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          SeekableInputStream in = file.getInputStream();
          for (int i = 0; i < 10; i++) {
            byte[] dataBuffer = new byte[TEST_DATA.length];
            in.read(dataBuffer);
            Assert.assertThat(dataBuffer, equalTo(TEST_DATA));
          }
          Assert.assertThat(in.read(), equalTo(-1));
          return null;
        }
      }));
      
      for (Future<Void> result : results) {
        try {
          result.get();
        } catch (ExecutionException e) {
          throw e.getCause();
        }
      }
    } finally {
      fs.delete();
    }
  }

  @Test
  public void testConcurrentReadAndWrite() throws Throwable {
    FileSystem fs = new OffheapFileSystem(new UpfrontAllocatingPageSource(new OffHeapBufferSource(), MemoryUnit.MEGABYTES.toBytes(10), MemoryUnit.MEGABYTES.toBytes(10)),
                                                MemoryUnit.KILOBYTES.toBytes(20), MemoryUnit.KILOBYTES.toBytes(20), 4);

    final File file = fs.getOrCreateDirectory("testConcurrentReadAndWrite").getOrCreateFile("testConcurrentReadAndWrite");

    Callable<Void> reader = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        SeekableInputStream is = file.getInputStream();
        long start = System.nanoTime();
        while ((System.nanoTime() - start) < TimeUnit.MINUTES.toNanos(3)) {
          int result = is.read();
          if (result == -1) {
            Thread.yield();
          } else if (result == 0) {
            Assert.assertThat(is.read(), equalTo(-1));
            return null;
          } else {
            Assert.assertThat(result, equalTo(100));
          }
        }
        throw new AssertionError("Failed to receive termination value");
      }
    };

    Callable<Void> writer = new Callable<Void>() {

      @Override
      public Void call() throws Exception {
        SeekableOutputStream os = file.getOutputStream();
        long start = System.nanoTime();
        for (int i = 0; i < 9000000 && (System.nanoTime() - start) < TimeUnit.MINUTES.toNanos(2); i++) {
          os.write((byte) 100);
        }
        os.write(0);
        os.flush();
        return null;
      }
    };

    ExecutorService executorService = Executors.newCachedThreadPool();
    try {
      Future<Void> writerResult = executorService.submit(writer);
      Future<Void> readerResult = executorService.submit(reader);
  
      try {
        writerResult.get();
      } catch (ExecutionException e) {
        throw e.getCause();
      }
      try {
        readerResult.get();
      } catch (ExecutionException e) {
        throw e.getCause();
      }
    } finally {
      executorService.shutdown();
    }
  }

  @Test
  public void testMultipleManagers() throws IOException {
    int largeCapacity = MemoryUnit.MEGABYTES.toBytes(10);
    int smallCapacity = MemoryUnit.MEGABYTES.toBytes(2);

    FileSystem largeFs = new OffheapFileSystem(new UpfrontAllocatingPageSource(new OffHeapBufferSource(), largeCapacity, largeCapacity),
                                                MemoryUnit.KILOBYTES.toBytes(20), MemoryUnit.KILOBYTES.toBytes(20), 4);

    FileSystem smallFs = new OffheapFileSystem(new UpfrontAllocatingPageSource(new OffHeapBufferSource(), smallCapacity, smallCapacity),
                                                MemoryUnit.KILOBYTES.toBytes(4), MemoryUnit.KILOBYTES.toBytes(4), 4);

    Directory largeDirectory = largeFs.getOrCreateDirectory("testMultipleManagers");
    Directory smallDirectory = smallFs.getOrCreateDirectory("testMultipleManagers");

    File largeFile = largeDirectory.getOrCreateFile("testMultipleManagers");
    File smallFile = smallDirectory.getOrCreateFile("testMultipleManagers");

    SeekableOutputStream largeOutputStream = largeFile.getOutputStream();
    SeekableOutputStream smallOutputStream = smallFile.getOutputStream();

    int largeFileLengthToWrite = (int) 0.9 * largeCapacity;
    for (int i = 0; i < largeFileLengthToWrite; i++) {
      largeOutputStream.write((byte) 100);
    }
    largeOutputStream.flush();
    Assert.assertEquals(largeFileLengthToWrite, largeFile.length());

    int smallFileLengthToWrite = (int) 0.9 * smallCapacity;
    for (int i = 0; i < smallFileLengthToWrite; i++) {
      smallOutputStream.write((byte) 100);
    }
    smallOutputStream.flush();
    Assert.assertEquals(smallFileLengthToWrite, smallFile.length());

    smallFs.delete();
    largeFs.delete();
  }
}
