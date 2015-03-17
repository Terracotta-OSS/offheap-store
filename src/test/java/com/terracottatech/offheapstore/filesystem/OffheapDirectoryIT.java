package com.terracottatech.offheapstore.filesystem;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.junit.Test;

import org.terracotta.offheapstore.buffersource.HeapBufferSource;
import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import com.terracottatech.offheapstore.filesystem.impl.OffheapFileSystem;
import org.terracotta.offheapstore.paging.UnlimitedPageSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;
import org.terracotta.offheapstore.util.MemoryUnit;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.number.OrderingComparison.lessThan;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class OffheapDirectoryIT {

  private static final long MAX_SIZE       = MemoryUnit.MEGABYTES.toBytes(50);
  private static final int  BLOCK_SIZE     = MemoryUnit.KILOBYTES.toBytes(8);
  private static final int  PAGE_SIZE      = BLOCK_SIZE * 64;
  private static final int  MAX_CHUNK_SIZE = PAGE_SIZE * 64;
  private static final int  MIN_CHUNK_SIZE = PAGE_SIZE * 8;
  private static final int  CONCURRENCY    = 4;

  @Test
  public void testCreateFile() throws IOException {
    FileSystem fs = new OffheapFileSystem(new UnlimitedPageSource(new HeapBufferSource()), BLOCK_SIZE, PAGE_SIZE, CONCURRENCY);
    try {
      Directory dir = fs.getOrCreateDirectory("testCreateFile");
      for (int i = 0; i < 10; i++) {
        String fileName = "/\\foo\\/" + Integer.toString(i);
        File f = dir.getOrCreateFile(fileName);
        assertTrue(System.currentTimeMillis() - f.lastModifiedTime() < 500);
      }
      for (int i = 0; i < 10; i++) {
        assertTrue(dir.fileExists("/\\foo\\/" + Integer.toString(i)));
      }
    } finally {
      fs.delete();
    }
  }

  @Test
  public void testDeleteAllFiles() throws IOException {
    FileSystem fs = new OffheapFileSystem(new UnlimitedPageSource(new HeapBufferSource()));
    try {
      Directory dir = fs.getOrCreateDirectory("testDeleteAllFiles");
      assertThat(dir.listFiles(), empty());
      for (int i = 0; i < 10; i++) {
        dir.getOrCreateFile("foo-" + Integer.toString(i));
      }
      dir.deleteAllFiles();
      assertThat(dir.listFiles(), empty());
    } finally {
      fs.delete();
    }
  }

  @Test
  public void testDeleteFile() throws IOException {
    FileSystem fs = new OffheapFileSystem(new UnlimitedPageSource(new HeapBufferSource()));
    try {
      Directory dir = fs.getOrCreateDirectory("testDeleteFile");
      assertFalse(dir.fileExists("bar"));
      dir.getOrCreateFile("bar");
      assertTrue(dir.fileExists("bar"));
      dir.deleteFile("bar");
      assertFalse(dir.fileExists("bar"));
    } finally {
      fs.delete();
    }
  }

  @Test
  public void testGetAllFilenames() throws IOException {
    FileSystem fs = new OffheapFileSystem(new UnlimitedPageSource(new HeapBufferSource()));
    try {
      Directory dir = fs.getOrCreateDirectory("testGetAllFilenames");
      for (int i = 0; i < 10; i++) {
        dir.getOrCreateFile("foo-" + Integer.toString(i));
      }
      Set<String> names = dir.listFiles();
      List<String> namesList = new ArrayList<String>(names);
      Collections.sort(namesList);
      assertThat(names, hasSize(10));
      for (int i = 0; i < 10; i++) {
        assertThat(namesList.get(i), is("foo-" + i));
      }
    } finally {
      fs.delete();
    }
  }

  private long[] getRandoms(long size, long sum) throws Exception {
    long iniSum = 0;
    System.out.println("sum = " + sum);
    long[] ret = new long[(int) size];
    for (int i = 0; i < ret.length; i++) {
      ret[i] = randomInRange(1, sum);
      iniSum += ret[i];
    }

    long finSum = 0;
    int i;
    for (i = 0; i < ret.length - 1; i++) {
      ret[i] = Math.round((sum * ret[i]) / iniSum);
      System.out.println("ret[" + i + "] = " + ret[i]);
      finSum += ret[i];
    }

    ret[i] = sum - finSum > 0 ? sum - finSum : 0;
    finSum += ret[i];
    System.out.println("ret[" + i + "] = " + ret[i]);

    if (finSum != sum) throw new Exception("Could not find " + size + " numbers adding up to " + sum
                                           + " . Final sum = " + finSum);
    return ret;
  }

  private long randomInRange(long min, long max) {
    Random rand = new Random();
    long ret = rand.nextInt((int) (max - min + 1)) + min;
    return ret;
  }

  @Test
  public void testMultipleFiles() throws IOException {
    FileSystem fs = new OffheapFileSystem(new UpfrontAllocatingPageSource(new OffHeapBufferSource(), MAX_SIZE, MAX_CHUNK_SIZE, MIN_CHUNK_SIZE), BLOCK_SIZE, PAGE_SIZE, CONCURRENCY);
    try {
      Directory dir = fs.getOrCreateDirectory("testMultipleFiles");
      int NUM_FILES = 3;
      dir.deleteAllFiles();
      ConcurrentMap<File, OutputStream> fileToStreamMap = new ConcurrentHashMap<File, OutputStream>();
      for (int j = 0; j < NUM_FILES; j++) {
        fileToStreamMap.put(dir.getOrCreateFile(((Integer) j).toString()), dir.getOrCreateFile(((Integer) j).toString())
            .getOutputStream());
      }
      long[] randomArray = new long[NUM_FILES];
      try {
        randomArray = getRandoms(NUM_FILES, (int) (0.70 * MAX_SIZE));
      } catch (Exception e) {
        e.printStackTrace();
      }
  
      int k = 0;
      long totalBytesInDir = 0;
      for (Map.Entry<File, OutputStream> entry : fileToStreamMap.entrySet()) {
        System.out.println("---- Writing to file number = " + k + "------");
        long numBytesToWrite = randomArray[k++];
        for (long l = 0; l < numBytesToWrite; l++) {
          entry.getValue().write((byte) 100);
        }
        entry.getValue().flush();
        System.out.println("Wrote  " + numBytesToWrite + "  bytes");
        long numBytesUsedForThisFile = numBytesToWrite > BLOCK_SIZE ? (int) Math.ceil((double) (numBytesToWrite)
                                                                                     / (double) BLOCK_SIZE)
                                                                     * BLOCK_SIZE : BLOCK_SIZE;
        totalBytesInDir += numBytesUsedForThisFile;
        System.out.println("Total bytes used  = " + numBytesUsedForThisFile);
        System.out.println("Total bytes in directory = " + totalBytesInDir);
        assertThat(entry.getKey().getSizeInBytes(), is(numBytesUsedForThisFile));
        assertThat(dir.getSizeInBytes(), is(totalBytesInDir));
      }
  
      k = 0;
      for (Map.Entry<File, OutputStream> entry : fileToStreamMap.entrySet()) {
        System.out.println("---- Deleting file number = " + k + "------");
        long numBytesToDelete = randomArray[k++];
        entry.getValue().close();
        dir.deleteFile(entry.getKey().getName());
        int numBytesFreed = numBytesToDelete > BLOCK_SIZE ? (int) Math.ceil((double) (numBytesToDelete)
                                                                            / (double) BLOCK_SIZE)
                                                            * BLOCK_SIZE : BLOCK_SIZE;
        totalBytesInDir -= numBytesFreed;
        assertThat(dir.getSizeInBytes(), is(totalBytesInDir));
      }
    } finally {
      fs.delete();
    }
  }
}
