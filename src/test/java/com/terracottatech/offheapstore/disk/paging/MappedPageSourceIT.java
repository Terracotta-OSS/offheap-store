/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.disk.paging;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.terracottatech.offheapstore.OffHeapHashMap;
import com.terracottatech.offheapstore.disk.AbstractDiskTest;
import com.terracottatech.offheapstore.storage.IntegerStorageEngine;
import com.terracottatech.offheapstore.storage.SplitStorageEngine;

/**
 *
 * @author Chris Dennis
 */
public class MappedPageSourceIT extends AbstractDiskTest {

  @Test
  public void testMappedBufferIsConnected() throws IOException {
    MappedPageSource source = new MappedPageSource(dataFile);
    try {
      MappedPage page = source.allocate(128, false, false, null);
      byte[] data = new byte[page.size()];
      Arrays.fill(data, (byte) 0xff);
      page.asByteBuffer().put(data);
      page.asByteBuffer().force();
      source.free(page);
    } finally {
      source.close();
    }

    RandomAccessFile raf = new RandomAccessFile(source.getFile(), "r");
    try {
      Assert.assertEquals(128, raf.length());
      byte[] data = new byte[(int) raf.length()];
      raf.readFully(data);

      for (byte b : data) {
        Assert.assertEquals((byte) 0xff, b);
      }
    } finally {
      raf.close();
    }
  }

  @Test
  public void testCreateLotsOfMappedBuffers() throws IOException {
    final int size = 1024 * 1024;
    final int count = 16;
    
    MappedPageSource source = new MappedPageSource(dataFile);
    try {
      byte[] data = new byte[1024];
      Arrays.fill(data, (byte) 0xff);

      List<MappedPage> pages = new ArrayList<MappedPage>();
      for (int i = 0; i < count; i++) {
        pages.add(source.allocate(size, false, false, null));
      }

      for (MappedPage p : pages) {
        MappedByteBuffer b = p.asByteBuffer();
        while (b.hasRemaining()) {
          b.put(data);
        }
        b.force();
      }
    } finally {
      source.close();
    }

    RandomAccessFile raf = new RandomAccessFile(source.getFile(), "r");
    try {
      Assert.assertEquals(size * count, raf.length());
    } finally {
      raf.close();
    }
  }

  @Test
  public void testStraightToFileMap() throws IOException {
    final int size = 1024;

    MappedPageSource source = new MappedPageSource(dataFile);
    try {
      OffHeapHashMap<Integer, Integer> map = new OffHeapHashMap<Integer, Integer>(source, new SplitStorageEngine<Integer, Integer>(new IntegerStorageEngine(), new IntegerStorageEngine()));

      for (int i = 0; i < size; i++) {
        map.put(i, i);
      }

      for (int i = 0; i < size; i++) {
        Assert.assertTrue(map.containsKey(i));
        Assert.assertEquals(i, map.get(i).intValue());
      }

      for (int i = 0; i < size; i++) {
        map.remove(i);
      }

      Assert.assertTrue(map.isEmpty());
      Assert.assertEquals(0, map.getDataOccupiedMemory());
      Assert.assertEquals(0, map.getOccupiedMemory());
    } finally {
      source.close();
    }
  }
}
