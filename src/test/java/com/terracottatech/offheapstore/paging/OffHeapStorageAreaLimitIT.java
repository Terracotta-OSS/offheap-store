/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.paging;

import com.terracottatech.offheapstore.disk.paging.MappedPageSource;
import com.terracottatech.offheapstore.util.PointerSizeParameterizedTest;
import static com.terracottatech.offheapstore.util.MemoryUnit.MEGABYTES;
import java.io.File;
import java.io.IOException;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 * @author Chris Dennis
 */
@Ignore
public class OffHeapStorageAreaLimitIT extends PointerSizeParameterizedTest {

  @Test
  public void testFixedPageSizes() throws IOException {
    File temp = new File("target/OffHeapStorageAreaLimitTest.temp");
    OffHeapStorageArea storage = new OffHeapStorageArea(getPointerSize(), null, new MappedPageSource(temp), MEGABYTES.toBytes(1), false, false);
    try {
      while (true) {
        if (storage.allocate(MEGABYTES.toBytes(1)) < 0) {
          System.err.println(storage);
          break;
        }
      }
    } finally {
      storage.destroy();
    }
  }

  @Test
  public void testVariablePageSizes() throws IOException {
    File temp = new File("target/OffHeapStorageAreaLimitTest.temp");
    OffHeapStorageArea storage = new OffHeapStorageArea(getPointerSize(), null, new MappedPageSource(temp), 1, Integer.MAX_VALUE,false, false);
    try {
      while (true) {
        if (storage.allocate(MEGABYTES.toBytes(1)) < 0) {
          System.err.println(storage);
          break;
        }
      }
    } finally {
      storage.destroy();
    }
  }
  
  @Test
  public void testCappedVariablePageSizes() throws IOException {
    File temp = new File("target/OffHeapStorageAreaLimitTest.temp");
    OffHeapStorageArea storage = new OffHeapStorageArea(getPointerSize(), null, new MappedPageSource(temp), 1, MEGABYTES.toBytes(8),false, false);
    try {
      while (true) {
        if (storage.allocate(MEGABYTES.toBytes(1)) < 0) {
          System.err.println(storage);
          break;
        }
      }
    } finally {
      storage.destroy();
    }
  }
}
