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
package org.terracotta.offheapstore.paging;

import org.terracotta.offheapstore.paging.OffHeapStorageArea;
import org.terracotta.offheapstore.disk.paging.MappedPageSource;
import org.terracotta.offheapstore.util.PointerSizeParameterizedTest;
import static org.terracotta.offheapstore.util.MemoryUnit.MEGABYTES;
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
