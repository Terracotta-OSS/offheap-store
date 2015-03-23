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

import org.terracotta.offheapstore.OffHeapHashMap;
import java.util.Map;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Test;

import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;
import org.terracotta.offheapstore.storage.IntegerStorageEngine;
import org.terracotta.offheapstore.storage.SplitStorageEngine;

import org.junit.Ignore;

/**
 *
 * @author cdennis
 */
@Ignore("not asserting anything")
public class OffHeapHashMapTableResizeIT {

  @Test
  public void testLargeTableResizes() {
    Logger logger = Logger.getLogger("com.terracottatech.offheapstore.OffHeapHashMap");
    logger.setLevel(Level.ALL);
    ConsoleHandler handler = new ConsoleHandler();
    handler.setLevel(Level.ALL);
    logger.addHandler(handler);
    try {
      Map<Integer, Integer> map = new OffHeapHashMap<Integer, Integer>(new UpfrontAllocatingPageSource(new OffHeapBufferSource(), 32 * 1024 * 1024, 32 * 1024 * 1024), new SplitStorageEngine<Integer, Integer>(new IntegerStorageEngine(), new IntegerStorageEngine()), 1);

      for (int i = 0; i < 500000; i++) {
        map.put(i, i);
      }
    } finally {
      logger.setLevel(Level.INFO);
      logger.removeHandler(handler);
    }
  }

  public static void main(String[] args) {
    new OffHeapHashMapTableResizeIT().testLargeTableResizes();
  }
}
