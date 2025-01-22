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
package org.terracotta.offheapstore;

import java.util.Map;

import org.junit.Test;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

import org.slf4j.LoggerFactory;
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
    Logger logger = (Logger)LoggerFactory.getLogger(OffHeapHashMap.class);
    Level oldLevel = logger.getLevel();
    logger.setLevel(Level.ALL);
    try {
      Map<Integer, Integer> map = new OffHeapHashMap<>(new UpfrontAllocatingPageSource(new OffHeapBufferSource(), 32 * 1024 * 1024, 32 * 1024 * 1024), new SplitStorageEngine<>(new IntegerStorageEngine(), new IntegerStorageEngine()), 1);

      for (int i = 0; i < 500000; i++) {
        map.put(i, i);
      }
    } finally {
      logger.setLevel(oldLevel);
    }
  }

  public static void main(String[] args) {
    new OffHeapHashMapTableResizeIT().testLargeTableResizes();
  }
}
