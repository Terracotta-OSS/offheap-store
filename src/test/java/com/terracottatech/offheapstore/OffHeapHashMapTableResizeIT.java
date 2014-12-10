/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore;

import java.util.Map;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Test;

import com.terracottatech.offheapstore.buffersource.OffHeapBufferSource;
import com.terracottatech.offheapstore.paging.UpfrontAllocatingPageSource;
import com.terracottatech.offheapstore.storage.IntegerStorageEngine;
import com.terracottatech.offheapstore.storage.SplitStorageEngine;

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
