/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.storage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import com.terracottatech.offheapstore.OffHeapHashMap;
import com.terracottatech.offheapstore.buffersource.HeapBufferSource;
import com.terracottatech.offheapstore.paging.UnlimitedPageSource;

/**
 *
 * @author cdennis
 */
public class IntegerStorageEngineIT {

  private static final Random rndm = new Random();
  private static final Integer[] TEST_CASES = new Integer[] {0, 1, -1, Integer.MAX_VALUE, Integer.MIN_VALUE, rndm.nextInt()};

  @Test
  public void testDirectUsage() {
    HalfStorageEngine<Integer> engine = new IntegerStorageEngine();

    for (Integer i : TEST_CASES) {
      int value = engine.write(i, 0);
      Integer result = engine.read(value);
      Assert.assertEquals(i, result);
    }
  }

  @Test
  public void testMapUsage() {
    Map<Integer, Integer> map = new OffHeapHashMap<Integer, Integer>(new UnlimitedPageSource(new HeapBufferSource()), new SplitStorageEngine<Integer, Integer>(new IntegerStorageEngine(), new IntegerStorageEngine()));

    Random rndm = new Random();

    for (Integer k : TEST_CASES) {
      Integer v = TEST_CASES[rndm.nextInt(TEST_CASES.length)];
      Assert.assertNull(map.put(k, v));
      Assert.assertTrue(map.containsKey(k));
      Assert.assertEquals(v.intValue(), map.get(k).intValue());
      Assert.assertTrue(map.containsValue(v.intValue()));
      Assert.assertTrue(map.keySet().contains(k));
      Assert.assertTrue(map.values().contains(v.intValue()));
    }

    Assert.assertEquals(TEST_CASES.length, map.size());

    for (Integer k : TEST_CASES) {
      Assert.assertNotNull(map.remove(k).intValue());
    }
  }

  @Test
  public void testKeysAreReadable() {
    Map<Integer, Integer> map = new OffHeapHashMap<Integer, Integer>(new UnlimitedPageSource(new HeapBufferSource()), new SplitStorageEngine<Integer, Integer>(new IntegerStorageEngine(), new IntegerStorageEngine()));

    for (Integer k : TEST_CASES) {
      Assert.assertNull(map.put(k, k));
    }

    Collection<Integer> copy = new ArrayList<Integer>(Arrays.asList(TEST_CASES));

    for (Integer k : map.keySet()) {
      Assert.assertTrue(copy.remove(k));
    }
    Assert.assertTrue(copy.isEmpty());
  }
}
