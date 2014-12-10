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
import com.terracottatech.offheapstore.buffersource.OffHeapBufferSource;
import com.terracottatech.offheapstore.paging.UnlimitedPageSource;

/**
 *
 * @author Chris Dennis
 */
public class LongStorageEngineIT {

  private static final Random rndm = new Random();
  private static final Long[] TEST_CASES = new Long[] {0L, 1L, -1L, (long) Integer.MAX_VALUE, (long) Integer.MIN_VALUE,
                                                       Long.MAX_VALUE, Long.MIN_VALUE, (long) rndm.nextInt(), rndm.nextLong()};

  @Test
  public void testDirectUsage() {
    StorageEngine<Long, Integer> engine = new LongStorageEngine<Integer>(new IntegerStorageEngine());

    for (Long l : TEST_CASES) {
      int hashCode = l.hashCode();
      long value = engine.writeMapping(l, 42, 0, 0);

      Long result = engine.readKey(value, hashCode);

      Assert.assertEquals(l, result);
    }
  }

  @Test
  public void testMapUsage() {
    Map<Long, Integer> map = new OffHeapHashMap<Long, Integer>(new UnlimitedPageSource(new OffHeapBufferSource()), new LongStorageEngine<Integer>(new IntegerStorageEngine()));

    for (Long l : TEST_CASES) {
      Assert.assertNull(map.put(l, l.intValue()));
      Assert.assertTrue(map.containsKey(l));
      Assert.assertEquals(l.intValue(), map.get(l).intValue());
      Assert.assertTrue(map.containsValue(l.intValue()));
      Assert.assertTrue(map.keySet().contains(l));
      Assert.assertTrue(map.values().contains(l.intValue()));
    }

    Assert.assertEquals(TEST_CASES.length, map.size());

    for (Long l : TEST_CASES) {
      Assert.assertEquals(l.intValue(), map.remove(l).intValue());
    }
  }

  @Test
  public void testKeysAreReadable() {
    Map<Long, Integer> map = new OffHeapHashMap<Long, Integer>(new UnlimitedPageSource(new OffHeapBufferSource()), new LongStorageEngine<Integer>(new IntegerStorageEngine()));

    for (Long l : TEST_CASES) {
      Assert.assertNull(map.put(l, l.intValue()));
    }

    Collection<Long> copy = new ArrayList<Long>(Arrays.asList(TEST_CASES));

    for (Long l : map.keySet()) {
      Assert.assertTrue(copy.remove(l));
    }
    Assert.assertTrue(copy.isEmpty());
  }
}
