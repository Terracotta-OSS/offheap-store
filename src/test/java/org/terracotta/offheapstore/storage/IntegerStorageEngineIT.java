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
package org.terracotta.offheapstore.storage;

import org.terracotta.offheapstore.storage.SplitStorageEngine;
import org.terracotta.offheapstore.storage.HalfStorageEngine;
import org.terracotta.offheapstore.storage.IntegerStorageEngine;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import org.terracotta.offheapstore.OffHeapHashMap;
import org.terracotta.offheapstore.buffersource.HeapBufferSource;
import org.terracotta.offheapstore.paging.UnlimitedPageSource;

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
    Map<Integer, Integer> map = new OffHeapHashMap<>(new UnlimitedPageSource(new HeapBufferSource()), new SplitStorageEngine<>(new IntegerStorageEngine(), new IntegerStorageEngine()));

    Random rndm = new Random();

    for (Integer k : TEST_CASES) {
      Integer v = TEST_CASES[rndm.nextInt(TEST_CASES.length)];
      Assert.assertNull(map.put(k, v));
      Assert.assertTrue(map.containsKey(k));
      Assert.assertEquals(v.intValue(), map.get(k).intValue());
      Assert.assertTrue(map.containsValue(v));
      Assert.assertTrue(map.keySet().contains(k));
      Assert.assertTrue(map.values().contains(v));
    }

    Assert.assertEquals(TEST_CASES.length, map.size());

    for (Integer k : TEST_CASES) {
      Assert.assertNotNull(map.remove(k));
    }
  }

  @Test
  public void testKeysAreReadable() {
    Map<Integer, Integer> map = new OffHeapHashMap<>(new UnlimitedPageSource(new HeapBufferSource()), new SplitStorageEngine<>(new IntegerStorageEngine(), new IntegerStorageEngine()));

    for (Integer k : TEST_CASES) {
      Assert.assertNull(map.put(k, k));
    }

    Collection<Integer> copy = new ArrayList<>(Arrays.asList(TEST_CASES));

    for (Integer k : map.keySet()) {
      Assert.assertTrue(copy.remove(k));
    }
    Assert.assertTrue(copy.isEmpty());
  }
}
