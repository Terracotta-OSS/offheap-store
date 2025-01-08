/*
 * Copyright 2015-2023 Terracotta, Inc., a Software AG company.
 * Copyright IBM Corp. 2024, 2025
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

import org.terracotta.offheapstore.storage.LongStorageEngine;
import org.terracotta.offheapstore.storage.IntegerStorageEngine;
import org.terracotta.offheapstore.storage.StorageEngine;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import org.terracotta.offheapstore.OffHeapHashMap;
import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import org.terracotta.offheapstore.paging.UnlimitedPageSource;

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
    StorageEngine<Long, Integer> engine = new LongStorageEngine<>(new IntegerStorageEngine());

    for (Long l : TEST_CASES) {
      int hashCode = l.hashCode();
      long value = engine.writeMapping(l, 42, 0, 0);

      Long result = engine.readKey(value, hashCode);

      Assert.assertEquals(l, result);
    }
  }

  @Test
  public void testMapUsage() {
    Map<Long, Integer> map = new OffHeapHashMap<>(new UnlimitedPageSource(new OffHeapBufferSource()), new LongStorageEngine<>(new IntegerStorageEngine()));

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
    Map<Long, Integer> map = new OffHeapHashMap<>(new UnlimitedPageSource(new OffHeapBufferSource()), new LongStorageEngine<>(new IntegerStorageEngine()));

    for (Long l : TEST_CASES) {
      Assert.assertNull(map.put(l, l.intValue()));
    }

    Collection<Long> copy = new ArrayList<>(Arrays.asList(TEST_CASES));

    for (Long l : map.keySet()) {
      Assert.assertTrue(copy.remove(l));
    }
    Assert.assertTrue(copy.isEmpty());
  }
}
