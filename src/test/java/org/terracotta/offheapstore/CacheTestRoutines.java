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
package org.terracotta.offheapstore;

import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.Assert;
import org.terracotta.offheapstore.exceptions.OversizeMappingException;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.number.OrderingComparison.lessThan;
import static org.junit.Assert.assertThat;

public final class CacheTestRoutines {

  private CacheTestRoutines() {
    //
  }

  public static void testCacheEviction(Map<Integer, Integer> cache, CapacityLimitedIntegerStorageEngineFactory engine) {
    Random rndm = new Random();

    int toAdd = 100 + rndm.nextInt(90);

    engine.setCapacity(toAdd);

    for (int i = 0; i < toAdd; i++) {
      cache.put(i, i);

      Assert.assertEquals(i + 1, cache.size());
      for (int j = 0; j <= i; j++) {
        Assert.assertTrue(cache.get(j) == j);
      }
    }

    int toEvict = 100 + rndm.nextInt(90);

    for (int i = toAdd; i < toEvict + toAdd; i++) {
      cache.put(i, i);

      Assert.assertEquals(toAdd, cache.size());
      Assert.assertTrue(cache.get(i) == i);
    }
  }

  public static void testCacheEvictionMinimal(Map<Integer, Integer> cache) {
    for (int i = 0; i < 200; i++) {
      cache.put(i, i);
      Assert.assertTrue(cache.get(i) == i);
    }
  }

  public static void testCacheEvictionThreshold(Map<Integer, byte[]> cache) {
    Assert.assertNull(cache.put(0, new byte[100]));
    try {
      cache.put(0, new byte[1024 * 1024]);
      Assert.fail("Expected OversizeMappingException");
    } catch (OversizeMappingException e) {
      //expected
    }
  }

  public static void testFillBehavior(Map<Integer, byte[]> cache) {
    for (int i = 0; i < 100000; i++) {
      Set<Integer> keySetCopy = new HashSet<>(cache.keySet());
      AbstractOffHeapMapIT.doFill(cache, i, new byte[i % 1024]);
      if (cache.containsKey(i)) {
        Assert.assertNotNull(cache.get(i));
        Assert.assertEquals(i % 1024, cache.get(i).length);
        Assert.assertTrue(keySetCopy.add(i));
        Assert.assertEquals(keySetCopy, cache.keySet());
      } else {
        Assert.assertNull(cache.get(i));
        Assert.assertEquals(keySetCopy, cache.keySet());
      }
    }

    assertThat(cache.isEmpty(), is(false));

    Set<Integer> keySetCopy = new HashSet<>(cache.keySet());

    for (Integer k : keySetCopy) {
      if (AbstractOffHeapMapIT.doFill(cache, k, new byte[(k % 1024) + 1]) == null) {
        if (cache.containsKey(k)) {
          Assert.assertEquals((k % 1024) + 1, cache.get(k).length);
        }
      } else {
        Assert.assertTrue(cache.containsKey(k));
        Assert.assertEquals((k % 1024) + 1, cache.get(k).length);
      }
    }
  }

  public static void testComputeEvictionBehavior(Map<Integer, byte[]> cache) {
    for (int i = 0; i < 100000; i++) {
      AbstractOffHeapMapIT.doComputeWithMetadata(cache, i, (t, u) -> MetadataTuple.metadataTuple(new byte[t % 1024], 0));
      assertThat(cache.get(i).length, is(i % 1024));
    }

    assertThat(cache.isEmpty(), is(false));
    assertThat(cache.size(), lessThan(100000));

    for (Integer k : cache.keySet()) {
      assertThat(cache.get(k).length, is(k % 1024));
    }
  }
}
