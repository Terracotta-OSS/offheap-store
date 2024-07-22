/*
 * Copyright 2015-2023 Terracotta, Inc., a Software AG company.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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

import java.util.Arrays;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.terracotta.offheapstore.OffHeapHashMap;
import org.terracotta.offheapstore.WriteLockedOffHeapClockCache;
import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import org.terracotta.offheapstore.paging.UnlimitedPageSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;
import org.terracotta.offheapstore.storage.portability.ByteArrayPortability;
import org.terracotta.offheapstore.storage.portability.SerializablePortability;
import org.terracotta.offheapstore.util.MemoryUnit;
import org.terracotta.offheapstore.util.OffHeapAndDiskStorageEngineDependentTest.OffHeapTestMode;
import org.terracotta.offheapstore.util.ParallelParameterized;
import org.terracotta.offheapstore.util.StorageEngineDependentTest;

@RunWith(Parameterized.class)
public class OffHeapBufferStorageEngineIT extends StorageEngineDependentTest {

  @ParallelParameterized.Parameters
  public static Iterable<Object[]> data() {
    return Arrays.asList(
            new Object[] {OffHeapTestMode.REGULAR_INT},
            new Object[] {OffHeapTestMode.REGULAR_LONG},
            new Object[] {OffHeapTestMode.COMPRESSING_INT},
            new Object[] {OffHeapTestMode.COMPRESSING_LONG}
    );
  }

  public OffHeapBufferStorageEngineIT(TestMode mode) {
    super(mode);
  }

  @Test
  public void testEmptyPayload() {
    StorageEngine<byte[], byte[]> engine = create(createPageSource(1, MemoryUnit.MEGABYTES), ByteArrayPortability.INSTANCE, ByteArrayPortability.INSTANCE);

    long p = engine.writeMapping(new byte[0], new byte[0], 0, 0);
    Assert.assertTrue(p >= 0);

    byte[] b = engine.readKey(p, 0);

    Assert.assertNotNull(b);
    Assert.assertEquals(0, b.length);
  }

  @Test
  public void testSmallPayloads() {
    StorageEngine<byte[], byte[]> engine = create(new UnlimitedPageSource(new OffHeapBufferSource()), ByteArrayPortability.INSTANCE, ByteArrayPortability.INSTANCE);

    Random rndm = new Random();

    for (int i = 0; i < 64; i++) {
      byte[] b = new byte[i];

      rndm.nextBytes(b);

      long p = engine.writeMapping(b, b, 0, 0);
      Assert.assertTrue(p >= 0);

      byte[] b2 = engine.readKey(p, 0);

      Assert.assertNotNull(b2);
      Assert.assertArrayEquals(b, b2);

      engine.freeMapping(p, 0, true);
    }
  }

  @Test
  public void testPortabilityCacheInvalidation() {
    OffHeapHashMap<Integer, int[]> map = new WriteLockedOffHeapClockCache<>(new UnlimitedPageSource(new OffHeapBufferSource()), create(new UpfrontAllocatingPageSource(new OffHeapBufferSource(), 1024, 1024), new SerializablePortability(), new SerializablePortability()));

    int[] mutable = new int[1];
    for (int i = 0; i < 100; i++) {
      mutable[0] = i;
      map.put(i, mutable);
      Assert.assertEquals(i, map.get(i)[0]);
    }
  }
}
