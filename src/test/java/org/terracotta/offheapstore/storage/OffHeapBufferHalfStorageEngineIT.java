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

import org.terracotta.offheapstore.storage.SplitStorageEngine;
import org.terracotta.offheapstore.storage.HalfStorageEngine;
import org.terracotta.offheapstore.storage.IntegerStorageEngine;
import org.terracotta.offheapstore.storage.OffHeapBufferHalfStorageEngine;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import org.terracotta.offheapstore.OffHeapHashMap;
import org.terracotta.offheapstore.WriteLockedOffHeapClockCache;
import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import org.terracotta.offheapstore.paging.UnlimitedPageSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;
import org.terracotta.offheapstore.storage.portability.ByteArrayPortability;
import org.terracotta.offheapstore.storage.portability.SerializablePortability;

/**
 *
 * @author cdennis
 */
public class OffHeapBufferHalfStorageEngineIT {

  @Test
  public void testEmptyPayload() {
    HalfStorageEngine<byte[]> engine = new OffHeapBufferHalfStorageEngine<>(new UnlimitedPageSource(new OffHeapBufferSource()), 1024, ByteArrayPortability.INSTANCE);

    int p = engine.write(new byte[0], 0);
    Assert.assertTrue(p >= 0);

    byte[] b = engine.read(p);

    Assert.assertNotNull(b);
    Assert.assertEquals(0, b.length);
  }

  @Test
  public void testSmallPayloads() {
    HalfStorageEngine<byte[]> engine = new OffHeapBufferHalfStorageEngine<>(new UnlimitedPageSource(new OffHeapBufferSource()), 1024, ByteArrayPortability.INSTANCE);

    Random rndm = new Random();

    for (int i = 0; i < 64; i++) {
      byte[] b = new byte[i];

      rndm.nextBytes(b);

      int p = engine.write(b, 0);
      Assert.assertTrue(p >= 0);

      byte[] b2 = engine.read(p);

      Assert.assertNotNull(b2);
      Assert.assertArrayEquals(b, b2);

      engine.free(p);
    }
  }

  @Test
  public void testPortabilityCacheInvalidation() {
    OffHeapHashMap<Integer, int[]> map = new WriteLockedOffHeapClockCache<>(new UnlimitedPageSource(new OffHeapBufferSource()), new SplitStorageEngine<>(new IntegerStorageEngine(), new OffHeapBufferHalfStorageEngine<>(new UpfrontAllocatingPageSource(new OffHeapBufferSource(), 1024, 1024), 1024, new SerializablePortability())));

    int[] mutable = new int[1];
    for (int i = 0; i < 100; i++) {
      mutable[0] = i;
      map.put(i, mutable);
      Assert.assertEquals(i, map.get(i)[0]);
    }
  }
}
