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

import org.terracotta.offheapstore.WriteLockedOffHeapClockCache;
import java.util.Map;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import org.terracotta.offheapstore.buffersource.HeapBufferSource;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.paging.UnlimitedPageSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;
import org.terracotta.offheapstore.storage.IntegerStorageEngine;
import org.terracotta.offheapstore.storage.OffHeapBufferHalfStorageEngine;
import org.terracotta.offheapstore.storage.SplitStorageEngine;
import org.terracotta.offheapstore.storage.StorageEngine;
import org.terracotta.offheapstore.storage.portability.ByteArrayPortability;
import org.terracotta.offheapstore.util.MemoryUnit;

public class ExtremeTableMutatingIT {

  @Test
  public void testExtremeTableMutations() {
    PageSource source = new UpfrontAllocatingPageSource(new HeapBufferSource(), MemoryUnit.MEGABYTES.toBytes(1), MemoryUnit.MEGABYTES.toBytes(1));
    StorageEngine<Integer, byte[]> storage = new SplitStorageEngine<>(new IntegerStorageEngine(), new OffHeapBufferHalfStorageEngine<>(source, MemoryUnit.KILOBYTES
      .toBytes(4), ByteArrayPortability.INSTANCE));
    Map<Integer, byte[]> cache = new EvilOffHeapClockCache<>(new UnlimitedPageSource(new HeapBufferSource()), storage);

    int i;
    for (i = 0; i == cache.size(); i++) {
      cache.put(i, new byte[i % 1024]);
    }

    for (int c = 0; c < i * 10; c++) {
      cache.put(i + c, new byte[(i + c) % 1024]);
    }

    for (int n = 0; n < i * 11; n++) {
      byte[] array = cache.get(n);
      if (array != null) {
        Assert.assertEquals(n % 1024, array.length);
      }
    }
  }

  static class EvilOffHeapClockCache<K, V> extends WriteLockedOffHeapClockCache<K, V> {

    private final Random rndm = new Random();

    public EvilOffHeapClockCache(PageSource source, StorageEngine<? super K, ? super V> storageEngine) {
      super(source, storageEngine);
    }

    @Override
    protected void storageEngineFailure(Object failure) {
      if (rndm.nextFloat() < 0.1f) {
        clear();
      } else {
        super.storageEngineFailure(failure);
      }
    }

  }
}
