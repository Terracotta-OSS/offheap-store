/*
 * Copyright 2015-2023 Terracotta, Inc., a Software AG company.
 * Copyright IBM Corp. 2026
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

import java.lang.reflect.Method;

import org.junit.Test;
import org.terracotta.offheapstore.buffersource.HeapBufferSource;
import org.terracotta.offheapstore.paging.UnlimitedPageSource;
import org.terracotta.offheapstore.storage.IntegerStorageEngine;
import org.terracotta.offheapstore.storage.SplitStorageEngine;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class AbstractOffHeapClockCacheTest {

  @Test
  public void testWrappedProbeSequenceIncludesSlotZero() throws Exception {
    TestClockCache cache = new TestClockCache();
    cache.setStatus(124, TestClockCache.USED | Metadata.PINNED);
    cache.setStatus(0, TestClockCache.USED);
    cache.setStatus(4, TestClockCache.USED);

    assertThat(cache.probeForEviction(124, 2), is(0));
    assertThat(cache.evictedIndex, is(4));
  }

  @Test
  public void testProbeSequenceDoesNotIncludeSlotAfterItsEnd() throws Exception {
    TestClockCache cache = new TestClockCache();
    cache.setStatus(16, TestClockCache.USED | Metadata.PINNED);
    cache.setStatus(20, TestClockCache.USED | Metadata.PINNED);
    cache.setStatus(24, TestClockCache.USED);
    cache.setStatus(4, TestClockCache.USED);

    assertThat(cache.probeForEviction(16, 2), is(-1));
    assertThat(cache.evictedIndex, is(4));
  }

  private static class TestClockCache extends ReadWriteLockedOffHeapClockCache<Integer, Integer> {
    private static final int USED = STATUS_USED;

    private int evictedIndex = -1;

    TestClockCache() {
      super(new UnlimitedPageSource(new HeapBufferSource()),
          new SplitStorageEngine<>(new IntegerStorageEngine(), new IntegerStorageEngine()), 32);
    }

    void setStatus(int slot, int status) {
      hashtable.put(slot + STATUS, status);
    }

    int probeForEviction(int start, int length) throws Exception {
      Method method = AbstractOffHeapClockCache.class.getDeclaredMethod("getEvictionIndex", int.class, int.class);
      method.setAccessible(true);
      return (Integer) method.invoke(this, start, length);
    }

    @Override
    public boolean evict(int index, boolean shrink) {
      evictedIndex = index;
      return true;
    }
  }
}
