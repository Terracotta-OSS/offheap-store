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
package org.terracotta.offheapstore.disk;

import org.terracotta.offheapstore.OffHeapHashMap;
import org.terracotta.offheapstore.disk.paging.MappedPageSource;
import org.terracotta.offheapstore.paging.OffHeapStorageArea;
import org.terracotta.offheapstore.paging.Page;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.storage.IntegerStorageEngine;
import org.terracotta.offheapstore.storage.SplitStorageEngine;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.hamcrest.core.Is;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author cdennis
 */
public class OffHeapDiskMapClearingIT extends AbstractDiskTest {

  @Test
  public void testFrequentClearing() throws IOException {
    CountingPageSource source = new CountingPageSource(new MappedPageSource(dataFile));
    Map<Integer, Integer> map = new OffHeapHashMap<>(source, new SplitStorageEngine<>(IntegerStorageEngine
      .instance(), IntegerStorageEngine.instance()));

    for (int i = 0; i < 100000; i++) {
      map.clear();
    }
    Assert.assertThat(source.allocations.get(), Is.is(1));
  }

  static class CountingPageSource implements PageSource {

    private final AtomicInteger allocations = new AtomicInteger();
    private final PageSource delegate;

    public CountingPageSource(PageSource delegate) {
      this.delegate = delegate;
    }

    @Override
    public Page allocate(int size, boolean thief, boolean victim, OffHeapStorageArea owner) {
      allocations.incrementAndGet();
      return delegate.allocate(size, thief, victim, owner);
    }

    @Override
    public void free(Page page) {
      delegate.free(page);
    }

  }
}
