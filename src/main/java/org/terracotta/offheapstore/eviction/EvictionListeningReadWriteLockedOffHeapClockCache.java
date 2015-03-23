/* 
 * Copyright 2015 Terracotta, Inc., a Software AG company.
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
package org.terracotta.offheapstore.eviction;

import java.util.concurrent.Callable;

import org.terracotta.offheapstore.ReadWriteLockedOffHeapClockCache;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.storage.StorageEngine;

/**
 *
 * @author Chris Dennis
 */
public class EvictionListeningReadWriteLockedOffHeapClockCache<K, V> extends ReadWriteLockedOffHeapClockCache<K, V>{

  private final EvictionListener<K, V> listener;

  public EvictionListeningReadWriteLockedOffHeapClockCache(EvictionListener<K, V> listener, PageSource source, StorageEngine<? super K, ? super V> storageEngine) {
    super(source, storageEngine);
    this.listener = listener;
  }

  public EvictionListeningReadWriteLockedOffHeapClockCache(EvictionListener<K, V> listener, PageSource source, StorageEngine<? super K, ? super V> storageEngine, int tableSize) {
    super(source, storageEngine, tableSize);
    this.listener = listener;
  }

  @Override
  public boolean evict(final int index, boolean shrink) {
    boolean evicted;
    try {
      listener.evicting(new Callable<Entry<K, V>>() {
        @Override
        public Entry<K, V> call() throws Exception {
          return getEntryAtTableOffset(index);
        }
      });
    } finally {
      evicted = super.evict(index, shrink);
    }
    return evicted;
  }
}
