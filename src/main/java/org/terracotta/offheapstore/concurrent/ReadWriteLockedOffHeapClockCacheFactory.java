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
package org.terracotta.offheapstore.concurrent;

import org.terracotta.offheapstore.ReadWriteLockedOffHeapClockCache;
import org.terracotta.offheapstore.eviction.EvictionListener;
import org.terracotta.offheapstore.eviction.EvictionListeningReadWriteLockedOffHeapClockCache;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.storage.StorageEngine;
import org.terracotta.offheapstore.util.Factory;

/**
 * Factory of {@link ReadWriteLockedOffHeapClockCache} instances.
 *
 * @param <K> the type of keys held by the generated caches
 * @param <V> the type of mapped values
 *
 * @author Chris Dennis
 */
public class ReadWriteLockedOffHeapClockCacheFactory<K, V> implements Factory<ReadWriteLockedOffHeapClockCache<K, V>> {

  private static final int DEFAULT_TABLE_SIZE = 128;

  private final Factory<? extends StorageEngine<? super K, ? super V>> storageEngineFactory;
  private final PageSource tableSource;
  private final int tableSize;

  private final EvictionListener<K, V> evictionListener;

  /**
   * Creates segments using the given table buffer source and storage engine
   * factory.
   *
   * @param tableSource buffer source from which the segment hash tables are allocated
   * @param storageEngineFactory factory for the segment storage engines
   */
  public ReadWriteLockedOffHeapClockCacheFactory(PageSource tableSource, Factory<? extends StorageEngine<? super K, ? super V>> storageEngineFactory) {
    this(tableSource, storageEngineFactory, DEFAULT_TABLE_SIZE);
  }
  
  /**
   * Creates segments using the given table buffer source and storage engine
   * factory.
   *
   * @param tableSource buffer source from which the segment hash tables are allocated
   * @param storageEngineFactory factory for the segment storage engines
   * @param evictionListener listener notified on evictions
   */
  public ReadWriteLockedOffHeapClockCacheFactory(PageSource tableSource, Factory<? extends StorageEngine<? super K, ? super V>> storageEngineFactory, EvictionListener<K, V> evictionListener) {
    this(tableSource, storageEngineFactory, evictionListener, DEFAULT_TABLE_SIZE);
  }

  /**
   * Creates segments using the given table buffer source, storage engine
   * factory, and initial table size.
   *
   * @param tableSource buffer source from which the segment hash tables are allocated
   * @param storageEngineFactory factory for the segment storage engines
   * @param tableSize initial table size for each segment
   */
  public ReadWriteLockedOffHeapClockCacheFactory(PageSource tableSource, Factory<? extends StorageEngine<? super K, ? super V>> storageEngineFactory, int tableSize) {
    this(tableSource, storageEngineFactory, null, tableSize);
  }

  /**
   * Creates segments using the given table buffer source, storage engine
   * factory, eviction listener and initial table size.
   *
   * @param tableSource buffer source from which the segment hash tables are allocated
   * @param storageEngineFactory factory for the segment storage engines
   * @param evictionListener  listener notified on evictions
   * @param tableSize initial table size for each segment
   */
  public ReadWriteLockedOffHeapClockCacheFactory(PageSource tableSource, Factory<? extends StorageEngine<? super K, ? super V>> storageEngineFactory, EvictionListener<K, V> evictionListener, int tableSize) {
    this.storageEngineFactory = storageEngineFactory;
    this.tableSource = tableSource;
    this.tableSize = tableSize;

    this.evictionListener = evictionListener;
  }

  /**
   * Creates a new {@code ReadWriteLockedOffHeapClockCache} for use in a
   * segmented cache.
   *
   * @return a new {@code ReadWriteLockedOffHeapClockCache}
   */
  @Override
  public ReadWriteLockedOffHeapClockCache<K, V> newInstance() {
    StorageEngine<? super K, ? super V> storageEngine = storageEngineFactory.newInstance();
    try {
      if (evictionListener == null) {
        return new ReadWriteLockedOffHeapClockCache<K, V>(tableSource, storageEngine, tableSize);
      } else {
        return new EvictionListeningReadWriteLockedOffHeapClockCache<K, V>(evictionListener, tableSource, storageEngine, tableSize);
      }
    } catch (RuntimeException e) {
      storageEngine.destroy();
      throw e;
    }
  }

}
