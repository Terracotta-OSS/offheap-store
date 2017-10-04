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

import org.terracotta.offheapstore.AbstractOffHeapClockCache;
import org.terracotta.offheapstore.ReadWriteLockedOffHeapClockCache;
import org.terracotta.offheapstore.eviction.EvictionListener;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.storage.StorageEngine;
import org.terracotta.offheapstore.util.Factory;

/**
 * A striped concurrent-read/exclusive-write clock cache.
 * <p>
 * This implementation uses instances of {@link ReadWriteLockedOffHeapClockCache}
 * for its segments.
 *
 * @param <K> the type of keys maintained by this cache
 * @param <V> the type of mapped values
 *
 * @author Chris Dennis
 * @see ReadWriteLockedOffHeapClockCache
 */
public class ConcurrentOffHeapClockCache<K, V> extends AbstractConcurrentOffHeapCache<K, V> {

  /**
   * Creates a cache using the given table buffer source and storage engine
   * factory.
   *
   * @param segmentFactory the factory to use to build segments
   * @param concurrency the number of segments to use
   */
  public ConcurrentOffHeapClockCache(Factory<? extends AbstractOffHeapClockCache<K, V>> segmentFactory, int concurrency) {
    super(segmentFactory, concurrency);
  }

  /**
   * Creates a cache using the given table buffer source and storage engine
   * factory.
   *
   * @param tableSource buffer source from which hash tables are allocated
   * @param storageEngineFactory factory for the segment storage engines
   */
  public ConcurrentOffHeapClockCache(PageSource tableSource, Factory<? extends StorageEngine<? super K, ? super V>> storageEngineFactory) {
    super(new ReadWriteLockedOffHeapClockCacheFactory<>(tableSource, storageEngineFactory));
  }

  /**
   * Creates a cache using the given table buffer source and storage engine
   * factory.
   *
   * @param tableSource buffer source from which hash tables are allocated
   * @param storageEngineFactory factory for the segment storage engines
   * @param evictionListener  listener notified on evictions
   */
  public ConcurrentOffHeapClockCache(PageSource tableSource, Factory<? extends StorageEngine<? super K, ? super V>> storageEngineFactory, EvictionListener<K, V> evictionListener) {
    super(new ReadWriteLockedOffHeapClockCacheFactory<>(tableSource, storageEngineFactory, evictionListener));
  }

  /**
   * Creates a cache using the given table buffer source, storage engine
   * factory, initial table size, and concurrency.
   *
   * @param tableSource buffer source from which hash tables are allocated
   * @param storageEngineFactory factory for the segment storage engines
   * @param tableSize initial table size (summed across all segments)
   * @param concurrency number of segments
   */
  public ConcurrentOffHeapClockCache(PageSource tableSource, Factory<? extends StorageEngine<? super K, ? super V>> storageEngineFactory, long tableSize,
      int concurrency) {
    super(new ReadWriteLockedOffHeapClockCacheFactory<>(tableSource, storageEngineFactory, (int) (tableSize / concurrency)), concurrency);
  }

  /**
   * Creates a cache using the given table buffer source, storage engine
   * factory, initial table size, and concurrency.
   *
   * @param tableSource buffer source from which hash tables are allocated
   * @param storageEngineFactory factory for the segment storage engines
   * @param evictionListener  listener notified on evictions
   * @param tableSize initial table size (summed across all segments)
   * @param concurrency number of segments
   */
  public ConcurrentOffHeapClockCache(PageSource tableSource, Factory<? extends StorageEngine<? super K, ? super V>> storageEngineFactory, EvictionListener<K, V> evictionListener,
          long tableSize, int concurrency) {
    super(new ReadWriteLockedOffHeapClockCacheFactory<>(tableSource, storageEngineFactory, evictionListener, (int) (tableSize / concurrency)), concurrency);
  }
}
