/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.concurrent;

import com.terracottatech.offheapstore.WriteLockedOffHeapClockCache;
import com.terracottatech.offheapstore.eviction.EvictionListener;
import com.terracottatech.offheapstore.paging.PageSource;
import com.terracottatech.offheapstore.storage.StorageEngine;
import com.terracottatech.offheapstore.util.Factory;

/**
 * A striped exclusive-read/write clock cache.
 * <p>
 * This implementation uses instances of {@link WriteLockedOffHeapClockCache} for
 * its segments.
 *
 * @param <K> the type of keys maintained by this cache
 * @param <V> the type of mapped values
 *
 * @author Chris Dennis
 * @see WriteLockedOffHeapClockCache
 */
public class ConcurrentWriteLockedOffHeapClockCache<K, V> extends AbstractConcurrentOffHeapCache<K, V> {

  /**
   * Creates a cache using the given table buffer source and storage engine
   * factory.
   *
   * @param tableSource buffer source from which hash tables are allocated
   * @param storageEngineFactory factory for the segment storage engines
   */
  public ConcurrentWriteLockedOffHeapClockCache(PageSource tableSource, Factory<? extends StorageEngine<? super K, ? super V>> storageEngineFactory) {
    super(new WriteLockedOffHeapClockCacheFactory<K, V>(tableSource, storageEngineFactory));
  }

  /**
   * Creates a cache using the given table buffer source and storage engine
   * factory.
   *
   * @param tableSource buffer source from which hash tables are allocated
   * @param storageEngineFactory factory for the segment storage engines
   * @param evictionListener  listener notified on evictions
   */
  public ConcurrentWriteLockedOffHeapClockCache(PageSource tableSource, Factory<? extends StorageEngine<? super K, ? super V>> storageEngineFactory, EvictionListener<K, V> evictionListener) {
    super(new WriteLockedOffHeapClockCacheFactory<K, V>(tableSource, storageEngineFactory, evictionListener));
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
  public ConcurrentWriteLockedOffHeapClockCache(PageSource tableSource, Factory<? extends StorageEngine<? super K, ? super V>> storageEngineFactory, long tableSize,
      int concurrency) {
    super(new WriteLockedOffHeapClockCacheFactory<K, V>(tableSource, storageEngineFactory, (int) (tableSize / concurrency)), concurrency);
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
  public ConcurrentWriteLockedOffHeapClockCache(PageSource tableSource, Factory<? extends StorageEngine<? super K, ? super V>> storageEngineFactory, EvictionListener<K, V> evictionListener,
          long tableSize, int concurrency) {
    super(new WriteLockedOffHeapClockCacheFactory<K, V>(tableSource, storageEngineFactory, evictionListener, (int) (tableSize / concurrency)), concurrency);
  }
}
