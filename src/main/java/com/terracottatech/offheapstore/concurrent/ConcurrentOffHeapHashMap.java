/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.concurrent;

import com.terracottatech.offheapstore.ReadWriteLockedOffHeapHashMap;
import com.terracottatech.offheapstore.paging.PageSource;
import com.terracottatech.offheapstore.storage.StorageEngine;
import com.terracottatech.offheapstore.util.Factory;

/**
 * A striped concurrent-read/exclusive-write map.
 * <p>
 * This implementation uses instances of {@link ReadWriteLockedOffHeapHashMap}
 * for its segments.
 *
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 *
 * @author Chris Dennis
 * @see ReadWriteLockedOffHeapHashMap
 */
public class ConcurrentOffHeapHashMap<K, V> extends AbstractConcurrentOffHeapMap<K, V> {

  /**
   * Creates a map using the given table buffer source and storage engine
   * factory.
   *
   * @param tableSource buffer source from which hash tables are allocated
   * @param storageEngineFactory factory for the segment storage engines
   */
  public ConcurrentOffHeapHashMap(PageSource tableSource, Factory<? extends StorageEngine<? super K, ? super V>> storageEngineFactory) {
    super(new ReadWriteLockedOffHeapHashMapFactory<K, V>(tableSource, storageEngineFactory));
  }

  public ConcurrentOffHeapHashMap(PageSource tableSource, boolean tableAllocationsSteal, Factory<? extends StorageEngine<? super K, ? super V>> storageEngineFactory) {
    super(new ReadWriteLockedOffHeapHashMapFactory<K, V>(tableSource, tableAllocationsSteal, storageEngineFactory));
  }

  public ConcurrentOffHeapHashMap(PageSource tableSource, Factory<? extends StorageEngine<? super K, ? super V>> storageEngineFactory, boolean latencyMonitoring) {
    super(new ReadWriteLockedOffHeapHashMapFactory<K, V>(tableSource, storageEngineFactory));
  }

  /**
   * Creates a map using the given table buffer source, storage engine
   * factory, initial table size, and concurrency.
   *
   * @param tableSource buffer source from which hash tables are allocated
   * @param storageEngineFactory factory for the segment storage engines
   * @param tableSize initial table size (summed across all segments)
   * @param concurrency number of segments
   */
  public ConcurrentOffHeapHashMap(PageSource tableSource, Factory<? extends StorageEngine<? super K, ? super V>> storageEngineFactory, long tableSize,
      int concurrency) {
    super(new ReadWriteLockedOffHeapHashMapFactory<K, V>(tableSource, storageEngineFactory, (int) (tableSize / concurrency)), concurrency);
  }

  public ConcurrentOffHeapHashMap(PageSource tableSource, boolean tableAllocationsSteal, Factory<? extends StorageEngine<? super K, ? super V>> storageEngineFactory, long tableSize,
      int concurrency) {
    super(new ReadWriteLockedOffHeapHashMapFactory<K, V>(tableSource, tableAllocationsSteal, storageEngineFactory, (int) (tableSize / concurrency)), concurrency);
  }
}
