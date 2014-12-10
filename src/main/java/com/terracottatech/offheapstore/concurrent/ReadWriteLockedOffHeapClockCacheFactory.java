/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.concurrent;

import com.terracottatech.offheapstore.ReadWriteLockedOffHeapClockCache;
import com.terracottatech.offheapstore.eviction.EvictionListener;
import com.terracottatech.offheapstore.eviction.EvictionListeningReadWriteLockedOffHeapClockCache;
import com.terracottatech.offheapstore.paging.PageSource;
import com.terracottatech.offheapstore.storage.StorageEngine;
import com.terracottatech.offheapstore.util.Factory;

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
