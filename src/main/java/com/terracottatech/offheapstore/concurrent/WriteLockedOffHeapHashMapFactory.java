/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.concurrent;

import com.terracottatech.offheapstore.WriteLockedOffHeapHashMap;
import com.terracottatech.offheapstore.paging.PageSource;
import com.terracottatech.offheapstore.storage.StorageEngine;
import com.terracottatech.offheapstore.util.Factory;

/**
 * Factory of {@link WriteLockedOffHeapHashMap} instances.
 *
 * @param <K> the type of keys held by the generated maps
 * @param <V> the type of mapped values
 *
 * @author Chris Dennis
 */
public class WriteLockedOffHeapHashMapFactory<K, V> implements Factory<WriteLockedOffHeapHashMap<K, V>> {

  private static final int DEFAULT_TABLE_SIZE = 128;

  private final Factory<? extends StorageEngine<? super K, ? super V>> storageEngineFactory;
  private final PageSource tableSource;
  private final int tableSize;
  
  /**
   * Creates segments using the given table buffer source and storage engine
   * factory.
   *
   * @param tableSource buffer source from which the segment hash tables are allocated
   * @param storageEngineFactory factory for the segment storage engines
   */
  public WriteLockedOffHeapHashMapFactory(PageSource tableSource, Factory<? extends StorageEngine<? super K, ? super V>> storageEngineFactory) {
    this(tableSource, storageEngineFactory, DEFAULT_TABLE_SIZE);
  }

  /**
   * Creates segments using the given table buffer source, storage engine
   * factory, and initial table size.
   *
   * @param tableSource buffer source from which the segment hash tables are allocated
   * @param storageEngineFactory factory for the segment storage engines
   * @param tableSize initial table size for each segment
   */
  public WriteLockedOffHeapHashMapFactory(PageSource tableSource, Factory<? extends StorageEngine<? super K, ? super V>> storageEngineFactory, int tableSize) {
    this.storageEngineFactory = storageEngineFactory;
    this.tableSource = tableSource;
    this.tableSize = tableSize;
  }

  /**
   * Creates a new {@code WriteLockedOffHeapHashMap} for use in a
   * segmented map.
   *
   * @return a new {@code WriteLockedOffHeapHashMap}
   */
  @Override
  public WriteLockedOffHeapHashMap<K, V> newInstance() {
    StorageEngine<? super K, ? super V> storageEngine = storageEngineFactory.newInstance();
    try {
      return new WriteLockedOffHeapHashMap<K, V>(tableSource, storageEngine, tableSize);
    } catch (RuntimeException e) {
      storageEngine.destroy();
      throw e;
    }
  }
}
