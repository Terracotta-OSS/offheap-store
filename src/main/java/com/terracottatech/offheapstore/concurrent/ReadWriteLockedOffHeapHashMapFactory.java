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
 * Factory of {@link ReadWriteLockedOffHeapHashMap} instances.
 *
 * @param <K> the type of keys held by the generated maps
 * @param <V> the type of mapped values
 *
 * @author Chris Dennis
 */
public class ReadWriteLockedOffHeapHashMapFactory<K, V> implements Factory<ReadWriteLockedOffHeapHashMap<K, V>> {

  private static final int DEFAULT_TABLE_SIZE = 128;

  private final Factory<? extends StorageEngine<? super K, ? super V>> storageEngineFactory;
  private final PageSource tableSource;
  private final boolean tableAllocationsSteal;
  private final int tableSize;
  
  /**
   * Creates segments using the given table buffer source and storage engine
   * factory.
   *
   * @param tableSource buffer source from which the segment hash tables are allocated
   * @param storageEngineFactory factory for the segment storage engines
   */
  public ReadWriteLockedOffHeapHashMapFactory(PageSource tableSource, Factory<? extends StorageEngine<? super K, ? super V>> storageEngineFactory) {
    this(tableSource, false, storageEngineFactory, DEFAULT_TABLE_SIZE);
  }

  public ReadWriteLockedOffHeapHashMapFactory(PageSource tableSource, boolean tableAllocationsSteal, Factory<? extends StorageEngine<? super K, ? super V>> storageEngineFactory) {
    this(tableSource, tableAllocationsSteal, storageEngineFactory, DEFAULT_TABLE_SIZE);
  }

  public ReadWriteLockedOffHeapHashMapFactory(PageSource tableSource, Factory<? extends StorageEngine<? super K, ? super V>> storageEngineFactory, int tableSize) {
    this(tableSource, false, storageEngineFactory, tableSize);
  }

  /**
   * Creates segments using the given table buffer source, storage engine
   * factory, and initial table size.
   *
   * @param tableSource buffer source from which the segment hash tables are allocated
   * @param storageEngineFactory factory for the segment storage engines
   * @param tableSize initial table size for each segment
   */
  public ReadWriteLockedOffHeapHashMapFactory(PageSource tableSource, boolean tableAllocationsSteal, Factory<? extends StorageEngine<? super K, ? super V>> storageEngineFactory, int tableSize) {
    this.storageEngineFactory = storageEngineFactory;
    this.tableSource = tableSource;
    this.tableAllocationsSteal = tableAllocationsSteal;
    this.tableSize = tableSize;
  }

  /**
   * Creates a new {@code ReadWriteLockedOffHeapHashMap} for use in a
   * segmented map.
   *
   * @return a new {@code ReadWriteLockedOffHeapHashMap}
   */
  @Override
  public ReadWriteLockedOffHeapHashMap<K, V> newInstance() {
    StorageEngine<? super K, ? super V> storageEngine = storageEngineFactory.newInstance();
    try {
      return new ReadWriteLockedOffHeapHashMap<K, V>(tableSource, tableAllocationsSteal, storageEngine, tableSize);
    } catch (RuntimeException e) {
      storageEngine.destroy();
      throw e;
    }
  }
}
