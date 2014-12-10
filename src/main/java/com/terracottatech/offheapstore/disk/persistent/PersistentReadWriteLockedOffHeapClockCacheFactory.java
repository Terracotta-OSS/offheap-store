/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.disk.persistent;

import com.terracottatech.offheapstore.disk.paging.MappedPageSource;
import com.terracottatech.offheapstore.util.Factory;

/**
 *
 * @author Chris Dennis
 */
public class PersistentReadWriteLockedOffHeapClockCacheFactory<K, V> implements Factory<PersistentReadWriteLockedOffHeapClockCache<K, V>> {

  private static final int DEFAULT_TABLE_SIZE = 128;

  private final Factory<? extends PersistentStorageEngine<? super K, ? super V>> storageEngineFactory;
  private final MappedPageSource tableSource;
  private final int tableSize;
  private final boolean bootstrap;

  public PersistentReadWriteLockedOffHeapClockCacheFactory(MappedPageSource tableSource, Factory<? extends PersistentStorageEngine<? super K, ? super V>> storageEngineFactory) {
    this(tableSource, storageEngineFactory, DEFAULT_TABLE_SIZE);
  }

  public PersistentReadWriteLockedOffHeapClockCacheFactory(MappedPageSource tableSource, Factory<? extends PersistentStorageEngine<? super K, ? super V>> storageEngineFactory, boolean bootstrap) {
    this(tableSource, storageEngineFactory, DEFAULT_TABLE_SIZE, bootstrap);
  }

  public PersistentReadWriteLockedOffHeapClockCacheFactory(MappedPageSource tableSource, Factory<? extends PersistentStorageEngine<? super K, ? super V>> storageEngineFactory, int tableSize) {
    this(tableSource, storageEngineFactory, tableSize, true);
  }
  
  public PersistentReadWriteLockedOffHeapClockCacheFactory(MappedPageSource tableSource, Factory<? extends PersistentStorageEngine<? super K, ? super V>> storageEngineFactory, int tableSize, boolean bootstrap) {
    this.storageEngineFactory = storageEngineFactory;
    this.tableSource = tableSource;
    this.tableSize = tableSize;
    this.bootstrap = bootstrap;
  }

  @Override
  public PersistentReadWriteLockedOffHeapClockCache<K, V> newInstance() {
    PersistentStorageEngine<? super K, ? super V> storageEngine = storageEngineFactory.newInstance();
    try {
      return new PersistentReadWriteLockedOffHeapClockCache<K, V>(tableSource, storageEngine, tableSize, bootstrap);
    } catch (RuntimeException e) {
      storageEngine.destroy();
      throw e;
    }
  }
}
