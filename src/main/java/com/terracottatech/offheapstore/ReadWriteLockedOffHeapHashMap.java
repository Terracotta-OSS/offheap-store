/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.terracottatech.offheapstore.paging.PageSource;
import com.terracottatech.offheapstore.storage.StorageEngine;

/**
 * A concurrent-read, exclusive-write off-heap hash-map.
 * <p>
 * This map uses a regular {@code ReentrantReadWriteLock} to provide read/write
 * exclusion/sharing properties.
 *
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 *
 * @author Chris Dennis
 */
public class ReadWriteLockedOffHeapHashMap<K, V> extends AbstractLockedOffHeapHashMap<K, V> {

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  public ReadWriteLockedOffHeapHashMap(PageSource tableSource, StorageEngine<? super K, ? super V> storageEngine) {
    super(tableSource, storageEngine);
  }

  public ReadWriteLockedOffHeapHashMap(PageSource tableSource, boolean tableAllocationsSteal, StorageEngine<? super K, ? super V> storageEngine) {
    super(tableSource, tableAllocationsSteal, storageEngine);
  }

  public ReadWriteLockedOffHeapHashMap(PageSource tableSource, StorageEngine<? super K, ? super V> storageEngine, int tableSize) {
    super(tableSource, storageEngine, tableSize);
  }

  public ReadWriteLockedOffHeapHashMap(PageSource tableSource, boolean tableAllocationsSteal, StorageEngine<? super K, ? super V> storageEngine, int tableSize) {
    super(tableSource, tableAllocationsSteal, storageEngine, tableSize);
  }

  @Override
  public Lock readLock() {
    return lock.readLock();
  }

  @Override
  public Lock writeLock() {
    return lock.writeLock();
  }

  @Override
  public ReentrantReadWriteLock getLock() {
    return lock;
  }
}
