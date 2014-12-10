/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.terracottatech.offheapstore.paging.PageSource;
import com.terracottatech.offheapstore.storage.StorageEngine;

/**
 * An exclusive-read/write off-heap clock cache.
 * <p>
 * This cache uses one of the unused bits in the off-heap entry's status value to
 * store the clock data.  This clock data is safe to update during read
 * operations since the cache provides exclusive-read/write characteristics.
 * Since clock eviction data resides in the hash-map's table, it is correctly
 * copied across during table resize operations.
 * <p>
 * The cache uses a regular {@code ReentrantLock} to provide exclusive read and
 * write operations.
 *
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 *
 * @author Chris Dennis
 */
public class WriteLockedOffHeapClockCache<K, V> extends AbstractOffHeapClockCache<K, V> {

  private final Lock lock = new ReentrantLock();

  public WriteLockedOffHeapClockCache(PageSource source, StorageEngine<? super K, ? super V> storageEngine) {
    super(source, storageEngine);
  }

  public WriteLockedOffHeapClockCache(PageSource source, boolean tableAllocationsSteal, StorageEngine<? super K, ? super V> storageEngine) {
    super(source, tableAllocationsSteal, storageEngine);
  }

  public WriteLockedOffHeapClockCache(PageSource source, StorageEngine<? super K, ? super V> storageEngine, int tableSize) {
    super(source, storageEngine, tableSize);
  }
  
  public WriteLockedOffHeapClockCache(PageSource source, boolean tableAllocationsSteal, StorageEngine<? super K, ? super V> storageEngine, int tableSize) {
    super(source, tableAllocationsSteal, storageEngine, tableSize);
  }
  
  @Override
  public Lock readLock() {
    return lock;
  }

  @Override
  public Lock writeLock() {
    return lock;
  }

  @Override
  public ReentrantReadWriteLock getLock() {
    throw new UnsupportedOperationException();
  }
}
