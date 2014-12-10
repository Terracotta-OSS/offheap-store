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
 * A concurrent-read, exclusive-write off-heap clock cache.
 * <p>
 * This cache uses a conventional clock cache eviction algorithm.  Clock
 * eviction data is stored in an on-heap atomic bit-set, in the form of an
 * {@link AtomicLongArray}.  This allows clock bits to be updated concurrently,
 * and hence allows concurrent readers.  This means the clock eviction data
 * occupied space in the Java heap.  Currently clock cache eviction data stored
 * in this cache does not survive table resize events.
 * <p>
 * The cache uses a regular {@code ReentrantReadWriteLock} to provide read/write
 * exclusion/sharing properties.
 *
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 *
 * @author Chris Dennis
 */
public class ReadWriteLockedOffHeapClockCache<K, V> extends AbstractOffHeapClockCache<K, V> {

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  
  public ReadWriteLockedOffHeapClockCache(PageSource source, StorageEngine<? super K, ? super V> storageEngine) {
    super(source, storageEngine);
  }

  public ReadWriteLockedOffHeapClockCache(PageSource source, boolean tableAllocationsSteal, StorageEngine<? super K, ? super V> storageEngine) {
    super(source, tableAllocationsSteal, storageEngine);
  }
  
  public ReadWriteLockedOffHeapClockCache(PageSource source, StorageEngine<? super K, ? super V> storageEngine, int tableSize) {
    super(source, storageEngine, tableSize);
  }
  
  public ReadWriteLockedOffHeapClockCache(PageSource source, boolean tableAllocationsSteal, StorageEngine<? super K, ? super V> storageEngine, int tableSize) {
    super(source, tableAllocationsSteal, storageEngine, tableSize);
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
