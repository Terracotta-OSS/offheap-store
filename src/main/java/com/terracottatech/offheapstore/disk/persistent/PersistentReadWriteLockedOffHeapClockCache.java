/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.disk.persistent;


import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.terracottatech.offheapstore.disk.paging.MappedPageSource;

/**
 * @author Chris Dennis
 */
public class PersistentReadWriteLockedOffHeapClockCache<K, V> extends AbstractPersistentOffHeapCache<K, V> {

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  public PersistentReadWriteLockedOffHeapClockCache(MappedPageSource source, PersistentStorageEngine<? super K, ? super V> storageEngine, boolean bootstrap) {
    super(source, storageEngine, bootstrap);
  }

  public PersistentReadWriteLockedOffHeapClockCache(MappedPageSource source, PersistentStorageEngine<? super K, ? super V> storageEngine, int tableSize, boolean bootstrap) {
    super(source, storageEngine, tableSize, bootstrap);
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
