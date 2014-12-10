/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.disk.persistent;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.terracottatech.offheapstore.disk.paging.MappedPageSource;

/**
 *
 * @author Chris Dennis
 */
public class PersistentReadWriteLockedOffHeapHashMap<K, V> extends AbstractPersistentLockedOffHeapHashMap<K, V> {

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  public PersistentReadWriteLockedOffHeapHashMap(MappedPageSource tableSource, PersistentStorageEngine<? super K, ? super V> storageEngine, boolean bootstrap) {
    super(tableSource, storageEngine, bootstrap);
  }

  public PersistentReadWriteLockedOffHeapHashMap(MappedPageSource tableSource, PersistentStorageEngine<? super K, ? super V> storageEngine, int tableSize, boolean bootstrap) {
    super(tableSource, storageEngine, tableSize, bootstrap);
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
