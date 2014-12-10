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
 * An exclusive-read/write off-heap hash-map.
 * <p>
 * This map uses a regular {@code ReentrantLock} to provide exclusive read and
 * write operations.
 *
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 *
 * @author Chris Dennis
 */
public class WriteLockedOffHeapHashMap<K, V> extends AbstractLockedOffHeapHashMap<K, V>{

  private final Lock lock = new ReentrantLock();

  public WriteLockedOffHeapHashMap(PageSource tableSource, StorageEngine<? super K, ? super V> storageEngine) {
    super(tableSource, storageEngine);
  }

  public WriteLockedOffHeapHashMap(PageSource tableSource, StorageEngine<? super K, ? super V> storageEngine, int tableSize) {
    super(tableSource, storageEngine, tableSize);
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
