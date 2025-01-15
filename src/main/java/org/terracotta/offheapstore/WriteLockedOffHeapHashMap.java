/*
 * Copyright 2015-2023 Terracotta, Inc., a Software AG company.
 * Copyright IBM Corp. 2024, 2025
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.terracotta.offheapstore;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.storage.StorageEngine;

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
