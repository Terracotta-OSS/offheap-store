/*
 * Copyright 2015-2023 Terracotta, Inc., a Software AG company.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.storage.StorageEngine;

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
