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
package org.terracotta.offheapstore.disk.persistent;


import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.terracotta.offheapstore.disk.paging.MappedPageSource;

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
