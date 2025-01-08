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

import org.terracotta.offheapstore.disk.paging.MappedPageSource;
import org.terracotta.offheapstore.util.Factory;

/**
 *
 * @author Chris Dennis
 */
public class PersistentReadWriteLockedOffHeapHashMapFactory<K, V> implements Factory<PersistentReadWriteLockedOffHeapHashMap<K, V>> {

  private static final int DEFAULT_TABLE_SIZE = 128;

  private final Factory<? extends PersistentStorageEngine<? super K, ? super V>> storageEngineFactory;
  private final MappedPageSource tableSource;
  private final int tableSize;
  private final boolean bootstrap;

  public PersistentReadWriteLockedOffHeapHashMapFactory(MappedPageSource tableSource, Factory<? extends PersistentStorageEngine<? super K, ? super V>> storageEngineFactory) {
    this(tableSource, storageEngineFactory, DEFAULT_TABLE_SIZE);
  }

  public PersistentReadWriteLockedOffHeapHashMapFactory(MappedPageSource tableSource, Factory<? extends PersistentStorageEngine<? super K, ? super V>> storageEngineFactory, boolean bootstrap) {
    this(tableSource, storageEngineFactory, DEFAULT_TABLE_SIZE, bootstrap);
  }

  public PersistentReadWriteLockedOffHeapHashMapFactory(MappedPageSource tableSource, Factory<? extends PersistentStorageEngine<? super K, ? super V>> storageEngineFactory, int tableSize) {
    this(tableSource, storageEngineFactory, tableSize, true);
  }

  public PersistentReadWriteLockedOffHeapHashMapFactory(MappedPageSource tableSource, Factory<? extends PersistentStorageEngine<? super K, ? super V>> storageEngineFactory, int tableSize, boolean bootstrap) {
    this.storageEngineFactory = storageEngineFactory;
    this.tableSource = tableSource;
    this.tableSize = tableSize;
    this.bootstrap = bootstrap;
  }

  @Override
  public PersistentReadWriteLockedOffHeapHashMap<K, V> newInstance() {
    PersistentStorageEngine<? super K, ? super V> storageEngine = storageEngineFactory.newInstance();
    try {
      return new PersistentReadWriteLockedOffHeapHashMap<>(tableSource, storageEngine, tableSize, bootstrap);
    } catch (RuntimeException e) {
      storageEngine.destroy();
      throw e;
    }
  }
}
