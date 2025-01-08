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
package org.terracotta.offheapstore.concurrent;

import org.terracotta.offheapstore.ReadWriteLockedOffHeapHashMap;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.storage.StorageEngine;
import org.terracotta.offheapstore.util.Factory;

/**
 * Factory of {@link ReadWriteLockedOffHeapHashMap} instances.
 *
 * @param <K> the type of keys held by the generated maps
 * @param <V> the type of mapped values
 *
 * @author Chris Dennis
 */
public class ReadWriteLockedOffHeapHashMapFactory<K, V> implements Factory<ReadWriteLockedOffHeapHashMap<K, V>> {

  private static final int DEFAULT_TABLE_SIZE = 128;

  private final Factory<? extends StorageEngine<? super K, ? super V>> storageEngineFactory;
  private final PageSource tableSource;
  private final boolean tableAllocationsSteal;
  private final int tableSize;

  /**
   * Creates segments using the given table buffer source and storage engine
   * factory.
   *
   * @param tableSource buffer source from which the segment hash tables are allocated
   * @param storageEngineFactory factory for the segment storage engines
   */
  public ReadWriteLockedOffHeapHashMapFactory(PageSource tableSource, Factory<? extends StorageEngine<? super K, ? super V>> storageEngineFactory) {
    this(tableSource, false, storageEngineFactory, DEFAULT_TABLE_SIZE);
  }

  public ReadWriteLockedOffHeapHashMapFactory(PageSource tableSource, boolean tableAllocationsSteal, Factory<? extends StorageEngine<? super K, ? super V>> storageEngineFactory) {
    this(tableSource, tableAllocationsSteal, storageEngineFactory, DEFAULT_TABLE_SIZE);
  }

  public ReadWriteLockedOffHeapHashMapFactory(PageSource tableSource, Factory<? extends StorageEngine<? super K, ? super V>> storageEngineFactory, int tableSize) {
    this(tableSource, false, storageEngineFactory, tableSize);
  }

  /**
   * Creates segments using the given table buffer source, storage engine
   * factory, and initial table size.
   *
   * @param tableSource buffer source from which the segment hash tables are allocated
   * @param tableAllocationsSteal whether table allocations should steal
   * @param storageEngineFactory factory for the segment storage engines
   * @param tableSize initial table size for each segment
   */
  public ReadWriteLockedOffHeapHashMapFactory(PageSource tableSource, boolean tableAllocationsSteal, Factory<? extends StorageEngine<? super K, ? super V>> storageEngineFactory, int tableSize) {
    this.storageEngineFactory = storageEngineFactory;
    this.tableSource = tableSource;
    this.tableAllocationsSteal = tableAllocationsSteal;
    this.tableSize = tableSize;
  }

  /**
   * Creates a new {@code ReadWriteLockedOffHeapHashMap} for use in a
   * segmented map.
   *
   * @return a new {@code ReadWriteLockedOffHeapHashMap}
   */
  @Override
  public ReadWriteLockedOffHeapHashMap<K, V> newInstance() {
    StorageEngine<? super K, ? super V> storageEngine = storageEngineFactory.newInstance();
    try {
      return new ReadWriteLockedOffHeapHashMap<>(tableSource, tableAllocationsSteal, storageEngine, tableSize);
    } catch (RuntimeException e) {
      storageEngine.destroy();
      throw e;
    }
  }
}
