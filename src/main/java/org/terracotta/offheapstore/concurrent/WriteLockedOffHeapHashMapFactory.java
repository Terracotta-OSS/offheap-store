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
package org.terracotta.offheapstore.concurrent;

import org.terracotta.offheapstore.WriteLockedOffHeapHashMap;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.storage.StorageEngine;
import org.terracotta.offheapstore.util.Factory;

/**
 * Factory of {@link WriteLockedOffHeapHashMap} instances.
 *
 * @param <K> the type of keys held by the generated maps
 * @param <V> the type of mapped values
 *
 * @author Chris Dennis
 */
public class WriteLockedOffHeapHashMapFactory<K, V> implements Factory<WriteLockedOffHeapHashMap<K, V>> {

  private static final int DEFAULT_TABLE_SIZE = 128;

  private final Factory<? extends StorageEngine<? super K, ? super V>> storageEngineFactory;
  private final PageSource tableSource;
  private final int tableSize;

  /**
   * Creates segments using the given table buffer source and storage engine
   * factory.
   *
   * @param tableSource buffer source from which the segment hash tables are allocated
   * @param storageEngineFactory factory for the segment storage engines
   */
  public WriteLockedOffHeapHashMapFactory(PageSource tableSource, Factory<? extends StorageEngine<? super K, ? super V>> storageEngineFactory) {
    this(tableSource, storageEngineFactory, DEFAULT_TABLE_SIZE);
  }

  /**
   * Creates segments using the given table buffer source, storage engine
   * factory, and initial table size.
   *
   * @param tableSource buffer source from which the segment hash tables are allocated
   * @param storageEngineFactory factory for the segment storage engines
   * @param tableSize initial table size for each segment
   */
  public WriteLockedOffHeapHashMapFactory(PageSource tableSource, Factory<? extends StorageEngine<? super K, ? super V>> storageEngineFactory, int tableSize) {
    this.storageEngineFactory = storageEngineFactory;
    this.tableSource = tableSource;
    this.tableSize = tableSize;
  }

  /**
   * Creates a new {@code WriteLockedOffHeapHashMap} for use in a
   * segmented map.
   *
   * @return a new {@code WriteLockedOffHeapHashMap}
   */
  @Override
  public WriteLockedOffHeapHashMap<K, V> newInstance() {
    StorageEngine<? super K, ? super V> storageEngine = storageEngineFactory.newInstance();
    try {
      return new WriteLockedOffHeapHashMap<>(tableSource, storageEngine, tableSize);
    } catch (RuntimeException e) {
      storageEngine.destroy();
      throw e;
    }
  }
}
