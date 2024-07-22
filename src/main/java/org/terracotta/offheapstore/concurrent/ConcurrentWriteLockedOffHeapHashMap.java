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
 * A striped exclusive-read/write map.
 * <p>
 * This implementation uses instances of {@link WriteLockedOffHeapHashMap}
 * for its segments.
 *
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 *
 * @author Chris Dennis
 * @see WriteLockedOffHeapHashMap
 */
public class ConcurrentWriteLockedOffHeapHashMap<K, V> extends AbstractConcurrentOffHeapMap<K, V>{

  /**
   * Creates a map using the given table buffer source and storage engine
   * factory.
   *
   * @param tableSource buffer source from which hash tables are allocated
   * @param storageEngineFactory factory for the segment storage engines
   */
  public ConcurrentWriteLockedOffHeapHashMap(PageSource tableSource, Factory<? extends StorageEngine<? super K, ? super V>> storageEngineFactory) {
    super(new WriteLockedOffHeapHashMapFactory<>(tableSource, storageEngineFactory));
  }

  /**
   * Creates a map using the given table buffer source, storage engine
   * factory, initial table size, and concurrency.
   *
   * @param tableSource buffer source from which hash tables are allocated
   * @param storageEngineFactory factory for the segment storage engines
   * @param tableSize initial table size (summed across all segments)
   * @param concurrency number of segments
   */
  public ConcurrentWriteLockedOffHeapHashMap(PageSource tableSource, Factory<? extends StorageEngine<? super K, ? super V>> storageEngineFactory, long tableSize,
      int concurrency) {
    super(new WriteLockedOffHeapHashMapFactory<>(tableSource, storageEngineFactory, (int) (tableSize / concurrency)), concurrency);
  }
}
