/*
 * Copyright 2015-2023 Terracotta, Inc., a Software AG company.
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
package org.terracotta.offheapstore.eviction;

import java.util.Map.Entry;
import java.util.concurrent.Callable;

/**
 * Listener interface used to monitor eviction in off-heap caches.
 *
 * @author Chris Dennis
 */
public interface EvictionListener<K, V> {

  /**
   * Called prior to the eviction of a cache mapping.
   * <p>
   * Implementors must be careful to not expose the callable outside the scope
   * of this method.  The behavior of the callable becomes undefined on return
   * from this method.
   *
   * @param callable callable for retrieving the evictee
   */
  void evicting(Callable<Entry<K, V>> callable);
}
