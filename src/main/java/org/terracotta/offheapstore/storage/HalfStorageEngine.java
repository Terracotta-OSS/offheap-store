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
package org.terracotta.offheapstore.storage;

import org.terracotta.offheapstore.storage.StorageEngine.Owner;

/**
 *
 * @author cdennis
 */
public interface HalfStorageEngine<T> {
  /**
   * Converts the supplied value object into it's encoded form.
   *
   * @param object a value object
   * @param hash the keys hash
   * @return encoded value
   */
  Integer write(T object, int hash);

  /**
   * Called to indicate that the associated encoded value is no longer needed.
   * <p>
   * This call can be used to free any associated resources tied to the
   * lifecycle of the supplied encoded value.
   *
   * @param encoding encoded value
   */
  void free(int encoding);

  /**
   * Converts the supplied encoded value into its correct object form.
   *
   * @param encoding encoded value
   * @return a decoded value object
   */
  T read(int encoding);

  /**
   * Called to determine the equality of the given Java object value against the
   * given encoded form.
   * <p>
   * Simple implementations will probably perform a decode on the given encoded
   * form in order to do a regular {@code Object.equals(Object)} comparison.
   * This method is provided to allow implementations to optimize this
   * comparison if possible.
   *
   * @param object a value object
   * @param encoding encoded value
   * @return {@code true} if the value and the encoding are equal
   */
  boolean equals(Object object, int encoding);

  /**
   * Called to indicate that all keys and values are now free.
   */
  void clear();

  /**
   * Returns a measure of the amount of memory allocated for this storage engine.
   *
   * @return memory allocated for this engine in bytes
   */
  long getAllocatedMemory();

  /**
   * Returns a measure of the amount of memory consumed by this storage engine.
   *
   * @return memory occupied by this engine in bytes
   */
  long getOccupiedMemory();

  /**
   * Returns a measure of the amount of vital memory allocated for this storage engine.
   *
   * @return vital memory allocated for this engine in bytes
   */
  long getVitalMemory();

  /**
   * Returns a measure of the total size of the keys and values stored in this storage engine.
   *
   * @return size of the stored keys and values in bytes
   */
  long getDataSize();

  /**
   * Invalidate any local key/value caches.
   * <p>
   * This is called to indicate the termination of a map write "phase".  Caching
   * is permitted within a write operation (i.e. to cache around allocation
   * failures during eviction processes).
   */
  void invalidateCache();

  void bind(Owner owner, long mask);

  void destroy();

  boolean shrink();
}
