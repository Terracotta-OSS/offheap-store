/* 
 * Copyright 2015 Terracotta, Inc., a Software AG company.
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

import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * An object that encodes map/cache keys and values to integers.
 * <p>
 * {@code StorageEngine} instances can choose their own method of value/key
 * encoding.  Keys that are small enough to be fully encoded in the
 * {@code Integer} return can be stored directly in the table, others could be
 * stored in some additional data structure.
 *
 * @param <K> key type handled by this engine
 * @param <V> value type handled by this engine
 *
 * @author Chris Dennis
 */
public interface StorageEngine<K, V> {

  /**
   * Converts the supplied value object into it's encoded form.
   *
   * @param value a value object
   * @return encoded value
   */
  Long writeMapping(K key, V value, int hash, int metadata);

  void attachedMapping(long encoding, int hash, int metadata);
  
  /**
   * Called to indicate that the associated encoded value is no longer needed.
   * <p>
   * This call can be used to free any associated resources tied to the
   * lifecycle of the supplied encoded value.
   *
   * @param encoding encoded value
   * @param hash hash of the freed mapping
   * @param removal marks removal from a map
   */
  void freeMapping(long encoding, int hash, boolean removal);
  
  /**
   * Converts the supplied encoded value into its correct object form.
   *
   * @param encoding encoded value
   * @return a decoded value object
   */
  V readValue(long encoding);

  /**
   * Called to determine the equality of the given Java object value against the
   * given encoded form.
   * <p>
   * Simple implementations will probably perform a decode on the given encoded
   * form in order to do a regular {@code Object.equals(Object)} comparison.
   * This method is provided to allow implementations to optimize this
   * comparison if possible.
   *
   * @param value a value object
   * @param encoding encoded value
   * @return {@code true} if the value and the encoding are equal
   */
  boolean equalsValue(Object value, long encoding);
  
  /**
   * Converts the supplied encoded key into its correct object form.
   *
   * @param encoding encoded key
   * @param hashCode hash-code of the decoded key
   * @return a decoded key object
   */
  K readKey(long encoding, int hashCode);
  
  /**
   * Called to determine the equality of the given object against the
   * given encoded form.
   * <p>
   * Simple implementations will probably perform a decode on the given encoded
   * form in order to do a regular {@code Object.equals(Object)} comparison.
   * This method is provided to allow implementations to optimize this
   * comparison if possible.
   *
   * @param key a key object
   * @param encoding encoded value
   * @return {@code true} if the key and the encoding are equal
   */
  boolean equalsKey(Object key, long encoding);

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
  public long getDataSize();

  /**
   * Invalidate any local key/value caches.
   * <p>
   * This is called to indicate the termination of a map write "phase".  Caching
   * is permitted within a write operation (i.e. to cache around allocation
   * failures during eviction processes).
   */
  public void invalidateCache();

  public void bind(Owner owner);
  
  public void destroy();

  public boolean shrink();

  public interface Owner extends ReadWriteLock {

    public Long getEncodingForHashAndBinary(int hash, ByteBuffer offHeapBinaryKey);

    public long getSize();

    public long installMappingForHashAndEncoding(int pojoHash, ByteBuffer offheapBinaryKey, ByteBuffer offheapBinaryValue, int metadata);

    public Iterable<Long> encodingSet();

    public boolean updateEncoding(int hashCode, long lastAddress, long compressed, long mask);

    public Integer getSlotForHashAndEncoding(int hash, long address, long mask);

    public boolean evict(int slot, boolean b);

    public boolean isThiefForTableAllocations();
    
  }
}
