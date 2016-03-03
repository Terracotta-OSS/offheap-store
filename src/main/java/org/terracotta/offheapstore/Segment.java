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
package org.terracotta.offheapstore;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.terracotta.offheapstore.concurrent.AbstractConcurrentOffHeapMap;
import org.terracotta.offheapstore.jdk8.BiFunction;
import org.terracotta.offheapstore.jdk8.Function;

/**
 * Implemented by maps that can be used as segments in a concurrent map.
 *
 * @see AbstractConcurrentOffHeapMap
 * @author Chris Dennis
 */
public interface Segment<K, V> extends ConcurrentMap<K, V>, MapInternals, ReadWriteLock {

  /**
   * See {@link OffHeapHashMap#fill(Object, Object)} for a detailed description.
   * 
   * @param key key with which the specified value is to be associated
   * @param value value to be associated with the specified key
   * @return the previous value associated with <tt>key</tt>, or
   *         <tt>null</tt> if there was no mapping for <tt>key</tt>
   *         (irrespective of whether the value was successfully installed).
   */
  V fill(K key, V value);

  V fill(K key, V value, int metadata);
  
  V put(K key, V value, int metadata);

  Integer getMetadata(K key, int mask);

  Integer getAndSetMetadata(K key, int mask, int values);

  V getValueAndSetMetadata(K key, int mask, int values);

  /**
   * Return the {@code ReentrantReadWriteLock} used by this segment.
   *
   * @return RRWL for this segment
   * @throws UnsupportedOperationException if this segment does not use a RRWL
   */
  ReentrantReadWriteLock getLock() throws UnsupportedOperationException;

  boolean removeNoReturn(Object key);
  
  void destroy();

  boolean shrink();

  /*
   * JDK-8-alike metadata methods
   */
  MetadataTuple<V> computeWithMetadata(K key, BiFunction<? super K, ? super MetadataTuple<V>, ? extends MetadataTuple<V>> remappingFunction);

  MetadataTuple<V> computeIfAbsentWithMetadata(K key, Function<? super K,? extends MetadataTuple<V>> mappingFunction);

  MetadataTuple<V> computeIfPresentWithMetadata(K key, BiFunction<? super K,? super MetadataTuple<V>,? extends MetadataTuple<V>> remappingFunction);
}
