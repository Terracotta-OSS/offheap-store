/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.terracottatech.offheapstore.concurrent.AbstractConcurrentOffHeapMap;

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

  boolean setMetadata(K key, int mask, int metadata);

  V getAndSetMetadata(K key, int mask, int metadata);

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
}
