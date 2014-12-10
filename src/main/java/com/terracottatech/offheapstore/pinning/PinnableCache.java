/*
 * All content copyright (c) 2011 Terracotta, Inc., except as may otherwise be
 * noted in a separate copyright notice. All rights reserved.
 */

package com.terracottatech.offheapstore.pinning;

import java.util.concurrent.ConcurrentMap;

public interface PinnableCache<K, V> extends ConcurrentMap<K, V> {

  boolean isPinned(Object key);

  void setPinning(K key, boolean pinned);

  V putPinned(final K key, final V value);

}
