/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.eviction;

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
