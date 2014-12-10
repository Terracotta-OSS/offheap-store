/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.eviction;

import java.util.concurrent.Callable;

import com.terracottatech.offheapstore.WriteLockedOffHeapClockCache;
import com.terracottatech.offheapstore.paging.PageSource;
import com.terracottatech.offheapstore.storage.StorageEngine;

/**
 *
 * @author Chris Dennis
 */
public class EvictionListeningWriteLockedOffHeapClockCache<K, V> extends WriteLockedOffHeapClockCache<K, V> {

  private final EvictionListener<K, V> listener;

  public EvictionListeningWriteLockedOffHeapClockCache(EvictionListener<K, V> listener, PageSource source, StorageEngine<? super K, ? super V> storageEngine) {
    super(source, storageEngine);
    this.listener = listener;
  }

  public EvictionListeningWriteLockedOffHeapClockCache(EvictionListener<K, V> listener, PageSource source, StorageEngine<? super K, ? super V> storageEngine, int tableSize) {
    super(source, storageEngine, tableSize);
    this.listener = listener;
  }

  @Override
  public boolean evict(final int index, boolean shrink) {
    boolean evicted;
    try {
      listener.evicting(new Callable<Entry<K, V>>() {
        @Override
        public Entry<K, V> call() throws Exception {
          return getEntryAtTableOffset(index);
        }
      });
    } finally {
      evicted = super.evict(index, shrink);
    }
    return evicted;
  }
}
