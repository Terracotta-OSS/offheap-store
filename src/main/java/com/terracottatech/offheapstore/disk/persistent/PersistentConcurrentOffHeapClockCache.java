/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.disk.persistent;

import java.io.IOException;
import java.io.ObjectInput;

import com.terracottatech.offheapstore.disk.paging.MappedPageSource;
import com.terracottatech.offheapstore.util.Factory;

/**
 *
 * @author Chris Dennis
 */
public class PersistentConcurrentOffHeapClockCache<K, V> extends AbstractPersistentConcurrentOffHeapMap<K, V> {

  public PersistentConcurrentOffHeapClockCache(MappedPageSource tableSource, Factory<? extends PersistentStorageEngine<? super K, ? super V>> storageEngineFactory) {
    super(new PersistentReadWriteLockedOffHeapClockCacheFactory<K, V>(tableSource, storageEngineFactory), false);
  }

  public PersistentConcurrentOffHeapClockCache(ObjectInput input, MappedPageSource tableSource, Factory<? extends PersistentStorageEngine<? super K, ? super V>> storageEngineFactory) throws IOException {
    super(new PersistentReadWriteLockedOffHeapClockCacheFactory<K, V>(tableSource, storageEngineFactory, false), readSegmentCount(input), false);
  }

  public PersistentConcurrentOffHeapClockCache(MappedPageSource tableSource, Factory<? extends PersistentStorageEngine<? super K, ? super V>> storageEngineFactory, long tableSize, int concurrency) {
    super(new PersistentReadWriteLockedOffHeapClockCacheFactory<K, V>(tableSource, storageEngineFactory, (int) (tableSize / concurrency)), concurrency, false);
  }
  
  public PersistentConcurrentOffHeapClockCache(ObjectInput input, MappedPageSource tableSource, Factory<? extends PersistentStorageEngine<? super K, ? super V>> storageEngineFactory, long tableSize, int concurrency) throws IOException {
    super(new PersistentReadWriteLockedOffHeapClockCacheFactory<K, V>(tableSource, storageEngineFactory, (int) (tableSize / concurrency), false), readSegmentCount(input), false);
  }
}
