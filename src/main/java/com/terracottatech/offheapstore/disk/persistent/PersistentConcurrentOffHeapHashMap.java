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
public class PersistentConcurrentOffHeapHashMap<K, V> extends AbstractPersistentConcurrentOffHeapMap<K, V> {

  public PersistentConcurrentOffHeapHashMap(MappedPageSource tableSource, Factory<? extends PersistentStorageEngine<? super K, ? super V>> storageEngineFactory) {
    super(new PersistentReadWriteLockedOffHeapHashMapFactory<K, V>(tableSource, storageEngineFactory), false);
  }

  public PersistentConcurrentOffHeapHashMap(ObjectInput input, MappedPageSource tableSource, Factory<? extends PersistentStorageEngine<? super K, ? super V>> storageEngineFactory) throws IOException {
    super(new PersistentReadWriteLockedOffHeapHashMapFactory<K, V>(tableSource, storageEngineFactory, false), readSegmentCount(input), false);
  }

  public PersistentConcurrentOffHeapHashMap(MappedPageSource tableSource, Factory<? extends PersistentStorageEngine<? super K, ? super V>> storageEngineFactory, long tableSize, int concurrency) {
    super(new PersistentReadWriteLockedOffHeapHashMapFactory<K, V>(tableSource, storageEngineFactory, (int) (tableSize / concurrency)), concurrency, false);
  }
  
  public PersistentConcurrentOffHeapHashMap(ObjectInput input, MappedPageSource tableSource, Factory<? extends PersistentStorageEngine<? super K, ? super V>> storageEngineFactory, long tableSize, int concurrency) throws IOException {
    super(new PersistentReadWriteLockedOffHeapHashMapFactory<K, V>(tableSource, storageEngineFactory, (int) (tableSize / concurrency), false), readSegmentCount(input), false);
  }
}
