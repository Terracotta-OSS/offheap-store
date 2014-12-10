package com.terracottatech.offheapstore.pinning;

import static com.terracottatech.offheapstore.util.MemoryUnit.KILOBYTES;

import java.util.concurrent.Callable;

import org.junit.internal.AssumptionViolatedException;

import com.terracottatech.offheapstore.eviction.EvictionListener;
import com.terracottatech.offheapstore.eviction.EvictionListeningWriteLockedOffHeapClockCache;
import com.terracottatech.offheapstore.paging.PageSource;
import com.terracottatech.offheapstore.storage.IntegerStorageEngine;
import com.terracottatech.offheapstore.storage.OffHeapBufferStorageEngine;
import com.terracottatech.offheapstore.storage.PointerSize;
import com.terracottatech.offheapstore.storage.SplitStorageEngine;
import com.terracottatech.offheapstore.storage.StorageEngine;
import com.terracottatech.offheapstore.storage.portability.ByteArrayPortability;
import com.terracottatech.offheapstore.storage.portability.SerializablePortability;

import org.hamcrest.core.Is;
import org.junit.Assume;

public class EvictionListeningWriteLockedPinningIT extends AbstractPinningIT {

  @SuppressWarnings("unchecked")
  @Override
  protected PinnableCache<Integer, Integer> createPinnedIntegerCache(PageSource source) {
    Assume.assumeThat(getPointerSize(), Is.is(PointerSize.INT));
    StorageEngine<Integer, Integer> storageEngine = new SplitStorageEngine(new IntegerStorageEngine(), new IntegerStorageEngine());
    return new EvictionListeningWriteLockedOffHeapClockCache<Integer, Integer>(new NullEvictionListener(), source, storageEngine);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected PinnableCache<Integer, byte[]> createPinnedByteArrayCache(PageSource source) {
    StorageEngine<Integer, byte[]> storageEngine = new OffHeapBufferStorageEngine<Integer, byte[]>(getPointerSize(), source, KILOBYTES.toBytes(1), new SerializablePortability(), ByteArrayPortability.INSTANCE);
    return new EvictionListeningWriteLockedOffHeapClockCache<Integer, byte[]>(new NullEvictionListener(), source, storageEngine);
  }

  @Override
  protected PinnableCache<Integer, byte[]> createSharingPinnedByteArrayCache(PageSource source) {
    throw new AssumptionViolatedException("Cannot create sharing eviction listening cache");
  }
  
  @SuppressWarnings("rawtypes")
  static class NullEvictionListener implements EvictionListener {

    @Override
    public void evicting(Callable evictee) {
      //ignore
    }
  }
}
