package com.terracottatech.offheapstore.pinning;

import static com.terracottatech.offheapstore.util.MemoryUnit.KILOBYTES;

import org.junit.internal.AssumptionViolatedException;

import com.terracottatech.offheapstore.concurrent.ConcurrentWriteLockedOffHeapClockCache;
import com.terracottatech.offheapstore.paging.PageSource;
import com.terracottatech.offheapstore.storage.IntegerStorageEngine;
import com.terracottatech.offheapstore.storage.OffHeapBufferStorageEngine;
import com.terracottatech.offheapstore.storage.PointerSize;
import com.terracottatech.offheapstore.storage.SplitStorageEngine;
import com.terracottatech.offheapstore.storage.StorageEngine;
import com.terracottatech.offheapstore.storage.portability.ByteArrayPortability;
import com.terracottatech.offheapstore.storage.portability.SerializablePortability;
import com.terracottatech.offheapstore.util.Factory;

import org.hamcrest.core.Is;
import org.junit.Assume;

public class ConcurrentWriteLockedPinningIT extends AbstractPinningIT {

  @Override
  protected PinnableCache<Integer, Integer> createPinnedIntegerCache(PageSource source) {
    Assume.assumeThat(getPointerSize(), Is.is(PointerSize.INT));
    Factory<? extends StorageEngine<Integer, Integer>> storageEngineFactory = SplitStorageEngine.createFactory(IntegerStorageEngine.createFactory(), IntegerStorageEngine.createFactory());
    return new ConcurrentWriteLockedOffHeapClockCache<Integer, Integer>(source, storageEngineFactory);
  }

  @Override
  protected PinnableCache<Integer, byte[]> createPinnedByteArrayCache(PageSource source) {
    Factory<OffHeapBufferStorageEngine<Integer, byte[]>> storageEngineFactory = OffHeapBufferStorageEngine.createFactory(getPointerSize(), source, KILOBYTES.toBytes(1), new SerializablePortability(), ByteArrayPortability.INSTANCE, false, false);
    return new ConcurrentWriteLockedOffHeapClockCache<Integer, byte[]>(source, storageEngineFactory);
  }

  @Override
  protected PinnableCache<Integer, byte[]> createSharingPinnedByteArrayCache(PageSource source) {
    throw new AssumptionViolatedException("Cannot create sharing concurrent cache");
  }  
}
