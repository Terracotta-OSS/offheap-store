package com.terracottatech.offheapstore.pinning;

import static com.terracottatech.offheapstore.util.MemoryUnit.KILOBYTES;

import com.terracottatech.offheapstore.ReadWriteLockedOffHeapClockCache;
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

public class ReadWriteLockedPinningIT extends AbstractPinningIT {

  @Override
  protected PinnableCache<Integer, Integer> createPinnedIntegerCache(PageSource source) {
    Assume.assumeThat(getPointerSize(), Is.is(PointerSize.INT));
    StorageEngine<Integer, Integer> storageEngine = new SplitStorageEngine(new IntegerStorageEngine(), new IntegerStorageEngine());
    return new ReadWriteLockedOffHeapClockCache<Integer, Integer>(source, storageEngine);
  }

  @Override
  protected PinnableCache<Integer, byte[]> createPinnedByteArrayCache(PageSource source) {
    StorageEngine<Integer, byte[]> storageEngine = new OffHeapBufferStorageEngine<Integer, byte[]>(getPointerSize(), source, KILOBYTES.toBytes(1), new SerializablePortability(), ByteArrayPortability.INSTANCE);
    return new ReadWriteLockedOffHeapClockCache<Integer, byte[]>(source, storageEngine);
  }

  @Override
  protected PinnableCache<Integer, byte[]> createSharingPinnedByteArrayCache(PageSource source) {
    StorageEngine<Integer, byte[]> storageEngine = new OffHeapBufferStorageEngine<Integer, byte[]>(getPointerSize(), source, KILOBYTES.toBytes(1), new SerializablePortability(), ByteArrayPortability.INSTANCE, true, true);
    return new ReadWriteLockedOffHeapClockCache<Integer, byte[]>(source, true, storageEngine);
  }
}
