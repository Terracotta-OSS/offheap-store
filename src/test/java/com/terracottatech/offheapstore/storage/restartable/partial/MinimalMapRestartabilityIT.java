package com.terracottatech.offheapstore.storage.restartable.partial;

import java.nio.ByteBuffer;
import java.util.Map;

import com.terracottatech.frs.RestartStore;
import com.terracottatech.frs.object.RegisterableObjectManager;
import com.terracottatech.offheapstore.OffHeapHashMap;
import com.terracottatech.offheapstore.ReadWriteLockedOffHeapHashMap;
import com.terracottatech.offheapstore.buffersource.OffHeapBufferSource;
import com.terracottatech.offheapstore.paging.PageSource;
import com.terracottatech.offheapstore.paging.UnlimitedPageSource;
import com.terracottatech.offheapstore.paging.UpfrontAllocatingPageSource;
import com.terracottatech.offheapstore.storage.portability.Portability;
import com.terracottatech.offheapstore.storage.restartable.AbstractRestartabilityIT;
import com.terracottatech.offheapstore.storage.restartable.OffHeapObjectManagerStripe;
import com.terracottatech.offheapstore.util.MemoryUnit;

public class MinimalMapRestartabilityIT extends AbstractRestartabilityIT {

  @Override
  protected <K, V> Map<K, V> createRestartableMap(long size, MemoryUnit unit, ByteBuffer id,
                                                  RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> persistence,
                                                  RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectMgr,
                                                  Portability<? super K> keyPortability,
                                                  Portability<? super V> valuePortability,
                                                  boolean synchronous) {
    PageSource source = new UpfrontAllocatingPageSource(new OffHeapBufferSource(), unit.toBytes(size), MemoryUnit.MEGABYTES.toBytes(1));
    RestartableMinimalStorageEngine<ByteBuffer, K, V> storageEngine = new RestartableMinimalStorageEngine<ByteBuffer, K, V>(id, persistence, synchronous, getPointerSize(), source, MemoryUnit.KILOBYTES.toBytes(1), keyPortability, valuePortability, 0.75f);
    OffHeapHashMap<K, V> map = new ReadWriteLockedOffHeapHashMap<K, V>(new UnlimitedPageSource(new OffHeapBufferSource()), storageEngine);
    objectMgr.registerObject(new OffHeapObjectManagerStripe<ByteBuffer>(id, map));
    return map;
  }
}
