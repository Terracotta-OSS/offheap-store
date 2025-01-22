package com.terracottatech.offheapstore.storage.restartable.partial;

import java.nio.ByteBuffer;
import java.util.Map;

import com.terracottatech.frs.RestartStore;
import com.terracottatech.frs.object.RegisterableObjectManager;
import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import org.terracotta.offheapstore.concurrent.ConcurrentOffHeapHashMap;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.paging.UnlimitedPageSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;
import org.terracotta.offheapstore.storage.StorageEngine;
import org.terracotta.offheapstore.storage.portability.Portability;
import com.terracottatech.offheapstore.storage.restartable.AbstractRestartabilityIT;
import com.terracottatech.offheapstore.storage.restartable.OffHeapObjectManagerStripe;
import org.terracotta.offheapstore.util.Factory;
import org.terracotta.offheapstore.util.MemoryUnit;

public class KeysOnlyConcurrentMapRestartabilityIT extends AbstractRestartabilityIT {

  @Override
  protected <K, V> Map<K, V> createRestartableMap(long size, MemoryUnit unit, ByteBuffer id,
                                                  RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> persistence,
                                                  RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectMgr,
                                                  Portability<? super K> keyPortability,
                                                  Portability<? super V> valuePortability,
                                                  boolean synchronous) {
    PageSource source = new UpfrontAllocatingPageSource(new OffHeapBufferSource(), unit.toBytes(size), MemoryUnit.MEGABYTES.toBytes(1));
    Factory<? extends StorageEngine<K, V>> storageEngineFactory = RestartableKeysOnlyStorageEngine.createKeysOnlyFactory(id, persistence, synchronous, getPointerSize(), source, MemoryUnit.KILOBYTES.toBytes(1), keyPortability, valuePortability, false, false, 0.75f);
    ConcurrentOffHeapHashMap<K, V> map = new ConcurrentOffHeapHashMap<K, V>(new UnlimitedPageSource(new OffHeapBufferSource()), storageEngineFactory);
    objectMgr.registerObject(new OffHeapObjectManagerStripe<ByteBuffer>(id, map));
    return map;
  }
}
