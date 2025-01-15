/*
 * Copyright 2014-2023 Terracotta, Inc., a Software AG company.
 * Copyright IBM Corp. 2024, 2025
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.terracottatech.offheapstore.storage.restartable.offheap;

import java.nio.ByteBuffer;
import java.util.Map;

import com.terracottatech.frs.RestartStore;
import com.terracottatech.frs.object.RegisterableObjectManager;
import org.terracotta.offheapstore.ReadWriteLockedOffHeapClockCache;
import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;
import org.terracotta.offheapstore.storage.OffHeapBufferStorageEngine;
import org.terracotta.offheapstore.storage.portability.Portability;
import com.terracottatech.offheapstore.storage.restartable.AbstractRestartabilityCacheIT;
import com.terracottatech.offheapstore.storage.restartable.LinkedNode;
import com.terracottatech.offheapstore.storage.restartable.LinkedNodePortability;
import com.terracottatech.offheapstore.storage.restartable.OffHeapObjectManagerStripe;
import com.terracottatech.offheapstore.storage.restartable.RestartableStorageEngine;
import org.terracotta.offheapstore.util.MemoryUnit;

public class CacheRestartabilityIT extends AbstractRestartabilityCacheIT {

  @Override
  protected <K, V> Map<K, V> createRestartableMap(long size, MemoryUnit unit, ByteBuffer id,
                                                  RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> persistence,
                                                  RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectMgr,
                                                  Portability<? super K> keyPortability,
                                                  Portability<? super V> valuePortability,
                                                  boolean synchronous) {
    PageSource source = new UpfrontAllocatingPageSource(new OffHeapBufferSource(), unit.toBytes(size), MemoryUnit.MEGABYTES.toBytes(1));
    OffHeapBufferStorageEngine<K, LinkedNode<V>> delegateEngine = new OffHeapBufferStorageEngine<K, LinkedNode<V>>(getPointerSize(), source, MemoryUnit.KILOBYTES.toBytes(1), keyPortability, new LinkedNodePortability<V>(valuePortability));
    RestartableStorageEngine<?, ByteBuffer, K, V> storageEngine = new RestartableStorageEngine<OffHeapBufferStorageEngine<K, LinkedNode<V>>, ByteBuffer, K, V>(id, persistence, delegateEngine, synchronous);
    ReadWriteLockedOffHeapClockCache<K, V> map = new ReadWriteLockedOffHeapClockCache<K, V>(source, storageEngine);
    objectMgr.registerObject(new OffHeapObjectManagerStripe<ByteBuffer>(id, map));
    return map;
  }
}
