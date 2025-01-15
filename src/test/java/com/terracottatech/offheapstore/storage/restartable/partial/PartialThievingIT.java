/*
 * Copyright IBM Corp. 2025
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
package com.terracottatech.offheapstore.storage.restartable.partial;

import com.terracottatech.frs.RestartStore;
import com.terracottatech.frs.RestartStoreException;
import com.terracottatech.frs.RestartStoreFactory;
import com.terracottatech.frs.config.FrsProperty;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.object.RegisterableObjectManager;
import com.terracottatech.offheapstore.ReadWriteLockedOffHeapClockCache;
import com.terracottatech.offheapstore.ReadWriteLockedOffHeapHashMap;
import com.terracottatech.offheapstore.buffersource.OffHeapBufferSource;
import com.terracottatech.offheapstore.exceptions.OversizeMappingException;
import com.terracottatech.offheapstore.paging.PageSource;
import com.terracottatech.offheapstore.paging.UpfrontAllocatingPageSource;
import com.terracottatech.offheapstore.storage.OffHeapBufferStorageEngine;
import com.terracottatech.offheapstore.storage.portability.ByteArrayPortability;
import com.terracottatech.offheapstore.storage.portability.Portability;
import com.terracottatech.offheapstore.storage.portability.StringPortability;
import com.terracottatech.offheapstore.storage.restartable.LinkedNode;
import com.terracottatech.offheapstore.storage.restartable.LinkedNodePortability;
import com.terracottatech.offheapstore.storage.restartable.OffHeapObjectManagerStripe;
import com.terracottatech.offheapstore.storage.restartable.RestartabilityTestUtilities;
import com.terracottatech.offheapstore.storage.restartable.RestartableStorageEngine;
import com.terracottatech.offheapstore.util.MemoryUnit;
import com.terracottatech.offheapstore.util.PointerSizeParameterizedTest;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import static com.terracottatech.offheapstore.util.MemoryUnit.KILOBYTES;
import static com.terracottatech.offheapstore.util.MemoryUnit.MEGABYTES;

public class PartialThievingIT extends PointerSizeParameterizedTest {

  @Test
  public void testThievingFromPartialCache() throws IOException, RestartStoreException, InterruptedException, ExecutionException {
    File directory = RestartabilityTestUtilities.createTempDirectory(getClass().getSimpleName() + ".testPutPath_");
    ByteBuffer mapId = ByteBuffer.wrap("map".getBytes("US-ASCII"));
    ByteBuffer cacheId = ByteBuffer.wrap("cache".getBytes("US-ASCII"));

    RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectMgr = RestartabilityTestUtilities.createObjectManager();
    RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> persistence = RestartStoreFactory.createStore(objectMgr, directory, new Properties());
    persistence.startup().get();
    try {
      byte[] value = new byte[100*1024];
      Portability<String> keyPortability = StringPortability.INSTANCE;
      Portability<byte[]> valuePortability = ByteArrayPortability.INSTANCE;

      PageSource source = new UpfrontAllocatingPageSource(new OffHeapBufferSource(), MEGABYTES.toBytes(16L), MEGABYTES.toBytes(16));

      RestartablePartialStorageEngine<ByteBuffer, String, byte[]> cacheStorageEngine = new RestartablePartialStorageEngine<ByteBuffer, String, byte[]>(cacheId, persistence, false, getPointerSize(), source, KILOBYTES.toBytes(256), keyPortability, valuePortability, 0.75f);
      ReadWriteLockedOffHeapClockCache<String, byte[]> cache = new ReadWriteLockedOffHeapClockCache<String, byte[]>(source, cacheStorageEngine);
      try {
        objectMgr.registerObject(new OffHeapObjectManagerStripe<ByteBuffer>(cacheId, cache));

        OffHeapBufferStorageEngine<String, LinkedNode<byte[]>> delegateEngine = new OffHeapBufferStorageEngine<String, LinkedNode<byte[]>>(getPointerSize(), source, KILOBYTES.toBytes(256), keyPortability, new LinkedNodePortability<byte[]>(valuePortability), true, false);
        RestartableStorageEngine<?, ByteBuffer, String, byte[]> mapStorageEngine = new RestartableStorageEngine<OffHeapBufferStorageEngine<String, LinkedNode<byte[]>>, ByteBuffer, String, byte[]>(mapId, persistence, delegateEngine, false);

        ReadWriteLockedOffHeapHashMap<String, byte[]> map = new ReadWriteLockedOffHeapHashMap<String, byte[]>(source, true, mapStorageEngine);
        try {
          objectMgr.registerObject(new OffHeapObjectManagerStripe<ByteBuffer>(mapId, map));

          //populate cache
          for (int i = 0; cache.size() < 1000; i++) {
            cache.put(Integer.toString(i), value);
          }

          try {
            for (int i = 0; ; i++) {
              map.put(Integer.toString(i), value);
            }
          } catch (OversizeMappingException e) {
            System.out.println("Map Put: " + map.size());
          }
        } finally {
          map.destroy();
        }
      } finally {
        cache.destroy();
      }
    } finally {
      persistence.shutdown();
    }

  }
}
