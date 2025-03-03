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
package com.terracottatech.offheapstore.storage.restartable.disk;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import com.terracottatech.frs.RestartStore;
import com.terracottatech.frs.object.RegisterableObjectManager;
import org.terracotta.offheapstore.concurrent.ConcurrentOffHeapHashMap;
import org.terracotta.offheapstore.disk.paging.MappedPageSource;
import org.terracotta.offheapstore.disk.storage.FileBackedStorageEngine;
import org.terracotta.offheapstore.storage.portability.Portability;
import com.terracottatech.offheapstore.storage.restartable.AbstractRestartabilityIT;
import com.terracottatech.offheapstore.storage.restartable.LinkedNode;
import com.terracottatech.offheapstore.storage.restartable.LinkedNodePortability;
import com.terracottatech.offheapstore.storage.restartable.OffHeapObjectManagerStripe;
import com.terracottatech.offheapstore.storage.restartable.RestartableStorageEngine;
import org.terracotta.offheapstore.util.Factory;
import org.terracotta.offheapstore.util.MemoryUnit;

import static java.lang.Math.max;
import static org.terracotta.offheapstore.util.MemoryUnit.BYTES;

@RunWith(BlockJUnit4ClassRunner.class)
public class ConcurrentDiskMapRestartabilityIT extends AbstractRestartabilityIT {

  private File testFile;

  @Before
  public void createFile() throws IOException {
    testFile = File.createTempFile(getClass().getSimpleName(), ".data");
    testFile.deleteOnExit();
  }
  
  @After
  public void deleteFile() {
    testFile.delete();
  }
  
  @Override
  protected <K, V> Map<K, V> createRestartableMap(long size,
                                                  MemoryUnit unit,
                                                  ByteBuffer id,
                                                  RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> persistence,
                                                  RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectMgr,
                                                  Portability<? super K> keyPortability,
                                                  Portability<? super V> valuePortability,
                                                  boolean synchronous) {
    MappedPageSource source;
    try {
      source = new MappedPageSource(testFile, unit.toBytes(size));
    } catch (IOException e) {
      throw new AssertionError(e);
    }
    Factory<RestartableStorageEngine<FileBackedStorageEngine<K, LinkedNode<V>>, ByteBuffer, K, V>> storageEngineFactory =
        RestartableStorageEngine.createFactory(id, persistence, FileBackedStorageEngine.<K, LinkedNode<V>>createFactory(source,
            max(unit.toBytes(size) / 160, 1024), BYTES, keyPortability,
        new LinkedNodePortability<V>(valuePortability)), synchronous);
    ConcurrentOffHeapHashMap<K, V> map = new ConcurrentOffHeapHashMap<K, V>(source, storageEngineFactory);
    objectMgr.registerObject(new OffHeapObjectManagerStripe<ByteBuffer>(id, map));
    return map;
  }
}
