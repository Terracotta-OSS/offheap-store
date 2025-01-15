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
package com.terracottatech.offheapstore.storage.restartable;

import java.nio.ByteBuffer;

import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;
import org.terracotta.offheapstore.storage.OffHeapBufferStorageEngine;
import org.terracotta.offheapstore.storage.portability.StringPortability;
import org.terracotta.offheapstore.util.MemoryUnit;

public class RestartableOffHeapBufferStorageEngineIT extends RestartableStorageEngineIT {

  @Override
  protected RestartableStorageEngine<?, String, String, String> createEngine() {
    PageSource source = new UpfrontAllocatingPageSource(new OffHeapBufferSource(), MemoryUnit.MEGABYTES.toBytes(4), MemoryUnit.KILOBYTES.toBytes(4));
    OffHeapBufferStorageEngine<String, LinkedNode<String>> delegate = create(source, MemoryUnit.KILOBYTES.toBytes(1), StringPortability.INSTANCE, new LinkedNodePortability<String>(StringPortability.INSTANCE));
    return new RestartableStorageEngine<OffHeapBufferStorageEngine<String, LinkedNode<String>>, String, String, String>("id", new NoOpRestartStore<String, ByteBuffer, ByteBuffer>(), delegate, true);
  }
}
