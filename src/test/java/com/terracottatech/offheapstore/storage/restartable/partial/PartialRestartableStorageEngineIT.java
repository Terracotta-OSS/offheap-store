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
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.offheapstore.storage.restartable.partial;

import com.terracottatech.frs.RestartStore;
import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;
import org.terracotta.offheapstore.storage.portability.StringPortability;
import org.terracotta.offheapstore.util.MemoryUnit;

import java.nio.ByteBuffer;

/**
 *
 * @author cdennis
 */
public class PartialRestartableStorageEngineIT extends MinimalRestartableStorageEngineIT {

  @Override
  protected RestartableMinimalStorageEngine<String, String, String> createEngine(RestartStore<String, ByteBuffer, ByteBuffer> frs) {
    PageSource source = new UpfrontAllocatingPageSource(new OffHeapBufferSource(), MemoryUnit.MEGABYTES.toBytes(1), MemoryUnit.MEGABYTES.toBytes(1));
    return new RestartablePartialStorageEngine<String, String, String>("id", frs, true, getPointerSize(), source, MemoryUnit.KILOBYTES.toBytes(1), StringPortability.INSTANCE, StringPortability.INSTANCE, 0.0f);
  }
}
