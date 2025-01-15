/*
 * Copyright 2015-2023 Terracotta, Inc., a Software AG company.
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
package org.terracotta.offheapstore.buffersource;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An unlimited direct byte buffer source.
 *
 * @author Chris Dennis
 */
public class OffHeapBufferSource implements BufferSource {

  private static final Logger LOGGER = LoggerFactory.getLogger(OffHeapBufferSource.class);
  
  
  @Override
  public ByteBuffer allocateBuffer(int size) {
    try {
      return ByteBuffer.allocateDirect(size);
    } catch (OutOfMemoryError e) {
      LOGGER.debug("Failed to allocate " + size + " byte offheap buffer", e);
      return null;
    }
  }

  @Override
  public String toString() {
    return "Off-Heap Buffer Source";
  }
}
