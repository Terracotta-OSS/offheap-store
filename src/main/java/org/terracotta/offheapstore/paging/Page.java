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
package org.terracotta.offheapstore.paging;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;

/**
 *
 * @author Chris Dennis
 */
public class Page {

  private final ByteBuffer buffer;

  private final OffHeapStorageArea binding;

  private final int index;
  private final int address;
  private boolean freeable;

  public Page(ByteBuffer buffer, OffHeapStorageArea binding) {
    this.buffer = buffer;
    this.binding = binding;
    this.index = -1;
    this.address = -1;
    this.freeable = false;
  }
  
  public Page(ByteBuffer buffer, int index, int address, OffHeapStorageArea binding) {
    this.buffer = buffer;
    this.binding = binding;
    this.index = index;
    this.address = address;
    this.freeable = true;
  }

  public ByteBuffer asByteBuffer() {
    return buffer;
  }

  public IntBuffer asIntBuffer() {
    return buffer.asIntBuffer();
  }

  public int size() {
    if (buffer == null) {
      return 0;
    } else {
      return buffer.capacity();
    }
  }

  public int index() {
    return index;
  }
  
  public int address() {
    return address;
  }
  
  public OffHeapStorageArea binding() {
    return binding;
  }

  synchronized boolean isFreeable() {
    if (freeable) {
      freeable = false;
      return true;
    } else {
      return false;
    }
  }
        
}
