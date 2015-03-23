/* 
 * Copyright 2015 Terracotta, Inc., a Software AG company.
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

import org.terracotta.offheapstore.buffersource.BufferSource;

/**
 *
 * @author cdennis
 */
public class UnlimitedPageSource implements PageSource {

  private final BufferSource source;

  public UnlimitedPageSource(BufferSource source) {
    this.source = source;
  }

  @Override
  public Page allocate(final int size, boolean thief, boolean victim, OffHeapStorageArea owner) {
    ByteBuffer buffer = source.allocateBuffer(size);
    if (buffer == null) {
      return null;
    } else {
      return new Page(buffer, owner);
    }
  }

  @Override
  public void free(Page page) {
    //no-op
  }
}
