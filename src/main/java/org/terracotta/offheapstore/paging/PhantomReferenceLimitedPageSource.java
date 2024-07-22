/*
 * Copyright 2015-2023 Terracotta, Inc., a Software AG company.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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

import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link PhantomReference} based limited byte buffer source.
 * <p>
 * This buffer source tracks 'freeing' of allocated byte buffers using phantom
 * references to the allocated buffers and an associated reference queue.  An
 * {@link AtomicLong} is then used to track number of available bytes for
 * allocation.
 *
 * @author Chris Dennis
 */
public class PhantomReferenceLimitedPageSource implements PageSource {

  private final ReferenceQueue<ByteBuffer> allocatedBuffers = new ReferenceQueue<>();
  private final Map<PhantomReference<ByteBuffer>, Integer> bufferSizes = new ConcurrentHashMap<>();

  private final AtomicLong max;

  /**
   * Create a source that will allocate at most {@code max} bytes.
   *
   * @param max the maximum total size of all available buffers
   */
  public PhantomReferenceLimitedPageSource(long max) {
      this.max = new AtomicLong(max);
  }

  /**
   * Allocates a byte buffer of the given size.
   * <p>
   * This {@code BufferSource} places no restrictions on the requested size of
   * the buffer.
   */
  @Override
  public Page allocate(int size, boolean thief, boolean victim, OffHeapStorageArea owner) {
      while (true) {
          processQueue();
          long now = max.get();
          if (now < size) {
            return null;
          } else if (max.compareAndSet(now, now - size)) {
            ByteBuffer buffer;
            try {
              buffer = ByteBuffer.allocateDirect(size);
            } catch (OutOfMemoryError e) {
              return null;
            }
            bufferSizes.put(new PhantomReference<>(buffer, allocatedBuffers), size);
            return new Page(buffer, owner);
          }
      }
  }

  /**
   * Frees the supplied buffer.
   * <p>
   * This implementation is a no-op, no validation of the supplied buffer is
   * attempted, as freeing of allocated buffers is monitored via phantom
   * references.
   */
  @Override
  public void free(Page buffer) {
    //no-op
  }

  private void processQueue() {
      while (true) {
          Reference<?> ref = allocatedBuffers.poll();

          if (ref == null) {
              return;
          } else {
              Integer size = bufferSizes.remove(ref);
              max.addAndGet(size.longValue());
          }
      }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("PhantomReferenceLimitedBufferSource\n");
    Collection<Integer> buffers = bufferSizes.values();
    sb.append("Bytes Available   : ").append(max.get()).append('\n');
    sb.append("Buffers Allocated : (count=").append(buffers.size()).append(") ").append(buffers);
    return sb.toString();
  }
}
