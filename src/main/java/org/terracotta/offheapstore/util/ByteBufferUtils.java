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
package org.terracotta.offheapstore.util;

import java.nio.ByteBuffer;

/**
 *
 * @author cdennis
 */
public final class ByteBufferUtils {

  private ByteBufferUtils() {
    //no instances
  }

  public static int totalLength(ByteBuffer[] buffers) {
    int total = 0;
    for (ByteBuffer buffer : buffers) {
      total += buffer.remaining();
    }
    return total;
  }

  public static ByteBuffer aggregate(ByteBuffer[] buffers) {
    if (buffers.length == 1) {
      return buffers[0];
    } else {
      ByteBuffer aggregate = ByteBuffer.allocate(totalLength(buffers));
      for (ByteBuffer element : buffers) {
        aggregate.put(element);
      }
      return (ByteBuffer) aggregate.flip();
    }
  }
}
