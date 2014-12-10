/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.buffersource;

import java.nio.ByteBuffer;

/**
 *
 * @author cdennis
 */
public class HeapBufferSource implements BufferSource {

  @Override
  public ByteBuffer allocateBuffer(int size) {
    return ByteBuffer.allocate(size);
  }

  @Override
  public String toString() {
    return "On-Heap Buffer Source";
  }
}
