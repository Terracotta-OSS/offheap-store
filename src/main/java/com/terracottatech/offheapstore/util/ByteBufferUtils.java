/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.terracottatech.offheapstore.util;

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

  public static final ByteBuffer aggregate(ByteBuffer[] buffers) {
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
