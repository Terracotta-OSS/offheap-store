/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.buffersource;

import java.nio.ByteBuffer;

/**
 * A source of NIO buffers of some type.
 * 
 * @author Chris Dennis
 * @param <> the buffer type this source manages
 */
public interface BufferSource {

  /**
   * Allocates a buffer of the given size.
   * <p>
   * If a suitable buffer cannot be allocated then {@code null} should be
   * returned.  Implementations may place restrictions on the valid size value
   * they will accept.
   *
   * @param size required buffer size
   * @return a buffer of the required size
   */
   ByteBuffer allocateBuffer(int size);
}
