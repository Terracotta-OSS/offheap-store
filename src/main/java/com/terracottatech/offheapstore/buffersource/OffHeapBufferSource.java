/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.buffersource;

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
