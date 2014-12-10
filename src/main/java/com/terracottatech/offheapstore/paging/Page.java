/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.paging;

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
