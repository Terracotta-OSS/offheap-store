/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */
package com.terracottatech.offheapstore.storage;

public enum PointerSize {
  
  INT(Integer.SIZE), LONG(Long.SIZE);

  private final int size;
  
  private PointerSize(int size) {
    this.size = size;
  }
  
  public int bitSize() {
    return size;
  }
  
  public int byteSize() {
    return size / Byte.SIZE;
  }
}
