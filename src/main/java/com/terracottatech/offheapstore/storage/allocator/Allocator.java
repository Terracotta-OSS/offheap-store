/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */
package com.terracottatech.offheapstore.storage.allocator;

public interface Allocator extends Iterable<Long> {

  long allocate(long size);
  
  void free(long address);
  
  void clear();

  void expand(long size);

  long occupied();

  void validateAllocator();

  long getLastUsedAddress();
  
  long getLastUsedPointer();
  
  int getMinimalSize();

  long getMaximumAddress();

}
