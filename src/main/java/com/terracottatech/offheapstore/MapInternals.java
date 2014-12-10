/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore;

/**
 *
 * @author Chris Dennis
 */
public interface MapInternals {

  /* Map oriented statistics */

  long getSize();

  long getTableCapacity();

  long getUsedSlotCount();

  long getRemovedSlotCount();

  int getReprobeLength();

  long getAllocatedMemory();

  long getOccupiedMemory();
  
  long getVitalMemory();

  long getDataAllocatedMemory();

  long getDataOccupiedMemory();
  
  long getDataVitalMemory();
  
  long getDataSize();
}
