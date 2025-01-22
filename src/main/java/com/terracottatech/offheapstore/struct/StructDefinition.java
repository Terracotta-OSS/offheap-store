/*
 * All content copyright (c) Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */

package com.terracottatech.offheapstore.struct;

import java.util.Map;

import org.terracotta.offheapstore.paging.OffHeapStorageArea;

/**
 *
 * @author cdennis
 */
public interface StructDefinition<T> {
  
  Map<String, StructType> structure();
  
  Pointer<T> allocate(OffHeapStorageArea storage);
}
