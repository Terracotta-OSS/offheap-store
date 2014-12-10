/*
 * All content copyright (c) Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */

package com.terracottatech.offheapstore.struct;

/**
 *
 * @author cdennis
 */
public interface Pointer<T> {

  T access();
  
  void free();
}
