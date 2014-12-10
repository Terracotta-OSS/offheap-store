/*
 * All content copyright (c) 2010-2012 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.util;

/**
 * A generic instance factory.
 * <p>
 * Implementations of this interface can be passed to the striped map
 * constructors to provide storage engine instances for the different stripes.
 *
 * @param <T> type constructed by the factory
 * 
 * @author Chris Dennis
 */
public interface Factory<T> {

  /**
   * Create a new instance.
   *
   * @return a new instance
   */
  T newInstance();
}
