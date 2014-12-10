/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.concurrent;

import com.terracottatech.offheapstore.MapInternals;

import java.util.List;

/**
 *
 * @author Chris Dennis
 */
public interface ConcurrentMapInternals extends MapInternals {

  List<MapInternals> getSegmentInternals();
}
