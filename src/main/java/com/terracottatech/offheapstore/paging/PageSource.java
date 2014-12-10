/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.paging;

/**
 *
 * @author cdennis
 */
public interface PageSource {

  /**
   * Attempt to allocate a page of the given size.
   * <p>
   * Allocations identified as thieves will if necessary 'steal' space from
   * previous allocations identified as 'victims' in order to fulfill the
   * allocation request.  <code>owner</code> is the area from which the
   * returned page can subsequently be stolen or recovered.  This is most likely
   * to be the calling instance.
   *
   * @param size
   * @param thief
   * @param victim
   * @param owner
   * @return
   */
  Page allocate(int size, boolean thief, boolean victim, OffHeapStorageArea owner);

  void free(Page page);
}
