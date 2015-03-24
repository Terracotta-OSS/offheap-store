/* 
 * Copyright 2015 Terracotta, Inc., a Software AG company.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.terracotta.offheapstore.paging;

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
   * @param size size of page to allocate
   * @param thief {@code true} if the allocation can steal space from victims
   * @param victim {@code true} if the allocated page should be eligible for stealing
   * @param owner owner from which subsequent steal should occur
   * @return an allocated page, or {@code null} in the case of failure
   */
  Page allocate(int size, boolean thief, boolean victim, OffHeapStorageArea owner);

  void free(Page page);
}
