/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.paging;

import java.nio.ByteBuffer;

import com.terracottatech.offheapstore.buffersource.BufferSource;

/**
 *
 * @author cdennis
 */
public class UnlimitedPageSource implements PageSource {

  private final BufferSource source;

  public UnlimitedPageSource(BufferSource source) {
    this.source = source;
  }

  @Override
  public Page allocate(final int size, boolean thief, boolean victim, OffHeapStorageArea owner) {
    ByteBuffer buffer = source.allocateBuffer(size);
    if (buffer == null) {
      return null;
    } else {
      return new Page(buffer, owner);
    }
  }

  @Override
  public void free(Page page) {
    //no-op
  }
}
