/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.disk.paging;

import java.nio.MappedByteBuffer;

import com.terracottatech.offheapstore.paging.Page;

/**
 *
 * @author cdennis
 */
public class MappedPage extends Page {

  public MappedPage(MappedByteBuffer buffer) {
    super(buffer, null);
  }

  @Override
  public MappedByteBuffer asByteBuffer() {
    return (MappedByteBuffer) super.asByteBuffer();
  }
}
