/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.buffersource;

import java.nio.ByteBuffer;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author cdennis
 */
public class OffHeapBufferSourceTest {

  @Test
  public void testAllocatedBufferProperties() {
    Random rndm = new Random();

    int size = rndm.nextInt(1024);
    ByteBuffer buffer = new HeapBufferSource().allocateBuffer(size);

    Assert.assertFalse(buffer.isDirect());
    Assert.assertEquals(size, buffer.capacity());
    Assert.assertEquals(size, buffer.remaining());
  }
}
