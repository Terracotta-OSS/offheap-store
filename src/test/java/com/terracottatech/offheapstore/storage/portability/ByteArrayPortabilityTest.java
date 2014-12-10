/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.storage.portability;

import java.nio.ByteBuffer;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author cdennis
 */
public class ByteArrayPortabilityTest {

  @Test
  public void testZeroLengthArray() {
    ByteArrayPortability test = ByteArrayPortability.INSTANCE;

    ByteBuffer b = test.encode(new byte[0]);

    byte[] read = test.decode(b);

    Assert.assertNotNull(read);
    Assert.assertEquals(0, read.length);
  }

  @Test
  public void testPopulatedArray() {
    ByteArrayPortability test = ByteArrayPortability.INSTANCE;

    Random rndm = new Random();

    byte[] before = new byte[rndm.nextInt(1024)];
    rndm.nextBytes(before);

    ByteBuffer b = test.encode(before);
    byte[] after = test.decode(b);

    Assert.assertNotNull(after);
    Assert.assertNotSame(before, after);
    Assert.assertArrayEquals(before, after);
  }
}
