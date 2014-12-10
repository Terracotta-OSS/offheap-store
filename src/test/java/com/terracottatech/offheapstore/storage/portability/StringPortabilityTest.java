/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.storage.portability;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author cdennis
 */
public class StringPortabilityTest {

  @Test
  public void testEmptyString() {
    StringPortability test = StringPortability.INSTANCE;

    String input = "";
    String result = test.decode(test.encode(input));
    Assert.assertNotNull(result);
    Assert.assertNotSame(input, result);
    Assert.assertEquals(input, result);
  }

  @Test
  public void testNormalString() {
    StringPortability test = StringPortability.INSTANCE;

    String input = "hello";
    String result = test.decode(test.encode(input));
    Assert.assertNotNull(result);
    Assert.assertNotSame(input, result);
    Assert.assertEquals(input, result);
  }

  @Test
  public void testInvalidUTFSequence() {
    StringPortability test = StringPortability.INSTANCE;

    String input = new String(new char[] {0xfdd0, 0xfdd1});
    String result = test.decode(test.encode(input));
    Assert.assertNotNull(result);
    Assert.assertNotSame(input, result);
    Assert.assertEquals(input, result);
  }
}
