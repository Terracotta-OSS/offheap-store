/*
 * Copyright 2015-2023 Terracotta, Inc., a Software AG company.
 * Copyright IBM Corp. 2024, 2026
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
package org.terracotta.offheapstore.storage.portability;

import java.nio.ByteBuffer;
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

  @Test
  public void testEqualsWithMatchingString() {
    StringPortability test = StringPortability.INSTANCE;
    String input = "testKey123";
    ByteBuffer encoded = test.encode(input);

    Assert.assertTrue(test.equals(input, encoded));
  }

  @Test
  public void testEqualsWithNonMatchingString() {
    StringPortability test = StringPortability.INSTANCE;
    ByteBuffer encoded = test.encode("testKey123");

    Assert.assertFalse(test.equals("testKey456", encoded));
  }

  @Test
  public void testEqualsWithDifferentLengthString() {
    StringPortability test = StringPortability.INSTANCE;
    ByteBuffer encoded = test.encode("testKey123");

    Assert.assertFalse(test.equals("testKey", encoded));
    Assert.assertFalse(test.equals("testKey123456", encoded));
  }

  @Test
  public void testEqualsWithEmptyString() {
    StringPortability test = StringPortability.INSTANCE;
    ByteBuffer encoded = test.encode("");

    Assert.assertTrue(test.equals("", encoded));
    Assert.assertFalse(test.equals("nonEmpty", encoded));
  }

  @Test
  public void testEqualsWithNonStringObject() {
    StringPortability test = StringPortability.INSTANCE;
    ByteBuffer encoded = test.encode("123");

    Assert.assertFalse(test.equals(123, encoded));
    Assert.assertFalse(test.equals(null, encoded));
  }
}
