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
package org.terracotta.offheapstore.storage.portability;

import org.terracotta.offheapstore.storage.portability.ByteArrayPortability;
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
