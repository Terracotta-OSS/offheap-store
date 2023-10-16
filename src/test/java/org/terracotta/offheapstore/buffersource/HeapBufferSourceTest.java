/*
 * Copyright 2015-2023 Terracotta, Inc., a Software AG company.
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
package org.terracotta.offheapstore.buffersource;

import org.terracotta.offheapstore.buffersource.HeapBufferSource;
import java.nio.ByteBuffer;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author Chris Dennis
 */
public class HeapBufferSourceTest {

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
