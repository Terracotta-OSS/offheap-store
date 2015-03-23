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
package org.terracotta.offheapstore.util;

import org.terracotta.offheapstore.util.MemoryUnit;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import org.junit.Test;

public class MemoryUnitTest {

  @Test
  public void testBasicConversions() {
    assertThat(MemoryUnit.BITS.toBits(1L), is(1L));
    assertThat(MemoryUnit.NIBBLES.toBits(1L), is(4L));
    assertThat(MemoryUnit.BYTES.toBits(1L), is(8L));
    assertThat(MemoryUnit.KILOBYTES.toBits(1L), is(8L * 1024L));
    assertThat(MemoryUnit.MEGABYTES.toBits(1L), is(8L * 1024L * 1024L));
    assertThat(MemoryUnit.GIGABYTES.toBits(1L), is(8L * 1024L * 1024L * 1024L));
    assertThat(MemoryUnit.TERABYTES.toBits(1L), is(8L * 1024L * 1024L * 1024L * 1024L));
    
    assertThat(MemoryUnit.BITS.toBytes(1L), is(0L));
    assertThat(MemoryUnit.NIBBLES.toBytes(1L), is(0L));
    assertThat(MemoryUnit.BYTES.toBytes(1L), is(1L));
    assertThat(MemoryUnit.KILOBYTES.toBytes(1L), is(1024L));
    assertThat(MemoryUnit.MEGABYTES.toBytes(1L), is(1024L * 1024L));
    assertThat(MemoryUnit.GIGABYTES.toBytes(1L), is(1024L * 1024L * 1024L));
    assertThat(MemoryUnit.TERABYTES.toBytes(1L), is(1024L * 1024L * 1024L * 1024L));

    assertThat(MemoryUnit.BITS.convert(1L, MemoryUnit.BITS), is(1L));
    assertThat(MemoryUnit.NIBBLES.convert(1L, MemoryUnit.BITS), is(0L));
    assertThat(MemoryUnit.BYTES.convert(1L, MemoryUnit.BITS), is(0L));
    assertThat(MemoryUnit.KILOBYTES.convert(1L, MemoryUnit.BITS), is(0L));
    assertThat(MemoryUnit.MEGABYTES.convert(1L, MemoryUnit.BITS), is(0L));
    assertThat(MemoryUnit.GIGABYTES.convert(1L, MemoryUnit.BITS), is(0L));
    assertThat(MemoryUnit.TERABYTES.convert(1L, MemoryUnit.BITS), is(0L));

    assertThat(MemoryUnit.BITS.convert(1L, MemoryUnit.NIBBLES), is(4L));
    assertThat(MemoryUnit.NIBBLES.convert(1L, MemoryUnit.NIBBLES), is(1L));
    assertThat(MemoryUnit.BYTES.convert(1L, MemoryUnit.NIBBLES), is(0L));
    assertThat(MemoryUnit.KILOBYTES.convert(1L, MemoryUnit.NIBBLES), is(0L));
    assertThat(MemoryUnit.MEGABYTES.convert(1L, MemoryUnit.NIBBLES), is(0L));
    assertThat(MemoryUnit.GIGABYTES.convert(1L, MemoryUnit.NIBBLES), is(0L));
    assertThat(MemoryUnit.TERABYTES.convert(1L, MemoryUnit.NIBBLES), is(0L));

    assertThat(MemoryUnit.BITS.convert(1L, MemoryUnit.BYTES), is(8L));
    assertThat(MemoryUnit.NIBBLES.convert(1L, MemoryUnit.BYTES), is(2L));
    assertThat(MemoryUnit.BYTES.convert(1L, MemoryUnit.BYTES), is(1L));
    assertThat(MemoryUnit.KILOBYTES.convert(1L, MemoryUnit.BYTES), is(0L));
    assertThat(MemoryUnit.MEGABYTES.convert(1L, MemoryUnit.BYTES), is(0L));
    assertThat(MemoryUnit.GIGABYTES.convert(1L, MemoryUnit.BYTES), is(0L));
    assertThat(MemoryUnit.TERABYTES.convert(1L, MemoryUnit.BYTES), is(0L));

    assertThat(MemoryUnit.BITS.convert(1L, MemoryUnit.KILOBYTES), is(8L * 1024L));
    assertThat(MemoryUnit.NIBBLES.convert(1L, MemoryUnit.KILOBYTES), is(2L * 1024L));
    assertThat(MemoryUnit.BYTES.convert(1L, MemoryUnit.KILOBYTES), is(1024L));
    assertThat(MemoryUnit.KILOBYTES.convert(1L, MemoryUnit.KILOBYTES), is(1L));
    assertThat(MemoryUnit.MEGABYTES.convert(1L, MemoryUnit.KILOBYTES), is(0L));
    assertThat(MemoryUnit.GIGABYTES.convert(1L, MemoryUnit.KILOBYTES), is(0L));
    assertThat(MemoryUnit.TERABYTES.convert(1L, MemoryUnit.KILOBYTES), is(0L));

    assertThat(MemoryUnit.BITS.convert(1L, MemoryUnit.MEGABYTES), is(8L * 1024L * 1024L));
    assertThat(MemoryUnit.NIBBLES.convert(1L, MemoryUnit.MEGABYTES), is(2L * 1024L * 1024L));
    assertThat(MemoryUnit.BYTES.convert(1L, MemoryUnit.MEGABYTES), is(1024L * 1024L));
    assertThat(MemoryUnit.KILOBYTES.convert(1L, MemoryUnit.MEGABYTES), is(1024L));
    assertThat(MemoryUnit.MEGABYTES.convert(1L, MemoryUnit.MEGABYTES), is(1L));
    assertThat(MemoryUnit.GIGABYTES.convert(1L, MemoryUnit.MEGABYTES), is(0L));
    assertThat(MemoryUnit.TERABYTES.convert(1L, MemoryUnit.MEGABYTES), is(0L));

    assertThat(MemoryUnit.BITS.convert(1L, MemoryUnit.GIGABYTES), is(8L * 1024L * 1024L * 1024L));
    assertThat(MemoryUnit.NIBBLES.convert(1L, MemoryUnit.GIGABYTES), is(2L * 1024L * 1024L * 1024L));
    assertThat(MemoryUnit.BYTES.convert(1L, MemoryUnit.GIGABYTES), is(1024L * 1024L * 1024L));
    assertThat(MemoryUnit.KILOBYTES.convert(1L, MemoryUnit.GIGABYTES), is(1024L * 1024L));
    assertThat(MemoryUnit.MEGABYTES.convert(1L, MemoryUnit.GIGABYTES), is(1024L));
    assertThat(MemoryUnit.GIGABYTES.convert(1L, MemoryUnit.GIGABYTES), is(1L));
    assertThat(MemoryUnit.TERABYTES.convert(1L, MemoryUnit.GIGABYTES), is(0L));

    assertThat(MemoryUnit.BITS.convert(1L, MemoryUnit.TERABYTES), is(8L * 1024L * 1024L * 1024L * 1024L));
    assertThat(MemoryUnit.NIBBLES.convert(1L, MemoryUnit.TERABYTES), is(2L * 1024L * 1024L * 1024L * 1024L));
    assertThat(MemoryUnit.BYTES.convert(1L, MemoryUnit.TERABYTES), is(1024L * 1024L * 1024L * 1024L));
    assertThat(MemoryUnit.KILOBYTES.convert(1L, MemoryUnit.TERABYTES), is(1024L * 1024L * 1024L));
    assertThat(MemoryUnit.MEGABYTES.convert(1L, MemoryUnit.TERABYTES), is(1024L * 1024L));
    assertThat(MemoryUnit.GIGABYTES.convert(1L, MemoryUnit.TERABYTES), is(1024L));
    assertThat(MemoryUnit.TERABYTES.convert(1L, MemoryUnit.TERABYTES), is(1L));
  }
    
  @Test
  public void testThresholdConditions() {
    assertThat(MemoryUnit.BITS.toBytes(7L), is(0L));
    assertThat(MemoryUnit.BITS.toBytes(9L), is(1L));
    assertThat(MemoryUnit.NIBBLES.toBytes(1L), is(0L));
    assertThat(MemoryUnit.NIBBLES.toBytes(3L), is(1L));

    assertThat(MemoryUnit.NIBBLES.convert(3L, MemoryUnit.BITS), is(0L));
    assertThat(MemoryUnit.NIBBLES.convert(5L, MemoryUnit.BITS), is(1L));
    assertThat(MemoryUnit.BYTES.convert(7L, MemoryUnit.BITS), is(0L));
    assertThat(MemoryUnit.BYTES.convert(9L, MemoryUnit.BITS), is(1L));
    assertThat(MemoryUnit.KILOBYTES.convert((8L * 1024L) - 1, MemoryUnit.BITS), is(0L));
    assertThat(MemoryUnit.KILOBYTES.convert((8L * 1024L) + 1, MemoryUnit.BITS), is(1L));
    assertThat(MemoryUnit.MEGABYTES.convert((8L * 1024L * 1024L) - 1, MemoryUnit.BITS), is(0L));
    assertThat(MemoryUnit.MEGABYTES.convert((8L * 1024L * 1024L) + 1, MemoryUnit.BITS), is(1L));
    assertThat(MemoryUnit.GIGABYTES.convert((8L * 1024L * 1024L * 1024L) - 1, MemoryUnit.BITS), is(0L));
    assertThat(MemoryUnit.GIGABYTES.convert((8L * 1024L * 1024L * 1024L) + 1, MemoryUnit.BITS), is(1L));
    assertThat(MemoryUnit.TERABYTES.convert((8L * 1024L * 1024L * 1024L * 1024L) - 1, MemoryUnit.BITS), is(0L));
    assertThat(MemoryUnit.TERABYTES.convert((8L * 1024L * 1024L * 1024L * 1024L) + 1, MemoryUnit.BITS), is(1L));

    assertThat(MemoryUnit.BYTES.convert(1L, MemoryUnit.NIBBLES), is(0L));
    assertThat(MemoryUnit.BYTES.convert(3L, MemoryUnit.NIBBLES), is(1L));
    assertThat(MemoryUnit.KILOBYTES.convert((2 * 1024L) - 1, MemoryUnit.NIBBLES), is(0L));
    assertThat(MemoryUnit.KILOBYTES.convert((2 * 1024L) + 1, MemoryUnit.NIBBLES), is(1L));
    assertThat(MemoryUnit.MEGABYTES.convert((2 * 1024L * 1024L) - 1, MemoryUnit.NIBBLES), is(0L));
    assertThat(MemoryUnit.MEGABYTES.convert((2 * 1024L * 1024L) + 1, MemoryUnit.NIBBLES), is(1L));
    assertThat(MemoryUnit.GIGABYTES.convert((2 * 1024L * 1024L * 1024L) - 1, MemoryUnit.NIBBLES), is(0L));
    assertThat(MemoryUnit.GIGABYTES.convert((2 * 1024L * 1024L * 1024L) + 1, MemoryUnit.NIBBLES), is(1L));
    assertThat(MemoryUnit.TERABYTES.convert((2 * 1024L * 1024L * 1024L * 1024L) - 1, MemoryUnit.NIBBLES), is(0L));
    assertThat(MemoryUnit.TERABYTES.convert((2 * 1024L * 1024L * 1024L * 1024L) + 1, MemoryUnit.NIBBLES), is(1L));

    assertThat(MemoryUnit.KILOBYTES.convert(1024L - 1, MemoryUnit.BYTES), is(0L));
    assertThat(MemoryUnit.KILOBYTES.convert(1024L + 1, MemoryUnit.BYTES), is(1L));
    assertThat(MemoryUnit.MEGABYTES.convert((1024L * 1024L) - 1, MemoryUnit.BYTES), is(0L));
    assertThat(MemoryUnit.MEGABYTES.convert((1024L * 1024L) + 1, MemoryUnit.BYTES), is(1L));
    assertThat(MemoryUnit.GIGABYTES.convert((1024L * 1024L * 1024L) - 1, MemoryUnit.BYTES), is(0L));
    assertThat(MemoryUnit.GIGABYTES.convert((1024L * 1024L * 1024L) + 1, MemoryUnit.BYTES), is(1L));
    assertThat(MemoryUnit.TERABYTES.convert((1024L * 1024L * 1024L * 1024L) - 1, MemoryUnit.BYTES), is(0L));
    assertThat(MemoryUnit.TERABYTES.convert((1024L * 1024L * 1024L * 1024L) + 1, MemoryUnit.BYTES), is(1L));

    assertThat(MemoryUnit.MEGABYTES.convert(1024L - 1, MemoryUnit.KILOBYTES), is(0L));
    assertThat(MemoryUnit.MEGABYTES.convert(1024L + 1, MemoryUnit.KILOBYTES), is(1L));
    assertThat(MemoryUnit.GIGABYTES.convert((1024L * 1024L) - 1, MemoryUnit.KILOBYTES), is(0L));
    assertThat(MemoryUnit.GIGABYTES.convert((1024L * 1024L) + 1, MemoryUnit.KILOBYTES), is(1L));
    assertThat(MemoryUnit.TERABYTES.convert((1024L * 1024L * 1024L) - 1, MemoryUnit.KILOBYTES), is(0L));
    assertThat(MemoryUnit.TERABYTES.convert((1024L * 1024L * 1024L) + 1, MemoryUnit.KILOBYTES), is(1L));

    assertThat(MemoryUnit.GIGABYTES.convert(1024L - 1, MemoryUnit.MEGABYTES), is(0L));
    assertThat(MemoryUnit.GIGABYTES.convert(1024L + 1, MemoryUnit.MEGABYTES), is(1L));
    assertThat(MemoryUnit.TERABYTES.convert((1024L * 1024L) - 1, MemoryUnit.MEGABYTES), is(0L));
    assertThat(MemoryUnit.TERABYTES.convert((1024L * 1024L) + 1, MemoryUnit.MEGABYTES), is(1L));

    assertThat(MemoryUnit.TERABYTES.convert(1024L - 1, MemoryUnit.GIGABYTES), is(0L));
    assertThat(MemoryUnit.TERABYTES.convert(1024L + 1, MemoryUnit.GIGABYTES), is(1L));
  }
}
