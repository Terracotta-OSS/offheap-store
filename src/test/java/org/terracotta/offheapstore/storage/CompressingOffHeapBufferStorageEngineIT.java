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
package org.terracotta.offheapstore.storage;

import org.terracotta.offheapstore.storage.OffHeapBufferStorageEngine;
import org.terracotta.offheapstore.OffHeapHashMap;
import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import org.terracotta.offheapstore.exceptions.OversizeMappingException;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.paging.UnlimitedPageSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;
import org.terracotta.offheapstore.storage.portability.Portability;
import org.terracotta.offheapstore.storage.portability.SerializablePortability;
import org.terracotta.offheapstore.util.MemoryUnit;
import org.terracotta.offheapstore.util.PointerSizeParameterizedTest;

import java.io.Serializable;
import java.util.Arrays;

import org.junit.Test;

import static org.hamcrest.core.Is.*;
import static org.hamcrest.core.IsEqual.*;
import static org.hamcrest.number.OrderingComparison.*;
import static org.junit.Assert.assertThat;

/**
 *
 * @author cdennis
 */
public class CompressingOffHeapBufferStorageEngineIT extends PointerSizeParameterizedTest {

  @Test
  public void testCompressionWithExplicitHoles() {
    PageSource source = new UnlimitedPageSource(new OffHeapBufferSource());
    Portability<Serializable> portability = new SerializablePortability();

    OffHeapHashMap<Integer, String> regular = new OffHeapHashMap<>(source, new OffHeapBufferStorageEngine<>(getPointerSize(), source, MemoryUnit.KILOBYTES
      .toBytes(1), portability, portability));
    OffHeapHashMap<Integer, String> compressed = new OffHeapHashMap<>(source, new OffHeapBufferStorageEngine<>(getPointerSize(), source, MemoryUnit.KILOBYTES
      .toBytes(1), portability, portability, 1.0f));

    for (int i = 0; i < 8 * 1024; i++) {
      regular.put(i, "foobar");
      compressed.put(i, "foobar");
    }

    assertThat(compressed, equalTo(regular));
    assertThat(compressed.getDataAllocatedMemory(), is(regular.getDataAllocatedMemory()));

    for (int i = 1; i < 8 * 1024; i += 2) {
      regular.remove(i);
      compressed.remove(i);
    }

    assertThat(compressed, equalTo(regular));
    assertThat(compressed.getDataAllocatedMemory(), lessThan(regular.getDataAllocatedMemory()));
  }

  @Test
  public void testCompressionWithLargeTerminalValue() {
    PageSource source1 = new UpfrontAllocatingPageSource(new OffHeapBufferSource(), MemoryUnit.KILOBYTES.toBytes(8), MemoryUnit.KILOBYTES.toBytes(8));
    PageSource source2 = new UpfrontAllocatingPageSource(new OffHeapBufferSource(), MemoryUnit.KILOBYTES.toBytes(8), MemoryUnit.KILOBYTES.toBytes(8));
    Portability<Serializable> portability = new SerializablePortability();

    OffHeapHashMap<Integer, String> regular = new OffHeapHashMap<>(source1, new OffHeapBufferStorageEngine<>(getPointerSize(), source1, MemoryUnit.KILOBYTES
      .toBytes(1), portability, portability));
    OffHeapHashMap<Integer, String> compressed = new OffHeapHashMap<>(source2, new OffHeapBufferStorageEngine<>(getPointerSize(), source2, MemoryUnit.KILOBYTES
      .toBytes(1), portability, portability, 1.0f));

    for (int i = 0; ; i++) {
      try {
        regular.put(i, "foobar");
        compressed.put(i, "foobar");
      } catch (OversizeMappingException e) {
        char[] big = new char[1 * 1024];
        Arrays.fill(big, '!');
        String bigString = new String(big);
        for (int j = i; j >= 0; j--) {
          regular.remove(j);
          compressed.remove(j);
          try {
            regular.put(-1, bigString);
            compressed.put(-1, bigString);
            break;
          } catch (OversizeMappingException f) {
            //ignore
          }
        }
        break;
      }
    }

    assertThat(compressed, equalTo(regular));
    assertThat(compressed.getDataAllocatedMemory(), is(regular.getDataAllocatedMemory()));

    int limit = regular.size() - 1;
    for (int i = 1; i < limit; i += 2) {
      regular.remove(i);
      compressed.remove(i);
    }

    assertThat(compressed, equalTo(regular));
    //assertThat(compressed.getDataAllocatedMemory(), lessThan(regular.getDataAllocatedMemory()));
  }
}
