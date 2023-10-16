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

import org.terracotta.offheapstore.storage.SerializableStorageEngine;
import java.util.Collections;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import org.terracotta.offheapstore.OffHeapHashMap;
import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import org.terracotta.offheapstore.paging.UnlimitedPageSource;
import org.terracotta.offheapstore.util.PointerSizeParameterizedTest;

public class SerializableStorageEngineIT extends PointerSizeParameterizedTest {

  @Test
  public void tableResizeTest() {
    Map<String, String> map = new OffHeapHashMap<>(new UnlimitedPageSource(new OffHeapBufferSource()), new SerializableStorageEngine(getPointerSize(), new UnlimitedPageSource(new OffHeapBufferSource()), 1));

    for (int i = 0; i < 100; i++) {
      map.put(Integer.toString(i), Integer.toString(i));

      for (int j = 0; j <= i; j++) {
        Assert.assertEquals(Integer.toString(j), map.get(Integer.toString(j)));
      }
    }

    Assert.assertEquals(100, map.size());
    Assert.assertFalse(map.isEmpty());

    for (int i = 0; i < 100; i++) {
      Assert.assertEquals(Integer.toString(i), map.remove(Integer.toString(i)));

      for (int j = i + 1; j < 100; j++) {
        Assert.assertEquals(Integer.toString(j), map.get(Integer.toString(j)));
      }
    }

    Assert.assertTrue(map.isEmpty());
  }

  @Test
  public void putAllClearTest() {
    Map<String, String> map = new OffHeapHashMap<>(new UnlimitedPageSource(new OffHeapBufferSource()), new SerializableStorageEngine(getPointerSize(), new UnlimitedPageSource(new OffHeapBufferSource()), 1));

    map.putAll(Collections.singletonMap("1", "1"));

    Assert.assertEquals(1, map.size());
    Assert.assertFalse(map.isEmpty());
    Assert.assertEquals(Collections.singletonMap("1", "1"), map);

    map.clear();

    Assert.assertEquals(0, map.size());
    Assert.assertTrue(map.isEmpty());
    Assert.assertEquals(Collections.emptyMap(), map);
  }
}
