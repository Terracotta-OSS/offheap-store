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
package org.terracotta.offheapstore.paging;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.hamcrest.core.CombinableMatcher;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.terracotta.offheapstore.buffersource.BufferSource;
import org.terracotta.offheapstore.buffersource.HeapBufferSource;
import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import org.terracotta.offheapstore.concurrent.ConcurrentOffHeapHashMap;
import org.terracotta.offheapstore.storage.PointerSize;
import org.terracotta.offheapstore.storage.StringStorageEngine;
import org.terracotta.offheapstore.util.Factory;
import org.terracotta.offheapstore.util.MemoryUnit;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.hamcrest.collection.IsArray.array;
import static org.hamcrest.core.IsAnything.anything;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.StringContains.containsString;
import static org.hamcrest.core.StringStartsWith.startsWith;

public class UpfrontAllocatingPageSourceTest {

  private static final Lock LOCK = new ReentrantLock();
  @Before
  public void lock() {
    LOCK.lock();
  }

  @After
  public void unlock() {
    LOCK.unlock();
  }

  @Test
  public void testUpfrontAllocatingBufferSource() {
    PageSource source = new UpfrontAllocatingPageSource(new OffHeapBufferSource(), 2 * 1024, 1024);

    Page p = source.allocate(1024, false, false, null);
    Assert.assertNotNull(p);
    Assert.assertEquals(1024, p.size());

    Assert.assertNull(source.allocate(1024 * 2, false, false, null));

    for (int i = 0; i < 8; i++) {
      Assert.assertEquals(128, source.allocate(128, false, false, null).size());
    }

    Assert.assertNull(source.allocate(1, false, false, null));
  }

  @Test
  public void testUpfrontAllocationFreeing() {
    PageSource test = new UpfrontAllocatingPageSource(new OffHeapBufferSource(), 1024, 1024);

    Page p = test.allocate(512, false, false, null);

    test.free(p);
  }

  @Test
  public void testUpfrontAllocationResizing() {
    Map<String, String> map = new ConcurrentOffHeapHashMap<String, String>(new UnlimitedPageSource(new OffHeapBufferSource()), new Factory<StringStorageEngine>() {

      private final PageSource source = new UpfrontAllocatingPageSource(new OffHeapBufferSource(), 1024 * 1024, 128 * 1024);
      @Override
      public StringStorageEngine newInstance() {
        return new StringStorageEngine(PointerSize.INT, source, 1);
      }
    }, 1, 4);

    for (int i = 0; i < 100; i++) {
      map.put(Integer.toString(i), "Hello World");
      Assert.assertTrue(map.containsKey(Integer.toString(i)));
    }
  }

  @Test
  public void testReallyLargeUpfrontAllocation() {
    try {
      new UpfrontAllocatingPageSource(new OffHeapBufferSource(), Long.MAX_VALUE, MemoryUnit.GIGABYTES.toBytes(1));
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      Assert.assertThat(e.getMessage(), CombinableMatcher.<String>either(containsString("physical memory")).or(containsString("allocate more off-heap memory")));
    }
  }

  @Test
  public void testVariableChunkSizesSuccess() {
    Map<Integer, Integer> chunks = new HashMap<Integer, Integer>();

    //Allocate 128k successfully
    chunks.put(64 * 1024, 1);
    chunks.put(32 * 1024, 1);
    chunks.put(16 * 1024, 2);

    TestChunkedBufferSource source = new TestChunkedBufferSource(chunks);
    new UpfrontAllocatingPageSource(source, 128 * 1024, 128 * 1024, 16 * 1024);
  }

  @Test
  public void testVariableChunkSizesFailureDueToLimitation() {
    Map<Integer, Integer> chunks = new HashMap<Integer, Integer>();

    //Allocate 128k successfully
    chunks.put(64 * 1024, 1);
    chunks.put(32 * 1024, 1);
    chunks.put(16 * 1024, 2);

    TestChunkedBufferSource source = new TestChunkedBufferSource(chunks);
    try {
      new UpfrontAllocatingPageSource(source, 128 * 1024, 128 * 1024, 32 * 1024);
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      //expected
    }
  }

  @Test
  public void testVariableChunkSizesFailureDueToExhaustion() {
    Map<Integer, Integer> chunks = new HashMap<Integer, Integer>();

    //Allocate 128k successfully
    chunks.put(64 * 1024, 1);
    chunks.put(32 * 1024, 1);
    chunks.put(16 * 1024, 1);

    TestChunkedBufferSource source = new TestChunkedBufferSource(chunks);
    try {
      new UpfrontAllocatingPageSource(source, 128 * 1024, 128 * 1024, 16 * 1024);
      Assert.fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      //expected
    }
  }

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testAllocatorLogging() throws IOException {
    File logLocation = tempFolder.newFolder("testAllocatorLogging");
    Assert.assertThat(logLocation.listFiles(), array());
    System.setProperty(UpfrontAllocatingPageSource.ALLOCATION_LOG_LOCATION, logLocation.getAbsolutePath());
    try {
      new UpfrontAllocatingPageSource(new HeapBufferSource(), 128 * 1024, 1024, 512);
    } finally {
      System.clearProperty(UpfrontAllocatingPageSource.ALLOCATION_LOG_LOCATION);
    }

    File[] files = logLocation.listFiles();
    Assert.assertThat(files, array(anything()));
    LineNumberReader reader = new LineNumberReader(new InputStreamReader(new FileInputStream(files[0]), "US-ASCII"));
    try {
      Assert.assertThat(reader.readLine(), startsWith("Timestamp: "));
      Assert.assertThat(reader.readLine(), equalTo("Allocating: 128KB"));
      Assert.assertThat(reader.readLine(), equalTo("Max Chunk: 1KB"));
      Assert.assertThat(reader.readLine(), equalTo("Min Chunk: 512B"));
      Assert.assertThat(reader.readLine(), equalTo("timestamp,duration,size,allocated,physfree,totalswap,freeswap,committed"));
      while (true) {
        String line = reader.readLine();
        if (line == null) {
          break;
        } else {
          Assert.assertThat(reader.readLine(), new TypeSafeMatcher<String>() {
            @Override
            protected boolean matchesSafely(String t) {
              return t.matches("(?:\\d+|null),(?:\\d+|null),(?:\\d+|null),(?:\\d+|null),(?:\\d+|null),(?:\\d+|null),(?:\\d+|null),(?:\\d+|null)");
            }

            @Override
            public void describeTo(Description description) {
              description.appendText("line matching \"(?:\\d+|null),(?:\\d+|null),(?:\\d+|null),(?:\\d+|null),(?:\\d+|null),(?:\\d+|null),(?:\\d+|null),(?:\\d+|null)\"");
            }
          });
        }
      }
    } finally {
      reader.close();
    }
  }

  static class TestChunkedBufferSource implements BufferSource {

    private final Map<Integer, Integer> chunks;

    public TestChunkedBufferSource(Map<Integer, Integer> chunks) {
      this.chunks = chunks;
    }

    @Override
    public ByteBuffer allocateBuffer(int size) {
      Integer available = chunks.get(size);

      if (available == null) {
        return null;
      } else if (available == 1) {
        chunks.remove(size);
        return ByteBuffer.allocate(size);
      } else if (available > 1) {
        chunks.put(size, available.intValue() - 1);
        return ByteBuffer.allocate(size);
      } else {
        return null;
      }
    }
  }
}
