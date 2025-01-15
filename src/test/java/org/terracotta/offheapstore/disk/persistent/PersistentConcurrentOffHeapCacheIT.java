/*
 * Copyright 2015-2023 Terracotta, Inc., a Software AG company.
 * Copyright IBM Corp. 2024, 2025
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
package org.terracotta.offheapstore.disk.persistent;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.junit.Assert;
import org.junit.Test;

import org.terracotta.offheapstore.disk.AbstractDiskTest;
import org.terracotta.offheapstore.disk.paging.MappedPageSource;
import org.terracotta.offheapstore.disk.storage.FileBackedStorageEngine;
import org.terracotta.offheapstore.disk.storage.PersistentIntegerStorageEngine;
import org.terracotta.offheapstore.disk.storage.PersistentSplitStorageEngine;
import org.terracotta.offheapstore.disk.storage.portability.PersistentByteArrayPortability;
import org.terracotta.offheapstore.disk.storage.portability.PersistentSerializablePortability;
import org.terracotta.offheapstore.util.MemoryUnit;

/**
 *
 * @author Chris Dennis
 */
public class PersistentConcurrentOffHeapCacheIT extends AbstractDiskTest {

  @Test
  public void testTableOnlyPersistence() throws IOException {
    byte[] indexData;

    PersistentConcurrentOffHeapClockCache<Integer, Integer> map = new PersistentConcurrentOffHeapClockCache<>(new MappedPageSource(dataFile), PersistentSplitStorageEngine
      .createPersistentFactory(PersistentIntegerStorageEngine.createPersistentFactory(), PersistentIntegerStorageEngine.createPersistentFactory()));
    try {
      for (int i = 0; i < 100; i++) {
        map.put(i, i);
      }

      indexData = persist(map);
    } finally {
      map.close();
    }


    PersistentConcurrentOffHeapClockCache<Integer, Integer> clone = null;
    try {
      ByteArrayInputStream bin = new ByteArrayInputStream(indexData);
      try (ObjectInputStream oin = new ObjectInputStream(bin)) {
        clone = new PersistentConcurrentOffHeapClockCache<>(oin, new MappedPageSource(dataFile, false), PersistentSplitStorageEngine
          .createPersistentFactory(PersistentIntegerStorageEngine.createPersistentFactory(), PersistentIntegerStorageEngine
            .createPersistentFactory()));
        clone.bootstrap(oin);
      }

      Assert.assertEquals(100, clone.size());

      for (int i = 0; i < 100; i++) {
        Assert.assertEquals(i, clone.get(i).intValue());
      }
    } finally {
      if (clone != null) {
        clone.close();
      }
    }
  }

  @Test
  public void testFullPersistence() throws IOException {
    byte[] indexData;

    long occupiedSize = -1;

    MappedPageSource source = new MappedPageSource(dataFile);
    PersistentConcurrentOffHeapClockCache<Integer, byte[]> map = new PersistentConcurrentOffHeapClockCache<>(source, FileBackedStorageEngine
      .createFactory(source, Long.MAX_VALUE, MemoryUnit.BYTES, new PersistentSerializablePortability(), PersistentByteArrayPortability.INSTANCE));
    try {
      for (int i = 0; i < 100; i++) {
        map.put(i, new byte[i]);
      }

      occupiedSize = map.getOccupiedMemory();
      indexData = persist(map);
    } finally {
      map.close();
    }


    PersistentConcurrentOffHeapClockCache<Integer, byte[]> clone = null;
    try {
      ByteArrayInputStream bin = new ByteArrayInputStream(indexData);
      try (ObjectInputStream din = new ObjectInputStream(bin)) {
        MappedPageSource clonedSource = new MappedPageSource(dataFile, false);
        clone = new PersistentConcurrentOffHeapClockCache<>(din, clonedSource, FileBackedStorageEngine.createFactory(clonedSource, Long.MAX_VALUE, MemoryUnit.BYTES, new PersistentSerializablePortability(), PersistentByteArrayPortability.INSTANCE, false));
        clone.bootstrap(din);
      }

      Assert.assertEquals(100, clone.size());
      Assert.assertEquals(occupiedSize, clone.getOccupiedMemory());

      for (int i = 0; i < 100; i++) {
        Assert.assertEquals(i, clone.get(i).length);
      }
    } finally {
      if (clone != null) {
        clone.close();
      }
    }
  }

  @Test
  public void testSerializableValuesPersistence() throws IOException {
    byte[] indexData;

    MappedPageSource source = new MappedPageSource(dataFile);
    PersistentConcurrentOffHeapClockCache<Integer, Serializable> map = new PersistentConcurrentOffHeapClockCache<>(source, FileBackedStorageEngine
      .createFactory(source, Long.MAX_VALUE, MemoryUnit.BYTES, new PersistentSerializablePortability(), new PersistentSerializablePortability()));
    try {
      for (int i = 0; i < 100; i++) {
        map.put(i, "Hello World");
      }

      indexData = persist(map);
    } finally {
      map.close();
    }


    PersistentConcurrentOffHeapClockCache<Integer, Serializable> clone = null;
    try {
      ByteArrayInputStream bin = new ByteArrayInputStream(indexData);
      try (ObjectInputStream din = new ObjectInputStream(bin)) {
        MappedPageSource clonedSource = new MappedPageSource(dataFile, false);
        clone = new PersistentConcurrentOffHeapClockCache<>(din, clonedSource, FileBackedStorageEngine.createFactory(clonedSource, Long.MAX_VALUE, MemoryUnit.BYTES, new PersistentSerializablePortability(), new PersistentSerializablePortability(), false));
        clone.bootstrap(din);
      }

      Assert.assertEquals(100, clone.size());

      for (int i = 0; i < 100; i++) {
        Assert.assertEquals("Hello World", clone.get(i));
      }
    } finally {
      if (clone != null) {
        clone.close();
      }
    }
  }

  @Test
  public void testSerializableValuesPersistenceWithNewTypes() throws IOException {
    byte[] indexData;

    MappedPageSource source = new MappedPageSource(dataFile);
    PersistentConcurrentOffHeapClockCache<Number, Serializable> map = new PersistentConcurrentOffHeapClockCache<>(source, FileBackedStorageEngine
      .createFactory(source, Long.MAX_VALUE, MemoryUnit.BYTES, new PersistentSerializablePortability(), new PersistentSerializablePortability()));
    try {
      for (int i = 0; i < 100; i++) {
        map.put(i, "Hello World");
      }

      indexData = persist(map);
    } finally {
      map.close();
    }


    PersistentConcurrentOffHeapClockCache<Number, Serializable> clone = null;
    try {
      ByteArrayInputStream bin = new ByteArrayInputStream(indexData);
      try (ObjectInputStream din = new ObjectInputStream(bin)) {
        MappedPageSource clonedSource = new MappedPageSource(dataFile, false);
        clone = new PersistentConcurrentOffHeapClockCache<>(din, clonedSource, FileBackedStorageEngine.createFactory(clonedSource, Long.MAX_VALUE, MemoryUnit.BYTES, new PersistentSerializablePortability(), new PersistentSerializablePortability(), false));
        clone.bootstrap(din);
      }

      Assert.assertEquals(100, clone.size());

      for (int i = 0; i < 100; i++) {
        Assert.assertEquals("Hello World", clone.get(i));
      }

      for (long i = 0L; i < 100L; i++) {
        clone.put(i, "Hello World");
      }

      Assert.assertEquals(200, clone.size());

      for (int i = 0; i < 100; i++) {
        Assert.assertEquals("Hello World", clone.get(i));
      }

      for (long i = 0L; i < 100L; i++) {
        Assert.assertEquals("Hello World", clone.get(i));
      }
    } finally {
      if (clone != null) {
        clone.close();
      }
    }
  }

  @Test
  public void testPersistenceOfPrimitiveClassType() throws IOException {
    final Class<?>[] primitives = new Class<?>[]{double.class, long.class, float.class, int.class, char.class,
      short.class, byte.class, boolean.class, void.class};

    byte[] indexData;

    MappedPageSource source = new MappedPageSource(dataFile);
    PersistentConcurrentOffHeapClockCache<Integer, Serializable> map = new PersistentConcurrentOffHeapClockCache<>(source, FileBackedStorageEngine
      .createFactory(source, Long.MAX_VALUE, MemoryUnit.BYTES, new PersistentSerializablePortability(), new PersistentSerializablePortability()));
    try {
      for (int i = 0; i < primitives.length; i++) {
        map.put(i, primitives[i]);
      }

      indexData = persist(map);
    } finally {
      map.close();
    }


    PersistentConcurrentOffHeapClockCache<Integer, Serializable> clone = null;
    try {
      ByteArrayInputStream bin = new ByteArrayInputStream(indexData);
      try (ObjectInputStream din = new ObjectInputStream(bin)) {
        MappedPageSource clonedSource = new MappedPageSource(dataFile, false);
        clone = new PersistentConcurrentOffHeapClockCache<>(din, clonedSource, FileBackedStorageEngine.createFactory(clonedSource, Long.MAX_VALUE, MemoryUnit.BYTES, new PersistentSerializablePortability(), new PersistentSerializablePortability(), false));
        clone.bootstrap(din);
      }

      Assert.assertEquals(primitives.length, clone.size());

      for (int i = 0; i < primitives.length; i++) {
        Assert.assertSame(primitives[i], clone.get(i));
      }
    } finally {
      if (clone != null) {
        clone.close();
      }
    }
  }

  @Test
  public void testFragmentedPersistence() throws IOException {
    byte[] indexData;

    MappedPageSource source = new MappedPageSource(dataFile);
    PersistentConcurrentOffHeapClockCache<Integer, byte[]> map = new PersistentConcurrentOffHeapClockCache<>(source, FileBackedStorageEngine
      .createFactory(source, Long.MAX_VALUE, MemoryUnit.BYTES, new PersistentSerializablePortability(), PersistentByteArrayPortability.INSTANCE));
    try {
      for (int i = 0; i < 100; i++) {
        map.put(i, new byte[i]);
      }

      for (int i = 0; i < 100; i+=2) {
        map.remove(i);
      }

      indexData = persist(map);
    } finally {
      map.close();
    }


    PersistentConcurrentOffHeapClockCache<Integer, byte[]> clone = null;
    try {
      ByteArrayInputStream bin = new ByteArrayInputStream(indexData);
      try (ObjectInputStream din = new ObjectInputStream(bin)) {
        MappedPageSource clonedSource = new MappedPageSource(dataFile, false);
        clone = new PersistentConcurrentOffHeapClockCache<>(din, clonedSource, FileBackedStorageEngine.createFactory(clonedSource, Long.MAX_VALUE, MemoryUnit.BYTES, new PersistentSerializablePortability(), PersistentByteArrayPortability.INSTANCE, false));
        clone.bootstrap(din);
      }

      Assert.assertEquals(50, clone.size());

      for (int i = 1; i < 100; i+=2) {
        Assert.assertEquals(i, clone.get(i).length);
      }
    } finally {
      if (clone != null) {
        clone.close();
      }
    }
  }

  private byte[] persist(PersistentConcurrentOffHeapClockCache<?, ?> map) throws IOException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    try (ObjectOutputStream oout = new ObjectOutputStream(bout)) {
      map.flush();
      map.persist(oout);
    }
    return bout.toByteArray();
  }
}
