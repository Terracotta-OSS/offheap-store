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
public class PersistentReadWriteLockedOffHeapHashMapIT extends AbstractDiskTest {

  @Test
  public void testTableOnlyPersistence() throws IOException {
    byte[] indexData;

    PersistentReadWriteLockedOffHeapHashMap<Integer, Integer> map = new PersistentReadWriteLockedOffHeapHashMap<Integer, Integer>(new MappedPageSource(dataFile), new PersistentSplitStorageEngine<Integer, Integer>(new PersistentIntegerStorageEngine(), new PersistentIntegerStorageEngine()), true);
    try {
      for (int i = 0; i < 100; i++) {
        map.put(i, i);
      }

      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      ObjectOutputStream dout = new ObjectOutputStream(bout);
      try {
        map.flush();
        map.persist(dout);
      } finally {
        dout.close();
      }
      indexData = bout.toByteArray();
    } finally {
      map.close();
    }


    PersistentReadWriteLockedOffHeapHashMap<Integer, Integer> clone = null;
    try {
      ByteArrayInputStream bin = new ByteArrayInputStream(indexData);
      ObjectInputStream din = new ObjectInputStream(bin);
      try {
        clone = new PersistentReadWriteLockedOffHeapHashMap<Integer, Integer>(new MappedPageSource(dataFile, false), new PersistentSplitStorageEngine<Integer, Integer>(new PersistentIntegerStorageEngine(), new PersistentIntegerStorageEngine()), false);
        clone.bootstrap(din);
      } finally {
        din.close();
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

    MappedPageSource source = new MappedPageSource(dataFile);
    PersistentReadWriteLockedOffHeapHashMap<Integer, byte[]> map = new PersistentReadWriteLockedOffHeapHashMap<Integer, byte[]>(source, new FileBackedStorageEngine<Integer, byte[]>(source, Long.MAX_VALUE, MemoryUnit.BYTES, new PersistentSerializablePortability(), PersistentByteArrayPortability.INSTANCE), true);
    try {
      for (int i = 0; i < 100; i++) {
        map.put(i, new byte[i]);
      }

      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      ObjectOutputStream dout = new ObjectOutputStream(bout);
      try {
        map.flush();
        map.persist(dout);
      } finally {
        dout.close();
      }
      indexData = bout.toByteArray();
    } finally {
      map.close();
    }


    PersistentReadWriteLockedOffHeapHashMap<Integer, byte[]> clone = null;
    try {
      ByteArrayInputStream bin = new ByteArrayInputStream(indexData);
      ObjectInputStream din = new ObjectInputStream(bin);
      try {
        MappedPageSource clonedSource = new MappedPageSource(dataFile, false);
        clone = new PersistentReadWriteLockedOffHeapHashMap<Integer, byte[]>(clonedSource, new FileBackedStorageEngine<Integer, byte[]>(clonedSource, Long.MAX_VALUE, MemoryUnit.BYTES, new PersistentSerializablePortability(), PersistentByteArrayPortability.INSTANCE, false), false);
        clone.bootstrap(din);
      } finally {
        din.close();
      }

      Assert.assertEquals(100, clone.size());

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
  public void testSerializableValuePersistence() throws IOException {
    byte[] indexData;

    MappedPageSource source = new MappedPageSource(dataFile);
    PersistentReadWriteLockedOffHeapHashMap<Integer, Serializable> map = new PersistentReadWriteLockedOffHeapHashMap<Integer, Serializable>(source, new FileBackedStorageEngine<Serializable, Serializable>(source, Long.MAX_VALUE, MemoryUnit.BYTES, new PersistentSerializablePortability(), new PersistentSerializablePortability()), true);
    try {
      for (int i = 0; i < 100; i++) {
        map.put(i, "Hello World");
      }

      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      ObjectOutputStream dout = new ObjectOutputStream(bout);
      try {
        map.flush();
        map.persist(dout);
      } finally {
        dout.close();
      }
      indexData = bout.toByteArray();
    } finally {
      map.close();
    }


    PersistentReadWriteLockedOffHeapHashMap<Integer, Serializable> clone = null;
    try {
      ByteArrayInputStream bin = new ByteArrayInputStream(indexData);
      ObjectInputStream din = new ObjectInputStream(bin);
      try {
        MappedPageSource clonedSource = new MappedPageSource(dataFile, false);
        clone = new PersistentReadWriteLockedOffHeapHashMap<Integer, Serializable>(clonedSource, new FileBackedStorageEngine<Serializable, Serializable>(clonedSource, Long.MAX_VALUE, MemoryUnit.BYTES, new PersistentSerializablePortability(), new PersistentSerializablePortability(), false), false);
        clone.bootstrap(din);
      } finally {
        din.close();
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
    PersistentReadWriteLockedOffHeapHashMap<Number, Serializable> map = new PersistentReadWriteLockedOffHeapHashMap<Number, Serializable>(source, new FileBackedStorageEngine<Serializable, Serializable>(source, Long.MAX_VALUE, MemoryUnit.BYTES, new PersistentSerializablePortability(), new PersistentSerializablePortability()), true);
    try {
      for (int i = 0; i < 100; i++) {
        map.put(i, "Hello World");
      }

      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      ObjectOutputStream dout = new ObjectOutputStream(bout);
      try {
        map.flush();
        map.persist(dout);
      } finally {
        dout.close();
      }
      indexData = bout.toByteArray();
    } finally {
      map.close();
    }


    PersistentReadWriteLockedOffHeapHashMap<Number, Serializable> clone = null;
    try {
      ByteArrayInputStream bin = new ByteArrayInputStream(indexData);
      ObjectInputStream din = new ObjectInputStream(bin);
      try {
        MappedPageSource clonedSource = new MappedPageSource(dataFile, false);
        clone = new PersistentReadWriteLockedOffHeapHashMap<Number, Serializable>(clonedSource, new FileBackedStorageEngine<Serializable, Serializable>(clonedSource, Long.MAX_VALUE, MemoryUnit.BYTES, new PersistentSerializablePortability(), new PersistentSerializablePortability(), false), false);
        clone.bootstrap(din);
      } finally {
        din.close();
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
    PersistentReadWriteLockedOffHeapHashMap<Integer, Serializable> map = new PersistentReadWriteLockedOffHeapHashMap<Integer, Serializable>(source, new FileBackedStorageEngine<Serializable, Serializable>(source, Long.MAX_VALUE, MemoryUnit.BYTES, new PersistentSerializablePortability(), new PersistentSerializablePortability()), true);
    try {
      for (int i = 0; i < primitives.length; i++) {
        map.put(i, primitives[i]);
      }

      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      ObjectOutputStream dout = new ObjectOutputStream(bout);
      try {
        map.flush();
        map.persist(dout);
      } finally {
        dout.close();
      }
      indexData = bout.toByteArray();
    } finally {
      map.close();
    }


    PersistentReadWriteLockedOffHeapHashMap<Integer, Serializable> clone = null;
    try {
      ByteArrayInputStream bin = new ByteArrayInputStream(indexData);
      ObjectInputStream din = new ObjectInputStream(bin);
      try {
        MappedPageSource clonedSource = new MappedPageSource(dataFile, false);
        clone = new PersistentReadWriteLockedOffHeapHashMap<Integer, Serializable>(clonedSource, new FileBackedStorageEngine<Serializable, Serializable>(clonedSource, Long.MAX_VALUE, MemoryUnit.BYTES, new PersistentSerializablePortability(), new PersistentSerializablePortability(), false), false);
        clone.bootstrap(din);
      } finally {
        din.close();
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
}
