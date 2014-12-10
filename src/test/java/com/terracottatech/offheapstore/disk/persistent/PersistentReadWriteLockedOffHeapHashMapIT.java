/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.disk.persistent;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.junit.Assert;
import org.junit.Test;

import com.terracottatech.offheapstore.disk.AbstractDiskTest;
import com.terracottatech.offheapstore.disk.paging.MappedPageSource;
import com.terracottatech.offheapstore.disk.storage.FileBackedStorageEngine;
import com.terracottatech.offheapstore.disk.storage.PersistentIntegerStorageEngine;
import com.terracottatech.offheapstore.disk.storage.PersistentSplitStorageEngine;
import com.terracottatech.offheapstore.disk.storage.portability.PersistentByteArrayPortability;
import com.terracottatech.offheapstore.disk.storage.portability.PersistentSerializablePortability;

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
        map.put(Integer.valueOf(i), Integer.valueOf(i));
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
    PersistentReadWriteLockedOffHeapHashMap<Integer, byte[]> map = new PersistentReadWriteLockedOffHeapHashMap<Integer, byte[]>(source, new FileBackedStorageEngine<Integer, byte[]>(source, new PersistentSerializablePortability(), PersistentByteArrayPortability.INSTANCE, 1024), true);
    try {
      for (int i = 0; i < 100; i++) {
        map.put(Integer.valueOf(i), new byte[i]);
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
        clone = new PersistentReadWriteLockedOffHeapHashMap<Integer, byte[]>(clonedSource, new FileBackedStorageEngine<Integer, byte[]>(clonedSource, new PersistentSerializablePortability(), PersistentByteArrayPortability.INSTANCE, 1024, false), false);
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
    PersistentReadWriteLockedOffHeapHashMap<Integer, Serializable> map = new PersistentReadWriteLockedOffHeapHashMap<Integer, Serializable>(source, new FileBackedStorageEngine<Serializable, Serializable>(source, new PersistentSerializablePortability(), new PersistentSerializablePortability(), 1024), true);
    try {
      for (int i = 0; i < 100; i++) {
        map.put(Integer.valueOf(i), "Hello World");
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
        clone = new PersistentReadWriteLockedOffHeapHashMap<Integer, Serializable>(clonedSource, new FileBackedStorageEngine<Serializable, Serializable>(clonedSource, new PersistentSerializablePortability(), new PersistentSerializablePortability(), 1024, false), false);
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
    PersistentReadWriteLockedOffHeapHashMap<Number, Serializable> map = new PersistentReadWriteLockedOffHeapHashMap<Number, Serializable>(source, new FileBackedStorageEngine<Serializable, Serializable>(source, new PersistentSerializablePortability(), new PersistentSerializablePortability(), 1024), true);
    try {
      for (int i = 0; i < 100; i++) {
        map.put(Integer.valueOf(i), "Hello World");
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
        clone = new PersistentReadWriteLockedOffHeapHashMap<Number, Serializable>(clonedSource, new FileBackedStorageEngine<Serializable, Serializable>(clonedSource, new PersistentSerializablePortability(), new PersistentSerializablePortability(), 1024, false), false);
        clone.bootstrap(din);
      } finally {
        din.close();
      }

      Assert.assertEquals(100, clone.size());

      for (int i = 0; i < 100; i++) {
        Assert.assertEquals("Hello World", clone.get(i));
      }
      
      for (long i = 0L; i < 100L; i++) {
        clone.put(Long.valueOf(i), "Hello World");
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
    PersistentReadWriteLockedOffHeapHashMap<Integer, Serializable> map = new PersistentReadWriteLockedOffHeapHashMap<Integer, Serializable>(source, new FileBackedStorageEngine<Serializable, Serializable>(source, new PersistentSerializablePortability(), new PersistentSerializablePortability(), 1024), true);
    try {
      for (int i = 0; i < primitives.length; i++) {
        map.put(Integer.valueOf(i), primitives[i]);
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
        clone = new PersistentReadWriteLockedOffHeapHashMap<Integer, Serializable>(clonedSource, new FileBackedStorageEngine<Serializable, Serializable>(clonedSource, new PersistentSerializablePortability(), new PersistentSerializablePortability(), 1024, false), false);
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
