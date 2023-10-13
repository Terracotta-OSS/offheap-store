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
public class PersistentConcurrentOffHeapMapIT extends AbstractDiskTest {

  @Test
  public void testTableOnlyPersistence() throws IOException {
    byte[] indexData;

    PersistentConcurrentOffHeapHashMap<Integer, Integer> map = new PersistentConcurrentOffHeapHashMap<Integer, Integer>(new MappedPageSource(dataFile), PersistentSplitStorageEngine.createPersistentFactory(PersistentIntegerStorageEngine.createPersistentFactory(), PersistentIntegerStorageEngine.createPersistentFactory()));
    try {
      for (int i = 0; i < 100; i++) {
        map.put(Integer.valueOf(i), Integer.valueOf(i));
      }

      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      ObjectOutputStream oout = new ObjectOutputStream(bout);
      try {
        map.flush();
        map.persist(oout);
      } finally {
        oout.close();
      }
      indexData = bout.toByteArray();
    } finally {
      map.close();
    }


    PersistentConcurrentOffHeapHashMap<Integer, Integer> clone = null;
    try {
      ByteArrayInputStream bin = new ByteArrayInputStream(indexData);
      ObjectInputStream oin = new ObjectInputStream(bin);
      try {
        clone = new PersistentConcurrentOffHeapHashMap<Integer, Integer>(oin, new MappedPageSource(dataFile, false), PersistentSplitStorageEngine.createPersistentFactory(PersistentIntegerStorageEngine.createPersistentFactory(), PersistentIntegerStorageEngine.createPersistentFactory()));
        clone.bootstrap(oin);
      } finally {
        oin.close();
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
    PersistentConcurrentOffHeapHashMap<Integer, byte[]> map = new PersistentConcurrentOffHeapHashMap<Integer, byte[]>(source, FileBackedStorageEngine.createFactory(source, 1024, new PersistentSerializablePortability(), PersistentByteArrayPortability.INSTANCE));
    try {
      for (int i = 0; i < 100; i++) {
        map.put(Integer.valueOf(i), new byte[i]);
      }

      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      ObjectOutputStream dout = new ObjectOutputStream(bout);
      occupiedSize = map.getOccupiedMemory();
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


    PersistentConcurrentOffHeapHashMap<Integer, byte[]> clone = null;
    try {
      ByteArrayInputStream bin = new ByteArrayInputStream(indexData);
      ObjectInputStream din = new ObjectInputStream(bin);
      try {
        MappedPageSource clonedSource = new MappedPageSource(dataFile, false);
        clone = new PersistentConcurrentOffHeapHashMap<Integer, byte[]>(din, clonedSource, FileBackedStorageEngine.createFactory(clonedSource, 1024, new PersistentSerializablePortability(), PersistentByteArrayPortability.INSTANCE, false));
        clone.bootstrap(din);
      } finally {
        din.close();
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
    PersistentConcurrentOffHeapHashMap<Integer, Serializable> map = new PersistentConcurrentOffHeapHashMap<Integer, Serializable>(source, FileBackedStorageEngine.createFactory(source, 1024, new PersistentSerializablePortability(), new PersistentSerializablePortability()));
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


    PersistentConcurrentOffHeapHashMap<Integer, Serializable> clone = null;
    try {
      ByteArrayInputStream bin = new ByteArrayInputStream(indexData);
      ObjectInputStream din = new ObjectInputStream(bin);
      try {
        MappedPageSource clonedSource = new MappedPageSource(dataFile, false);
        clone = new PersistentConcurrentOffHeapHashMap<Integer, Serializable>(din, clonedSource, FileBackedStorageEngine.createFactory(clonedSource, 1024, new PersistentSerializablePortability(), new PersistentSerializablePortability(), false));
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
    PersistentConcurrentOffHeapHashMap<Number, Serializable> map = new PersistentConcurrentOffHeapHashMap<Number, Serializable>(source, FileBackedStorageEngine.createFactory(source, 1024, new PersistentSerializablePortability(), new PersistentSerializablePortability()));
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


    PersistentConcurrentOffHeapHashMap<Number, Serializable> clone = null;
    try {
      ByteArrayInputStream bin = new ByteArrayInputStream(indexData);
      ObjectInputStream din = new ObjectInputStream(bin);
      try {
        MappedPageSource clonedSource = new MappedPageSource(dataFile, false);
        clone = new PersistentConcurrentOffHeapHashMap<Number, Serializable>(din, clonedSource, FileBackedStorageEngine.createFactory(clonedSource, 1024, new PersistentSerializablePortability(), new PersistentSerializablePortability(), false));
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
    PersistentConcurrentOffHeapHashMap<Integer, Serializable> map = new PersistentConcurrentOffHeapHashMap<Integer, Serializable>(source, FileBackedStorageEngine.createFactory(source, 1024, new PersistentSerializablePortability(), new PersistentSerializablePortability()));
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


    PersistentConcurrentOffHeapHashMap<Integer, Serializable> clone = null;
    try {
      ByteArrayInputStream bin = new ByteArrayInputStream(indexData);
      ObjectInputStream din = new ObjectInputStream(bin);
      try {
        MappedPageSource clonedSource = new MappedPageSource(dataFile, false);
        clone = new PersistentConcurrentOffHeapHashMap<Integer, Serializable>(din, clonedSource, FileBackedStorageEngine.createFactory(clonedSource, 1024, new PersistentSerializablePortability(), new PersistentSerializablePortability(), false));
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

  @Test
  public void testFragmentedPersistence() throws IOException {
    byte[] indexData;

    MappedPageSource source = new MappedPageSource(dataFile);
    PersistentConcurrentOffHeapHashMap<Integer, byte[]> map = new PersistentConcurrentOffHeapHashMap<Integer, byte[]>(source, FileBackedStorageEngine.createFactory(source, 1024, new PersistentSerializablePortability(), PersistentByteArrayPortability.INSTANCE));
    try {
      for (int i = 0; i < 100; i++) {
        map.put(Integer.valueOf(i), new byte[i]);
      }

      for (int i = 0; i < 100; i+=2) {
        map.remove(Integer.valueOf(i));
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


    PersistentConcurrentOffHeapHashMap<Integer, byte[]> clone = null;
    try {
      ByteArrayInputStream bin = new ByteArrayInputStream(indexData);
      ObjectInputStream din = new ObjectInputStream(bin);
      try {
        MappedPageSource clonedSource = new MappedPageSource(dataFile, false);
        clone = new PersistentConcurrentOffHeapHashMap<Integer, byte[]>(din, clonedSource, FileBackedStorageEngine.createFactory(clonedSource, 1024, new PersistentSerializablePortability(), PersistentByteArrayPortability.INSTANCE, false));
        clone.bootstrap(din);
      } finally {
        din.close();
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
}
