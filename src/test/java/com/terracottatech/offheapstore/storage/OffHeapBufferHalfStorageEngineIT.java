/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.storage;

import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import com.terracottatech.offheapstore.OffHeapHashMap;
import com.terracottatech.offheapstore.WriteLockedOffHeapClockCache;
import com.terracottatech.offheapstore.buffersource.OffHeapBufferSource;
import com.terracottatech.offheapstore.paging.UnlimitedPageSource;
import com.terracottatech.offheapstore.paging.UpfrontAllocatingPageSource;
import com.terracottatech.offheapstore.storage.portability.ByteArrayPortability;
import com.terracottatech.offheapstore.storage.portability.SerializablePortability;

/**
 *
 * @author cdennis
 */
public class OffHeapBufferHalfStorageEngineIT {

  @Test
  public void testEmptyPayload() {
    HalfStorageEngine<byte[]> engine = new OffHeapBufferHalfStorageEngine<byte[]>(new UnlimitedPageSource(new OffHeapBufferSource()), 1024, ByteArrayPortability.INSTANCE);

    int p = engine.write(new byte[0], 0);
    Assert.assertTrue(p >= 0);

    byte[] b = engine.read(p);

    Assert.assertNotNull(b);
    Assert.assertEquals(0, b.length);
  }

  @Test
  public void testSmallPayloads() {
    HalfStorageEngine<byte[]> engine = new OffHeapBufferHalfStorageEngine<byte[]>(new UnlimitedPageSource(new OffHeapBufferSource()), 1024, ByteArrayPortability.INSTANCE);

    Random rndm = new Random();

    for (int i = 0; i < 64; i++) {
      byte[] b = new byte[i];

      rndm.nextBytes(b);

      int p = engine.write(b, 0);
      Assert.assertTrue(p >= 0);

      byte[] b2 = engine.read(p);

      Assert.assertNotNull(b2);
      Assert.assertArrayEquals(b, b2);

      engine.free(p);
    }
  }

  @Test
  public void testPortabilityCacheInvalidation() {
    OffHeapHashMap<Integer, int[]> map = new WriteLockedOffHeapClockCache<Integer, int[]>(new UnlimitedPageSource(new OffHeapBufferSource()), new SplitStorageEngine<Integer, int[]>(new IntegerStorageEngine(), new OffHeapBufferHalfStorageEngine<int[]>(new UpfrontAllocatingPageSource(new OffHeapBufferSource(), 1024, 1024), 1024, new SerializablePortability())));

    int[] mutable = new int[1];
    for (int i = 0; i < 100; i++) {
      mutable[0] = i;
      map.put(i, mutable);
      Assert.assertEquals(i, map.get(i)[0]);
    }
  }
}
