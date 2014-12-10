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
import com.terracottatech.offheapstore.util.PointerSizeEngineTypeParameterizedTest;

public class OffHeapBufferStorageEngineIT extends PointerSizeEngineTypeParameterizedTest {

  @Test
  public void testEmptyPayload() {
    StorageEngine<byte[], byte[]> engine = create(new UnlimitedPageSource(new OffHeapBufferSource()), 1024, ByteArrayPortability.INSTANCE, ByteArrayPortability.INSTANCE);
    
    long p = engine.writeMapping(new byte[0], new byte[0], 0, 0);
    Assert.assertTrue(p >= 0);
    
    byte[] b = engine.readKey(p, 0);
    
    Assert.assertNotNull(b);
    Assert.assertEquals(0, b.length);
  }
  
  @Test
  public void testSmallPayloads() {
    StorageEngine<byte[], byte[]> engine = create(new UnlimitedPageSource(new OffHeapBufferSource()), 1024, ByteArrayPortability.INSTANCE, ByteArrayPortability.INSTANCE);
    
    Random rndm = new Random();
    
    for (int i = 0; i < 64; i++) {
      byte[] b = new byte[i];
      
      rndm.nextBytes(b);
      
      long p = engine.writeMapping(b, b, 0, 0);
      Assert.assertTrue(p >= 0);
      
      byte[] b2 = engine.readKey(p, 0);
      
      Assert.assertNotNull(b2);
      Assert.assertArrayEquals(b, b2);
      
      engine.freeMapping(p, 0, true);
    }
  }

  @Test
  public void testPortabilityCacheInvalidation() {
    OffHeapHashMap<Integer, int[]> map = new WriteLockedOffHeapClockCache<Integer, int[]>(new UnlimitedPageSource(new OffHeapBufferSource()), create(new UpfrontAllocatingPageSource(new OffHeapBufferSource(), 1024, 1024), 1024, new SerializablePortability(), new SerializablePortability()));

    int[] mutable = new int[1];
    for (int i = 0; i < 100; i++) {
      mutable[0] = i;
      map.put(i, mutable);
      Assert.assertEquals(i, map.get(i)[0]);
    }
  }
}
