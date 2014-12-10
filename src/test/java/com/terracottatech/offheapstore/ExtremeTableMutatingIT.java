package com.terracottatech.offheapstore;

import java.util.Map;
import java.util.Random;

import junit.framework.Assert;

import org.junit.Test;

import com.terracottatech.offheapstore.buffersource.HeapBufferSource;
import com.terracottatech.offheapstore.paging.PageSource;
import com.terracottatech.offheapstore.paging.UnlimitedPageSource;
import com.terracottatech.offheapstore.paging.UpfrontAllocatingPageSource;
import com.terracottatech.offheapstore.storage.IntegerStorageEngine;
import com.terracottatech.offheapstore.storage.OffHeapBufferHalfStorageEngine;
import com.terracottatech.offheapstore.storage.SplitStorageEngine;
import com.terracottatech.offheapstore.storage.StorageEngine;
import com.terracottatech.offheapstore.storage.portability.ByteArrayPortability;
import com.terracottatech.offheapstore.util.MemoryUnit;

public class ExtremeTableMutatingIT {

  @Test
  public void testExtremeTableMutations() {
    PageSource source = new UpfrontAllocatingPageSource(new HeapBufferSource(), MemoryUnit.MEGABYTES.toBytes(1), MemoryUnit.MEGABYTES.toBytes(1));
    StorageEngine<Integer, byte[]> storage = new SplitStorageEngine<Integer, byte[]>(new IntegerStorageEngine(), new OffHeapBufferHalfStorageEngine<byte[]>(source, MemoryUnit.KILOBYTES.toBytes(4), ByteArrayPortability.INSTANCE));
    Map<Integer, byte[]> cache = new EvilOffHeapClockCache<Integer, byte[]>(new UnlimitedPageSource(new HeapBufferSource()), storage);

    int i;
    for (i = 0; i == cache.size(); i++) {
      cache.put(i, new byte[i % 1024]);
    }
    
    for (int c = 0; c < i * 10; c++) {
      cache.put(i + c, new byte[(i + c) % 1024]);
    }
    
    for (int n = 0; n < i * 11; n++) {
      byte[] array = cache.get(n);
      if (array != null) {
        Assert.assertEquals(n % 1024, array.length);
      }
    }
  }
  
  static class EvilOffHeapClockCache<K, V> extends WriteLockedOffHeapClockCache<K, V> {

    private final Random rndm = new Random();
    
    public EvilOffHeapClockCache(PageSource source, StorageEngine<? super K, ? super V> storageEngine) {
      super(source, storageEngine);
    }

    @Override
    protected void storageEngineFailure(Object failure) {
      if (rndm.nextFloat() < 0.1f) {
        clear();
      } else {
        super.storageEngineFailure(failure);
      }
    }
    
  }
}
