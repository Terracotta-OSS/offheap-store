package com.terracottatech.offheapstore;

import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.Assert;

import com.terracottatech.offheapstore.exceptions.OversizeMappingException;

public final class CacheTestRoutines {

  private CacheTestRoutines() {
    //
  }

  public static void testCacheEviction(Map<Integer, Integer> cache, CapacityLimitedIntegerStorageEngineFactory engine) {
    Random rndm = new Random();
    
    int toAdd = 100 + rndm.nextInt(90);
    
    engine.setCapacity(toAdd);
    
    for (int i = 0; i < toAdd; i++) {
      cache.put(i, i);

      Assert.assertEquals(i + 1, cache.size());
      for (int j = 0; j <= i; j++) {
        Assert.assertTrue(cache.get(j).intValue() == j);
      }
    }
    
    int toEvict = 100 + rndm.nextInt(90);
    
    for (int i = toAdd; i < toEvict + toAdd; i++) {
      cache.put(i, i);

      Assert.assertEquals(toAdd, cache.size());
      Assert.assertTrue(cache.get(i).intValue() == i);
    }
  }

  public static void testCacheEvictionMinimal(Map<Integer, Integer> cache) {
    for (int i = 0; i < 200; i++) {
      cache.put(i, i);
      Assert.assertTrue(cache.get(i).intValue() == i);
    }
  }

  public static void testCacheEvictionThreshold(Map<Integer, byte[]> cache) {
    Assert.assertNull(cache.put(0, new byte[100]));
    try {
      cache.put(0, new byte[1024 * 1024]);
      Assert.fail("Expected OversizeMappingException");
    } catch (OversizeMappingException e) {
      //expected
    }
  }

  public static void testFillBehavior(Map<Integer, byte[]> cache) {
    for (int i = 0; i < 100000; i++) {
      Set<Integer> keySetCopy = new HashSet<Integer>(cache.keySet());
      MapTestRoutines.doFill(cache, i, new byte[i % 1024]);
      if (cache.containsKey(i)) {
        Assert.assertNotNull(cache.get(i));
        Assert.assertEquals(i % 1024, cache.get(i).length);
        Assert.assertTrue(keySetCopy.add(i));
        Assert.assertEquals(keySetCopy, cache.keySet());
      } else {
        Assert.assertNull(cache.get(i));
        Assert.assertEquals(keySetCopy, cache.keySet());
      }
    }
    
    Set<Integer> keySetCopy = new HashSet<Integer>(cache.keySet());
    
    for (Integer k : keySetCopy) {
      if (MapTestRoutines.doFill(cache, k, new byte[(k % 1024) + 1]) == null) {
        if (cache.containsKey(k)) {
          Assert.assertEquals((k % 1024) + 1, cache.get(k).length); 
        }
      } else {
        Assert.assertTrue(cache.containsKey(k));
        Assert.assertEquals((k % 1024) + 1, cache.get(k).length); 
      }
    }
  }  
}
