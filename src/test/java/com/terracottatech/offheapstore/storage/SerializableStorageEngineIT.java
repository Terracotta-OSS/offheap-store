package com.terracottatech.offheapstore.storage;

import java.util.Collections;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.terracottatech.offheapstore.OffHeapHashMap;
import com.terracottatech.offheapstore.buffersource.OffHeapBufferSource;
import com.terracottatech.offheapstore.paging.UnlimitedPageSource;
import com.terracottatech.offheapstore.util.PointerSizeParameterizedTest;

public class SerializableStorageEngineIT extends PointerSizeParameterizedTest {

  @Test
  public void tableResizeTest() {
    Map<String, String> map = new OffHeapHashMap<String, String>(new UnlimitedPageSource(new OffHeapBufferSource()), new SerializableStorageEngine(getPointerSize(), new UnlimitedPageSource(new OffHeapBufferSource()), 1));
    
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
    Map<String, String> map = new OffHeapHashMap<String, String>(new UnlimitedPageSource(new OffHeapBufferSource()), new SerializableStorageEngine(getPointerSize(), new UnlimitedPageSource(new OffHeapBufferSource()), 1));
    
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
