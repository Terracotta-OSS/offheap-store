/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.paging;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import com.terracottatech.offheapstore.WriteLockedOffHeapClockCache;
import com.terracottatech.offheapstore.buffersource.HeapBufferSource;
import com.terracottatech.offheapstore.buffersource.OffHeapBufferSource;
import com.terracottatech.offheapstore.storage.IntegerStorageEngine;
import com.terracottatech.offheapstore.storage.OffHeapBufferHalfStorageEngine;
import com.terracottatech.offheapstore.storage.SplitStorageEngine;
import com.terracottatech.offheapstore.storage.portability.ByteArrayPortability;
import com.terracottatech.offheapstore.util.PointerSizeParameterizedTest;

/**
 *
 * @author cdennis
 */
public class OffHeapStorageAreaTest extends PointerSizeParameterizedTest {

  @Test
  public void testRecoveryOfPages() {
    GettablePageSource source = new GettablePageSource();

    Map<Integer, byte[]> test = new WriteLockedOffHeapClockCache<Integer, byte[]>(new UnlimitedPageSource(new OffHeapBufferSource()), new SplitStorageEngine<Integer, byte[]>(new IntegerStorageEngine(), new OffHeapBufferHalfStorageEngine<byte[]>(source, 1024, ByteArrayPortability.INSTANCE)));

    int put = 0;
    while (source.allocated.size() < 2) {
      test.put(put++, new byte[128]);
    }

    Assert.assertEquals(put, test.size());
    source.release();
    Assert.assertEquals(put - 1, test.size());

    Assert.assertNull(test.get(put - 1));
    for (int i = 0; i < put - 1; i++) {
      Assert.assertNotNull(test.get(i));
    }

    source.release();
    Assert.assertTrue(test.isEmpty());

    for (int i = 0; i < put; i++) {
      Assert.assertNull(test.get(i));
    }
  }

  @Test
  public void testVariablePageSize() {
    PageSource source = new UnlimitedPageSource(new HeapBufferSource());
    
    OffHeapStorageArea storage = new OffHeapStorageArea(getPointerSize(), null, source, 1, 1024, false, false);

    Map<Integer, Long> locations = new HashMap<Integer, Long>();
    
    for (int i = 0; i < 2048; i++) {
      long pointer = storage.allocate(Integer.SIZE / Byte.SIZE);
      storage.writeInt(pointer, i);
      locations.put(i, pointer);
    }
    
    System.err.println(storage);
    
    for (int i = 0; i < 2048; i++) {
      int pointer = locations.get(i).intValue();
      Assert.assertEquals(i, storage.readInt(pointer));
    }
    
    for (Long pointer : locations.values()) {
      storage.free(pointer);
    }
  }
  
//  @Test
//  public void testVariablePageSizeAddressLogic() {
//    PageSource source = new UnlimitedPageSource(new HeapBufferSource());
//    
//    OffHeapStorageArea storage = new OffHeapStorageArea(null, source, 1, 1024, false, false);
//    
//    for (int i = 0, address = 0; i < 100; i++) {
//      int size = storage.pageSizeFor(i);
//      int base = storage.addressForPage(i);
//      Assert.assertTrue(size <= 1024);
//      Assert.assertEquals(address, base);
//      address += size;
////      System.err.println("Page : " + i);
////      System.err.println("Size : " + size);
////      System.err.println("Base : " + base);
////      System.err.println();
//    }
//    
//    for (int i = 0; i < 10240; i++) {
//      int page = storage.pageIndexFor(i);
//      int pageAddress = storage.pageAddressFor(i);
//      Assert.assertEquals(i, pageAddress + storage.addressForPage(page));
////      System.err.println("Address      : " + i);
////      System.err.println("Page Index   : " + page);
////      System.err.println("Page Address : " + pageAddress);
////      System.err.println();
//    }
//  }
  
  static class GettablePageSource implements PageSource {

    final Random rndm = new Random();
    final PageSource delegate = new UnlimitedPageSource(new OffHeapBufferSource());
    final List<Page> allocated = new LinkedList<Page>();

    @Override
    public Page allocate(int size, boolean thief, boolean victim, OffHeapStorageArea owner) {
      Page p = delegate.allocate(size, thief, victim, owner);
      allocated.add(p);
      return p;
    }

    @Override
    public void free(Page page) {
      delegate.free(page);
      allocated.remove(page);
    }

    public void release() {
      Page p = allocated.get(rndm.nextInt(allocated.size()));
      p.binding().release(new LinkedList<Page>(Collections.singleton(p)));
      free(p);
    }
  }
}
