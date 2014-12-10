/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.offheapstore.disk;

import com.terracottatech.offheapstore.OffHeapHashMap;
import com.terracottatech.offheapstore.disk.paging.MappedPageSource;
import com.terracottatech.offheapstore.paging.OffHeapStorageArea;
import com.terracottatech.offheapstore.paging.Page;
import com.terracottatech.offheapstore.paging.PageSource;
import com.terracottatech.offheapstore.storage.IntegerStorageEngine;
import com.terracottatech.offheapstore.storage.SplitStorageEngine;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.hamcrest.core.Is;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author cdennis
 */
public class OffHeapDiskMapClearingIT extends AbstractDiskTest {
  
  @Test
  public void testFrequentClearing() throws IOException {
    CountingPageSource source = new CountingPageSource(new MappedPageSource(dataFile));
    Map<Integer, Integer> map = new OffHeapHashMap<Integer, Integer>(source, new SplitStorageEngine<Integer, Integer>(IntegerStorageEngine.instance(), IntegerStorageEngine.instance()));

    for (int i = 0; i < 100000; i++) {
      map.clear();
    }
    Assert.assertThat(source.allocations.get(), Is.is(1));
  }
  
  static class CountingPageSource implements PageSource {
    
    private final AtomicInteger allocations = new AtomicInteger();
    private final PageSource delegate;

    public CountingPageSource(PageSource delegate) {
      this.delegate = delegate;
    }

    @Override
    public Page allocate(int size, boolean thief, boolean victim, OffHeapStorageArea owner) {
      allocations.incrementAndGet();
      return delegate.allocate(size, thief, victim, owner);
    }

    @Override
    public void free(Page page) {
      delegate.free(page);
    }
    
  }
}
