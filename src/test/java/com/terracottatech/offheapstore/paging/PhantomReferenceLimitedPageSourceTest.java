package com.terracottatech.offheapstore.paging;

import static com.terracottatech.offheapstore.util.RetryAssert.assertBy;
import static org.hamcrest.core.IsNull.notNullValue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

public class PhantomReferenceLimitedPageSourceTest {

  @Test
  public void testExhaustion() {
    testExhaustion(new PhantomReferenceLimitedPageSource(64));
  }
  
  private static void testExhaustion(PageSource source) {
    Page p = source.allocate(64, false, false, null);
    Assert.assertNotNull(p);
    
    Assert.assertNull(source.allocate(1, false, false, null));
    
    Assert.assertNotNull(p);
  }
  
  @Test
  public void testRecycle() throws InterruptedException {
    final PageSource source = new PhantomReferenceLimitedPageSource(64);
    
    testExhaustion(source);

    assertBy(30, TimeUnit.SECONDS, new Callable<Page>() {
      @Override
      public Page call() throws Exception {
        System.gc();
        System.runFinalization();
        return source.allocate(64, false, false, null);
      }
      
    }, notNullValue());
  }
  
  @Test
  public void testExhaustionThroughManyAllocations() {
    PageSource source = new PhantomReferenceLimitedPageSource(64);

    List<Page> bs = new ArrayList<Page>();
    for (int i = 0; i < 64; i++) {
      Page p = source.allocate(1, false, false, null);
      Assert.assertNotNull(p);
      bs.add(p);
    }
    
    Assert.assertNull(source.allocate(1, false, false, null));
    
    Assert.assertEquals(64, bs.size());
  }

  @Test
  public void testPartialRecycle() throws InterruptedException {
    final PageSource source = new PhantomReferenceLimitedPageSource(128);

    Page p = source.allocate(64, false, false, null);
    Assert.assertNotNull(p);
    
    testExhaustion(source);

    assertBy(30, TimeUnit.SECONDS, new Callable<Page>() {
      @Override
      public Page call() throws Exception {
        System.gc();
        System.runFinalization();
        return source.allocate(64, false, false, null);
      }
      
    }, notNullValue());
    Assert.assertNotNull(p);
  }
}
