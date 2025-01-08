/*
 * Copyright 2015-2023 Terracotta, Inc., a Software AG company.
 * Copyright IBM Corp. 2024, 2025
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.terracotta.offheapstore.paging;

import org.terracotta.offheapstore.paging.Page;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.paging.PhantomReferenceLimitedPageSource;
import static org.terracotta.offheapstore.util.RetryAssert.assertBy;
import static org.hamcrest.core.IsNull.notNullValue;

import java.util.ArrayList;
import java.util.List;
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

    assertBy(30, TimeUnit.SECONDS, () -> {
      System.gc();
      System.runFinalization();
      return source.allocate(64, false, false, null);
    }, notNullValue());
  }

  @Test
  public void testExhaustionThroughManyAllocations() {
    PageSource source = new PhantomReferenceLimitedPageSource(64);

    List<Page> bs = new ArrayList<>();
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

    assertBy(30, TimeUnit.SECONDS, () -> {
      System.gc();
      System.runFinalization();
      return source.allocate(64, false, false, null);
    }, notNullValue());
    Assert.assertNotNull(p);
  }
}
