package com.terracottatech.offheapstore.concurrent;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import com.terracottatech.offheapstore.AbstractConcurrentOffHeapMapIT;
import com.terracottatech.offheapstore.buffersource.OffHeapBufferSource;
import com.terracottatech.offheapstore.paging.PageSource;
import com.terracottatech.offheapstore.paging.UnlimitedPageSource;
import com.terracottatech.offheapstore.storage.IntegerStorageEngine;
import com.terracottatech.offheapstore.storage.OffHeapBufferHalfStorageEngine;
import com.terracottatech.offheapstore.storage.SplitStorageEngine;
import com.terracottatech.offheapstore.storage.portability.ByteArrayPortability;
import com.terracottatech.offheapstore.util.Generator;
import com.terracottatech.offheapstore.util.Generator.SpecialInteger;

public class ConcurrentWriteLockedOffHeapHashMapIT extends AbstractConcurrentOffHeapMapIT {

  @Override
  protected ConcurrentMap<SpecialInteger, SpecialInteger> createGoodMap() {
    return new ConcurrentWriteLockedOffHeapHashMap<SpecialInteger, SpecialInteger>(new UnlimitedPageSource(new OffHeapBufferSource()), Generator.GOOD_FACTORY, 1, 16);
  }

  @Override
  protected ConcurrentMap<SpecialInteger, SpecialInteger> createBadMap() {
    return new ConcurrentWriteLockedOffHeapHashMap<SpecialInteger, SpecialInteger>(new UnlimitedPageSource(new OffHeapBufferSource()), Generator.BAD_FACTORY, 1, 16);
  }

  @Override
  protected Map<Integer, byte[]> createOffHeapBufferMap(PageSource source) {
    return new ConcurrentWriteLockedOffHeapHashMap<Integer, byte[]>(source, SplitStorageEngine.createFactory(IntegerStorageEngine.createFactory(), OffHeapBufferHalfStorageEngine.createFactory(source, 1024, ByteArrayPortability.INSTANCE)));
  }
}
