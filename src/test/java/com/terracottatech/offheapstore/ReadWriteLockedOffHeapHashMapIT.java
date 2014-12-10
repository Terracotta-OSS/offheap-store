package com.terracottatech.offheapstore;

import java.util.concurrent.ConcurrentMap;

import com.terracottatech.offheapstore.buffersource.OffHeapBufferSource;
import com.terracottatech.offheapstore.paging.PageSource;
import com.terracottatech.offheapstore.paging.UnlimitedPageSource;
import com.terracottatech.offheapstore.storage.IntegerStorageEngine;
import com.terracottatech.offheapstore.storage.OffHeapBufferHalfStorageEngine;
import com.terracottatech.offheapstore.storage.SplitStorageEngine;
import com.terracottatech.offheapstore.storage.portability.ByteArrayPortability;
import com.terracottatech.offheapstore.util.Generator;
import com.terracottatech.offheapstore.util.Generator.SpecialInteger;

public class ReadWriteLockedOffHeapHashMapIT extends AbstractConcurrentOffHeapMapIT {

  @Override
  protected ConcurrentMap<SpecialInteger, SpecialInteger> createGoodMap() {
    return new ReadWriteLockedOffHeapHashMap<SpecialInteger, SpecialInteger>(new UnlimitedPageSource(new OffHeapBufferSource()), Generator.GOOD_ENGINE, 1);
  }

  @Override
  protected ConcurrentMap<SpecialInteger, SpecialInteger> createBadMap() {
    return new ReadWriteLockedOffHeapHashMap<SpecialInteger, SpecialInteger>(new UnlimitedPageSource(new OffHeapBufferSource()), Generator.BAD_ENGINE, 1);
  }

  @Override
  protected ConcurrentMap<Integer, byte[]> createOffHeapBufferMap(PageSource source) {
    return new ReadWriteLockedOffHeapHashMap<Integer, byte[]>(source, new SplitStorageEngine<Integer, byte[]>(new IntegerStorageEngine(), new OffHeapBufferHalfStorageEngine<byte[]>(source, 1024, ByteArrayPortability.INSTANCE)));
  }
}
