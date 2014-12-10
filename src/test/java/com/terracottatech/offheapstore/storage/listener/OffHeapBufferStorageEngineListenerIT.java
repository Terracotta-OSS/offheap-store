package com.terracottatech.offheapstore.storage.listener;

import com.terracottatech.offheapstore.buffersource.HeapBufferSource;
import com.terracottatech.offheapstore.paging.UnlimitedPageSource;
import com.terracottatech.offheapstore.storage.OffHeapBufferStorageEngine;
import com.terracottatech.offheapstore.storage.PointerSize;
import com.terracottatech.offheapstore.storage.portability.StringPortability;

public class OffHeapBufferStorageEngineListenerIT extends AbstractListenerIT {

  @Override
  protected ListenableStorageEngine<String, String> createStorageEngine() {
    return new OffHeapBufferStorageEngine<String, String>(PointerSize.INT, new UnlimitedPageSource(new HeapBufferSource()), 1024, StringPortability.INSTANCE, StringPortability.INSTANCE);
  }

}
