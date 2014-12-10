package com.terracottatech.offheapstore.storage.restartable;

import java.nio.ByteBuffer;

import com.terracottatech.offheapstore.buffersource.OffHeapBufferSource;
import com.terracottatech.offheapstore.paging.PageSource;
import com.terracottatech.offheapstore.paging.UpfrontAllocatingPageSource;
import com.terracottatech.offheapstore.storage.OffHeapBufferStorageEngine;
import com.terracottatech.offheapstore.storage.portability.StringPortability;
import com.terracottatech.offheapstore.util.MemoryUnit;

public class RestartableOffHeapBufferStorageEngineIT extends RestartableStorageEngineIT {

  @Override
  protected RestartableStorageEngine<?, String, String, String> createEngine() {
    PageSource source = new UpfrontAllocatingPageSource(new OffHeapBufferSource(), MemoryUnit.MEGABYTES.toBytes(4), MemoryUnit.KILOBYTES.toBytes(4));
    OffHeapBufferStorageEngine<String, LinkedNode<String>> delegate = create(source, MemoryUnit.KILOBYTES.toBytes(1), StringPortability.INSTANCE, new LinkedNodePortability<String>(StringPortability.INSTANCE));
    return new RestartableStorageEngine<OffHeapBufferStorageEngine<String, LinkedNode<String>>, String, String, String>("id", new NoOpRestartStore<String, ByteBuffer, ByteBuffer>(), delegate, true);
  }
}
