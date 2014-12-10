/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.offheapstore.storage.restartable.partial;

import com.terracottatech.frs.RestartStore;
import com.terracottatech.offheapstore.buffersource.OffHeapBufferSource;
import com.terracottatech.offheapstore.paging.PageSource;
import com.terracottatech.offheapstore.paging.UpfrontAllocatingPageSource;
import com.terracottatech.offheapstore.storage.portability.StringPortability;
import com.terracottatech.offheapstore.util.MemoryUnit;

import java.nio.ByteBuffer;

/**
 *
 * @author cdennis
 */
public class KeysOnlyRestartableStorageEngineIT extends MinimalRestartableStorageEngineIT {

  @Override
  protected RestartableMinimalStorageEngine<String, String, String> createEngine(RestartStore<String, ByteBuffer, ByteBuffer> frs) {
    PageSource source = new UpfrontAllocatingPageSource(new OffHeapBufferSource(), MemoryUnit.MEGABYTES.toBytes(1), MemoryUnit.MEGABYTES.toBytes(1));
    return new RestartableKeysOnlyStorageEngine<String, String, String>("id", frs, true, getPointerSize(), source, MemoryUnit.KILOBYTES.toBytes(1), StringPortability.INSTANCE, StringPortability.INSTANCE, 0.0f);
  }
}
