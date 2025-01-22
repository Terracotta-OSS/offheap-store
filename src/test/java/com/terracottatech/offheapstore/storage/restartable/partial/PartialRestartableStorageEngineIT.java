/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.offheapstore.storage.restartable.partial;

import com.terracottatech.frs.RestartStore;
import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;
import org.terracotta.offheapstore.storage.portability.StringPortability;
import org.terracotta.offheapstore.util.MemoryUnit;

import java.nio.ByteBuffer;

/**
 *
 * @author cdennis
 */
public class PartialRestartableStorageEngineIT extends MinimalRestartableStorageEngineIT {

  @Override
  protected RestartableMinimalStorageEngine<String, String, String> createEngine(RestartStore<String, ByteBuffer, ByteBuffer> frs) {
    PageSource source = new UpfrontAllocatingPageSource(new OffHeapBufferSource(), MemoryUnit.MEGABYTES.toBytes(1), MemoryUnit.MEGABYTES.toBytes(1));
    return new RestartablePartialStorageEngine<String, String, String>("id", frs, true, getPointerSize(), source, MemoryUnit.KILOBYTES.toBytes(1), StringPortability.INSTANCE, StringPortability.INSTANCE, 0.0f);
  }
}
