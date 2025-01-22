package com.terracottatech.offheapstore.storage.restartable.disk;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import com.terracottatech.frs.RestartStore;
import com.terracottatech.frs.object.RegisterableObjectManager;
import org.terracotta.offheapstore.concurrent.ConcurrentOffHeapClockCache;
import org.terracotta.offheapstore.disk.paging.MappedPageSource;
import org.terracotta.offheapstore.disk.storage.FileBackedStorageEngine;
import org.terracotta.offheapstore.storage.portability.Portability;
import com.terracottatech.offheapstore.storage.restartable.AbstractRestartabilityCacheIT;
import com.terracottatech.offheapstore.storage.restartable.LinkedNode;
import com.terracottatech.offheapstore.storage.restartable.LinkedNodePortability;
import com.terracottatech.offheapstore.storage.restartable.OffHeapObjectManagerStripe;
import com.terracottatech.offheapstore.storage.restartable.RestartableStorageEngine;
import org.terracotta.offheapstore.util.Factory;
import org.terracotta.offheapstore.util.MemoryUnit;

@RunWith(BlockJUnit4ClassRunner.class)
public class ConcurrentDiskCacheRestartabilityIT extends AbstractRestartabilityCacheIT {

  private File testFile;

  @Before
  public void createFile() throws IOException {
    testFile = File.createTempFile(getClass().getSimpleName(), ".data");
    testFile.deleteOnExit();
  }
  
  @After
  public void deleteFile() {
    testFile.delete();
  }

  @Override
  protected <K, V> Map<K, V> createRestartableMap(long size,
                                                  MemoryUnit unit,
                                                  ByteBuffer id,
                                                  RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> persistence,
                                                  RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectMgr,
                                                  Portability<? super K> keyPortability,
                                                  Portability<? super V> valuePortability,
                                                  boolean synchronous) {
    MappedPageSource source;
    try {
      source = new MappedPageSource(testFile, unit.toBytes(size));
    } catch (IOException e) {
      throw new AssertionError(e);
    }
    Factory<RestartableStorageEngine<FileBackedStorageEngine<K, LinkedNode<V>>, ByteBuffer, K, V>> storageEngineFactory = RestartableStorageEngine.createFactory(id, persistence, FileBackedStorageEngine.<K, LinkedNode<V>>createFactory(source, 1, keyPortability, new LinkedNodePortability<V>(valuePortability)), synchronous);
    ConcurrentOffHeapClockCache<K, V> map = new ConcurrentOffHeapClockCache<K, V>(source, storageEngineFactory);
    objectMgr.registerObject(new OffHeapObjectManagerStripe<ByteBuffer>(id, map));
    return map;
  }
  
}
