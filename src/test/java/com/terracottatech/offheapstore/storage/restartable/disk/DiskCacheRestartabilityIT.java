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
import com.terracottatech.offheapstore.ReadWriteLockedOffHeapClockCache;
import com.terracottatech.offheapstore.disk.paging.MappedPageSource;
import com.terracottatech.offheapstore.disk.storage.FileBackedStorageEngine;
import com.terracottatech.offheapstore.storage.portability.Portability;
import com.terracottatech.offheapstore.storage.restartable.AbstractRestartabilityCacheIT;
import com.terracottatech.offheapstore.storage.restartable.LinkedNode;
import com.terracottatech.offheapstore.storage.restartable.LinkedNodePortability;
import com.terracottatech.offheapstore.storage.restartable.OffHeapObjectManagerStripe;
import com.terracottatech.offheapstore.storage.restartable.RestartableStorageEngine;
import com.terracottatech.offheapstore.util.MemoryUnit;

@RunWith(BlockJUnit4ClassRunner.class)
public class DiskCacheRestartabilityIT extends AbstractRestartabilityCacheIT {

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
    FileBackedStorageEngine<K, LinkedNode<V>> delegateEngine = new FileBackedStorageEngine<K, LinkedNode<V>>(source, keyPortability, new LinkedNodePortability<V>(valuePortability), 1);
    RestartableStorageEngine<?, ByteBuffer, K, V> storageEngine = new RestartableStorageEngine<FileBackedStorageEngine<K, LinkedNode<V>>, ByteBuffer, K, V>(id, persistence, delegateEngine, synchronous);
    ReadWriteLockedOffHeapClockCache<K, V> map = new ReadWriteLockedOffHeapClockCache<K, V>(source, storageEngine);
    objectMgr.registerObject(new OffHeapObjectManagerStripe<ByteBuffer>(id, map));
    return map;
  }
  
}
