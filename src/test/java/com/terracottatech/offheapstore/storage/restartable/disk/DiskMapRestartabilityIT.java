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
import org.terracotta.offheapstore.OffHeapHashMap;
import org.terracotta.offheapstore.ReadWriteLockedOffHeapHashMap;
import org.terracotta.offheapstore.disk.paging.MappedPageSource;
import org.terracotta.offheapstore.disk.storage.FileBackedStorageEngine;
import org.terracotta.offheapstore.storage.portability.Portability;
import com.terracottatech.offheapstore.storage.restartable.AbstractRestartabilityIT;
import com.terracottatech.offheapstore.storage.restartable.LinkedNode;
import com.terracottatech.offheapstore.storage.restartable.LinkedNodePortability;
import com.terracottatech.offheapstore.storage.restartable.OffHeapObjectManagerStripe;
import com.terracottatech.offheapstore.storage.restartable.RestartableStorageEngine;
import org.terracotta.offheapstore.util.MemoryUnit;

import static java.lang.Math.max;
import static org.terracotta.offheapstore.util.MemoryUnit.BYTES;

@RunWith(BlockJUnit4ClassRunner.class)
public class DiskMapRestartabilityIT extends AbstractRestartabilityIT {

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
  protected <K, V> Map<K, V> createRestartableMap(long size, MemoryUnit unit, ByteBuffer id,
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
    FileBackedStorageEngine<K, LinkedNode<V>> delegateEngine = new FileBackedStorageEngine<K, LinkedNode<V>>(source,
        max(unit.toBytes(size) / 10, 1024), BYTES, keyPortability, new LinkedNodePortability<V>(valuePortability));
    RestartableStorageEngine<?, ByteBuffer, K, V> storageEngine = new RestartableStorageEngine<FileBackedStorageEngine<K, LinkedNode<V>>, ByteBuffer, K, V>(id, persistence, delegateEngine, synchronous);
    OffHeapHashMap<K, V> map = new ReadWriteLockedOffHeapHashMap<K, V>(source, storageEngine);
    objectMgr.registerObject(new OffHeapObjectManagerStripe<ByteBuffer>(id, map));
    return map;
  }
}
