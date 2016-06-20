package com.terracottatech.offheapstore.storage.restartable;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import org.terracotta.offheapstore.disk.paging.MappedPageSource;
import org.terracotta.offheapstore.disk.storage.FileBackedStorageEngine;
import org.terracotta.offheapstore.storage.portability.StringPortability;

import static org.terracotta.offheapstore.util.MemoryUnit.BYTES;

@RunWith(BlockJUnit4ClassRunner.class)
public class RestartableFileBackedStorageEngineIT extends RestartableStorageEngineIT {

  private File testFile;

  @Before
  public void createFile() throws IOException {
    testFile = File.createTempFile("RestartableFileBackedStorageEngineTest", ".data");
    testFile.deleteOnExit();
  }
  
  @After
  public void deleteFile() {
    testFile.delete();
  }
  
  @Override
  protected RestartableStorageEngine<?, String, String, String> createEngine() {
    try {
      MappedPageSource source = new MappedPageSource(testFile);
      FileBackedStorageEngine<String, LinkedNode<String>> delegate = new FileBackedStorageEngine<String, LinkedNode<String>>(source,
          Long.MAX_VALUE, BYTES, StringPortability.INSTANCE, new LinkedNodePortability<String>(StringPortability.INSTANCE));
      return new RestartableStorageEngine<FileBackedStorageEngine<String, LinkedNode<String>>, String, String, String>("id", new NoOpRestartStore<String, ByteBuffer, ByteBuffer>(), delegate, true);
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  } 
}
