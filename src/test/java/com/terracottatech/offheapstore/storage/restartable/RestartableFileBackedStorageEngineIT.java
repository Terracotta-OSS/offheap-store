package com.terracottatech.offheapstore.storage.restartable;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import com.terracottatech.offheapstore.disk.paging.MappedPageSource;
import com.terracottatech.offheapstore.disk.storage.FileBackedStorageEngine;
import com.terracottatech.offheapstore.storage.portability.StringPortability;

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
      FileBackedStorageEngine<String, LinkedNode<String>> delegate = new FileBackedStorageEngine<String, LinkedNode<String>>(source, StringPortability.INSTANCE, new LinkedNodePortability<String>(StringPortability.INSTANCE), 1);
      return new RestartableStorageEngine<FileBackedStorageEngine<String, LinkedNode<String>>, String, String, String>("id", new NoOpRestartStore<String, ByteBuffer, ByteBuffer>(), delegate, true);
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  } 
}
