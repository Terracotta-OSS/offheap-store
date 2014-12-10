package com.terracottatech.offheapstore.storage.listener;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;

import com.terracottatech.offheapstore.disk.paging.MappedPageSource;
import com.terracottatech.offheapstore.disk.storage.FileBackedStorageEngine;
import com.terracottatech.offheapstore.storage.portability.StringPortability;

public class FileBackedStorageEngineListenerIT extends AbstractListenerIT {

  protected File dataFile;

  @Before
  public void createDataFile() throws IOException {
    dataFile = File.createTempFile(getClass().getSimpleName(), ".data");
    dataFile.deleteOnExit();
  }

  @After
  public void destroyDataFile() {
    dataFile.delete();
  }
  
  @Override
  protected ListenableStorageEngine<String, String> createStorageEngine() {
    try {
      return new FileBackedStorageEngine<String, String>(new MappedPageSource(dataFile), StringPortability.INSTANCE, StringPortability.INSTANCE, 1024);
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

}
