/*
 * Copyright 2014-2023 Terracotta, Inc., a Software AG company.
 * Copyright IBM Corp. 2024, 2025
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
