/*
 * Copyright 2014-2023 Terracotta, Inc., a Software AG company.
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

import com.terracottatech.frs.object.RegisterableObjectManager;
import org.junit.Assert;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

public final class RestartabilityTestUtilities {
  
  private RestartabilityTestUtilities() {
    //static util class
  }
  
  public static RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> createObjectManager() {
    return new RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer>();
  }
  
  public static File createTempDirectory(String prefix) throws IOException {
    File storage = File.createTempFile(prefix, "");
    Assert.assertTrue(storage.delete());
    Assert.assertTrue(storage.mkdirs());
    Assert.assertTrue(storage.isDirectory());
    storage.deleteOnExit();
    return storage;
  }

}
