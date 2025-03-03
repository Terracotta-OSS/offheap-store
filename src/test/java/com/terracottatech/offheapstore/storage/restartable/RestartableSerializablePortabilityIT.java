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

import static org.terracotta.offheapstore.util.MemoryUnit.MEGABYTES;

import java.io.File;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashMap;

import org.hamcrest.core.IsEqual;
import org.junit.Assert;
import org.junit.Test;

import com.terracottatech.frs.RestartStore;
import com.terracottatech.frs.RestartStoreFactory;
import com.terracottatech.frs.object.RegisterableObjectManager;
import com.terracottatech.offheapstore.storage.restartable.portability.RestartableSerializablePortability;

public class RestartableSerializablePortabilityIT {

  @Test
  public void testCreateMapping() throws Exception {
    ByteBuffer id = ByteBuffer.wrap("testCreateMapping".getBytes("US-ASCII"));

    RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectMgr = RestartabilityTestUtilities.createObjectManager();
    RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> persistence = 
            RestartStoreFactory.createStore(objectMgr,
                                            RestartabilityTestUtilities.createTempDirectory(
                                                    getClass().getSimpleName() + ".testCreateMapping_"),
                                            MEGABYTES.toBytes(1));
    persistence.startup().get();
    try {
        RestartableSerializablePortability<ByteBuffer> subject = new RestartableSerializablePortability<ByteBuffer>(id, persistence, true);
  
        objectMgr.registerObject(subject);
  
        Serializable simpleObject = new Integer(42);
        Assert.assertThat(subject.decode(subject.encode(simpleObject)), IsEqual.equalTo(simpleObject));
  
        HashMap<String, Serializable> complexObject = new HashMap<String, Serializable>();
        complexObject.put("foo", new Date());      
        Assert.assertThat(subject.decode(subject.encode(complexObject)), IsEqual.equalTo((Serializable) complexObject));
    } finally {
      persistence.shutdown();
    }
  }
  
  @Test
  public void testRecoveryMappings() throws Exception {
    File directory = RestartabilityTestUtilities.createTempDirectory(getClass().getSimpleName() + ".testRecoveryMappings");
    ByteBuffer id = ByteBuffer.wrap("testRecoveryMappings".getBytes("US-ASCII"));
    
    Serializable simpleObject = new Integer(42);
    ByteBuffer simpleBinary;
    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectMgr = RestartabilityTestUtilities.createObjectManager();
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> persistence =
              RestartStoreFactory.createStore(objectMgr, directory,
                                              MEGABYTES.toBytes(1));
      persistence.startup().get();
      try {
        RestartableSerializablePortability<ByteBuffer> subject = new RestartableSerializablePortability<ByteBuffer>(id, persistence, true);

        objectMgr.registerObject(subject);

        simpleBinary = subject.encode(simpleObject);
      } finally {
        persistence.shutdown();
      }
    }
    
    HashMap<String, Serializable> complexObject = new HashMap<String, Serializable>();
    complexObject.put("foo", new Date());      
    ByteBuffer complexBinary;
    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectMgr = RestartabilityTestUtilities.createObjectManager();
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> persistence =
              RestartStoreFactory.createStore(objectMgr, directory, MEGABYTES.toBytes(1));
      try {
        RestartableSerializablePortability<ByteBuffer> subject = new RestartableSerializablePortability<ByteBuffer>(id, persistence, true);

        objectMgr.registerObject(subject);

        persistence.startup().get();

        Assert.assertThat(subject.decode(simpleBinary.duplicate()), IsEqual.equalTo(simpleObject));
        Assert.assertThat(simpleBinary, IsEqual.equalTo(subject.encode(simpleObject)));

        complexBinary = subject.encode(complexObject);
      } finally {
        persistence.shutdown();
      }
    }

    {
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectMgr = RestartabilityTestUtilities.createObjectManager();
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> persistence =
              RestartStoreFactory.createStore(objectMgr, directory, MEGABYTES.toBytes(1));
      try {
        RestartableSerializablePortability<ByteBuffer> subject = new RestartableSerializablePortability<ByteBuffer>(id, persistence, true);

        objectMgr.registerObject(subject);

        persistence.startup().get();

        Assert.assertThat(subject.decode(simpleBinary.duplicate()), IsEqual.equalTo(simpleObject));
        Assert.assertThat(simpleBinary, IsEqual.equalTo(subject.encode(simpleObject)));

        Assert.assertThat(subject.decode(complexBinary.duplicate()), IsEqual.equalTo((Serializable) complexObject));
        Assert.assertThat(complexBinary, IsEqual.equalTo(subject.encode(complexObject)));
      } finally {
        persistence.shutdown();
      }
    }
  }
}
