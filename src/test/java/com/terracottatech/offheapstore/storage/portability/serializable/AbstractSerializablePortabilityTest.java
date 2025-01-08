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
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.offheapstore.storage.portability.serializable;

import com.terracottatech.frs.RestartStore;
import com.terracottatech.frs.RestartStoreFactory;
import com.terracottatech.frs.object.RegisterableObjectManager;
import org.terracotta.offheapstore.storage.portability.Portability;
import com.terracottatech.offheapstore.storage.restartable.RestartabilityTestUtilities;
import com.terracottatech.offheapstore.storage.restartable.portability.RestartableSerializablePortability;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

import static org.terracotta.offheapstore.util.MemoryUnit.MEGABYTES;

/**
 *
 * @author cdennis
 */
public abstract class AbstractSerializablePortabilityTest {

  protected final Portability<Serializable> createPortability() {
    try {
      return new ContinuallyRestartingPortability(RestartabilityTestUtilities.createTempDirectory("AbstractSerializablePortabilityTest"));
    } catch (IOException ex) {
      throw new AssertionError(ex);
    }
  }

  private static class ContinuallyRestartingPortability implements Portability<Serializable> {
    
    private final File storage;

    private Instance startup() throws Exception {
      ByteBuffer id = ByteBuffer.wrap("ContinuallyRestartingPortability".getBytes("US-ASCII"));
      RegisterableObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectMgr = RestartabilityTestUtilities.createObjectManager();
      RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> persistence = RestartStoreFactory.createStore(objectMgr, storage, MEGABYTES.toBytes(1));
      RestartableSerializablePortability<ByteBuffer> subject = new RestartableSerializablePortability<ByteBuffer>(id, persistence, true);

      objectMgr.registerObject(subject);
      persistence.startup().get();
      
      return new Instance(persistence, subject);
    }
    
    public ContinuallyRestartingPortability(File storage) {
      this.storage = storage;
    }

    @Override
    public ByteBuffer encode(Serializable object) {
      try {
        Instance instance = startup();
        try {
          return instance.getPortability().encode(object);
        } finally {
          instance.close();
        }
      } catch (Exception e) {
        throw new AssertionError(e);
      }
    }

    @Override
    public Serializable decode(ByteBuffer buffer) {
      try {
        Instance instance = startup();
        try {
          return instance.getPortability().decode(buffer);
        } finally {
          instance.close();
        }
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new AssertionError(e);
      }
    }

    @Override
    public boolean equals(Object object, ByteBuffer buffer) {
      try {
        Instance instance = startup();
        try {
          return instance.getPortability().equals(object, buffer);
        } finally {
          instance.close();
        }
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new AssertionError(e);
      }
    }
    
    static class Instance implements Closeable {
      
      private final RestartStore<?, ?, ?> persistence;
      private final Portability<Serializable> portability;

      public Instance(RestartStore<?, ?, ?> persistence, Portability<Serializable> portability) {
        this.persistence = persistence;
        this.portability = portability;
      }

      @Override
      public void close() throws IOException {
        try {
          persistence.shutdown();
        } catch (InterruptedException ex) {
          throw new IOException(ex);
        }
      }
      
      Portability<Serializable> getPortability() {
        return portability;
      }
    }
  }
  
}
