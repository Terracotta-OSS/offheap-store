/* 
 * Copyright 2015 Terracotta, Inc., a Software AG company.
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
package org.terracotta.offheapstore.disk.storage.portability;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectStreamClass;
import java.io.Serializable;
import java.util.Map.Entry;

import org.terracotta.offheapstore.disk.persistent.PersistentPortability;
import org.terracotta.offheapstore.storage.portability.SerializablePortability;
import org.terracotta.offheapstore.util.FindbugsSuppressWarnings;

/**
 *
 * @author Chris Dennis
 */
public class PersistentSerializablePortability extends SerializablePortability implements PersistentPortability<Serializable> {

  private static final int MAGIC = 0xfeedbeef;

  public PersistentSerializablePortability() {
    super();
  }
  
  public PersistentSerializablePortability(ClassLoader classLoader) {
    super(classLoader);
  }

  @Override
  public void flush() throws IOException {
    //no-op
  }

  @Override
  public void close() throws IOException {
    //no-op
  }

  @Override
  @FindbugsSuppressWarnings("JLM_JSR166_UTILCONCURRENT_MONITORENTER")
  public void persist(ObjectOutput output) throws IOException {
    output.writeInt(MAGIC);
    synchronized (lookup) {
      for (Entry<Object, Object> e : lookup.entrySet()) {
        if (e.getKey() instanceof Integer) {
          output.writeInt(((Integer) e.getKey()));
          output.writeObject(e.getValue());
        }
      }
      output.writeInt(-1);
    }
  }

  @Override
  @FindbugsSuppressWarnings("JLM_JSR166_UTILCONCURRENT_MONITORENTER")
  public void bootstrap(ObjectInput input) throws IOException {
    if (input.readInt() != MAGIC) {
      throw new IOException("Wrong magic number");
    }

    synchronized (lookup) {
      int nextIndex = 0;
      while (true) {
        int representation = input.readInt();

        if (representation == -1) {
          if (nextStreamIndex == 0) {
            nextStreamIndex = nextIndex;
          } else if (nextStreamIndex != nextIndex) {
            throw new IOException("Cannot bootstrap already used instance");
          }
          break;
        } else {
          ObjectStreamClass osc;
          try {
            osc = (ObjectStreamClass) input.readObject();
          } catch (ClassNotFoundException e) {
            throw new IOException(e);
          }
          SerializableDataKey key = new SerializableDataKey(disconnect(osc), true);
          ObjectStreamClass oldOsc = (ObjectStreamClass) lookup.putIfAbsent(representation, osc);
          Integer oldRep = (Integer) lookup.putIfAbsent(key, representation);
          if (oldRep != null && !oldRep.equals(representation)) {
            throw new IOException("Existing colliding class mapping detected");
          } else if (oldOsc != null && !oldOsc.getName().equals(osc.getName())) {
            throw new IOException("Existing colliding class mapping detected");
          } else {
            nextIndex = Math.max(nextIndex, representation + 1);
          }
        }
      }
    }
  }
}
