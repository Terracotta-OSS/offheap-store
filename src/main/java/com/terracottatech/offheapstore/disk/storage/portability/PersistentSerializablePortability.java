/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.disk.storage.portability;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectStreamClass;
import java.io.Serializable;
import java.util.Map.Entry;

import com.terracottatech.offheapstore.disk.persistent.PersistentPortability;
import com.terracottatech.offheapstore.storage.portability.SerializablePortability;
import com.terracottatech.offheapstore.util.FindbugsSuppressWarnings;

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
          output.writeInt(((Integer) e.getKey()).intValue());
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
