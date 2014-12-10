/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.disk.storage.portability;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.terracottatech.offheapstore.disk.persistent.PersistentPortability;
import com.terracottatech.offheapstore.storage.portability.ByteArrayPortability;

/**
 *
 * @author Chris Dennis
 */
public class PersistentByteArrayPortability extends ByteArrayPortability implements PersistentPortability<byte[]> {

  public static final PersistentByteArrayPortability INSTANCE = new PersistentByteArrayPortability();
  
  private PersistentByteArrayPortability() {
    //singleton
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
  public void persist(ObjectOutput output) throws IOException {
    //no-op
  }

  @Override
  public void bootstrap(ObjectInput input) throws IOException {
    //no-op
  }

}
