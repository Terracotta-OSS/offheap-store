/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.disk.storage.portability;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.junit.Test;

/**
 *
 * @author Chris Dennis
 */
public class PersistentSerializablePortabilityTest {
  
  @Test
  public void testAddingMappingsToRecoveredPortability() throws IOException {
    PersistentSerializablePortability portability = new PersistentSerializablePortability();
    portability.encode(Integer.valueOf(0));
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    ObjectOutputStream oout = new ObjectOutputStream(bout);
    try {
      portability.persist(oout);
    } finally {
      oout.close();
    }
    byte[] persisted = bout.toByteArray();
    
    PersistentSerializablePortability recovered = new PersistentSerializablePortability();
    ByteArrayInputStream bin = new ByteArrayInputStream(persisted);
    ObjectInputStream oin = new ObjectInputStream(bin);
    try {
      recovered.bootstrap(oin);
    } finally {
      oin.close();
    }
    
    recovered.encode(Long.valueOf(0));
  }
}
