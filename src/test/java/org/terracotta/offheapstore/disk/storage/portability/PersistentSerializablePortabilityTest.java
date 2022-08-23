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

import org.terracotta.offheapstore.disk.storage.portability.PersistentSerializablePortability;
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
    portability.encode(0);
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    try (ObjectOutputStream oout = new ObjectOutputStream(bout)) {
      portability.persist(oout);
    }
    byte[] persisted = bout.toByteArray();

    PersistentSerializablePortability recovered = new PersistentSerializablePortability();
    ByteArrayInputStream bin = new ByteArrayInputStream(persisted);
    try (ObjectInputStream oin = new ObjectInputStream(bin)) {
      recovered.bootstrap(oin);
    }

    recovered.encode(0L);
  }
}
