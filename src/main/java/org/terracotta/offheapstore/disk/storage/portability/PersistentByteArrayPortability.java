/*
 * Copyright 2015-2023 Terracotta, Inc., a Software AG company.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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

import org.terracotta.offheapstore.disk.persistent.PersistentPortability;
import org.terracotta.offheapstore.storage.portability.ByteArrayPortability;

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
