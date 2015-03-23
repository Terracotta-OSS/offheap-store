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
package org.terracotta.offheapstore.storage.portability;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;

import org.terracotta.offheapstore.disk.persistent.PersistentPortability;

public class StringPortability implements PersistentPortability<String> {

  public static final StringPortability INSTANCE = new StringPortability();
  
  private StringPortability() {
    //singleton
  }
  
  @Override
  public ByteBuffer encode(String object) {
    ByteBuffer buffer = ByteBuffer.allocate(object.length() * 2);
    buffer.asCharBuffer().put(object).clear();
    return buffer;
  }

  @Override
  public String decode(ByteBuffer buffer) {
    return buffer.asCharBuffer().toString();
  }

  @Override
  public boolean equals(Object value, ByteBuffer readBuffer) {
    return value.equals(decode(readBuffer));
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
