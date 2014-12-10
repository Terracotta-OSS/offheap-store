/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */

package com.terracottatech.offheapstore.storage.portability;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;

import com.terracottatech.offheapstore.disk.persistent.PersistentPortability;

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
