/*
 * All content copyright (c) Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */

package com.terracottatech.offheapstore.struct.dynamic;

import java.nio.ByteBuffer;

import org.terracotta.offheapstore.paging.OffHeapStorageArea;
import com.terracottatech.offheapstore.struct.StructAccessor;


/**
 *
 * @author cdennis
 */
class DynamicStructAccessor implements StructAccessor {

  private final DynamicStructDefinition definition;
  private final OffHeapStorageArea storage;
  private final long address;

  DynamicStructAccessor(DynamicStructDefinition definition, OffHeapStorageArea storage, long address) {
    this.definition = definition;
    this.storage = storage;
    this.address = address;
  }
  
  @Override
  public boolean getBoolean(String name) {
    return definition.booleanField(name).read(storage, address);
  }

  @Override
  public void setBoolean(String name, boolean value) {
    definition.booleanField(name).write(storage, address, value);
  }

  @Override
  public char getCharacter(String name) {
    return definition.charField(name).read(storage, address);
  }

  @Override
  public void setCharacter(String name, char value) {
    definition.charField(name).write(storage, address, value);
  }

  @Override
  public int getInteger(String name) {
    return definition.intField(name).read(storage, address);
  }

  @Override
  public void setInteger(String name, int value) {
    definition.intField(name).write(storage, address, value);
  }

  @Override
  public long getLong(String name) {
    return definition.longField(name).read(storage, address);
  }

  @Override
  public void setLong(String name, long value) {
    definition.longField(name).write(storage, address, value);
  }

  @Override
  public double getDouble(String name) {
    return definition.doubleField(name).read(storage, address);
  }

  @Override
  public void setDouble(String name, double value) {
    definition.doubleField(name).write(storage, address, value);
  }

  @Override
  public CharSequence getString(String name) {
    return definition.stringField(name).read(storage, address);
  }

  @Override
  public void setString(String name, CharSequence value) {
    definition.stringField(name).write(storage, address, value);
  }
  
  @Override
  public ByteBuffer getBytes(String name) {
    return definition.bytesField(name).read(storage, address);
  }

  @Override
  public void setBytes(String name, ByteBuffer value) {
    definition.bytesField(name).write(storage, address, value);
  }
}
