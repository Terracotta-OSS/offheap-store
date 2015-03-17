/*
 * All content copyright (c) Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */

package com.terracottatech.offheapstore.struct.dynamic;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.terracotta.offheapstore.paging.OffHeapStorageArea;
import com.terracottatech.offheapstore.struct.Field;
import com.terracottatech.offheapstore.struct.Field.BooleanField;
import com.terracottatech.offheapstore.struct.Field.BytesField;
import com.terracottatech.offheapstore.struct.Field.CharacterField;
import com.terracottatech.offheapstore.struct.Field.DoubleField;
import com.terracottatech.offheapstore.struct.Field.IntegerField;
import com.terracottatech.offheapstore.struct.Field.LongField;
import com.terracottatech.offheapstore.struct.Field.StringField;
import com.terracottatech.offheapstore.struct.Layout;
import com.terracottatech.offheapstore.struct.Pointer;
import com.terracottatech.offheapstore.struct.StructAccessor;
import com.terracottatech.offheapstore.struct.StructDefinition;
import com.terracottatech.offheapstore.struct.StructType;

import static com.terracottatech.offheapstore.struct.Layout.layout;

/**
 *
 * @author cdennis
 */
class DynamicStructDefinition implements StructDefinition {

  private final Map<String, StructType> structure;
  private final Map<String, Field> fields;
  private final int length;
  
  DynamicStructDefinition(Map<String, StructType> fields) {
    this.structure = Collections.unmodifiableMap(new HashMap<String, StructType>(fields));
    Layout<String> layout = layout(fields);
    this.fields = layout.layout();
    this.length = layout.length();
  }

  @Override
  public Map<String, StructType> structure() {
    return structure;
  }

  @Override
  public Pointer<StructAccessor> allocate(OffHeapStorageArea storage) {
    long address = storage.allocate(length);
    for (Field f : fields.values()) {
      f.initialize(storage, address);
    }
    if (address < 0) {
      return null;
    } else {
      return new DynamicPointer(storage, address);
    }
  }

  BooleanField booleanField(String name) {
    return (BooleanField) getField(name);
  }
  
  CharacterField charField(String name) {
    return (CharacterField) getField(name);
  }
  
  IntegerField intField(String name) {
    return (IntegerField) getField(name);
  }
  
  LongField longField(String name) {
    return (LongField) getField(name);
  }
  
  DoubleField doubleField(String name) {
    return (DoubleField) getField(name);
  }
 
  StringField stringField(String name) {
    return (StringField) getField(name);
  }

  BytesField bytesField(String name) {
    return (BytesField) getField(name);
  }
  
  private Field getField(String name) {
    return fields.get(name);
  }
  
  class DynamicPointer implements Pointer<StructAccessor> {
    
    private final OffHeapStorageArea storage;
    private final long address;

    DynamicPointer(OffHeapStorageArea storage, long address) {
      this.storage = storage;
      this.address = address;
    }
    
    @Override
    public void free() {
      for (Field f : fields.values()) {
        f.free(storage, address);
      }
      storage.free(address);
    }

    @Override
    public StructAccessor access() {
      return new DynamicStructAccessor(DynamicStructDefinition.this, storage, address);
    }
  }
}
