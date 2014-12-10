/*
 * All content copyright (c) Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */

package com.terracottatech.offheapstore.struct.dynamic;

import com.terracottatech.offheapstore.struct.StructAccessor;
import com.terracottatech.offheapstore.struct.StructBuilder;
import com.terracottatech.offheapstore.struct.StructDefinition;
import com.terracottatech.offheapstore.struct.StructType;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author cdennis
 */
public class DynamicStructBuilder extends StructBuilder {

  private final Map<String, StructType> fields = new HashMap<String, StructType>();
  
  public DynamicStructBuilder bool(String name) {
    return add(name, StructType.BOOLEAN);
  }
  
  public DynamicStructBuilder character(String name) {
    return add(name, StructType.CHARACTER);
  }
  
  public DynamicStructBuilder integer(String name) {
    return add(name, StructType.INTEGER);
  }
  
  public DynamicStructBuilder longint(String name) {
    return add(name, StructType.LONG);
  }
  
  public DynamicStructBuilder doublefloat(String name) {
    return add(name, StructType.DOUBLE);
  }
  
  public DynamicStructBuilder string(String name) {
    return add(name, StructType.STRING);
  }
  
  public DynamicStructBuilder bytes(String name) {
    return add(name, StructType.BYTES);
  }
  
  public DynamicStructBuilder add(String name, StructType type) {
    if (fields.containsKey(name)) {
      throw new IllegalStateException("Field '" + name + "' already defined as type " + fields.get(name));
    } else {
      fields.put(name, type);
      return this;
    }
  }
  
  public StructDefinition<StructAccessor> build() {
    return new DynamicStructDefinition(fields);
  }
}
