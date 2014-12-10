/*
 * All content copyright (c) Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */

package com.terracottatech.offheapstore.struct;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import com.terracottatech.offheapstore.struct.Field.BooleanField;
import com.terracottatech.offheapstore.struct.Field.BytesField;
import com.terracottatech.offheapstore.struct.Field.CharacterField;
import com.terracottatech.offheapstore.struct.Field.DoubleField;
import com.terracottatech.offheapstore.struct.Field.IntegerField;
import com.terracottatech.offheapstore.struct.Field.LongField;
import com.terracottatech.offheapstore.struct.Field.StringField;

/**
 *
 * @author cdennis
 */
public class Layout<T extends Comparable> {
  
  public static <T extends Comparable<T>> Layout<T> layout(Map<T, StructType> fields) {
    Map<T, Field> layout = new HashMap<T, Field>();
    int offset = 0;
    for (Entry<StructType, SortedSet<T>> typedFields : gatherFields(fields).entrySet()) {
      for (T identifier : typedFields.getValue()) {
        Field field = createField(typedFields.getKey(), offset);
        layout.put(identifier, field);
        offset += field.length();
      }
    }
    return new Layout(layout, offset);
  }

  private final Map<T, Field> layout;
  private final int length;
  
  private Layout(Map<T, Field> layout, int length) {
    this.layout = Collections.unmodifiableMap(layout);
    this.length = length;
  }

  public int length() {
    return length;
  }
  
  public Map<T, Field> layout() {
    return layout;
  }
  
  private static <T extends Comparable<T>> EnumMap<StructType, SortedSet<T>> gatherFields(Map<T, StructType> fields) {
    EnumMap<StructType, SortedSet<T>> gathered = new EnumMap<StructType, SortedSet<T>>(StructType.class);
    for (Entry<T, StructType> e : fields.entrySet()) {
      SortedSet<T> typedFields = gathered.get(e.getValue());
      if (typedFields == null) {
        typedFields = new TreeSet<T>();
        gathered.put(e.getValue(), typedFields);
      }
      typedFields.add(e.getKey());
    }
    return gathered;
  }
  
  private static Field createField(StructType type, int offset) {
    switch (type) {
      case BOOLEAN: return new BooleanField(offset);
      case CHARACTER: return new CharacterField(offset);
      case INTEGER: return new IntegerField(offset);
      case LONG: return new LongField(offset);
      case DOUBLE: return new DoubleField(offset);
      case STRING: return new StringField(offset);
      case BYTES: return new BytesField(offset);
    }
    throw new AssertionError();
  }
}
