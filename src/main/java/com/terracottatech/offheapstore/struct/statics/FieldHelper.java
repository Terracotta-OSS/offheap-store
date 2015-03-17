/*
 * All content copyright (c) Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */

package com.terracottatech.offheapstore.struct.statics;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;

import org.terracotta.offheapstore.paging.OffHeapStorageArea;
import com.terracottatech.offheapstore.struct.Field;
import com.terracottatech.offheapstore.struct.StructType;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

import static com.terracottatech.offheapstore.struct.StructType.BOOLEAN;
import static com.terracottatech.offheapstore.struct.StructType.BYTES;
import static com.terracottatech.offheapstore.struct.StructType.CHARACTER;
import static com.terracottatech.offheapstore.struct.StructType.DOUBLE;
import static com.terracottatech.offheapstore.struct.StructType.INTEGER;
import static com.terracottatech.offheapstore.struct.StructType.LONG;
import static com.terracottatech.offheapstore.struct.StructType.STRING;
import static org.objectweb.asm.Opcodes.ACC_PUBLIC;
import static org.objectweb.asm.Type.INT_TYPE;
import static org.objectweb.asm.Type.LONG_TYPE;
import static org.objectweb.asm.Type.getType;
import static org.objectweb.asm.commons.GeneratorAdapter.ADD;
import static org.objectweb.asm.commons.Method.getMethod;

/**
 *
 * @author cdennis
 */
abstract class FieldHelper implements Comparable<FieldHelper> {

  private static final Type STORAGE_TYPE = getType(OffHeapStorageArea.class);
  private static final Type ADDRESS_TYPE = LONG_TYPE;
  
  private final String name;
  private final Type thisType;
  private final Method reader;
  private final Method writer;

  FieldHelper(Type thisType, PropertyDescriptor prop) {
    this.name = prop.getName();
    this.thisType = thisType;
    this.reader = prop.getReadMethod();
    this.writer = prop.getWriteMethod();
  }

  void write(Field field, ClassWriter cw) {
    GeneratorAdapter readGenerator = new GeneratorAdapter(ACC_PUBLIC, getMethod(reader), null, null, cw);
    readGenerator.loadThis();
    readGenerator.getField(thisType, "storage", STORAGE_TYPE);
    readGenerator.loadThis();
    readGenerator.getField(thisType, "address", ADDRESS_TYPE);
    readGenerator.push(field.offset());
    readGenerator.cast(INT_TYPE, LONG_TYPE);
    readGenerator.math(ADD, LONG_TYPE);
    Method structReader = getStructReader();
    readGenerator.invokeStatic(getType(structReader.getDeclaringClass()), getMethod(structReader));
    readGenerator.returnValue();
    readGenerator.endMethod();

    GeneratorAdapter writeGenerator = new GeneratorAdapter(ACC_PUBLIC, getMethod(writer), null, null, cw);
    writeGenerator.loadThis();
    writeGenerator.getField(thisType, "storage", STORAGE_TYPE);
    writeGenerator.loadThis();
    writeGenerator.getField(thisType, "address", ADDRESS_TYPE);
    writeGenerator.push(field.offset());
    writeGenerator.cast(INT_TYPE, LONG_TYPE);
    writeGenerator.math(ADD, LONG_TYPE);
    writeGenerator.loadArgs();
    Method structWriter = getStructWriter();
    writeGenerator.invokeStatic(getType(structWriter.getDeclaringClass()), getMethod(structWriter));
    writeGenerator.returnValue();
    writeGenerator.endMethod();
  }

  void initialize(Field value, GeneratorAdapter initAdapter) {
    //nothing
  }
  
  void free(Field value, GeneratorAdapter initAdapter) {
    //nothing
  }
  
  abstract StructType getStructType();

  @Override
  public final int compareTo(FieldHelper o) {
    return name.compareTo(o.name);
  }

  @Override
  public final boolean equals(Object o) {
    if (o instanceof FieldHelper) {
      return name.equals(((FieldHelper) o).name);
    } else {
      return false;
    }
  }

  @Override
  public final int hashCode() {
    return name.hashCode();
  }
  
  public static FieldHelper createFieldHelper(Type thisType, PropertyDescriptor prop) {
    Class<?> propertyType = prop.getPropertyType();
    if (!isStructProperty(prop)) {
      return null;
    } else if (Boolean.TYPE.equals(propertyType)) {
      return new BooleanFieldHelper(thisType, prop);
    } else if (Character.TYPE.equals(propertyType)) {
      return new CharacterFieldHelper(thisType, prop);
    } else if (Integer.TYPE.equals(propertyType)) {
      return new IntegerFieldHelper(thisType, prop);
    } else if (Long.TYPE.equals(propertyType)) {
      return new LongFieldHelper(thisType, prop);
    } else if (Double.TYPE.equals(propertyType)) {
      return new DoubleFieldHelper(thisType, prop);
    } else if (CharSequence.class.equals(propertyType)) {
      return new StringFieldHelper(thisType, prop);
    } else if (ByteBuffer.class.equals(propertyType)) {
      return new BytesFieldHelper(thisType, prop);
    } else {
      throw new IllegalArgumentException("Property " + prop.getName() + " has type " + prop.getPropertyType() + " which is not supported");
    }
  }

  private static boolean isStructProperty(PropertyDescriptor prop) {
    Method reader = prop.getReadMethod();
    Method writer = prop.getWriteMethod();
    if (reader == null) {
      if (Modifier.isAbstract(writer.getModifiers())) {
        throw new IllegalArgumentException("Struct property " + prop.getName() + " has no reader method");
      } else {
        return false;
      }
    } else if (Modifier.isAbstract(reader.getModifiers())) {
      if (writer == null) {
        throw new IllegalArgumentException("Struct property " + prop.getName() + " has no writer method");
      } else if (Modifier.isAbstract(writer.getModifiers())) {
        return true;
      } else {
        throw new IllegalArgumentException("Struct property " + prop.getName() + " has a concrete writer method");
      }
    } else {
      if (writer == null) {
        return false;
      } else if (Modifier.isAbstract(writer.getModifiers())) {
        throw new IllegalArgumentException("Struct property " + prop.getName() + " has a concrete reader method");
      } else {
        return false;
      }
    }
  }
  
  protected abstract Method getStructWriter();

  protected abstract Method getStructReader();

  static class BooleanFieldHelper extends FieldHelper {

    private static final Method STRUCT_READER = method(Field.class, "readBoolean", OffHeapStorageArea.class, Long.TYPE);
    private static final Method STRUCT_WRITER = method(Field.class, "writeBoolean", OffHeapStorageArea.class, Long.TYPE, Boolean.TYPE);
    
    private BooleanFieldHelper(Type thisType, PropertyDescriptor desc) {
      super(thisType, desc);
    }


    @Override
    StructType getStructType() {
      return BOOLEAN;
    }

    @Override
    protected Method getStructWriter() {
      return STRUCT_WRITER;
    }

    @Override
    protected Method getStructReader() {
      return STRUCT_READER;
    }
  }
  
  static class CharacterFieldHelper extends FieldHelper {

    private static final Method STRUCT_READER = method(Field.class, "readCharacter", OffHeapStorageArea.class, Long.TYPE);
    private static final Method STRUCT_WRITER = method(Field.class, "writeCharacter", OffHeapStorageArea.class, Long.TYPE, Character.TYPE);
    
    private CharacterFieldHelper(Type thisType, PropertyDescriptor desc) {
      super(thisType, desc);
    }


    @Override
    StructType getStructType() {
      return CHARACTER;
    }

    @Override
    protected Method getStructWriter() {
      return STRUCT_WRITER;
    }

    @Override
    protected Method getStructReader() {
      return STRUCT_READER;
    }
  }

  static class IntegerFieldHelper extends FieldHelper {

    private static final Method STRUCT_READER = method(Field.class, "readInteger", OffHeapStorageArea.class, Long.TYPE);
    private static final Method STRUCT_WRITER = method(Field.class, "writeInteger", OffHeapStorageArea.class, Long.TYPE, Integer.TYPE);
    
    private IntegerFieldHelper(Type thisType, PropertyDescriptor desc) {
      super(thisType, desc);
    }


    @Override
    StructType getStructType() {
      return INTEGER;
    }

    @Override
    protected Method getStructWriter() {
      return STRUCT_WRITER;
    }

    @Override
    protected Method getStructReader() {
      return STRUCT_READER;
    }
  }

  static class LongFieldHelper extends FieldHelper {

    private static final Method STRUCT_READER = method(Field.class, "readLong", OffHeapStorageArea.class, Long.TYPE);
    private static final Method STRUCT_WRITER = method(Field.class, "writeLong", OffHeapStorageArea.class, Long.TYPE, Long.TYPE);
    
    private LongFieldHelper(Type thisType, PropertyDescriptor desc) {
      super(thisType, desc);
    }


    @Override
    StructType getStructType() {
      return LONG;
    }
    
    @Override
    protected Method getStructWriter() {
      return STRUCT_WRITER;
    }

    @Override
    protected Method getStructReader() {
      return STRUCT_READER;
    }
  }

  static class DoubleFieldHelper extends FieldHelper {

    private static final Method STRUCT_READER = method(Field.class, "readDouble", OffHeapStorageArea.class, Long.TYPE);
    private static final Method STRUCT_WRITER = method(Field.class, "writeDouble", OffHeapStorageArea.class, Long.TYPE, Double.TYPE);
    
    private DoubleFieldHelper(Type thisType, PropertyDescriptor desc) {
      super(thisType, desc);
    }


    @Override
    StructType getStructType() {
      return DOUBLE;
    }

    @Override
    protected Method getStructWriter() {
      return STRUCT_WRITER;
    }

    @Override
    protected Method getStructReader() {
      return STRUCT_READER;
    }
  }

  static class StringFieldHelper extends FieldHelper {

    private static final Method STRUCT_READER = method(Field.class, "readString", OffHeapStorageArea.class, Long.TYPE);
    private static final Method STRUCT_WRITER = method(Field.class, "writeString", OffHeapStorageArea.class, Long.TYPE, CharSequence.class);
    private static final Method INIT_WRITER = method(Field.class, "writeLong", OffHeapStorageArea.class, Long.TYPE, Long.TYPE);
    
    private StringFieldHelper(Type thisType, PropertyDescriptor desc) {
      super(thisType, desc);
    }


    @Override
    StructType getStructType() {
      return STRING;
    }

    @Override
    protected Method getStructWriter() {
      return STRUCT_WRITER;
    }

    @Override
    protected Method getStructReader() {
      return STRUCT_READER;
    }
    
    @Override
    void initialize(Field field, GeneratorAdapter initAdapter) {
      initAdapter.loadArgs();
      initAdapter.push(field.offset());
      initAdapter.cast(INT_TYPE, LONG_TYPE);
      initAdapter.math(ADD, LONG_TYPE);
      initAdapter.push(-1L);
      initAdapter.invokeStatic(getType(INIT_WRITER.getDeclaringClass()), getMethod(INIT_WRITER));
    }

    @Override
    void free(Field field, GeneratorAdapter adapter) {
      adapter.loadArgs();
      adapter.push(field.offset());
      adapter.cast(INT_TYPE, LONG_TYPE);
      adapter.math(ADD, LONG_TYPE);
      adapter.push((Type) null);
      adapter.invokeStatic(getType(STRUCT_WRITER.getDeclaringClass()), getMethod(STRUCT_WRITER));
    }
  }

  static class BytesFieldHelper extends FieldHelper {

    private static final Method STRUCT_READER = method(Field.class, "readBytes", OffHeapStorageArea.class, Long.TYPE);
    private static final Method STRUCT_WRITER = method(Field.class, "writeBytes", OffHeapStorageArea.class, Long.TYPE, ByteBuffer.class);
    private static final Method INIT_WRITER = method(Field.class, "writeLong", OffHeapStorageArea.class, Long.TYPE, Long.TYPE);
    
    private BytesFieldHelper(Type thisType, PropertyDescriptor desc) {
      super(thisType, desc);
    }


    @Override
    StructType getStructType() {
      return BYTES;
    }

    @Override
    protected Method getStructWriter() {
      return STRUCT_WRITER;
    }

    @Override
    protected Method getStructReader() {
      return STRUCT_READER;
    }
    
    @Override
    void initialize(Field field, GeneratorAdapter adapter) {
      adapter.loadArgs();
      adapter.push(field.offset());
      adapter.cast(INT_TYPE, LONG_TYPE);
      adapter.math(ADD, LONG_TYPE);
      adapter.push(-1L);
      adapter.invokeStatic(getType(INIT_WRITER.getDeclaringClass()), getMethod(INIT_WRITER));
    }

    @Override
    void free(Field field, GeneratorAdapter adapter) {
      adapter.loadArgs();
      adapter.push(field.offset());
      adapter.cast(INT_TYPE, LONG_TYPE);
      adapter.math(ADD, LONG_TYPE);
      adapter.push((Type) null);
      adapter.invokeStatic(getType(STRUCT_WRITER.getDeclaringClass()), getMethod(STRUCT_WRITER));
    }

  }

  static Method method(Class<?> klazz, String name, Class<?> ... parameters) {
    try {
      return klazz.getDeclaredMethod(name, parameters);
    } catch (NoSuchMethodException e) {
      throw new AssertionError(e);
    }
  }
}
