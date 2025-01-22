/*
 * All content copyright (c) Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */

package com.terracottatech.offheapstore.struct.statics;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.terracotta.offheapstore.paging.OffHeapStorageArea;
import com.terracottatech.offheapstore.struct.Field;
import com.terracottatech.offheapstore.struct.Layout;
import com.terracottatech.offheapstore.struct.Pointer;
import com.terracottatech.offheapstore.struct.StructDefinition;
import com.terracottatech.offheapstore.struct.StructType;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

import static com.terracottatech.offheapstore.struct.Layout.layout;
import static com.terracottatech.offheapstore.struct.statics.FieldHelper.createFieldHelper;
import static java.util.Collections.unmodifiableMap;
import static java.util.UUID.randomUUID;
import static org.objectweb.asm.ClassWriter.*;
import static org.objectweb.asm.Opcodes.*;
import static org.objectweb.asm.Type.*;
import static org.objectweb.asm.commons.Method.getMethod;

/**
 *
 * @author cdennis
 */
public class StaticStructDefinition<T> implements StructDefinition<T> {

  private static final StructClassLoader CLASSLOADER = AccessController.doPrivileged(new PrivilegedAction<StructClassLoader>() {
    @Override
    public StructClassLoader run() {
      return new StructClassLoader();
    }
  });
  private static final String STORAGE_DESCRIPTOR = getDescriptor(OffHeapStorageArea.class);
  private static final String CONSTRUCTOR_DESCRIPTOR = getMethodDescriptor(VOID_TYPE, 
          new Type[] {getType(OffHeapStorageArea.class), LONG_TYPE} );

  private final Constructor<? extends T> constructor;
  private final Method initialize;
  private final Method free;
  private final int length;
  private final Map<String, StructType> structure;

  public StaticStructDefinition(Class<T> template) throws IllegalArgumentException {
    if (!template.isInterface()) {
      Set<java.lang.reflect.Field> fields = getAllDeclaredFields(template);
      if (!fields.isEmpty()) {
        throw new IllegalArgumentException("Abstract template class must be stateless: " + fields);
      }
    }
    try {
      BeanInfo info = Introspector.getBeanInfo(template);

      Map<String, StructType> structure = new HashMap<String, StructType>();
      Type generatedType = Type.getObjectType(template.getName().replace('.', '/') + "_Struct_" + randomUUID().toString());

      Map<FieldHelper, StructType> fields = new HashMap<FieldHelper, StructType>();
      for (PropertyDescriptor property : info.getPropertyDescriptors()) {
        FieldHelper helper = createFieldHelper(generatedType, property);
        if (helper != null) {
          fields.put(helper, helper.getStructType());
          structure.put(property.getName(), helper.getStructType());
        }
      }

      ClassWriter cw = createClassWriter(generatedType, template);

      Layout<FieldHelper> layout = layout(fields);

      GeneratorAdapter initAdapter = new GeneratorAdapter(ACC_PUBLIC | ACC_STATIC, 
              getMethod("void initialize(" + OffHeapStorageArea.class.getName() + ", long)"),
              null, null, cw);
      for (Entry<FieldHelper, Field> e : layout.layout().entrySet()) {
        e.getKey().initialize(e.getValue(), initAdapter);
      }
      initAdapter.returnValue();
      initAdapter.endMethod();

      GeneratorAdapter freeAdapter = new GeneratorAdapter(ACC_PUBLIC | ACC_STATIC, 
              getMethod("void free(" + OffHeapStorageArea.class.getName() + ", long)"),
              null, null, cw);
      for (Entry<FieldHelper, Field> e : layout.layout().entrySet()) {
        e.getKey().free(e.getValue(), freeAdapter);
      }
      freeAdapter.returnValue();
      freeAdapter.endMethod();
      
      for (Entry<FieldHelper, Field> e : layout.layout().entrySet()) {
        e.getKey().write(e.getValue(), cw);
      }

      cw.visitEnd();

      Class<? extends T> klazz = CLASSLOADER.loadStructClass(generatedType.getClassName(), cw.toByteArray()).asSubclass(template);
      try {
        this.constructor = klazz.getConstructor(OffHeapStorageArea.class, Long.TYPE);
        this.initialize = klazz.getDeclaredMethod("initialize", OffHeapStorageArea.class, Long.TYPE);
        this.free = klazz.getDeclaredMethod("free", OffHeapStorageArea.class, Long.TYPE);
      } catch (NoSuchMethodException e) {
        throw new AssertionError(e);
      }
      this.length = layout.length();
      this.structure = unmodifiableMap(structure);
    } catch (IntrospectionException e) {
      throw new IllegalArgumentException(e);
    }
  }
  
  private static ClassWriter createClassWriter(Type generatedType, Class<?> template) {
    ClassWriter cw = new ClassWriter(COMPUTE_FRAMES | COMPUTE_MAXS);
    if (template.isInterface()) {
      cw.visit(V1_6, ACC_PUBLIC, generatedType.getInternalName(), null, getInternalName(Object.class), new String[] {getInternalName(template)});
    } else {
      cw.visit(V1_6, ACC_PUBLIC, generatedType.getInternalName(), null, getInternalName(template), null);
    }

    cw.visitField(ACC_PRIVATE | ACC_FINAL, "storage", STORAGE_DESCRIPTOR, null, null);
    cw.visitField(ACC_PRIVATE | ACC_FINAL, "address", LONG_TYPE.getDescriptor(), null, null);
    MethodVisitor mv = cw.visitMethod(ACC_PUBLIC, "<init>", CONSTRUCTOR_DESCRIPTOR, null, null);
    mv.visitCode();
    if (template.isInterface()) {
      mv.visitVarInsn(ALOAD, 0);
      mv.visitMethodInsn(INVOKESPECIAL, getInternalName(Object.class), "<init>", "()V");
    } else {
      mv.visitVarInsn(ALOAD, 0);
      mv.visitMethodInsn(INVOKESPECIAL, getInternalName(template), "<init>", "()V");
    }
    mv.visitVarInsn(ALOAD, 0);
    mv.visitVarInsn(ALOAD, 1);
    mv.visitFieldInsn(PUTFIELD, generatedType.getInternalName(), "storage", STORAGE_DESCRIPTOR);
    mv.visitVarInsn(ALOAD, 0);
    mv.visitVarInsn(LLOAD, 2);
    mv.visitFieldInsn(PUTFIELD, generatedType.getInternalName(), "address", LONG_TYPE.getDescriptor());
    mv.visitInsn(RETURN);
    mv.visitMaxs(0, 0);
    mv.visitEnd();
    return cw;
  }

  private static Set<java.lang.reflect.Field> getAllDeclaredFields(Class<?> klazz) {
    Set<java.lang.reflect.Field> fields = new HashSet<java.lang.reflect.Field>();
    for (Class<?> c = klazz; c != null; c = c.getSuperclass()) {
      fields.addAll(Arrays.asList(c.getDeclaredFields()));
    }
    return Collections.unmodifiableSet(fields);
  }
  
  @Override
  public Map<String, StructType> structure() {
    return structure;
  }

  @Override
  public Pointer<T> allocate(OffHeapStorageArea storage) {
    long address = storage.allocate(length);
    try {
      initialize.invoke(null, storage, address);
    } catch (IllegalAccessException e) {
      throw translateReflectionException(e);
    } catch (InvocationTargetException e) {
      throw translateReflectionException(e);
    }
    if (address < 0) {
      return null;
    } else {
      return new StaticPointer(storage, address);
    }
  }

  private class StaticPointer implements Pointer<T> {

    private final OffHeapStorageArea storage;
    private final long address;
    
    StaticPointer(OffHeapStorageArea storage, long address) {
      this.storage = storage;
      this.address = address;
    }

    @Override
    public T access() {
      try {
        return constructor.newInstance(storage, address);
      } catch (InstantiationException ex) {
        throw translateReflectionException(ex);
      } catch (IllegalAccessException ex) {
        throw translateReflectionException(ex);
      } catch (InvocationTargetException ex) {
        throw translateReflectionException(ex);
      }
    }

    @Override
    public void free() {
      try {
        free.invoke(null, storage, address);
      } catch (IllegalAccessException ex) {
        throw translateReflectionException(ex);
      } catch (InvocationTargetException ex) {
        throw translateReflectionException(ex);
      }
      storage.free(address);
    }
  }
  
  private static RuntimeException translateReflectionException(Exception e) {
    return new RuntimeException(e);
  }
  
  static class StructClassLoader extends ClassLoader {
    
    public Class<?> loadStructClass(String name, byte[] definition) {
      return defineClass(name, definition, 0, definition.length);
    }
  }
}
