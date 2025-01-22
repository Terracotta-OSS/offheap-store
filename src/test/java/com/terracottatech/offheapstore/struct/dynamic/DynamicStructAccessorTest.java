/*
 * All content copyright (c) Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */

package com.terracottatech.offheapstore.struct.dynamic;

import org.terracotta.offheapstore.paging.OffHeapStorageArea;
import com.terracottatech.offheapstore.struct.Field.BooleanField;
import com.terracottatech.offheapstore.struct.Field.BytesField;
import com.terracottatech.offheapstore.struct.Field.CharacterField;
import com.terracottatech.offheapstore.struct.Field.DoubleField;
import com.terracottatech.offheapstore.struct.Field.IntegerField;
import com.terracottatech.offheapstore.struct.Field.LongField;
import com.terracottatech.offheapstore.struct.Field.StringField;

import java.nio.ByteBuffer;

import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 *
 * @author cdennis
 */
public class DynamicStructAccessorTest {
  
  @Test
  public void testGetBoolean() {
    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);
    BooleanField field = mock(BooleanField.class);
    when(field.read(storage, 42L)).thenReturn(true);
    DynamicStructDefinition defn = mock(DynamicStructDefinition.class);
    when(defn.booleanField("foo")).thenReturn(field);
    DynamicStructAccessor accessor = new DynamicStructAccessor(defn, storage, 42L);
    
    assertThat(accessor.getBoolean("foo"), is(true));
  }

  @Test
  public void testSetBoolean() {
    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);
    BooleanField field = mock(BooleanField.class);
    DynamicStructDefinition defn = mock(DynamicStructDefinition.class);
    when(defn.booleanField("foo")).thenReturn(field);
    DynamicStructAccessor accessor = new DynamicStructAccessor(defn, storage, 42L);

    accessor.setBoolean("foo", true);
    verify(field).write(storage, 42L, true);
  }

  @Test
  public void testGetCharacter() {
    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);
    CharacterField field = mock(CharacterField.class);
    when(field.read(storage, 42L)).thenReturn('c');
    DynamicStructDefinition defn = mock(DynamicStructDefinition.class);
    when(defn.charField("foo")).thenReturn(field);
    DynamicStructAccessor accessor = new DynamicStructAccessor(defn, storage, 42L);
    
    assertThat(accessor.getCharacter("foo"), is('c'));
  }

  @Test
  public void testSetCharacter() {
    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);
    CharacterField field = mock(CharacterField.class);
    DynamicStructDefinition defn = mock(DynamicStructDefinition.class);
    when(defn.charField("foo")).thenReturn(field);
    DynamicStructAccessor accessor = new DynamicStructAccessor(defn, storage, 42L);

    accessor.setCharacter("foo", 'd');
    verify(field).write(storage, 42L, 'd');
  }

  @Test
  public void testGetInteger() {
    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);
    IntegerField field = mock(IntegerField.class);
    when(field.read(storage, 42L)).thenReturn(99);
    DynamicStructDefinition defn = mock(DynamicStructDefinition.class);
    when(defn.intField("foo")).thenReturn(field);
    DynamicStructAccessor accessor = new DynamicStructAccessor(defn, storage, 42L);
    
    assertThat(accessor.getInteger("foo"), is(99));
  }

  @Test
  public void testSetInteger() {
    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);
    IntegerField field = mock(IntegerField.class);
    DynamicStructDefinition defn = mock(DynamicStructDefinition.class);
    when(defn.intField("foo")).thenReturn(field);
    DynamicStructAccessor accessor = new DynamicStructAccessor(defn, storage, 42L);

    accessor.setInteger("foo", 99);
    verify(field).write(storage, 42L, 99);
  }

  @Test
  public void testGetLong() {
    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);
    LongField field = mock(LongField.class);
    when(field.read(storage, 42L)).thenReturn(99L);
    DynamicStructDefinition defn = mock(DynamicStructDefinition.class);
    when(defn.longField("foo")).thenReturn(field);
    DynamicStructAccessor accessor = new DynamicStructAccessor(defn, storage, 42L);
    
    assertThat(accessor.getLong("foo"), is(99L));
  }

  @Test
  public void testSetLong() {
    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);
    LongField field = mock(LongField.class);
    DynamicStructDefinition defn = mock(DynamicStructDefinition.class);
    when(defn.longField("foo")).thenReturn(field);
    DynamicStructAccessor accessor = new DynamicStructAccessor(defn, storage, 42L);

    accessor.setLong("foo", 99L);
    verify(field).write(storage, 42L, 99L);
  }

  @Test
  public void testGetDouble() {
    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);
    DoubleField field = mock(DoubleField.class);
    when(field.read(storage, 42L)).thenReturn(Math.PI);
    DynamicStructDefinition defn = mock(DynamicStructDefinition.class);
    when(defn.doubleField("foo")).thenReturn(field);
    DynamicStructAccessor accessor = new DynamicStructAccessor(defn, storage, 42L);
    
    assertThat(accessor.getDouble("foo"), is(Math.PI));
  }

  @Test
  public void testSetDouble() {
    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);
    DoubleField field = mock(DoubleField.class);
    DynamicStructDefinition defn = mock(DynamicStructDefinition.class);
    when(defn.doubleField("foo")).thenReturn(field);
    DynamicStructAccessor accessor = new DynamicStructAccessor(defn, storage, 42L);

    accessor.setDouble("foo", Math.PI);
    verify(field).write(storage, 42L, Math.PI);
  }

  @Test
  public void testGetString() {
    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);
    StringField field = mock(StringField.class);
    when(field.read(storage, 42L)).thenReturn("Hello");
    DynamicStructDefinition defn = mock(DynamicStructDefinition.class);
    when(defn.stringField("foo")).thenReturn(field);
    DynamicStructAccessor accessor = new DynamicStructAccessor(defn, storage, 42L);
    
    assertThat(accessor.getString("foo").toString(), is("Hello"));
  }

  @Test
  public void testSetString() {
    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);
    StringField field = mock(StringField.class);
    DynamicStructDefinition defn = mock(DynamicStructDefinition.class);
    when(defn.stringField("foo")).thenReturn(field);
    DynamicStructAccessor accessor = new DynamicStructAccessor(defn, storage, 42L);

    accessor.setString("foo", "Hello");
    verify(field).write(storage, 42L, "Hello");
  }

  @Test
  public void testGetBytes() {
    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);
    ByteBuffer data = ByteBuffer.allocate(42);
    BytesField field = mock(BytesField.class);
    when(field.read(storage, 42L)).thenReturn(data);
    DynamicStructDefinition defn = mock(DynamicStructDefinition.class);
    when(defn.bytesField("foo")).thenReturn(field);
    DynamicStructAccessor accessor = new DynamicStructAccessor(defn, storage, 42L);
    
    assertThat(accessor.getBytes("foo"), is(data));
  }

  @Test
  public void testSetBytes() {
    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);
    BytesField field = mock(BytesField.class);
    DynamicStructDefinition defn = mock(DynamicStructDefinition.class);
    when(defn.bytesField("foo")).thenReturn(field);
    DynamicStructAccessor accessor = new DynamicStructAccessor(defn, storage, 42L);

    ByteBuffer data = ByteBuffer.allocate(42);
    accessor.setBytes("foo", data);
    verify(field).write(storage, 42L, data);
  }
}
