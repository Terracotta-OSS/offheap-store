/*
 * All content copyright (c) Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */

package com.terracottatech.offheapstore.struct;

import java.nio.ByteBuffer;

import com.terracottatech.offheapstore.paging.OffHeapStorageArea;
import com.terracottatech.offheapstore.struct.Field.BooleanField;
import com.terracottatech.offheapstore.struct.Field.BytesField;
import com.terracottatech.offheapstore.struct.Field.CharacterField;
import com.terracottatech.offheapstore.struct.Field.DoubleField;
import com.terracottatech.offheapstore.struct.Field.IntegerField;
import com.terracottatech.offheapstore.struct.Field.LongField;
import com.terracottatech.offheapstore.struct.Field.StringField;

import org.junit.Test;
import org.mockito.InOrder;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 *
 * @author cdennis
 */
public class FieldTest {
  
  @Test
  public void testOffsetCalculation() {
    Field f = new Field(0, 2) {};
    assertThat(f.address(1L), is(3L));
  }
  
  @Test
  public void testBooleanFieldRead() {
    BooleanField f = new BooleanField(1);

    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);
    when(storage.readByte(3L)).thenReturn((byte) 1);
    when(storage.readByte(4L)).thenReturn((byte) 0);
    when(storage.readByte(5L)).thenReturn((byte) -1);
    
    assertThat(f.read(storage, 2), is(true));
    assertThat(f.read(storage, 3), is(false));
    assertThat(f.read(storage, 4), is(true));
  }
  
  @Test
  public void testBooleanFieldWrite() {
    BooleanField f = new BooleanField(1);
    
    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);
    
    f.write(storage, 2, true);
    verify(storage, times(1)).writeByte(3L, (byte) 0xff);
    
    f.write(storage, 3, false);
    verify(storage, times(1)).writeByte(4L, (byte) 0);
  }

  @Test
  public void testCharacterFieldRead() {
    CharacterField f = new CharacterField(1);

    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);
    when(storage.readShort(3L)).thenReturn((short) 1);
    when(storage.readShort(4L)).thenReturn((short) ~0);
    
    assertThat(f.read(storage, 2), is((char) 1));
    assertThat(f.read(storage, 3), is(Character.MAX_VALUE));
  }
  
  @Test
  public void testCharacterFieldWrite() {
    CharacterField f = new CharacterField(1);
    
    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);
    
    f.write(storage, 2, (char) 1);
    verify(storage, times(1)).writeShort(3L, (short) 1);
    
    f.write(storage, 3, Character.MAX_VALUE);
    verify(storage, times(1)).writeShort(4L, (short) ~0);
  }

  @Test
  public void testIntegerFieldRead() {
    IntegerField f = new IntegerField(1);

    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);
    when(storage.readInt(3L)).thenReturn(1);
    when(storage.readInt(4L)).thenReturn(Integer.MAX_VALUE);
    when(storage.readInt(5L)).thenReturn(Integer.MIN_VALUE);
    
    assertThat(f.read(storage, 2), is(1));
    assertThat(f.read(storage, 3), is(Integer.MAX_VALUE));
    assertThat(f.read(storage, 4), is(Integer.MIN_VALUE));
  }
  
  @Test
  public void testIntegerFieldWrite() {
    IntegerField f = new IntegerField(1);
    
    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);
    
    f.write(storage, 2, 1);
    verify(storage, times(1)).writeInt(3L, 1);
    
    f.write(storage, 3, Integer.MAX_VALUE);
    verify(storage, times(1)).writeInt(4L, Integer.MAX_VALUE);
    
    f.write(storage, 4, Integer.MIN_VALUE);
    verify(storage, times(1)).writeInt(5L, Integer.MIN_VALUE);
  }

  @Test
  public void testLongFieldRead() {
    LongField f = new LongField(1);

    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);
    when(storage.readLong(3L)).thenReturn(1L);
    when(storage.readLong(4L)).thenReturn(Long.MAX_VALUE);
    when(storage.readLong(5L)).thenReturn(Long.MIN_VALUE);
    
    assertThat(f.read(storage, 2), is(1L));
    assertThat(f.read(storage, 3), is(Long.MAX_VALUE));
    assertThat(f.read(storage, 4), is(Long.MIN_VALUE));
  }
  
  @Test
  public void testLongFieldWrite() {
    LongField f = new LongField(1);
    
    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);
    
    f.write(storage, 2, 1L);
    verify(storage, times(1)).writeLong(3L, 1L);
    
    f.write(storage, 3, Long.MAX_VALUE);
    verify(storage, times(1)).writeLong(4L, Long.MAX_VALUE);
    
    f.write(storage, 4, Long.MIN_VALUE);
    verify(storage, times(1)).writeLong(5L, Long.MIN_VALUE);
  }

  @Test
  public void testDoubleFieldRead() {
    DoubleField f = new DoubleField(1);

    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);
    when(storage.readLong(3L)).thenReturn(Double.doubleToRawLongBits(-0.0));
    when(storage.readLong(4L)).thenReturn(Double.doubleToRawLongBits(0.0));
    when(storage.readLong(5L)).thenReturn(Double.doubleToRawLongBits(Double.NaN));
    when(storage.readLong(6L)).thenReturn(Double.doubleToRawLongBits(Double.MIN_VALUE));
    when(storage.readLong(7L)).thenReturn(Double.doubleToRawLongBits(Double.MAX_VALUE));
    when(storage.readLong(8L)).thenReturn(Double.doubleToRawLongBits(Double.NEGATIVE_INFINITY));
    when(storage.readLong(9L)).thenReturn(Double.doubleToRawLongBits(Double.POSITIVE_INFINITY));
    
    assertThat(f.read(storage, 2), is(-0.0));
    assertThat(f.read(storage, 3), is(0.0));
    assertThat(f.read(storage, 4), is(Double.NaN));
    assertThat(f.read(storage, 5), is(Double.MIN_VALUE));
    assertThat(f.read(storage, 6), is(Double.MAX_VALUE));
    assertThat(f.read(storage, 7), is(Double.NEGATIVE_INFINITY));
    assertThat(f.read(storage, 8), is(Double.POSITIVE_INFINITY));
  }
  
  @Test
  public void testDoubleFieldWrite() {
    DoubleField f = new DoubleField(1);
    
    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);
    
    f.write(storage, 2, -0.0);
    verify(storage, times(1)).writeLong(3L, Double.doubleToRawLongBits(-0.0));
    
    f.write(storage, 2, 0.0);
    verify(storage, times(1)).writeLong(3L, Double.doubleToRawLongBits(0.0));

    f.write(storage, 2, Double.NaN);
    verify(storage, times(1)).writeLong(3L, Double.doubleToRawLongBits(Double.NaN));

    f.write(storage, 2, Double.MIN_VALUE);
    verify(storage, times(1)).writeLong(3L, Double.doubleToRawLongBits(Double.MIN_VALUE));

    f.write(storage, 2, Double.MAX_VALUE);
    verify(storage, times(1)).writeLong(3L, Double.doubleToRawLongBits(Double.MAX_VALUE));

    f.write(storage, 2, Double.NEGATIVE_INFINITY);
    verify(storage, times(1)).writeLong(3L, Double.doubleToRawLongBits(Double.NEGATIVE_INFINITY));

    f.write(storage, 2, Double.POSITIVE_INFINITY);
    verify(storage, times(1)).writeLong(3L, Double.doubleToRawLongBits(Double.POSITIVE_INFINITY));
  }

  @Test
  public void testStringFieldInitialize() {
    StringField f = new StringField(1);
    
    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);
    
    f.initialize(storage, 2);
    verify(storage).writeLong(3, -1L);
  }

  @Test
  public void testStringFieldFreeEmpty() {
    StringField f = new StringField(1);
    
    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);
    when(storage.readLong(3L)).thenReturn(-1L);
    
    f.free(storage, 2);
    verify(storage, never()).free(anyLong());
  }

  @Test
  public void testStringFieldFreeOccupied() {
    StringField f = new StringField(1);
    
    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);
    when(storage.readLong(3L)).thenReturn(42L);
    
    f.free(storage, 2);
    verify(storage, times(1)).free(42L);
  }
  
  @Test
  public void testStringFieldRead() {
    StringField f = new StringField(1);

    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);
    when(storage.readLong(3L)).thenReturn(-1L);
    
    when(storage.readLong(4L)).thenReturn(10L);
    when(storage.readInt(10L)).thenReturn(0);
    when(storage.readBuffer(14L, 0)).thenReturn(ByteBuffer.allocate(0));
    
    when(storage.readLong(5L)).thenReturn(15L);
    when(storage.readInt(15L)).thenReturn(10);
    ByteBuffer boingBytes = ByteBuffer.allocate(10);
    boingBytes.asCharBuffer().append("BOING");
    when(storage.readBuffer(19L, 10)).thenReturn(boingBytes);
    
    assertThat(f.read(storage, 2), nullValue());
    assertThat(f.read(storage, 3).toString(), is(""));
    assertThat(f.read(storage, 4).toString(), is("BOING"));
  }
  
  @Test
  public void testStringFieldWrite() {
    StringField f = new StringField(1);
    
    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);

    when(storage.readLong(3L)).thenReturn(42L);
    f.write(storage, 2, null);
    InOrder o1 = inOrder(storage);
    o1.verify(storage, times(1)).writeLong(3L, -1L);
    o1.verify(storage, times(1)).free(42L);
    
    when(storage.readLong(4L)).thenReturn(42L);
    when(storage.allocate(14)).thenReturn(101L);
    f.write(storage, 3, "BOING");
    InOrder o2 = inOrder(storage);
    o2.verify(storage, times(1)).writeInt(101L, 10);
    ByteBuffer boingBytes = ByteBuffer.allocate(10);
    boingBytes.asCharBuffer().append("BOING");
    o2.verify(storage, times(1)).writeBuffer(105L, boingBytes);
    o2.verify(storage, times(1)).writeLong(4L, 101L);
    o2.verify(storage, times(1)).free(42L);
  }

  @Test
  public void testBytesFieldInitialize() {
    BytesField f = new BytesField(1);
    
    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);
    
    f.initialize(storage, 2);
    verify(storage).writeLong(3, -1L);
  }

  @Test
  public void testBytesFieldFreeEmpty() {
    BytesField f = new BytesField(1);
    
    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);
    when(storage.readLong(3L)).thenReturn(-1L);
    
    f.free(storage, 2);
    verify(storage, never()).free(anyLong());
  }

  @Test
  public void testBytesFieldFreeOccupied() {
    BytesField f = new BytesField(1);
    
    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);
    when(storage.readLong(3L)).thenReturn(42L);
    
    f.free(storage, 2);
    verify(storage, times(1)).free(42L);
  }

  @Test
  public void testBytesFieldRead() {
    BytesField f = new BytesField(1);

    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);
    when(storage.readLong(3L)).thenReturn(-1L);
    
    when(storage.readLong(4L)).thenReturn(10L);
    when(storage.readInt(10L)).thenReturn(0);
    when(storage.readBuffer(14L, 0)).thenReturn(ByteBuffer.allocate(0));
    
    when(storage.readLong(5L)).thenReturn(15L);
    when(storage.readInt(15L)).thenReturn(10);
    ByteBuffer boingBytes = ByteBuffer.allocate(10);
    boingBytes.asCharBuffer().append("BOING");
    when(storage.readBuffer(19L, 10)).thenReturn(boingBytes);
    
    assertThat(f.read(storage, 2), nullValue());
    assertThat(f.read(storage, 3).capacity(), is(0));
    assertThat(f.read(storage, 4).asCharBuffer().toString(), is("BOING"));
  }
  
  @Test
  public void testBytesFieldWrite() {
    BytesField f = new BytesField(1);
    
    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);

    when(storage.readLong(3L)).thenReturn(42L);
    f.write(storage, 2, null);
    InOrder o1 = inOrder(storage);
    o1.verify(storage, times(1)).writeLong(3L, -1L);
    o1.verify(storage, times(1)).free(42L);
    
    when(storage.readLong(4L)).thenReturn(42L);
    when(storage.allocate(14)).thenReturn(101L);
    ByteBuffer boingBytes = ByteBuffer.allocate(10);
    boingBytes.asCharBuffer().append("BOING");
    f.write(storage, 3, boingBytes.duplicate());
    InOrder o2 = inOrder(storage);
    o2.verify(storage, times(1)).writeInt(101L, 10);
    o2.verify(storage, times(1)).writeBuffer(105L, boingBytes);
    o2.verify(storage, times(1)).writeLong(4L, 101L);
    o2.verify(storage, times(1)).free(42L);
  }
}
