/*
 * All content copyright (c) Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */

package com.terracottatech.offheapstore.struct.statics;

import java.nio.ByteBuffer;
import java.util.Date;

import org.terracotta.offheapstore.paging.OffHeapStorageArea;
import com.terracottatech.offheapstore.struct.Pointer;
import com.terracottatech.offheapstore.struct.StructAccessor;
import com.terracottatech.offheapstore.struct.StructBuilder;
import com.terracottatech.offheapstore.struct.StructDefinition;

import org.junit.Test;

import static com.terracottatech.offheapstore.struct.StructType.BYTES;
import static com.terracottatech.offheapstore.struct.StructType.INTEGER;
import static com.terracottatech.offheapstore.struct.StructType.STRING;
import static java.util.Collections.singletonMap;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 *
 * @author cdennis
 */
public class StaticStructDefinitionTest {
  
  @Test(expected = IllegalArgumentException.class)
  public void testStatefulClassIsInvalidTemplate() {
    StructBuilder.newStaticStruct(StatefulTemplate.class);
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void testMissingReader() {
    StructBuilder.newStaticStruct(MissingReader.class);
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void testMissingWriter() {
    StructBuilder.newStaticStruct(MissingWriter.class);
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void testConcreteReader() {
    StructBuilder.newStaticStruct(ConcreteReader.class);
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void testConcreteWriter() {
    StructBuilder.newStaticStruct(ConcreteWriter.class);
  }
  
  @Test
  public void testAbsentReader() {
    StructDefinition<?> defn = StructBuilder.newStaticStruct(AbsentReader.class);
    assertThat(defn.structure().keySet(), empty());
  }
  
  @Test
  public void testAbsentWriter() {
    StructDefinition<?> defn = StructBuilder.newStaticStruct(AbsentWriter.class);
    assertThat(defn.structure().keySet(), empty());
  }

  @Test
  public void testConcretePair() {
    StructDefinition<?> defn = StructBuilder.newStaticStruct(ConcretePair.class);
    assertThat(defn.structure().keySet(), empty());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIllegalPropertyType() {
    StructBuilder.newStaticStruct(IllegalPropertyType.class);
  }
  
  @Test
  public void testAllocationFailureReturnsNull() {
    StructDefinition<FooInteger> definition = new StaticStructDefinition(FooInteger.class);
    
    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);
    when(storage.allocate(anyLong())).thenReturn(-1L);

    assertThat(definition.allocate(storage), nullValue());
  }
  
  @Test
  public void testAllocationWhenSuccessfullReturns() {
    StructDefinition<FooInteger> definition = new StaticStructDefinition(FooInteger.class);
    
    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);
    when(storage.allocate(anyLong())).thenReturn(42L);

    assertThat(definition.allocate(storage), notNullValue());
  }
  
  @Test
  public void testBooleanFieldAccess() {
    StructDefinition<FooBoolean> definition = new StaticStructDefinition(FooBoolean.class);
    
    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);
    when(storage.allocate(anyLong())).thenReturn(42L);
    when(storage.readByte(42L)).thenReturn((byte) 1);
    
    FooBoolean bool = definition.allocate(storage).access();
    assertThat(bool.getFoo(), is(true));
    
    bool.setFoo(false);
    verify(storage).writeByte(42L, (byte) 0);
  }

  @Test
  public void testCharacterFieldAccess() {
    StructDefinition<FooCharacter> definition = new StaticStructDefinition(FooCharacter.class);
    
    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);
    when(storage.allocate(anyLong())).thenReturn(42L);
    when(storage.readShort(42L)).thenReturn((short) 'c');
    
    FooCharacter character = definition.allocate(storage).access();
    assertThat(character.getFoo(), is('c'));
    
    character.setFoo('d');
    verify(storage).writeShort(42L, (short) 'd');
  }

  @Test
  public void testIntegerFieldAccess() {
    StructDefinition<FooInteger> definition = new StaticStructDefinition(FooInteger.class);
    
    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);
    when(storage.allocate(anyLong())).thenReturn(42L);
    when(storage.readInt(42L)).thenReturn(99);
    
    FooInteger integer = definition.allocate(storage).access();
    assertThat(integer.getFoo(), is(99));
    
    integer.setFoo(98);
    verify(storage).writeInt(42L, 98);
  }
  
  @Test
  public void testLongFieldAccess() {
    StructDefinition<FooLong> definition = new StaticStructDefinition(FooLong.class);
    
    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);
    when(storage.allocate(anyLong())).thenReturn(42L);
    when(storage.readLong(42L)).thenReturn(99L);
    
    FooLong longInt = definition.allocate(storage).access();
    assertThat(longInt.getFoo(), is(99L));
    
    longInt.setFoo(-99L);
    verify(storage).writeLong(42L, -99L);
  }
  
  @Test
  public void testDoubleFieldAccess() {
    StructDefinition<FooDouble> definition = new StaticStructDefinition(FooDouble.class);
    
    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);
    when(storage.allocate(anyLong())).thenReturn(42L);
    when(storage.readLong(42L)).thenReturn(Double.doubleToLongBits(Math.PI));
    
    FooDouble floating = definition.allocate(storage).access();
    assertThat(floating.getFoo(), is(Math.PI));
    
    floating.setFoo(Math.E);
    verify(storage).writeLong(42L, Double.doubleToLongBits(Math.E));
  }
  
  @Test
  public void testPointerFreeDelegatesCorrectly() {
    StructDefinition<FooInteger> definition = new StaticStructDefinition(FooInteger.class);
    
    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);
    when(storage.allocate(anyLong())).thenReturn(42L);

    Pointer<?> p = definition.allocate(storage);
    
    p.free();
    verify(storage, times(1)).free(42L);
  }
  
  @Test
  public void testPointerFreeCascadesForStringCorrectly() {
    StructDefinition<FooString> definition = new StaticStructDefinition(FooString.class);
    
    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);
    when(storage.allocate(anyLong())).thenReturn(42L, 54L);
    when(storage.readLong(42L)).thenReturn(-1L, 54L);
    
    Pointer<FooString> p = definition.allocate(storage);
    p.access().setFoo("Zaphod");
    
    p.free();
    verify(storage, times(1)).free(54L);
    verify(storage, times(1)).free(42L);
  }

  @Test
  public void testPointerFreeCascadesForBytesCorrectly() {
    StructDefinition<FooBytes> definition = new StaticStructDefinition(FooBytes.class);
    
    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);
    when(storage.allocate(anyLong())).thenReturn(42L, 54L);
    when(storage.readLong(42L)).thenReturn(-1L, 54L);
    
    Pointer<FooBytes> p = definition.allocate(storage);
    p.access().setFoo(ByteBuffer.allocate(12));

    p.free();
    verify(storage, times(1)).free(54L);
    verify(storage, times(1)).free(42L);
  }
  
  static abstract class StatefulTemplate {
    
    private final int bar = 2;
    
    public abstract int getFoo();
    public abstract void setFoo(int foo);
  }

  interface MissingReader {
    
    void setFoo(int foo);
  }
  
  interface MissingWriter {

    int getFoo();
  }
  
  public static abstract class ConcreteReader {
    
    public int getFoo() {
      return 1;
    }
    
    public abstract void setFoo(int foo);
  }
  
  public static abstract class ConcreteWriter {
    
    public abstract int getFoo();
    public void setFoo(int foo) {
      //no-op
    }
  }
  
  public static abstract class AbsentReader {

    public void setFoo(int foo) {
      //no-op
    }
  }
  
  public static abstract class AbsentWriter {
    
    public int getFoo() {
      return 1;
    }
  }
  
  public static abstract class ConcretePair {
    
    public int getFoo() {
      return 1;
    }
    
    public void setFoo(int foo) {
      //no-op
    }
  }
  
  public interface FooBoolean {
    
    public boolean getFoo();
    public void setFoo(boolean foo);
  }

  public interface FooCharacter {
    
    public char getFoo();
    public void setFoo(char foo);
  }

  public interface FooInteger {
    
    public int getFoo();
    public void setFoo(int foo);
  }
  
  public interface FooLong {
    
    public long getFoo();
    public void setFoo(long foo);
  }
  
  public interface FooDouble {
    
    public double getFoo();
    public void setFoo(double foo);
  }
  
  public interface FooString {
    
    public CharSequence getFoo();
    public void setFoo(CharSequence foo);
  }
  
  public interface FooBytes {
    
    public ByteBuffer getFoo();
    public void setFoo(ByteBuffer foo);
  }
  
  public static abstract class IllegalPropertyType {
    
    public abstract Date getFoo();
    public abstract void setFoo(Date foo);
  }
}
