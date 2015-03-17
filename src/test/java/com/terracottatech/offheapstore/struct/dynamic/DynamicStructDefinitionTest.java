/*
 * All content copyright (c) Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */

package com.terracottatech.offheapstore.struct.dynamic;


import java.nio.ByteBuffer;

import org.terracotta.offheapstore.paging.OffHeapStorageArea;
import com.terracottatech.offheapstore.struct.Pointer;
import com.terracottatech.offheapstore.struct.StructAccessor;
import com.terracottatech.offheapstore.struct.StructDefinition;

import org.junit.Test;

import static com.terracottatech.offheapstore.struct.StructType.BYTES;
import static com.terracottatech.offheapstore.struct.StructType.INTEGER;
import static com.terracottatech.offheapstore.struct.StructType.STRING;
import static java.util.Collections.singletonMap;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 *
 * @author cdennis
 */
public class DynamicStructDefinitionTest {
  
  @Test
  public void testAllocationFailureReturnsNull() {
    StructDefinition definition = new DynamicStructDefinition(singletonMap("foo", INTEGER));
    
    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);
    when(storage.allocate(anyLong())).thenReturn(-1L);

    assertThat(definition.allocate(storage), nullValue());
  }
  
  @Test
  public void testAllocationWhenSuccessfullReturns() {
    StructDefinition definition = new DynamicStructDefinition(singletonMap("foo", INTEGER));
    
    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);
    when(storage.allocate(anyLong())).thenReturn(42L);

    assertThat(definition.allocate(storage), notNullValue());
  }
  
  @Test
  public void testPointerFreeDelegatesCorrectly() {
    StructDefinition definition = new DynamicStructDefinition(singletonMap("foo", INTEGER));
    
    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);
    when(storage.allocate(anyLong())).thenReturn(42L);

    Pointer<?> p = definition.allocate(storage);
    
    p.free();
    verify(storage, times(1)).free(42L);
  }
  
  @Test
  public void testPointerFreeCascadesForStringCorrectly() {
    StructDefinition definition = new DynamicStructDefinition(singletonMap("foo", STRING));
    
    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);
    when(storage.allocate(anyLong())).thenReturn(42L, 54L);
    when(storage.readLong(42L)).thenReturn(-1L, 54L);
    
    Pointer<StructAccessor> p = definition.allocate(storage);
    p.access().setString("foo", "Zaphod");
    
    p.free();
    verify(storage, times(1)).free(54L);
    verify(storage, times(1)).free(42L);
  }

  @Test
  public void testPointerFreeCascadesForBytesCorrectly() {
    StructDefinition definition = new DynamicStructDefinition(singletonMap("foo", BYTES));
    
    OffHeapStorageArea storage = mock(OffHeapStorageArea.class);
    when(storage.allocate(anyLong())).thenReturn(42L, 54L);
    when(storage.readLong(42L)).thenReturn(-1L, 54L);
    
    Pointer<StructAccessor> p = definition.allocate(storage);
    p.access().setBytes("foo", ByteBuffer.allocate(12));

    p.free();
    verify(storage, times(1)).free(54L);
    verify(storage, times(1)).free(42L);
  }
}
