/*
 * All content copyright (c) Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */

package com.terracottatech.offheapstore.struct.dynamic;

import com.terracottatech.offheapstore.struct.dynamic.DynamicStructBuilder;

import org.junit.Test;

import static com.terracottatech.offheapstore.struct.StructBuilder.newDynamicStruct;
import static com.terracottatech.offheapstore.struct.StructType.BOOLEAN;
import static com.terracottatech.offheapstore.struct.StructType.BYTES;
import static com.terracottatech.offheapstore.struct.StructType.CHARACTER;
import static com.terracottatech.offheapstore.struct.StructType.DOUBLE;
import static com.terracottatech.offheapstore.struct.StructType.INTEGER;
import static com.terracottatech.offheapstore.struct.StructType.LONG;
import static com.terracottatech.offheapstore.struct.StructType.STRING;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 *
 * @author cdennis
 */
public class DynamicStructBuilderTest {
  
  @Test
  public void testDuplicateFieldDefinition() {
    DynamicStructBuilder builder = newDynamicStruct().integer("foo");
    try {
      builder.longint("foo");
      fail();
    } catch (IllegalStateException e) {
      //expected
    }
  }
  
  @Test
  public void testAddingBooleanField() {
    DynamicStructBuilder builder = newDynamicStruct().bool("foo");
    assertThat(builder.build().structure(), hasEntry("foo", BOOLEAN));
  }
 
  @Test
  public void testAddingCharacterField() {
    DynamicStructBuilder builder = newDynamicStruct().character("foo");
    assertThat(builder.build().structure(), hasEntry("foo", CHARACTER));
  }

  @Test
  public void testAddingIntegerField() {
    DynamicStructBuilder builder = newDynamicStruct().integer("foo");
    assertThat(builder.build().structure(), hasEntry("foo", INTEGER));
  }

  @Test
  public void testAddingLongIntegerField() {
    DynamicStructBuilder builder = newDynamicStruct().longint("foo");
    assertThat(builder.build().structure(), hasEntry("foo", LONG));
  }

  @Test
  public void testAddingDoubleField() {
    DynamicStructBuilder builder = newDynamicStruct().doublefloat("foo");
    assertThat(builder.build().structure(), hasEntry("foo", DOUBLE));
  }
  
  @Test
  public void testAddingStringField() {
    DynamicStructBuilder builder = newDynamicStruct().string("foo");
    assertThat(builder.build().structure(), hasEntry("foo", STRING));
  }

  @Test
  public void testAddingBytesField() {
    DynamicStructBuilder builder = newDynamicStruct().bytes("foo");
    assertThat(builder.build().structure(), hasEntry("foo", BYTES));
  }
}
