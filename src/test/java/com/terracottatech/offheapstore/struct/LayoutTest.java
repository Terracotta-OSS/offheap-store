/*
 * All content copyright (c) Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */

package com.terracottatech.offheapstore.struct;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import static java.util.Collections.singletonMap;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.junit.Assert.assertThat;

/**
 *
 * @author cdennis
 */
public class LayoutTest {
  
  @Test
  public void testEmptyLayout() {
    Layout<String> layout = Layout.layout(Collections.<String, StructType>emptyMap());
    assertThat(layout.length(), is(0));
    assertThat(layout.layout().keySet(), empty());
  }

  @Test
  public void testSingleFieldLayout() {
    for (StructType t : StructType.values()) {
      Layout<String> layout = Layout.layout(singletonMap("foo", t));
      assertThat(layout.length(), greaterThan(0));
      assertThat(layout.layout().keySet(), hasSize(1));
      assertThat(layout.layout().keySet(), hasItem("foo"));
      assertThat(layout.layout().get("foo").offset(), is(0));
      assertThat(layout.length(), is(layout.layout().get("foo").length()));
    }
  }
  
  @Test
  public void testTwoFieldOrdering() {
    for (StructType a : StructType.values()) {
      for (StructType b : StructType.values()) {
        Map<String, StructType> fields = new HashMap<String, StructType>();
        fields.put("a", a);
        fields.put("b", b);
        Layout<String> layout = Layout.layout(fields);
        
        Field aField = layout.layout().get("a");
        Field bField = layout.layout().get("b");

        /*
         * Fields are ordered by their type enum representation
         */
        if (a.ordinal() < b.ordinal()) {
          assertThat(aField.offset(), is(0));
          assertThat(bField.offset(), is(aField.length()));
          assertThat(layout.length(), is(bField.offset() + bField.length()));
        } else if (a.ordinal() > b.ordinal()) {
          assertThat(bField.offset(), is(0));
          assertThat(aField.offset(), is(bField.length()));
          assertThat(layout.length(), is(aField.offset() + aField.length()));
        } else {
          /* 
           * Identically typed fields are ordered by name
           */
          assertThat(aField.offset(), is(0));
          assertThat(bField.offset(), is(aField.length()));
          assertThat(layout.length(), is(bField.offset() + bField.length()));
        }
      }
    }
  }
}
