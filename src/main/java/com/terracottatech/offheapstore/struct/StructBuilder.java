/*
 * All content copyright (c) Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */

package com.terracottatech.offheapstore.struct;

import com.terracottatech.offheapstore.struct.statics.StaticStructDefinition;
import com.terracottatech.offheapstore.struct.dynamic.DynamicStructBuilder;

/**
 *
 * @author cdennis
 */
public abstract class StructBuilder {
  
  public static DynamicStructBuilder newDynamicStruct() {
    return new DynamicStructBuilder();
  }
  
  public static <T> StructDefinition<T> newStaticStruct(Class<T> template) {
    return new StaticStructDefinition(template);
  }
}
