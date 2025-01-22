/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.offheapstore.util;

import org.terracotta.offheapstore.storage.PointerSize;

import java.util.ArrayList;
import java.util.Collection;

import org.junit.runner.RunWith;

/**
 *
 * @author cdennis
 */
@RunWith(ParallelParameterized.class)
public abstract class PointerSizeParameterizedTest {
  
  @ParallelParameterized.Parameters(name = "pointer-size={0}")
  public static Collection<Object[]> data() {
    Collection<Object[]> widths = new ArrayList<Object[]>();
    for (PointerSize width : PointerSize.values()) {
      widths.add(new Object[] {width});
    }
    return widths;
  }

  @ParallelParameterized.Parameter(0)
  public volatile PointerSize pointerSize;
  
  protected PointerSize getPointerSize() {
    return pointerSize;
  }
}
