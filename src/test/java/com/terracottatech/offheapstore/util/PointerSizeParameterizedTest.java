/*
 * Copyright 2014-2023 Terracotta, Inc., a Software AG company.
 * Copyright IBM Corp. 2024, 2025
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
