/*
 * Copyright 2015-2023 Terracotta, Inc., a Software AG company.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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
package org.terracotta.offheapstore.util;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.InvocationTargetException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author cdennis
 */
public class PhysicalMemory {

  private static final Logger LOGGER = LoggerFactory.getLogger(PhysicalMemory.class);
  private static final OperatingSystemMXBean OS_BEAN = ManagementFactory.getOperatingSystemMXBean();

  public static Long totalPhysicalMemory() {
    return getAttribute("getTotalPhysicalMemorySize");
  }

  public static Long freePhysicalMemory() {
    return getAttribute("getFreePhysicalMemorySize");
  }

  public static Long totalSwapSpace() {
    return getAttribute("getTotalSwapSpaceSize");
  }

  public static Long freeSwapSpace() {
    return getAttribute("getFreeSwapSpaceSize");
  }

  public static Long ourCommittedVirtualMemory() {
    return getAttribute("getCommittedVirtualMemorySize");
  }

  private static <T> T getAttribute(String name) {
    LOGGER.trace("Bean lookup for {}", name);
    for (Class<?> s = OS_BEAN.getClass(); s != null; s = s.getSuperclass()) {
      @SuppressWarnings("unchecked")
      T result = invokeyMethod(name, s);
      if (result != null) {
        return result;
      }
    }
    for (Class<?> i : OS_BEAN.getClass().getInterfaces()) {
      @SuppressWarnings("unchecked")
      T result = invokeyMethod(name, i);
      return result;
    }
    LOGGER.trace("Returning null for {}", name);
    return null;
  }

  private static <T> T invokeyMethod(String name, Class<?> s) {
    try {
      @SuppressWarnings("unchecked")
      T result = (T) s.getMethod(name).invoke(OS_BEAN);
      LOGGER.trace("Bean lookup successful using {}, got {}", s, result);
      return result;
    } catch (SecurityException | InvocationTargetException | IllegalArgumentException | IllegalAccessException | NoSuchMethodException e) {
      LOGGER.trace("Bean lookup failed on {}", s, e);
    }
    return null;
  }

  public static void main(String[] args) {
    System.out.println("Total Physical Memory: " + DebuggingUtils.toBase2SuffixedString(totalPhysicalMemory()) + "B");
    System.out.println("Free Physical Memory: " + DebuggingUtils.toBase2SuffixedString(freePhysicalMemory()) + "B");
    System.out.println("Total Swap Space: " + DebuggingUtils.toBase2SuffixedString(totalSwapSpace()) + "B");
    System.out.println("Free Swap Space: " + DebuggingUtils.toBase2SuffixedString(freeSwapSpace()) + "B");
    System.out.println("Committed Virtual Memory: " + DebuggingUtils.toBase2SuffixedString(ourCommittedVirtualMemory()) + "B");
  }
}
