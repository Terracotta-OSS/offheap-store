/*
 * All content copyright (c) 2010 Terracotta, Inc., except as may otherwise be noted in a separate copyright
 * notice. All rights reserved.
 */
package com.terracottatech.offheapstore.util;

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
      try {
        T result = (T) s.getMethod(name).invoke(OS_BEAN);
        LOGGER.trace("Bean lookup successful using {}, got {}", s, result);
        return result;
      } catch (SecurityException e) {
        LOGGER.trace("Bean lookup failed on {}", s, e);
      } catch (NoSuchMethodException e) {
        LOGGER.trace("Bean lookup failed on {}", s, e);
      } catch (IllegalAccessException e) {
        LOGGER.trace("Bean lookup failed on {}", s, e);
      } catch (IllegalArgumentException e) {
        LOGGER.trace("Bean lookup failed on {}", s, e);
      } catch (InvocationTargetException e) {
        LOGGER.trace("Bean lookup failed on {}", s, e);
      }
    }
    for (Class<?> i : OS_BEAN.getClass().getInterfaces()) {
      try {
        T result = (T) i.getMethod(name).invoke(OS_BEAN);
        LOGGER.trace("Bean lookup successful using {}, got {}", i, result);
        return result;
      } catch (SecurityException e) {
        LOGGER.trace("Bean lookup failed on {}", i, e);
      } catch (NoSuchMethodException e) {
        LOGGER.trace("Bean lookup failed on {}", i, e);
      } catch (IllegalAccessException e) {
        LOGGER.trace("Bean lookup failed on {}", i, e);
      } catch (IllegalArgumentException e) {
        LOGGER.trace("Bean lookup failed on {}", i, e);
      } catch (InvocationTargetException e) {
        LOGGER.trace("Bean lookup failed on {}", i, e);
      }
    }
    LOGGER.trace("Returning null for {}", name);
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
