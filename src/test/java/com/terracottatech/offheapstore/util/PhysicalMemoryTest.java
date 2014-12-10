/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.offheapstore.util;

import java.lang.management.OperatingSystemMXBean;
import java.security.Permission;

import org.junit.Assert;
import org.junit.Test;

import static org.hamcrest.core.AnyOf.anyOf;
import static org.hamcrest.core.IsNull.nullValue;
import static org.hamcrest.number.OrderingComparison.greaterThanOrEqualTo;
import static org.hamcrest.number.OrderingComparison.lessThanOrEqualTo;

/**
 *
 * @author cdennis
 */
public class PhysicalMemoryTest {
  
  @Test
  public void testMemoryInvariants() {
    Assert.assertThat(PhysicalMemory.freePhysicalMemory(), anyOf(nullValue(Long.class), lessThanOrEqualTo(PhysicalMemory.totalPhysicalMemory())));
    Assert.assertThat(PhysicalMemory.freeSwapSpace(), anyOf(nullValue(Long.class), lessThanOrEqualTo(PhysicalMemory.totalSwapSpace())));
    Assert.assertThat(PhysicalMemory.ourCommittedVirtualMemory(), anyOf(nullValue(Long.class), greaterThanOrEqualTo(0L)));
  }
  
  @Test
  public void testBehaviorWithSecurityManager() {
    final Thread testThread = Thread.currentThread();
    System.setSecurityManager(new SecurityManager() {

      @Override
      public void checkMemberAccess(Class<?> clazz, int which) {
        if (Thread.currentThread() == testThread && OperatingSystemMXBean.class.isAssignableFrom(clazz)) {
          throw new SecurityException();
        }
      }

      @Override
      public void checkPackageAccess(String pkg) {
        if (Thread.currentThread() == testThread && pkg.startsWith("com.sun.")) {
          throw new SecurityException();
        }
      }

      public void checkPermission(Permission perm) {
      }
    });
    try {
      Assert.assertThat(PhysicalMemory.totalPhysicalMemory(), nullValue());
      Assert.assertThat(PhysicalMemory.freePhysicalMemory(), nullValue());
      Assert.assertThat(PhysicalMemory.totalSwapSpace(), nullValue());
      Assert.assertThat(PhysicalMemory.freeSwapSpace(), nullValue());
      Assert.assertThat(PhysicalMemory.ourCommittedVirtualMemory(), nullValue());
    } finally {
      System.setSecurityManager(null);
    }
  }
}
