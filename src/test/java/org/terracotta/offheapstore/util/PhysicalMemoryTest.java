/*
 * Copyright 2015-2023 Terracotta, Inc., a Software AG company.
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
package org.terracotta.offheapstore.util;

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

  private static final RuntimePermission CHECK_MEMBER_ACCESS_PERMISSION = new RuntimePermission("accessDeclaredMembers");

  @Test
  public void testMemoryInvariants() {
    Assert.assertThat(PhysicalMemory.freePhysicalMemory(), anyOf(nullValue(Long.class), lessThanOrEqualTo(PhysicalMemory.totalPhysicalMemory())));
    Assert.assertThat(PhysicalMemory.freeSwapSpace(), anyOf(nullValue(Long.class), lessThanOrEqualTo(PhysicalMemory.totalSwapSpace())));
    Assert.assertThat(PhysicalMemory.ourCommittedVirtualMemory(), anyOf(nullValue(Long.class), greaterThanOrEqualTo(0L)));
  }

  @Test
  public void testBehaviorWithSecurityManager() {
    Thread testThread = Thread.currentThread();
    System.setSecurityManager(new SecurityManager() {
      @Override
      public void checkPackageAccess(String pkg) {
        if (Thread.currentThread() == testThread && pkg.startsWith("com.sun.")) {
          throw new SecurityException();
        }
      }

      @Override
      public void checkPermission(Permission perm) {
        // To be allowed to set the security manager back to null at the end of the test
        if (perm.getName().equals("setSecurityManager")) {
          return;
        }
        // PhysicalMemory does reflection on OperatingSystemMXBean. It can fails it the user has a SecurityManager configured
        // We make sure that in this case, we correctly map those failures to a null return. This is what this test is testing
        if (Thread.currentThread() == testThread && CHECK_MEMBER_ACCESS_PERMISSION.equals(perm)) {
          throw new SecurityException();
        }
        super.checkPermission(perm);
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
