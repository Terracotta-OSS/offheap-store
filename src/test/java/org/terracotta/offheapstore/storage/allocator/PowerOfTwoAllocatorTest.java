/*
 * Copyright 2015-2023 Terracotta, Inc., a Software AG company.
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
package org.terracotta.offheapstore.storage.allocator;

import org.terracotta.offheapstore.storage.allocator.PowerOfTwoAllocator;
import org.terracotta.offheapstore.storage.allocator.PowerOfTwoAllocator.Packing;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import static org.terracotta.offheapstore.storage.allocator.PowerOfTwoAllocator.Packing.FLOOR;
import static org.terracotta.offheapstore.storage.allocator.PowerOfTwoAllocator.Packing.CEILING;

/**
 *
 * @author Chris Dennis
 */
public class PowerOfTwoAllocatorTest {

  @Test
  public void testUniformSizedAllocationsFromLeft() {
    PowerOfTwoAllocator test = new PowerOfTwoAllocator(100);

    for (int i = 0; i < 100; i++) {
      Assert.assertEquals(i, test.allocate(1, FLOOR));
    }

    Assert.assertEquals(100, test.occupied());
  }

  @Test
  public void testUniformSizedFreesFromLeft() {
    PowerOfTwoAllocator test = new PowerOfTwoAllocator(100);

    List<Integer> allocated = new ArrayList<>();

    for (int i = 0; i < 100; i++) {
      allocated.add(test.allocate(1, FLOOR));
    }

    Random rndm = new Random();
    for (int i = 0; i < 100; i++) {
      test.free(allocated.remove(rndm.nextInt(allocated.size())), 1);
    }
    Assert.assertEquals(0, test.occupied());
  }

  @Test
  public void testUniformRepeatedAllocFreeFromLeft() {
    PowerOfTwoAllocator test = new PowerOfTwoAllocator(16 * 1024);

    for (int i = 1; i < 1024; i <<= 1) {
      List<Integer> pointers = new ArrayList<>();
      for (int k = 0; k < 16; k++) {
        int p = test.allocate(i, FLOOR);
        pointers.add(p);
      }
      for (Integer p : pointers) {
        test.free(p, i);
      }
      Assert.assertEquals("Testing regions of size " + i, 0, test.occupied());
    }
  }

  @Test
  public void testUniformSizedAllocationsFromRight() {
    PowerOfTwoAllocator test = new PowerOfTwoAllocator(100);

    for (int i = 0; i < 100; i++) {
      Assert.assertEquals(99 - i, test.allocate(1, CEILING));
    }

    Assert.assertEquals(100, test.occupied());
  }

  @Test
  public void testUniformSizedFreesFromRight() {
    PowerOfTwoAllocator test = new PowerOfTwoAllocator(100);

    List<Integer> allocated = new ArrayList<>();

    for (int i = 0; i < 100; i++) {
      allocated.add(test.allocate(1, CEILING));
    }

    Random rndm = new Random();
    for (int i = 0; i < 100; i++) {
      test.free(allocated.remove(rndm.nextInt(allocated.size())), 1);
    }
    Assert.assertEquals(0, test.occupied());
  }

  @Test
  public void testUniformRepeatedAllocFreeFromRight() {
    PowerOfTwoAllocator test = new PowerOfTwoAllocator(16 * 1024);

    for (int i = 1; i < 1024; i <<= 1) {
      List<Integer> pointers = new ArrayList<>();
      for (int k = 0; k < 16; k++) {
        int p = test.allocate(i, CEILING);
        pointers.add(p);
      }
      for (Integer p : pointers) {
        test.free(p, i);
      }
      Assert.assertEquals("Testing regions of size " + i, 0, test.occupied());
    }
  }

  @Test
  public void testRandomAllocFree() {
    PowerOfTwoAllocator test = new PowerOfTwoAllocator(100 * 1024 * 1024);

    List<AllocatedRegion> allocated = new ArrayList<>();
    Random rndm = new Random();

    for (int i = 0; i < 1000; i++) {
      if (rndm.nextBoolean()) {
        int size = rndm.nextInt(10);
        int p = test.allocate(1 << size, rndm.nextBoolean() ? FLOOR : CEILING);
        if (p >= 0) {
          allocated.add(new AllocatedRegion(p, 1 << size));
        }
      } else {
        if (!allocated.isEmpty()) {
          AllocatedRegion r = allocated.remove(rndm.nextInt(allocated.size()));
          test.free(r.address, r.size);
        }
      }
    }
  }

  @Test
  public void testUniformSizedClaims() {
    PowerOfTwoAllocator test = new PowerOfTwoAllocator(100);

    for (int i = 0; i < 100; i++) {
      test.claim(i, 1);
    }

    Assert.assertEquals(100, test.occupied());
  }

  @Test
  public void testRandomFindsAndClaims() {
    PowerOfTwoAllocator test = new PowerOfTwoAllocator(100 * 1024 * 1024);
    PowerOfTwoAllocator reference = new PowerOfTwoAllocator(100 * 1024 * 1024);

    List<AllocatedRegion> allocated = new ArrayList<>();
    Random rndm = new Random();

    for (int i = 0; i < 1000; i++) {
      if (rndm.nextBoolean()) {
        int size = rndm.nextInt(10);
        Packing packing = rndm.nextBoolean() ? FLOOR : CEILING;
        int p = reference.allocate(1 << size, packing);
        Assert.assertEquals(p, test.find(1 << size, packing));
        test.claim(p, 1 << size);
        if (p >= 0) {
          allocated.add(new AllocatedRegion(p, 1 << size));
        }
      } else {
        if (!allocated.isEmpty()) {
          AllocatedRegion r = allocated.remove(rndm.nextInt(allocated.size()));
          reference.free(r.address, r.size);
          test.free(r.address, r.size);
        }
      }
    }
  }

  static class AllocatedRegion {

    private final int address;
    private final int size;

    public AllocatedRegion(int address, int size) {
      this.address = address;
      this.size = size;
    }
  }
}
