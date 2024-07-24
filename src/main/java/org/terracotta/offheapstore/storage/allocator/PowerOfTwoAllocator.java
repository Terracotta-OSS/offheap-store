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
package org.terracotta.offheapstore.storage.allocator;

import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;
import org.terracotta.offheapstore.util.AATreeSet;
import org.terracotta.offheapstore.util.DebuggingUtils;
import static org.terracotta.offheapstore.util.Validation.shouldValidate;
import static org.terracotta.offheapstore.util.Validation.validate;

/**
 * An augmented AA tree allocator with unusual alignment/allocation properties.
 * <p>
 * This allocator allocates only power-of-two size chunks.  In addition these
 * chunks are then only allocated on alignment with their own size.  Hence a
 * chunk of 2<sup>n</sup> size can only be allocated to an address satisfying
 * a=2<sup>n</sup>x where x is an integer.
 *
 * @author Chris Dennis
 */
public class PowerOfTwoAllocator extends AATreeSet<Region> {

  private static final boolean DEBUG = Boolean.getBoolean(PowerOfTwoAllocator.class.getName() + ".DEBUG");
  private static final boolean VALIDATING = shouldValidate(PowerOfTwoAllocator.class);

  private final int size;
  /**
   * This is volatile because we read it without any locking through
   * {@link UpfrontAllocatingPageSource#getAllocatedSizeUnSync()}
   */
  private volatile int occupied;

  /**
   * Create a power-of-two allocator with the given initial 'free' size area.
   *
   * @param size initial free size
   */
  public PowerOfTwoAllocator(int size) {
    super();
    add(new Region(0, size - 1));
    this.size = size;
  }

  public int allocate(int size, Packing packing) {
    if (Integer.bitCount(size) != 1) {
      throw new AssertionError("Size " + size + " is not a power of two");
    }

    final Region r = findRegion(size, packing);
    if (r == null) {
      return -1;
    }
    Region current = removeAndReturn(r.start());
    Region newRange = current.remove(r);
    if (newRange != null) {
      insert(current);
      insert(newRange);
    } else if (!current.isNull()) {
      insert(current);
    }
    occupied += r.size();
    validateFreeSpace();

    return r.start();
  }

  public void free(int address, int length) {
    if (length != 0) {
      free(new Region(address, address + length - 1));
      occupied -= length;
      validateFreeSpace();
    }
  }

  public void tryFree(int address, int length) {
    if (length == 0) {
      return;
    } else if (tryFree(new Region(address, address + length - 1))) {
      occupied -= length;
      validateFreeSpace();
    }
  }

  public int find(int size, Packing packing) {
    if (Integer.bitCount(size) != 1) {
      throw new AssertionError("Size " + size + " is not a power of two");
    }

    final Region r = findRegion(size, packing);
    if (r == null) {
      return -1;
    } else {
      return r.start();
    }
  }

  public void claim(int address, int size) {
    Region current = removeAndReturn(address);
    Region r = new Region(address, address + size - 1);
    Region newRange = current.remove(r);
    if (newRange != null) {
      insert(current);
      insert(newRange);
    } else if (!current.isNull()) {
      insert(current);
    }
    occupied += size;
    validateFreeSpace();
  }

  public int occupied() {
    return occupied;
  }

  @Override
  public Region removeAndReturn(Object o) {
      Region r = super.removeAndReturn(o);
      if (r != null) {
        return new Region(r);
      } else {
        return null;
      }
  }

  @Override
  public Region find(Object o) {
      Region r = super.find(o);
      if (r != null) {
          return new Region(r);
      } else {
          return null;
      }
  }

  private void free(Region r) {
    // Step 1 : Check if the previous number is present, if so add to the same Range.
    Region prev = removeAndReturn(r.start() - 1);
    if (prev != null) {
      prev.merge(r);
      Region next = removeAndReturn(r.end() + 1);
      if (next != null) {
        prev.merge(next);
      }
      insert(prev);
      return;
    }

    // Step 2 : Check if the next number is present, if so add to the same Range.
    Region next = removeAndReturn(r.end() + 1);
    if (next != null) {
      next.merge(r);
      insert(next);
      return;
    }

    // Step 3: Add a new range for just this number.
    insert(r);
  }

  private boolean tryFree(Region r) {
    // Step 1 : Check if the previous number is present, if so add to the same Range.
    Region prev = removeAndReturn(r.start() - 1);
    if (prev != null) {
      if (prev.tryMerge(r)) {
        Region next = removeAndReturn(r.end() + 1);
        if (next != null) {
          prev.merge(next);
        }
        insert(prev);
        return true;
      } else {
        insert(prev);
        return false;
      }
    }

    // Step 2 : Check if the next number is present, if so add to the same Range.
    Region next = removeAndReturn(r.end() + 1);
    if (next != null) {
      if (next.tryMerge(r)) {
        insert(next);
        return true;
      } else {
        insert(next);
        return false;
      }
    }

    // Step 3: Add a new range for just this number.
    return tryInsert(r);
  }
  /**
   * Insert into the tree.
   *
   * @param x the item to insert.
   */
  private void insert(Region x) {
    if (!tryInsert(x)) {
      throw new AssertionError(x + " is already inserted");
    }
  }

  private boolean tryInsert(Region x) {
    return add(x);
  }

  /**
   * Find a region of the given size.
   */
  private Region findRegion(int size, Packing packing) {
    validate(!VALIDATING || Integer.bitCount(size) == 1);

    Node<Region> currentNode = getRoot();
    Region currentRegion = currentNode.getPayload();
    if (currentRegion == null || (currentRegion.available() & size) == 0) {
      //no region big enough for us...
      return null;
    } else {
      while (true) {
        Node<Region> prefered = packing.prefered(currentNode);
        Region preferedRegion = prefered.getPayload();
        if (preferedRegion != null && (preferedRegion.available() & size) != 0) {
          currentNode = prefered;
          currentRegion = preferedRegion;
        } else if ((currentRegion.availableHere() & size) != 0) {
          return packing.slice(currentRegion, size);
        } else {
          Node<Region> fallback = packing.fallback(currentNode);
          Region fallbackRegion = fallback.getPayload();
          if (fallbackRegion != null && (fallbackRegion.available() & size) != 0) {
            currentNode = fallback;
            currentRegion = fallbackRegion;
          } else {
            throw new AssertionError();
          }
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    Region rootRegion = getRoot().getPayload();
    StringBuilder sb = new StringBuilder("PowerOfTwoAllocator: Occupied ");
    sb.append(DebuggingUtils.toBase2SuffixedString(occupied())).append("B");
    sb.append(" [Largest Available Area ").append(DebuggingUtils.toBase2SuffixedString(Integer.highestOneBit(rootRegion == null ? 0 : rootRegion.available()))).append("B]");
    if (DEBUG) {
      sb.append("\nFree Regions = ").append(super.toString()).append("");
    }
    return sb.toString();
  }

  private void validateFreeSpace() {
    if (VALIDATING) {
      Region rootRegion = getRoot().getPayload();
      if (occupied() != (size - (rootRegion == null ? 0 : rootRegion.treeSize()))) {
        throw new AssertionError("Occupied:" + occupied() + " Size-TreeSize:" + (size - (rootRegion == null ? 0 : rootRegion.treeSize())));
      }
    }
  }

  public enum Packing {
    FLOOR {

      @Override
      Node<Region> prefered(Node<Region> node) {
        return node.getLeft();
      }

      @Override
      Node<Region> fallback(Node<Region> node) {
        return node.getRight();
      }

      @Override
      Region slice(Region region, int size) {
        int mask = size - 1;
        int a = (region.start() + mask) & ~mask;
        return new Region(a, a + size - 1);
      }
    },

    CEILING {

      @Override
      Node<Region> prefered(Node<Region> node) {
        return node.getRight();
      }

      @Override
      Node<Region> fallback(Node<Region> node) {
        return node.getLeft();
      }

      @Override
      Region slice(Region region, int size) {
        int mask = size - 1;
        int a = (region.end() + 1) & ~mask;
        return new Region(a - size, a - 1);
      }
    };

    abstract Node<Region> prefered(Node<Region> node);

    abstract Node<Region> fallback(Node<Region> node);

    abstract Region slice(Region region, int size);
  }
}
