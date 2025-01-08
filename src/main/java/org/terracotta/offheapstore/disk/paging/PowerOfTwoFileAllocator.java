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
package org.terracotta.offheapstore.disk.paging;

import static org.terracotta.offheapstore.util.Validation.shouldValidate;
import static org.terracotta.offheapstore.util.Validation.validate;

/**
 * An augmented AA tree allocator with unusual alignment/allocation properties.
 * <p>
 * This allocator allocates only power-of-two size chunks.  In addition these
 * chunks are then only allocated on alignment with their own size.  Hence a
 * chunk of 2<sup>n</sup> size can only be allocated to an address satisfying
 * a=2<sup>n</sup>x where x is a long.
 *
 * @author Chris Dennis
 */
public class PowerOfTwoFileAllocator {

  private static final boolean DEBUG = Boolean.getBoolean(PowerOfTwoFileAllocator.class.getName() + ".DEBUG");
  private static final boolean VALIDATING = shouldValidate(PowerOfTwoFileAllocator.class);

  private static final Region NULL_NODE = new Region();

  private Region root = NULL_NODE;
  private Region deletedNode;
  private Region lastNode;
  private Region deletedElement;

  private long occupied;

  public PowerOfTwoFileAllocator() {
    this(Long.MAX_VALUE);
  }

  public PowerOfTwoFileAllocator(long size) {
    this.root = new Region(0, size);
  }

  public Long allocate(long size) {
    if (Long.bitCount(size) != 1) {
      throw new AssertionError("Size " + size + " is not a power of two");
    }

    final Region r = find(size);

    if (r == null) {
      return null;
    } else {
      mark(r);
      return r.start();
    }
  }

  public void free(long address, long length) {
    if (length != 0) {
      Region r = new Region(address, address + length - 1);
      free(r);
      freed(r);
    }
  }

  public void mark(long address, long length) {
    if (length != 0) {
      mark(new Region(address, address + length - 1));
    }
  }

  public long occupied() {
    return occupied;
  }

  private void allocated(Region r) {
    occupied += r.size();
  }

  private void freed(Region r) {
    occupied -= r.size();
  }

  private void mark(Region r) {
    Region current = remove(find(r));
    Region newRange = current.remove(r);
    if (newRange != null) {
      insert(current);
      insert(newRange);
    } else if (!current.isNull()) {
      insert(current);
    }
    allocated(r);
  }

  private void free(Region r) {
    // Step 1 : Check if the previous number is present, if so add to the same Range.
    Region prev = find(new Region(r.start() - 1));
    if (prev != null) {
      prev = remove(prev);
      prev.merge(r);
      Region next = remove((new Region(r.end() + 1)));
      if (next != null) {
        prev.merge(next);
      }
      insert(prev);
      return;
    }

    // Step 2 : Check if the next number is present, if so add to the same Range.
    Region next = find(new Region(r.end() + 1));
    if (next != null) {
      next = remove(next);
      next.merge(r);
      insert(next);
      return;
    }

    // Step 3: Add a new range for just this number.
    insert(r);
  }

  /**
   * Insert into the tree.
   *
   * @param x the item to insert.
   */
  private void insert(Region x) {
    this.root = insert(x, this.root);
  }

  /**
   * Remove from the tree.
   *
   * @param x the item to remove.
   */
  private Region remove(Region x) {
    this.deletedNode = NULL_NODE;
    this.root = remove(x, this.root);
    Region d = this.deletedElement;
    // deletedElement is set to null to free the reference,
    // deletedNode is not freed as it will endup pointing to a valid node.
    this.deletedElement = null;
    if (d == null) {
      return null;
    } else {
      return new Region(d);
    }
  }

  /**
   * Find a region of the given size.
   */
  private Region find(long size) {
    validate(!VALIDATING || Long.bitCount(size) == 1);

    Region current = this.root;
    if ((current.available() & size) == 0) {
      //no region big enough for us...
      return null;
    } else {
      while (true) {
        if (current.left != null && (current.left.available() & size) != 0) {
          current = current.left;
        } else if ((current.availableHere() & size) != 0) {
          long mask = size - 1;
          long a = (current.start() + mask) & ~mask;
          return new Region(a, a + size - 1);
        } else if (current.right != null && (current.right.available() & size) != 0) {
          current = current.right;
        } else {
          throw new AssertionError();
        }
      }
    }
  }

  /**
   * Find an item in the tree.
   *
   * @param x
   * the item to search for.
   * @return the matching item of null if not found.
   */
  private Region find(Region x) {
    Region current = this.root;
    while (current != NULL_NODE) {
      long res = x.orderRelativeTo(current);
      if (res < 0) {
        current = current.left;
      } else if (res > 0) {
        current = current.right;
      } else {
        return current;
      }
    }
    return null;
  }

  /**
   * Internal method to insert into a subtree.
   *
   * @param x
   * the item to insert.
   * @param t
   * the node that roots the tree.
   * @return the new root.
   * if x is already present.
   */
  private Region insert(Region x, Region t) {
    if (t == NULL_NODE) {
      t = x;
    } else if (x.orderRelativeTo(t) < 0) {
      t.left(insert(x, t.left));
    } else if (x.orderRelativeTo(t) > 0) {
      t.right(insert(x, t.right));
    } else {
      throw new AssertionError("Cannot insert " + x + " into " + this);
    }
    t = skew(t);
    t = split(t);
    return t;
  }

  /**
   * Internal method to remove from a subtree.
   *
   * @param x
   * the item to remove.
   * @param t
   * the node that roots the tree.
   * @return the new root.
   */
  private Region remove(Region x, Region t) {
    if (t != NULL_NODE) {
      // Step 1: Search down the tree and set lastNode and deletedNode
      this.lastNode = t;
      if (x.orderRelativeTo(t) < 0) {
        t.left(remove(x, t.left));
      } else {
        this.deletedNode = t;
        t.right(remove(x, t.right));
      }
      // Step 2: If at the bottom of the tree and
      // x is present, we remove it
      if (t == this.lastNode) {
        if (this.deletedNode != NULL_NODE && x.orderRelativeTo(this.deletedNode) == 0) {
          this.deletedNode.swap(t);
          this.deletedElement = t;
          t = t.right;
        }
      } else if (t.left.level < t.level - 1 || t.right.level < t.level - 1) {
        // Step 3: Otherwise, we are not at the bottom; re-balance
        if (t.right.level > --t.level) {
          t.right.level = t.level;
        }
        t = skew(t);
        t.right(skew(t.right));
        t.right.right(skew(t.right.right));
        t = split(t);
        t.right(split(t.right));
      }
    }
    return t;
  }

  /**
   * Skew primitive for AA-trees.
   *
   * @param t
   * the node that roots the tree.
   * @return the new root after the rotation.
   */
  private static Region skew(Region t) {
    if (t.left.level == t.level) {
      t = rotateWithLeftChild(t);
    }
    return t;
  }

  /**
   * Split primitive for AA-trees.
   *
   * @param t
   * the node that roots the tree.
   * @return the new root after the rotation.
   */
  private static Region split(Region t) {
    if (t.right.right.level == t.level) {
      t = rotateWithRightChild(t);
      t.level++;
    }
    return t;
  }

  /**
   * Rotate binary tree node with left child.
   */
  private static Region rotateWithLeftChild(Region k2) {
    Region k1 = k2.left;
    k2.left(k1.right);
    k1.right(k2);
    return k1;
  }

  /**
   * Rotate binary tree node with right child.
   */
  private static Region rotateWithRightChild(Region k1) {
    Region k2 = k1.right;
    k1.right(k2.left);
    k2.left(k1);
    return k2;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(super.toString());
    if (DEBUG) {
      sb.append("\nFree Regions = ").append(root.dump()).append("");
    }
    return sb.toString();
  }

  /**
   * Class that represents the regions held within this set.
   */
  static class Region {

    private Region left;
    private Region right;
    private int level;
    private long start;
    private long end;

    private long availableBitSet;

    Region() {
      this.start = 1L;
      this.end = 0L;
      this.level = 0;
      this.left = this;
      this.right = this;
      availableBitSet = 0L;
    }

    Region(long value) {
      this(value, value);
    }

    /**
     * Creates a region containing the given range of values (inclusive).
     */
    Region(long start, long end) {
      this.start = start;
      this.end = end;
      this.left = NULL_NODE;
      this.right = NULL_NODE;
      this.level = 1;
      updateAvailable();
    }

    /**
     * Create a shallow copy of a region.
     * <p>
     * The new Region has NULL left and right children.
     */
    Region(Region r) {
      this(r.start(), r.end());
    }

    long available() {
      if (left == NULL_NODE && right == NULL_NODE) {
        return availableHere();
      } else {
        return availableBitSet;
      }
    }

    private void updateAvailable() {
      availableBitSet = availableHere() | left.available() | right.available();
    }

    long availableHere() {
      long bits = 0;

      for (int i = 0; i < Long.SIZE - 1; i++) {
        long size = 1L << i;
        long mask = size - 1;

        long a = (start + mask) & ~mask;

        if ((end - a) >= (size - 1)) {
          bits |= size;
        }
      }
      return bits;
    }

    void left(Region l) {
      this.left = l;
      updateAvailable();
    }

    void right(Region r) {
      this.right = r;
      updateAvailable();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
      if (this == NULL_NODE) {
        return "EMPTY";
      } else {
        return "Range(" + this.start + "," + this.end + ")" + " available:" + Long.toBinaryString(this.availableHere());
      }
    }

    private String dump() {
      String ds = "";
      if (this.left != NULL_NODE) {
        ds = "(" + this.left.dump();
        ds += " <= " + String.valueOf(this);
      } else {
        ds += "(" + String.valueOf(this);
      }
      if (this.right != NULL_NODE) {
        ds += " => " + this.right.dump() + ")";
      } else {
        ds += ")";
      }
      return ds;
    }

    /**
     * Returns the size of this range (the number of values within its bounds).
     */
    public long size() {
      // since it is all inclusive
      return isNull() ? 0 : this.end - this.start + 1;
    }

    public boolean isNull() {
      return this.start > this.end;
    }

    public Region remove(Region r) {
      if (r.start < this.start || r.end > this.end) {
        throw new AssertionError("Ranges : Illegal value passed to remove : " + this + " remove called for : " + r);
      }
      if (this.start == r.start) {
        this.start = r.end + 1;
        updateAvailable();
        return null;
      } else if (this.end == r.end) {
        this.end = r.start - 1;
        updateAvailable();
        return null;
      } else {
        Region newRegion = new Region(r.end + 1, this.end);
        this.end = r.start - 1;
        updateAvailable();
        return newRegion;
      }
    }

    /**
     * Merge the supplied region into this region (if they are adjoining).
     *
     * @param r region to merge
     */
    public void merge(Region r) {
      if (this.start == r.end + 1) {
        this.start = r.start;
      } else if (this.end == r.start - 1) {
        this.end = r.end;
      } else {
        throw new AssertionError("Ranges : Merge called on non contiguous values : [this]:" + this + " and " + r);
      }
      updateAvailable();
    }

    /**
     * Order this region relative to another.
     */
    public int orderRelativeTo(Region other) {
      if (this.start < other.start) {
        return -1;
      } else if (this.end > other.end) {
        return 1;
      } else {
        return 0;
      }
    }

    private void swap(Region other) {
      long temp = this.start;
      this.start = other.start;
      other.start = temp;
      temp = this.end;
      this.end = other.end;
      other.end = temp;
      updateAvailable();
    }

    /**
     * Returns the start of this range (inclusive).
     */
    public long start() {
      return start;
    }

    public long end() {
      return end;
    }
  }
}
