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
package org.terracotta.offheapstore.disk.storage;

import org.terracotta.offheapstore.util.AATreeSet.AbstractTreeNode;
import org.terracotta.offheapstore.util.AATreeSet.Node;

/**
 * Class that represents the regions held within this set.
 */
class Region extends AbstractTreeNode<Region> implements Comparable<Comparable<?>> {

  private long start;
  private long end;
  private long availableBitSet;
  
  Region(long value) {
    this(value, value);
  }

  /**
   * Creates a region containing the given range of values (inclusive).
   */
  Region(long start, long end) {
    this.start = start;
    this.end = end;
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
    if (getLeft().getPayload() == null && getRight().getPayload() == null) {
      return availableHere();
    } else {
      return availableBitSet;
    }
  }

  long availableHere() {
    long bits = 0;

    for (int i = 0; i < Long.SIZE - 1; i++) {
      long size = 1L << i;
      long mask = size - 1;

      long a = (start + mask) & ~mask;

      if ((end - a + 1) >= size) {
        bits |= size;
      }
    }
    return bits;
  }
  
  private void updateAvailable() {
    Region left = getLeft().getPayload();
    Region right = getRight().getPayload();
    long leftAvailable = left == null ? 0 : left.available();
    long rightAvailable = right == null ? 0 : right.available();
    availableBitSet = availableHere() | leftAvailable | rightAvailable;
  }

  @Override
  public void setLeft(Node<Region> l) {
    super.setLeft(l);
    updateAvailable();
  }

  @Override
  public void setRight(Node<Region> r) {
    super.setRight(r);
    updateAvailable();
  }
  
  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    return "Range(" + this.start + "," + this.end + ")" + " available:" + Long.toBinaryString(this.availableHere());
  }

  /**
   * Returns the size of this range (the number of values within its bounds).
   */
  public long size() {
    // since it is all inclusive
    return isNull() ? 0 : this.end - this.start + 1;
  }

  protected boolean isNull() {
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
  @Override
  public int compareTo(Comparable<?> other) {
    if (other instanceof Region) {
      Region r = (Region) other;
      if (this.start < r.start) {
        return -1;
      } else if (this.end > r.end) {
        return 1;
      } else {
        return 0;
      }
    } else if (other instanceof Long) {
      Long l = (Long) other;
      if (l > end) {
          return -1;
      } else if (l < start) {
          return 1;
      } else {
          return 0;
      }
    } else {
      throw new AssertionError();
    }
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof Region) {
      Region r = (Region) other;
      return (this.start == r.start) && (this.end == r.end);
    } else {
      throw new AssertionError();
    }
  }
  
  @Override
  public int hashCode() {
    return (3 * (int) this.start) ^ (7 * (int) (this.start >>> Integer.SIZE))
        ^ (5 * (int) this.end) ^ (11 * (int) (this.end >>> Integer.SIZE));
  }
  
  @Override
  public Region getPayload() {
    return this;
  }
  
  @Override
  public void swapPayload(Node<Region> other) {
    if (other instanceof Region) {
      Region r = (Region) other;
      long temp = this.start;
      this.start = r.start;
      r.start = temp;
      temp = this.end;
      this.end = r.end;
      r.end = temp;
      updateAvailable();
    } else {
      throw new AssertionError();
    }
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