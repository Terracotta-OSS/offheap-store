package com.terracottatech.offheapstore.storage.allocator;

import com.terracottatech.offheapstore.util.AATreeSet.AbstractTreeNode;
import com.terracottatech.offheapstore.util.AATreeSet.Node;

/**
 * Class that represents the regions held within this set.
 */
class Region extends AbstractTreeNode<Region> implements Comparable<Comparable<?>> {

  private int start;
  private int end;
  private int availableBitSet;

  Region(int value) {
    this(value, value);
  }

  /**
   * Creates a region containing the given range of values (inclusive).
   */
  Region(int start, int end) {
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

  int available() {
    if (getLeft().getPayload() == null && getRight().getPayload() == null) {
      return availableHere();
    } else {
      return availableBitSet;
    }
  }

  private void updateAvailable() {
    Region left = getLeft().getPayload();
    Region right = getRight().getPayload();
    int leftAvailable = left == null ? 0 : left.available();
    int rightAvailable = right == null ? 0 : right.available();
    availableBitSet = availableHere() | leftAvailable | rightAvailable;
  }

  int availableHere() {
    int bits = 0;

    for (int i = 0; i < Integer.SIZE - 1; i++) {
      int size = 1 << i;
      int mask = size - 1;

      int a = (start + mask) & ~mask;

      if ((end - a + 1) >= size) {
        bits |= size;
      }
    }
    return bits;
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
    return "Range(" + this.start + "," + this.end + ")" + " available:" + Integer.toBinaryString(this.availableHere());
  }

  /**
   * Returns the size of this range (the number of values within its bounds).
   */
  public int size() {
    // since it is all inclusive
    return isNull() ? 0 : this.end - this.start + 1;
  }

  protected boolean isNull() {
    return this.start > this.end;
  }
  
  int treeSize() {
    int treeSize = size();
    Region left = getLeft().getPayload();
    treeSize += (left == null ? 0 : left.treeSize());
    Region right = getRight().getPayload();
    treeSize += (right == null ? 0 : right.treeSize());
    return treeSize;
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

  public boolean tryMerge(Region r) {
    if (this.start == r.end + 1) {
      this.start = r.start;
    } else if (this.end == r.start - 1) {
      this.end = r.end;
    } else if (this.start <= r.start && this.end >= r.end) {
      return false;
    } else {
      throw new AssertionError("Ranges : Merge called on non contiguous values : [this]:" + this + " and " + r);
    }
    updateAvailable();
    return true;
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
    } else if (other instanceof Integer) {
      Integer l = (Integer) other;
      if (l.intValue() > end) {
          return -1;
      } else if (l.intValue() < start) {
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
    return (3 * this.start) ^ (7 * this.end);
  }
  
  @Override
  public Region getPayload() {
    return this;
  }
  
  @Override
  public void swapPayload(Node<Region> other) {
    if (other instanceof Region) {
      Region r = (Region) other;
      int temp = this.start;
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
  public int start() {
    return start;
  }

  public int end() {
    return end;
  }
}