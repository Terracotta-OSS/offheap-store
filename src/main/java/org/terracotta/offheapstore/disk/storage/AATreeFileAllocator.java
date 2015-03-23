/* 
 * Copyright 2015 Terracotta, Inc., a Software AG company.
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.terracotta.offheapstore.util.AATreeSet;
import static org.terracotta.offheapstore.util.Validation.shouldValidate;
import static org.terracotta.offheapstore.util.Validation.validate;

/**
 * An augmented AA tree based allocator.
 * <p>
 * This allocator maintains an augmented AA tree of free regions.  Tree nodes
 * are augmented with the size of the maximum and minimum contiguous free region
 * linked beneath them in the tree.  Regions being freed are merged with
 * adjacent free regions and the tree structure updated to reflect the resultant
 * region.
 * <p>
 * Allocations are performed in a very approximate best fit manner.  Assuming
 * that there is a large enough free region in the tree, tree navigation
 * decisions proceed as follows:
 * <ol>
 * <li>if the requested size is smaller than or equal to the smallest contiguous
 * region then find the smallest contiguous region and use it</li>
 * <li>if the current node is perfectly sized then use it</li>
 * <li>pick the child with the smallest contiguous subnode that will hold us - and then go to 2</li>
 * <li>if no such child exists use the current node</li>
 * </ol>
 * <p>
 * This allocator will experience bad fragmentation affects when not used with
 * uniformly sized allocations calls.  Since the AA Tree is stored in the Java
 * object heap this can lead to excessive heap usage.
 *
 * @author Chris Dennis
 */
public class AATreeFileAllocator extends AATreeSet<Region> {

  private static final boolean VALIDATING = shouldValidate(AATreeFileAllocator.class);
  private static final int MAGIC = 0x43485249;
  private static final int MAGIC_REGION = 0x53544f50;

  private final long capacity;

  private long occupied;

  /**
   * Create an abstract allocator using the given buffer source and initial
   * size.
   * <p>
   * This initial size will be used to size the buffer returned from the clear
   * call.
   *
   * @param size initial buffer size
   */
  public AATreeFileAllocator(long size) {
    super();
    this.capacity = size;
    add(new Region(0, capacity - 1));
  }

  public AATreeFileAllocator(long size, DataInput input) throws IOException {
    super();
    this.capacity = size;
    this.occupied = size;

    if (input.readInt() != MAGIC) {
      throw new IOException("Invalid magic number");
    }

    while (true) {
      int magic = input.readInt();
      if (magic == -1) {
        break;
      } else if (magic != MAGIC_REGION) {
        throw new IOException("Invalid magic number");
      }
      
      long start = input.readLong();
      long end = input.readLong();
      Region r = new Region(start, end);
      add(r);
      freed(r);
    }
  }
  
  public long allocate(long size) {
    if (Long.bitCount(size) != 1) {
      size = Long.highestOneBit(size) << 1;
    }

    final Region r = find(size);
    if (r == null) {
      return -1;
    }
    Region current = removeAndReturn(Long.valueOf(r.start()));
    Region newRange = current.remove(r);
    if (newRange != null) {
      add(current);
      add(newRange);
    } else if (!current.isNull()) {
      add(current);
    }
    allocated(r);
    return r.start();
  }

  public void free(long address, long length) {
    if (Long.bitCount(length) != 1) {
      length = Long.highestOneBit(length) << 1;
    }
    
    if (length != 0) {
      Region r = new Region(address, address + length - 1);
      free(r);
      freed(r);
    }
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

  public long occupied() {
    return occupied;
  }
  
  public long capacity() {
    return capacity;
  }

  private void allocated(Region r) {
    occupied += r.size();
  }

  private void freed(Region r) {
    occupied -= r.size();
  }

  private void free(Region r) {
    // Step 1 : Check if the previous number is present, if so add to the same Range.
    Region prev = removeAndReturn(Long.valueOf(r.start() - 1));
    if (prev != null) {
      prev.merge(r);
      Region next = removeAndReturn(Long.valueOf(r.end() + 1));
      if (next != null) {
        prev.merge(next);
      }
      add(prev);
      return;
    }

    // Step 2 : Check if the next number is present, if so add to the same Range.
    Region next = removeAndReturn(Long.valueOf(r.end() + 1));
    if (next != null) {
      next.merge(r);
      add(next);
      return;
    }

    // Step 3: Add a new range for just this number.
    add(r);
  }
  
  /**
   * Find a region of the given size.
   */
  private Region find(long size) {
    validate(!VALIDATING || Long.bitCount(size) == 1);

    Node<Region> currentNode = getRoot();
    Region currentRegion = currentNode.getPayload();
    if (currentRegion == null || (currentRegion.available() & size) == 0) {
      //no region big enough for us...
      return null;
    } else {
      while (true) {
        Region left = currentNode.getLeft().getPayload();
        if (left != null && (left.available() & size) != 0) {
          currentNode = currentNode.getLeft();
          currentRegion = currentNode.getPayload();
        } else if ((currentRegion.availableHere() & size) != 0) {
          long mask = size - 1;
          long a = (currentRegion.start() + mask) & ~mask;
          return new Region(a, a + size - 1);
        } else {
          Region right = currentNode.getRight().getPayload();
          if (right != null && (right.available() & size) != 0) {
            currentNode = currentNode.getRight();
            currentRegion = currentNode.getPayload();
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
    return "RegionSet = { " + super.toString() + " }";
  }

  void persist(DataOutput output) throws IOException {
    output.writeInt(MAGIC);

    for (Region r : this) {
      persist(output, r);
    }

    output.writeInt(-1);
  }

  void persist(DataOutput output, Region r) throws IOException {
    output.writeInt(MAGIC_REGION);
    output.writeLong(r.start());
    output.writeLong(r.end());
  }
}
